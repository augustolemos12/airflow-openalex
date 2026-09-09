"""
DAG de Ingesta desde OpenAlex API

Este DAG consulta la API REST de OpenAlex utilizando paginación por cursor,
extrae información sobre publicaciones académicas, procesa la respuesta en formato tabular,
guarda el resultado en CSV y realiza validaciones de calidad de datos.

**Tres niveles de resiliencia**, en orden:

1. Sensor de disponibilidad — sondea la API cada 5 minutos durante hasta 30 antes de
   declarar la fuente caída. Un corte corto no arruina la corrida.
2. Descarga normal con paginación por cursor — el camino feliz.
3. Respaldo congelado — si OpenAlex no responde, el DAG termina exitosamente
   entregando el último snapshot conocido.

**Dos capas del modelo medallón:**

* **Bronce** (`land_bronze`) — los registros JSON crudos en formato JSONL, comprimidos con gzip. Es la única
  tarea que toca la red. No interpreta nada.
* **Plata** (`refine_silver`) — filas tipadas, una por paper, deduplicadas y validadas.
  No toca la red: lee del bronce.
"""
from __future__ import annotations

import gzip
import json
import logging
from pathlib import Path

import pendulum
from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import Param, PokeReturnValue, dag, task
from airflow.task.trigger_rule import TriggerRule

import pandas as pd

log = logging.getLogger(__name__)

OUTPUT_DIR = Path("/usr/local/airflow/include/output")
# Capas del modelo medallón
BRONCE_DIR = OUTPUT_DIR / "bronze"
PLATA_DIR  = OUTPUT_DIR / "silver"

# Respaldo de último recurso
#   SEMILLA  — versionada en el repo, siempre disponible desde el primer clone.
#   ULTIMO_OK — la sobreescribe `save` al final de cada corrida full exitosa.
FROZEN_DIR = Path("/usr/local/airflow/include/frozen")
SEMILLA    = FROZEN_DIR / "openalex_snapshot.csv"
ULTIMO_OK  = FROZEN_DIR / "ultimo_ok.csv"

CONN_ID  = "openalex_api"
BASE_URL = "https://api.openalex.org/works"


# ----------------
# Funciones helper
# ----------------

def bronze_path(fecha_str: str) -> Path:
    """Ruta canónica del archivo bronce para una fecha dada."""
    return BRONCE_DIR / f"openalex_raw_{fecha_str}.json.gz"

def fetch_page(session, cursor: str, correo: str, api_key: str | None) -> tuple[list, str | None]:
    """Pide una página de 100 works a OpenAlex y devuelve (resultados, next_cursor).

    Encapsula la construcción de parámetros y el manejo de la respuesta para
    mantener el bucle en land_bronze limpio y legible.
    """
    parametros = {
        "mailto":   correo,
        "per_page": 100,
        "cursor":   cursor,
        "filter":   "publication_year:2023,has_abstract:true",
    }
    if api_key:
        parametros["api_key"] = api_key

    resp = session.get(BASE_URL, params=parametros, timeout=30)
    # raise_for_status() es un método de la clase Response que se usa para 
    # lanzar una excepción si el código de estado de la respuesta es un error
    resp.raise_for_status()
    # resp.json() es un método de la clase Response que se usa para 
    # convertir la respuesta en un objeto python
    data = resp.json()
    # data.get("results", []) devuelve la lista de works, si no existe devuelve una lista vacia
    # data.get("meta", {}).get("next_cursor") devuelve el cursor para la siguiente página
    return data.get("results", []), data.get("meta", {}).get("next_cursor")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="openalex_ingest",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    tags=["ciencia-de-datos", "proyecto-integrador", "openalex"],
    doc_md=__doc__,
    params={
        "filas_objetivo": Param(
            12000, type="integer", title="Cantidad de papers",
        ),
        "correo_api": Param(
            "tu_correo@utn.edu.ar", type="string", title="Polite Pool Email",
            description="OpenAlex pide un mail para darte prioridad en la descarga.",
        ),
        "force": Param(
            False, type="boolean", title="Forzar la descarga",
            description=(
                "Si es True, descarga aunque el bronce del día ya exista en disco. "
                "Útil para forzar una re-ingesta sobre el mismo snapshot."
            ),
        ),
    },
)
def openalex_ingest():

    # ------------------------------------------------------------------
    # 1. SENSOR — espera que la API esté disponible
    # ------------------------------------------------------------------

    @task.sensor(poke_interval=300, timeout=1800, mode="reschedule", soft_fail=True)
    def wait_for_source(**context) -> PokeReturnValue:
        """Sondea OpenAlex cada 5 min durante hasta 30. Libera el worker entre intentos.

        `mode="reschedule"` devuelve el worker al pool entre cada sondeo.

        `soft_fail=True` hace que, al agotarse el tiempo, la tarea quede en
        `skipped` en lugar de `failed`. No responder no es un error del pipeline;
        es una condición que sabemos manejar en el branch siguiente.
        """
        correo = context["params"]["correo_api"]
        try:
            # get_conn() devuelve una instancia de la clase HTTPConnection que 
            # se usa para hacer peticiones HTTP
            session = HttpHook(method="GET", http_conn_id=CONN_ID).get_conn()
            # get() es un método de la clase HTTPConnection que se usa para 
            # hacer peticiones GET
            resp = session.get(
                BASE_URL,
                params={"mailto": correo, "per_page": 1,
                        "filter": "publication_year:2023,has_abstract:true"},
                timeout=15,
            )
            resp.raise_for_status()
            if not resp.json().get("results"):
                log.warning("OpenAlex respondió pero la lista vino vacía.")
                return PokeReturnValue(is_done=False)
            log.info("OpenAlex responde correctamente. Pipeline habilitado.")
            return PokeReturnValue(is_done=True, xcom_value=True)
        except Exception as e:
            log.warning("OpenAlex no responde (%s). Reintento en 5 min.", e)
            return PokeReturnValue(is_done=False)

    # ------------------------------------------------------------------
    # 2. BRANCH — decide si descargar o usar el respaldo
    # ------------------------------------------------------------------

    @task.branch(trigger_rule=TriggerRule.ALL_DONE)
    def check_source(**context) -> str:
        """Dirige el flujo según lo que reportó el sensor."""
        ok = context["ti"].xcom_pull(task_ids="wait_for_source")
        if ok:
            return "land_bronze"
        log.error("OpenAlex no respondió en 30 min. Se activa el respaldo.")
        return "load_frozen"

    # ------------------------------------------------------------------
    # 3. BRONCE — descarga y almacena los JSON crudos comprimidos
    # ------------------------------------------------------------------

    @task(retries=3, retry_delay=pendulum.duration(seconds=15))
    def land_bronze(**context) -> str:
        """Capa Bronce: descarga y guarda JSONL comprimidos con gzip iterativamente."""
        dag_run   = context["dag_run"]
        momento   = dag_run.logical_date or dag_run.run_after
        fecha_str = momento.date().isoformat()
        force     = context["params"]["force"]

        destino = bronze_path(fecha_str)

        if destino.exists() and destino.stat().st_size > 0 and not force:
            log.info("Bronce del %s ya existe (%s bytes). Omitiendo.", fecha_str, destino.stat().st_size)
            return str(destino)

        hook     = HttpHook(method="GET", http_conn_id=CONN_ID)
        conexion = hook.get_connection(CONN_ID)
        api_key  = conexion.password

        filas_objetivo = context["params"]["filas_objetivo"]
        correo         = context["params"]["correo_api"]
        cursor         = "*"
        total_descargados = 0

        session = hook.get_conn()
        log.info("Capa Bronce: iniciando descarga desde OpenAlex API...")

        destino.parent.mkdir(parents=True, exist_ok=True)
        
        # Abrimos el gzip de escritura acá, y escribimos a medida que paginamos
        with gzip.open(destino, "wt", encoding="utf-8") as f:
            while total_descargados < filas_objetivo and cursor is not None:
                resultados, cursor = fetch_page(session, cursor, correo, api_key)
                
                # Escribimos los 100 resultados de esta página y liberamos la memoria
                for work in resultados:
                    f.write(json.dumps(work, ensure_ascii=False) + "\n")
                
                total_descargados += len(resultados)
                log.info("Registros acumulados: %s", total_descargados)

        log.info("Capa Bronce guardada en %s (%s registros)", destino, total_descargados)
        return str(destino)

    # ------------------------------------------------------------------
    # 4. RESPALDO — rama de último recurso
    # ------------------------------------------------------------------

    @task
    def load_frozen() -> str:
        """Devuelve el CSV más fresco disponible: último éxito o semilla del repo.

        Prefiere ULTIMO_OK (la sobreescribe cada corrida completa exitosa).
        Si no existe todavía, cae a la semilla versionada en el repositorio.
        Así el arranque en frío está cubierto y la frescura la da el último éxito.
        """
        for ruta, origen in (
            (ULTIMO_OK, "última corrida completa exitosa"),
            (SEMILLA,   "semilla versionada en el repositorio"),
        ):
            if not ruta.exists():
                continue
            try:
                filas = sum(1 for _ in open(ruta, encoding="utf-8")) - 1
            except Exception:
                filas = "?"
            log.warning(
                "OpenAlex no respondió. Usando respaldo (%s): %s filas. "
                "ATENCIÓN: estos datos NO son de hoy.", origen, filas,
            )
            return str(ruta)

        raise FileNotFoundError(
            f"OpenAlex no responde y no hay ningún respaldo en {FROZEN_DIR}. "
            "Falta la semilla openalex_snapshot.csv en include/frozen/."
        )

    # ------------------------------------------------------------------
    # 5. PLATA — tipado, limpieza y estructurado
    # ------------------------------------------------------------------

    @task
    def refine_silver(ruta_bronce: str) -> str:
        """Capa Plata: lee el bronce, tipa, limpia y genera el CSV estructurado.

        No toca la red. Lee el JSONL comprimido directamente desde la capa Bronce,
        procesando los registros línea por línea para reducir el consumo de memoria.
        """
        import csv

        COLUMNAS = [
            "id", "title", "publication_year", "cited_by_count",
            "is_oa", "authors_count", "referenced_works_count",
            "institutions_count", "countries_count", "type",
        ]
        # CHUNK_SIZE = 500 es el tamaño del buffer que se usa para escribir en el archivo CSV
        CHUNK_SIZE = 500

        PLATA_DIR.mkdir(parents=True, exist_ok=True)
        destino_plata = PLATA_DIR / "openalex_silver.csv"
        ids_vistos: set = set()
        total_filas = 0

        with gzip.open(ruta_bronce, "rt", encoding="utf-8") as f_in, \
             open(destino_plata, "w", newline="", encoding="utf-8") as f_out:
            
            writer = csv.DictWriter(f_out, fieldnames=COLUMNAS)
            writer.writeheader()
            buffer = []

            # Leer línea por línea sin saturar la RAM
            for linea in f_in:
                work = json.loads(linea)
                work_id = work.get("id")
                if not work_id:
                    log.warning("Work sin ID encontrado. Se omite.")
                    continue

                work_id = work_id.rsplit("/", 1)[-1]

                if work_id in ids_vistos:
                    continue

                ids_vistos.add(work_id)

                buffer.append({
                    "id": work_id,
                    "title": work.get("title"),
                    "publication_year": work.get("publication_year"),
                    "cited_by_count": work.get("cited_by_count"),
                    "is_oa": work.get("open_access", {}).get("is_oa"),
                    "authors_count": len(work.get("authorships", [])),
                    "referenced_works_count": len(work.get("referenced_works", [])),
                    "institutions_count": len({
                        institution.get("id")
                        for authorship in work.get("authorships", [])
                        for institution in authorship.get("institutions", [])
                        if institution.get("id")
                    }),
                    "countries_count": len({
                        country
                        for authorship in work.get("authorships", [])
                        for country in authorship.get("countries", [])
                        if country
                    }),
                    "type": work.get("type"),
                })
                if len(buffer) >= CHUNK_SIZE:
                    writer.writerows(buffer)
                    total_filas += len(buffer)
                    buffer.clear()

            if buffer:
                writer.writerows(buffer)
                total_filas += len(buffer)

        log.info("Capa Plata procesada: %s filas -> %s", total_filas, destino_plata)
        return str(destino_plata)

    # ------------------------------------------------------------------
    # 6. VALIDACIÓN — chequeos antes de publicar
    # ------------------------------------------------------------------

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def validate(desde_fuente: str | None, desde_respaldo: str | None) -> str:
        """Chequeos duros. Si alguno falla, el DAG falla: no se publica basura.

        `NONE_FAILED_MIN_ONE_SUCCESS` permite que una de las dos ramas esté en
        `skipped` y la tarea igualmente corra, siempre que la otra haya dado datos.
        """
        ruta = desde_fuente or desde_respaldo
        if ruta is None:
            raise ValueError("Ninguna rama produjo un archivo para validar.")

        df = pd.read_csv(ruta)
        problemas = []

        if len(df) < 1000:
            problemas.append(f"Volumen insuficiente: solo {len(df)} filas.")
        if df["cited_by_count"].isna().any():
            problemas.append(
                f"Target 'cited_by_count' tiene {df['cited_by_count'].isna().sum()} nulos."
            )
        if df["id"].duplicated().any():
            problemas.append(f"Existen {df['id'].duplicated().sum()} IDs duplicados.")

        if problemas:
            raise ValueError("Validación fallida en capa Plata:\n  - " + "\n  - ".join(problemas))

        log.info("Validación OK: %s filas superaron todos los chequeos.", len(df))
        return ruta

    # ------------------------------------------------------------------
    # 7. SAVE — entregable final + refresco del respaldo
    # ------------------------------------------------------------------

    @task
    def save(ruta_validada: str, **context) -> str:
        """Genera el entregable final fechado y refresca el respaldo ULTIMO_OK."""
        import shutil

        dag_run   = context["dag_run"]
        momento   = dag_run.logical_date or dag_run.run_after
        fecha_str = momento.date().isoformat()

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        destino_final = OUTPUT_DIR / f"openalex_{fecha_str}.csv"
        shutil.copy(ruta_validada, destino_final)
        log.info("Entregable final escrito en %s", destino_final)

        # Refrescar el respaldo, sólo si los datos vinieron de la fuente (no
        # del respaldo mismo: pisar ULTIMO_OK con él sería un movimiento nulo).
        vino_del_respaldo = Path(ruta_validada).parent == FROZEN_DIR
        if vino_del_respaldo:
            log.info("El respaldo no se toca: estos datos salieron de él.")
        else:
            FROZEN_DIR.mkdir(parents=True, exist_ok=True)
            shutil.copy(destino_final, ULTIMO_OK)
            log.info("Respaldo actualizado -> %s", ULTIMO_OK)

        return str(destino_final)

    # ------------------------------------------------------------------
    # Grafo de dependencias
    # ------------------------------------------------------------------
    espera    = wait_for_source()
    rama      = check_source()
    bronce    = land_bronze()
    congelado = load_frozen()
    plata     = refine_silver(bronce)

    # Las tareas de decisión no se pasan datos entre sí: sólo ordenan el flujo.
    espera >> rama
    rama >> [bronce, congelado]

    validado = validate(plata, congelado)
    save(validado)


openalex_ingest()
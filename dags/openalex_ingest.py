"""
DAG de Ingesta desde OpenAlex API

Este DAG consulta la API REST de OpenAlex tomando una muestra aleatoria reproducible
(`sample` + `seed`), extrae información sobre publicaciones académicas, procesa la respuesta en formato tabular,
guarda el resultado en CSV y realiza validaciones de calidad de datos.

**Tres niveles de resiliencia**, en orden:

1. Sensor de disponibilidad — sondea la API cada 5 minutos durante hasta 30 antes de
   declarar la fuente caída. Un corte corto no arruina la corrida.
2. Descarga normal de una muestra aleatoria, paginada — el camino feliz.
3. Respaldo congelado — si OpenAlex no responde, el DAG termina exitosamente
   entregando el último snapshot conocido.

**Dos capas del modelo medallón:**

* **Bronce** (`land_bronze`) — los registros JSON crudos en formato JSONL, comprimidos con gzip.
  No interpreta nada.
* **Plata** (`refine_silver`) — filas tipadas, una por paper, deduplicadas y validadas.
  No toca la red: lee del bronce.
* **Plata enriquecida** (`enrich_silver`) — suma a cada paper datos de su revista, sus
  instituciones y sus autores, pedidos a otros endpoints de OpenAlex. Sólo usa
  información anterior al año de publicación, para que no haya fuga del target.
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
# Comprimido: con todas las columnas de la API, sin comprimir pesa ~200 MB.
ULTIMO_OK  = FROZEN_DIR / "ultimo_ok.csv.gz"

CONN_ID  = "openalex_api"
API_URL  = "https://api.openalex.org"
BASE_URL = f"{API_URL}/works"

# Población de estudio: artículos de Ciencia de Materiales de 2019 con abstract, sin retractados.
# Filtrar en la API evita descargar works que después descartaríamos.
# El campo se identifica por el tema principal del artículo (primary_topic):
# fields/25 = Materials Science, dentro del dominio Physical Sciences.
# 2019 da más de 5 años de exposición a citas: con papers recientes el target
# queda aplastado contra 0.
CAMPO_ID = 25
ANIO     = 2019
FILTRO = (
    f"publication_year:{ANIO},has_abstract:true,type:article,is_retracted:false,"
    f"primary_topic.field.id:{CAMPO_ID}"
)

# Sin `sort`, OpenAlex devuelve los works ordenados por cited_by_count descendente:
# bajar "los primeros N" sesga la muestra hacia los papers más citados.
# Por eso pedimos una muestra aleatoria con `sample` + `seed`.
# `sample` admite como máximo 10.000 works y no funciona con cursor, sólo con `page`.
MAX_MUESTRA = 10_000
POR_PAGINA  = 200

# Columnas derivadas de la capa Plata, listas para el análisis. Van primero en el CSV;
# después vienen todas las columnas de la API aplanadas (ver `aplanar`).
# `validate` rechaza cualquier archivo que no tenga al menos estas.
COLUMNAS_PLATA = [
    "id", "title",
    # Targets: citas crudas y top 10% normalizado por campo y año
    "cited_by_count", "is_top10_percent",
    # Autores
    "authors_count", "institutions_count", "countries_count",
    # Referencias
    "referenced_works_count",
    # Temática
    "domain", "field",
    # Acceso abierto
    "is_oa", "oa_status", "apc_paid",
    # Fuente de publicación
    "source_type", "source_is_core", "is_in_doaj",
    # Controles
    "funders_count", "language", "publication_month", "abstract_length",
    # Texto del abstract reconstruido
    "abstract",
]
FILAS_MINIMAS = 5000

# Columnas que agrega `enrich_silver`. Van después de COLUMNAS_PLATA.
# Las métricas "actuales" de OpenAlex (h-index del autor, 2yr_mean_citedness de la
# revista) incluyen citas posteriores al paper, incluso las del propio paper: serían
# fuga del target. Por eso la historia se calcula con `counts_by_year`, sumando sólo
# los años anteriores a ANIO. OpenAlex guarda `counts_by_year` desde 2012.
COLUMNAS_ENRIQUECIDAS = [
    # Revista: producción y citas antes de la publicación
    "source_pre_works", "source_pre_cites", "source_pre_cites_per_work", "source_apc_usd",
    # Autores: trayectoria previa del primero y del último (suele ser el jefe del grupo)
    "first_author_pre_works", "first_author_pre_cites",
    "last_author_pre_works", "last_author_pre_cites",
    "first_author_country",
    # Instituciones: h-index de la mejor institución del paper. Es el valor actual,
    # pero con decenas de miles de works por institución un paper no lo mueve.
    "institution_max_h_index",
]
# El filtro `openalex_id:A|B|...` admite hasta 100 IDs por pedido.
POR_LOTE_ENTIDADES = 100

# Campos de la API que no se aplanan tal cual. El abstract es un dict
# {palabra: [posiciones]}: aplanarlo crearía una columna por palabra.
# Se reemplaza por `abstract` (texto reconstruido) y `abstract_length`.
CAMPOS_NO_APLANADOS = {"abstract_inverted_index"}


# ----------------
# Funciones helper
# ----------------

def bronze_path(fecha_str: str) -> Path:
    """Ruta canónica del archivo bronce para una fecha dada."""
    return BRONCE_DIR / f"openalex_raw_{fecha_str}.json.gz"

def fetch_page(session, pagina: int, tamanio: int, semilla: int,
               correo: str, api_key: str | None) -> list:
    """Pide una página de una muestra aleatoria de works y devuelve los resultados.

    La misma (tamanio, semilla) devuelve siempre la misma muestra, así que
    las páginas de un lote no se pisan entre sí y la descarga es reproducible.
    """
    parametros = {
        "mailto":   correo,
        "per_page": POR_PAGINA,
        "page":     pagina,
        "sample":   tamanio,
        "seed":     semilla,
        "filter":   FILTRO,
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
    return data.get("results", [])

def aplanar(obj: dict, prefijo: str = "") -> dict:
    """Aplana un work de la API en columnas de CSV.

    - Objetos anidados → una columna por hoja, con `_` como separador
      (primary_location.source.type → primary_location_source_type).
    - Listas → texto JSON en la celda. Las de primer nivel suman `<lista>_count`.
    - Valores null → se omiten; el CSV los deja vacíos.
    """
    plano = {}
    for clave, valor in obj.items():
        nombre = f"{prefijo}{clave}"
        if valor is None or (not prefijo and clave in CAMPOS_NO_APLANADOS):
            continue
        if isinstance(valor, dict):
            plano.update(aplanar(valor, f"{nombre}_"))
        elif isinstance(valor, list):
            plano[nombre] = json.dumps(valor, ensure_ascii=False)
            if not prefijo:
                plano[f"{nombre}_count"] = len(valor)
        else:
            plano[nombre] = valor
    return plano

def reconstruir_abstract(indice: dict) -> str:
    """Convierte el índice invertido {palabra: [posiciones]} en el texto original."""
    por_posicion = {p: palabra for palabra, posiciones in indice.items() for p in posiciones}
    return " ".join(por_posicion[p] for p in sorted(por_posicion))

def fila_plata(work: dict, work_id: str) -> dict:
    """Construye la fila de Plata: columnas derivadas + todas las de la API aplanadas.

    Si una columna aplanada se llama igual que una derivada, gana la derivada
    (ej. `id` acortado, `language` normalizado).
    """
    # Estos objetos pueden venir como null en el JSON, no sólo faltar:
    # `or {}` cubre ambos casos.
    topic  = work.get("primary_topic") or {}
    source = (work.get("primary_location") or {}).get("source") or {}
    fecha  = work.get("publication_date") or ""
    # El abstract viene como índice invertido {palabra: [posiciones]}:
    # la cantidad de posiciones es la cantidad de palabras.
    abstract = work.get("abstract_inverted_index") or {}
    # OpenAlex mezcla códigos ("en", "EN", "ENG"): lo llevamos a ISO 639-1.
    idioma = (work.get("language") or "").lower() or None
    if idioma == "eng":
        idioma = "en"
    autorias = work.get("authorships") or []

    fila = {
        "id": work_id,
        "title": work.get("title"),
        "cited_by_count": work.get("cited_by_count"),
        "is_top10_percent": (work.get("citation_normalized_percentile") or {})
                            .get("is_in_top_10_percent"),
        "is_oa": (work.get("open_access") or {}).get("is_oa"),
        "authors_count": len(autorias),
        "referenced_works_count": len(work.get("referenced_works") or []),
        "institutions_count": len({
            institution.get("id")
            for authorship in autorias
            for institution in authorship.get("institutions") or []
            if institution.get("id")
        }),
        "countries_count": len({
            country
            for authorship in autorias
            for country in authorship.get("countries") or []
            if country
        }),
        "domain": (topic.get("domain") or {}).get("display_name"),
        "field": (topic.get("field") or {}).get("display_name"),
        "oa_status": (work.get("open_access") or {}).get("oa_status"),
        "apc_paid": work.get("apc_paid") is not None,
        "source_type": source.get("type"),
        "source_is_core": source.get("is_core"),
        "is_in_doaj": source.get("is_in_doaj"),
        "funders_count": len(work.get("funders") or []),
        "language": idioma,
        "publication_month": int(fecha[5:7]) if len(fecha) >= 7 else None,
        "abstract_length": sum(len(posiciones) for posiciones in abstract.values()),
        "abstract": reconstruir_abstract(abstract) or None,
    }
    for clave, valor in aplanar(work).items():
        fila.setdefault(clave, valor)
    return fila

def id_corto(url) -> str | None:
    """https://openalex.org/S123 -> S123. Deja pasar los vacíos (NaN, None)."""
    return url.rsplit("/", 1)[-1] if isinstance(url, str) else None

def fetch_entidades(session, tipo: str, ids: set, select: str,
                    correo: str, api_key: str | None) -> dict:
    """Pide entidades de OpenAlex (sources, authors, institutions) por ID, de a 100.

    Devuelve {id_corto: entidad}. Los IDs que OpenAlex no encuentra no aparecen.
    """
    ids = sorted(ids)
    entidades = {}
    for inicio in range(0, len(ids), POR_LOTE_ENTIDADES):
        lote = ids[inicio:inicio + POR_LOTE_ENTIDADES]
        parametros = {
            "mailto":   correo,
            "filter":   "openalex_id:" + "|".join(lote),
            "per_page": POR_LOTE_ENTIDADES,
            "select":   select,
        }
        if api_key:
            parametros["api_key"] = api_key
        resp = session.get(f"{API_URL}/{tipo}", params=parametros, timeout=60)
        resp.raise_for_status()
        for entidad in resp.json().get("results", []):
            entidades[id_corto(entidad["id"])] = entidad
    log.info("%s: %s de %s encontradas", tipo, len(entidades), len(ids))
    return entidades

def historia_previa(entidad: dict | None) -> tuple[int | None, int | None]:
    """Works publicados y citas recibidas por la entidad ANTES de ANIO."""
    if not entidad:
        return None, None
    anteriores = [c for c in entidad.get("counts_by_year") or [] if c["year"] < ANIO]
    return (sum(c["works_count"] for c in anteriores),
            sum(c["cited_by_count"] for c in anteriores))

def columnas_enriquecidas(fila: pd.Series, revistas: dict, autores: dict,
                          instituciones: dict) -> dict:
    """Arma las COLUMNAS_ENRIQUECIDAS de un paper a partir de las entidades ya descargadas."""
    autorias = json.loads(fila["authorships"]) if isinstance(fila["authorships"], str) else []
    revista  = revistas.get(id_corto(fila["primary_location_source_id"]))
    primero  = autores.get(id_corto(autorias[0]["author"].get("id"))) if autorias else None
    # Con un solo autor, "último" sería el mismo: se deja vacío.
    ultimo   = autores.get(id_corto(autorias[-1]["author"].get("id"))) if len(autorias) > 1 else None
    h_inst = [
        instituciones[i]["summary_stats"]["h_index"]
        for i in {id_corto(inst.get("id")) for a in autorias for inst in a.get("institutions") or []}
        if i in instituciones
    ]

    rev_works, rev_cites = historia_previa(revista)
    pri_works, pri_cites = historia_previa(primero)
    ult_works, ult_cites = historia_previa(ultimo)
    return {
        "source_pre_works":          rev_works,
        "source_pre_cites":          rev_cites,
        "source_pre_cites_per_work": rev_cites / rev_works if rev_works else None,
        "source_apc_usd":            (revista or {}).get("apc_usd"),
        "first_author_pre_works":    pri_works,
        "first_author_pre_cites":    pri_cites,
        "last_author_pre_works":     ult_works,
        "last_author_pre_cites":     ult_cites,
        "first_author_country":      ((autorias[0].get("countries") or [None])[0]
                                      if autorias else None),
        "institution_max_h_index":   max(h_inst) if h_inst else None,
    }


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
            12000, type="integer", title="Cantidad aproximada de papers",
        ),
        "correo_api": Param(
            "tu_correo@utn.edu.ar", type="string", title="Polite Pool Email",
            description="OpenAlex pide un mail para darte prioridad en la descarga.",
        ),
        "semilla": Param(
            42, type="integer", title="Semilla de muestreo",
            description=(
                "Seed de la muestra aleatoria de OpenAlex. La misma semilla "
                "reproduce la misma muestra."
            ),
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
                params={"mailto": correo, "per_page": 1, "filter": FILTRO},
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
        semilla        = context["params"]["semilla"]
        total_descargados = 0

        session = hook.get_conn()
        log.info("Capa Bronce: iniciando descarga desde OpenAlex API...")

        destino.parent.mkdir(parents=True, exist_ok=True)

        # Como `sample` admite hasta 10.000 works, pedimos lotes con semillas
        # consecutivas hasta llegar al objetivo. Dos muestras distintas pueden
        # compartir algún work; la capa Plata deduplica por ID.
        lote = 0
        # Abrimos el gzip de escritura acá, y escribimos a medida que paginamos
        with gzip.open(destino, "wt", encoding="utf-8") as f:
            while total_descargados < filas_objetivo:
                tamanio      = min(MAX_MUESTRA, filas_objetivo - total_descargados)
                semilla_lote = semilla + lote
                paginas      = -(-tamanio // POR_PAGINA)  # división con redondeo hacia arriba
                descargados_lote = 0

                log.info("Lote %s: muestra de %s works (seed=%s)", lote, tamanio, semilla_lote)
                for pagina in range(1, paginas + 1):
                    resultados = fetch_page(session, pagina, tamanio, semilla_lote, correo, api_key)
                    if not resultados:
                        break

                    # Escribimos los resultados de esta página y liberamos la memoria
                    for work in resultados:
                        f.write(json.dumps(work, ensure_ascii=False) + "\n")

                    descargados_lote  += len(resultados)
                    total_descargados += len(resultados)
                    log.info("Registros acumulados: %s", total_descargados)

                if descargados_lote == 0:
                    log.warning("El lote %s vino vacío. Se corta la descarga.", lote)
                    break
                lote += 1

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
            # Tamaño en disco y no filas: el CSV puede estar comprimido y sus
            # celdas (abstract, JSON) pueden tener saltos de línea.
            log.warning(
                "OpenAlex no respondió. Usando respaldo (%s): %s (%.1f MB). "
                "ATENCIÓN: estos datos NO son de hoy.",
                origen, ruta.name, ruta.stat().st_size / 1e6,
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

        El CSV trae primero las columnas derivadas (COLUMNAS_PLATA) y después
        todas las de la API aplanadas. Como no todos los works traen los mismos
        campos, se hacen dos pasadas: la primera junta el conjunto de columnas
        y la segunda escribe las filas. Así no hace falta tener todo en memoria.
        """
        import csv

        # CHUNK_SIZE = 500 es el tamaño del buffer que se usa para escribir en el archivo CSV
        CHUNK_SIZE = 500

        PLATA_DIR.mkdir(parents=True, exist_ok=True)
        destino_plata = PLATA_DIR / "openalex_silver.csv.gz"
        ids_vistos: set = set()
        total_filas = 0

        # Pasada 1: columnas en orden de aparición, con las derivadas al principio
        columnas = dict.fromkeys(COLUMNAS_PLATA)
        with gzip.open(ruta_bronce, "rt", encoding="utf-8") as f_in:
            for linea in f_in:
                columnas.update(dict.fromkeys(fila_plata(json.loads(linea), "")))
        log.info("Capa Plata: %s columnas (%s derivadas + API aplanada)",
                 len(columnas), len(COLUMNAS_PLATA))

        # Pasada 2: escritura
        with gzip.open(ruta_bronce, "rt", encoding="utf-8") as f_in, \
             gzip.open(destino_plata, "wt", newline="", encoding="utf-8") as f_out:

            writer = csv.DictWriter(f_out, fieldnames=list(columnas))
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

                buffer.append(fila_plata(work, work_id))
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
    # 5b. PLATA ENRIQUECIDA — revista, autores e instituciones
    # ------------------------------------------------------------------

    @task(retries=3, retry_delay=pendulum.duration(seconds=15))
    def enrich_silver(ruta_plata: str, **context) -> str:
        """Suma a cada paper datos de su revista, sus autores y sus instituciones.

        Los works traen sólo los IDs de esas entidades; sus métricas viven en otros
        endpoints de OpenAlex (/sources, /authors, /institutions). Se piden una sola
        vez por entidad, de a 100 por llamada (~300 llamadas para 10.000 papers).
        """
        hook     = HttpHook(method="GET", http_conn_id=CONN_ID)
        api_key  = hook.get_connection(CONN_ID).password
        correo   = context["params"]["correo_api"]
        session  = hook.get_conn()

        # Sólo las columnas con IDs: el archivo completo tiene cientos de columnas.
        ids = pd.read_csv(ruta_plata, usecols=["id", "primary_location_source_id", "authorships"])
        autorias = ids["authorships"].map(lambda s: json.loads(s) if isinstance(s, str) else [])

        ids_revistas = set(ids["primary_location_source_id"].map(id_corto).dropna())
        ids_autores, ids_instituciones = set(), set()
        for a in autorias:
            if a:
                ids_autores.add(id_corto(a[0]["author"].get("id")))
                ids_autores.add(id_corto(a[-1]["author"].get("id")))
            ids_instituciones |= {id_corto(i.get("id")) for au in a for i in au.get("institutions") or []}
        ids_autores.discard(None)
        ids_instituciones.discard(None)

        revistas = fetch_entidades(session, "sources", ids_revistas,
                                   "id,apc_usd,counts_by_year", correo, api_key)
        autores = fetch_entidades(session, "authors", ids_autores,
                                  "id,counts_by_year", correo, api_key)
        instituciones = fetch_entidades(session, "institutions", ids_instituciones,
                                        "id,summary_stats", correo, api_key)

        extra = pd.DataFrame(
            [columnas_enriquecidas(fila, revistas, autores, instituciones)
             for _, fila in ids.iterrows()],
            columns=COLUMNAS_ENRIQUECIDAS,
        )
        extra.insert(0, "id", ids["id"])

        # Reescribe Plata por bloques, con las columnas nuevas después de COLUMNAS_PLATA.
        destino = PLATA_DIR / "openalex_silver_enriched.csv.gz"
        with gzip.open(destino, "wt", newline="", encoding="utf-8") as f_out:
            for n, bloque in enumerate(pd.read_csv(ruta_plata, chunksize=2000, low_memory=False)):
                bloque = bloque.merge(extra, on="id", how="left")
                resto = [c for c in bloque.columns
                         if c not in COLUMNAS_PLATA and c not in COLUMNAS_ENRIQUECIDAS]
                bloque[COLUMNAS_PLATA + COLUMNAS_ENRIQUECIDAS + resto].to_csv(
                    f_out, index=False, header=(n == 0))

        log.info("Plata enriquecida: %s filas -> %s. Cobertura: revista %.0f%%, "
                 "primer autor %.0f%%, institución %.0f%%",
                 len(extra), destino,
                 100 * extra["source_pre_works"].notna().mean(),
                 100 * extra["first_author_pre_works"].notna().mean(),
                 100 * extra["institution_max_h_index"].notna().mean())
        return str(destino)

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

        # Sin el esquema completo, los chequeos siguientes no tienen sentido.
        # Pasa, por ejemplo, con un respaldo generado por una versión anterior del DAG.
        # nrows=0 lee sólo el encabezado (pandas descomprime .gz solo).
        columnas = pd.read_csv(ruta, nrows=0).columns
        faltantes = [c for c in COLUMNAS_PLATA + COLUMNAS_ENRIQUECIDAS if c not in columnas]
        if faltantes:
            raise ValueError(f"Validación fallida: faltan las columnas {faltantes} en {ruta}.")

        # Sólo cargamos lo que se chequea: el archivo completo tiene cientos de columnas.
        df = pd.read_csv(ruta, usecols=["id", "cited_by_count", "is_top10_percent"])

        problemas = []

        if len(df) < FILAS_MINIMAS:
            problemas.append(f"Volumen insuficiente: {len(df)} filas (mínimo {FILAS_MINIMAS}).")
        if df["cited_by_count"].isna().any():
            problemas.append(
                f"Target 'cited_by_count' tiene {df['cited_by_count'].isna().sum()} nulos."
            )
        if df["id"].duplicated().any():
            problemas.append(f"Existen {df['id'].duplicated().sum()} IDs duplicados.")

        # OpenAlex no calcula el percentil para algunos works: no se descartan,
        # pero dejamos constancia para decidir en el análisis.
        sin_percentil = df["is_top10_percent"].isna().sum()
        if sin_percentil:
            log.warning("'is_top10_percent' tiene %s nulos (%.1f%%).",
                        sin_percentil, 100 * sin_percentil / len(df))

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
        # Conserva la extensión de origen: .csv.gz desde Plata, .csv desde la semilla.
        extension = "".join(Path(ruta_validada).suffixes)
        destino_final = OUTPUT_DIR / f"openalex_{fecha_str}{extension}"
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
    plata     = enrich_silver(refine_silver(bronce))

    # Las tareas de decisión no se pasan datos entre sí: sólo ordenan el flujo.
    espera >> rama
    rama >> [bronce, congelado]

    validado = validate(plata, congelado)
    save(validado)


openalex_ingest()
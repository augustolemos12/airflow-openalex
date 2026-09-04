"""
### Dataset canónico FIFA — Ciencia de Datos, UTN FRM 2026

Construye el dataset de jugadores de la materia a partir del listado público
de sofifa.com.

El pipeline recorre las ligas del catálogo de sofifa, pide cada página del
listado con las 76 columnas que la fuente expone, normaliza al esquema de la
cátedra y valida el resultado antes de escribirlo.

**Dos capas, y son dos tareas distintas en el grafo**, siguiendo el modelo
medallón:

* **Bronce** (`land_bronze`) -- el HTML tal como lo devolvió sofifa, guardado
  comprimido y particionado por snapshot. Es la única tarea que toca la
  fuente. No interpreta nada.
* **Plata** (`refine_silver` + `consolidate`) -- filas tipadas, una por
  jugador, deduplicadas y validadas. No toca la red: lee del bronce.

La separación no es decorativa. Una página que ya está en bronce no se vuelve
a pedir, así que la segunda corrida sobre el mismo snapshot no genera ni una
request. Y si mañana aparece un bug en el parseo, se corrige la plata y se
reprocesa el bronce que ya está en disco, sin volver a scrapear.

La tercera capa del modelo -- **oro**: features del modelo, agregados por liga
-- no se construye acá. Se arma en las unidades 3 y 4, sobre esta misma plata.

Corre **todos los días**, pero trabaja sólo cuando hay algo nuevo: sofifa
publica parches cada una o dos semanas, así que la mayoría de las corridas
terminan enseguida sin bajar nada. Eso no es un desperdicio, es lo normal en un
pipeline sano.

**Tres niveles de degradación**, en orden:

1. HTTP directo con `urllib` — el camino normal, medio segundo por página.
2. Playwright + Chromium — si Cloudflare devuelve 403.
3. Snapshot congelado en el repo — si sofifa está caído o cambió de estructura.

Y antes de degradar, **espera**: si la fuente no responde, el sensor reintenta
durante media hora antes de darla por perdida. Un corte de diez minutos no
tiene por qué arruinar la corrida del día.

Un pipeline de producción no tiene una fuente: tiene un plan para cuando la
fuente falla. Eso es lo que se ve en el grafo.
"""
from __future__ import annotations

import gzip
import logging
import time
from pathlib import Path

import pendulum
from airflow.sdk import Param, PokeReturnValue, Variable, dag, task
from airflow.task.trigger_rule import TriggerRule

from fifa import schema
from fifa.leagues import fetch_catalog
from fifa.sofifa import (PAGE_SIZE, EndOfLeague, fetch, page_url, parse_page,
                         snapshot_meta)
from fifa.transform import to_row

log = logging.getLogger(__name__)

OUTPUT_DIR = Path("/usr/local/airflow/include/output")

# Las dos primeras capas del modelo medallón, cada una en su carpeta.
#   BRONCE -- el HTML tal como lo devolvió sofifa, sin interpretar.
#   PLATA  -- filas tipadas, una por jugador, listas para analizar.
# La tercera capa, oro, no se construye acá: son las features del modelo y
# los agregados de la app, y se arman en las unidades 3 y 4 sobre esta plata.
BRONCE_DIR = OUTPUT_DIR / "bronze"
PLATA_DIR = OUTPUT_DIR / "silver"
PARTIAL_DIR = PLATA_DIR / "_parciales"

# Esta función genera y devuelve la ruta donde debe vivir una página HTML cruda.
# roster es el identificador del snapshot de sofifa, league_id es la liga y offset es la pagina
def bronze_path(roster, league_id, offset) -> Path:
    """Dónde vive una página cruda.

    El snapshot y la liga van en la **ruta**, no en el nombre del archivo:
    cada actualización de sofifa es un lote independiente, así conviven
    varios sin pisarse y se puede borrar uno entero sin tocar los demás.
    Es el mismo particionado que usa cualquier data lake sobre S3, con
    carpetas en lugar de prefijos de bucket.
    """
    return (BRONCE_DIR / f"roster={roster}" / f"liga={league_id}"
            / f"pagina_{offset:05d}.html.gz")


def bronze_write(destino: Path, html: str) -> None:
    """Guarda una página cruda y comprimida: el HTML baja unas ocho veces."""
    # Crea de forma recursiva (si es necesario) todos los directorios intermedios de la ruta. exist_ok=True evita error si ya existen.
    destino.parent.mkdir(parents=True, exist_ok=True)
    # Una vez creados los directorios, abre el archivo en modo texto ("wt"), pero dentro de un contenedor gzip, por lo que el contenido se escribe comprimido.
    with gzip.open(destino, "wt", encoding="utf-8") as f:
        f.write(html)


def bronze_read(ruta: Path) -> str:
    # Abre el archivo en modo lectura texto ("rt"), descomprimiéndolo automáticamente.
    with gzip.open(ruta, "rt", encoding="utf-8") as f:
        return f.read()

# El respaldo tiene dos capas:
#   SEMILLA  -- versionada en el repositorio, viaja con el proyecto y nunca se
#               toca. Garantiza que el respaldo existe desde el primer clone,
#               antes de que nadie haya corrido el DAG.
#   ULTIMO_OK -- lo reescribe cada corrida completa exitosa. Ignorado por git.
# load_frozen prefiere ULTIMO_OK y cae a SEMILLA. Así el arranque en frío está
# cubierto por la semilla y la frescura por el último éxito.
# Acá se anota el último snapshot que se procesó bien. Es lo que le permite al
# DAG saber, en la corrida de mañana, si hay algo nuevo que hacer.
VAR_ULTIMO_ROSTER = "fifa_ultimo_roster"

FROZEN_DIR = Path("/usr/local/airflow/include/frozen")
SEMILLA = FROZEN_DIR / "fifa_snapshot.csv"
ULTIMO_OK = FROZEN_DIR / "ultimo_ok.csv"


@dag(
    dag_id="fifa_ingest",
    # Mira la fuente todos los días. `catchup=False` evita que, si el entorno
    # estuvo apagado una semana, Airflow intente recuperar las siete corridas.
    schedule="@daily",
    start_date=pendulum.datetime(2026, 8, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    # Cuántas ligas se bajan a la vez. Medido contra sofifa: más de 8
    # conexiones simultáneas no acelera, el sitio empieza a frenar.
    max_active_tasks=8,
    tags=["ciencia-de-datos", "unidad-1", "dataset-canonico"],
    doc_md=__doc__,
    # params permite enviar parámetros al dag en tiempo de ejecución. Es un diccionario donde cada clave es el nombre del parámetro.
    # "Param" es una clase de airflow que permite definir parámetros. El primer parámetro es el valor por defecto, el segundo es el tipo, el tercero es el título y el cuarto es la descripción.
    params={
        #mode permite ejecutar el dag con un subset de datos o con el dataset completo.
        "mode": Param(
            "subset", enum=["subset", "full"],
            title="Modo de corrida",
            description=("subset: la primera página de cada liga (~3.000 jugadores, "
                         "~30 s). full: el catálogo completo, 18.936 jugadores "
                         "de 52 ligas, ~1 min y medio."),
        ),
        #roster permite elegir el roster de sofifa que se quiere procesar. Si es "null", se procesa el roster más reciente. Si es un entero, se procesa el roster con ese id.
        "roster": Param(
            None, type=["null", "integer"],
            title="Snapshot a congelar",
            description=("Id del roster de sofifa, ej. 260046 = FC 26 actualización 46. "
                         "Vacío usa el más reciente. Fijarlo hace la corrida reproducible."),
        ),
        #engine es para elegir entre http y browser.
        "engine": Param(
            "auto", enum=["auto", "http", "browser"],
            title="Motor de descarga",
            description="auto prueba HTTP y cae a navegador si recibe 403.",
        ),
        #force permite forzar la corrida ignorando las cachés y bajando de nuevo las páginas que ya están en bronce.
        "force": Param(
            False, type="boolean",
            title="Forzar la corrida",
            description=("Ignora todas las cachés: baja aunque el snapshot no "
                         "haya cambiado, y vuelve a pedir las páginas que ya "
                         "están en bronce."),
        ),
    },
)
def fifa_ingest():
    
    @task.sensor(poke_interval=300, timeout=1800, mode="reschedule",
                 soft_fail=True)
    # PokeReturnValue es una clase de airflow que permite retornar un valor desde un sensor. Si es True, el sensor termina, si es False, el sensor reintenta.
    # poke_interval es cada cuánto se sondea, y mode qué se hace entre cada sondeo.
    def wait_for_source(**context) -> PokeReturnValue:
        """Espera a que sofifa esté disponible. Sondea cada 5 min, hasta 30.

        Un sensor sirve para esperar algo que está **fuera del control del
        DAG**: que un sitio vuelva, que aparezca un archivo que deja otro
        sistema, que termine un proceso ajeno. Para esperar a una tarea del
        mismo DAG no hace falta un sensor: para eso están las dependencias.

        `mode="reschedule"` es la parte interesante. En vez de ocupar un worker
        durmiendo cinco minutos, la tarea se libera y Airflow la vuelve a
        encolar más tarde: sondea, se libera, vuelve. 
        Con un solo sensor da igual; con cincuenta esperando, es la diferencia 
        entre un scheduler sano y uno tapado. 
        Si mode = "poke", el worker queda ocupado 30 minutos esperando a que el sensor termine.

        `soft_fail=True` hace que, al agotarse el tiempo, la tarea quede en
        `skipped` y no en `failed`: no responder no es un error del pipeline,
        es una condición que sabemos manejar.
        """
        params = context["params"]
        try:
            #El fetch se usa para descargar una página.
            #page_url es una función que genera la url de la página.
            #parse_page es una función que parsea la página: la convierte en una lista de filas.
            #snapshot_meta es una función que obtiene el snapshot de la página.
            html = fetch(page_url(league_id=13, offset=0, roster=params["roster"]),
                         engine=params["engine"])
            filas = parse_page(html)
            if not filas:
                log.warning("sofifa respondió pero la tabla vino vacía")
                return PokeReturnValue(is_done=False)
            meta = snapshot_meta(html)
            log.info("sofifa responde: %s filas de prueba, snapshot %s",
                     len(filas), meta)
            return PokeReturnValue(is_done=True, xcom_value=meta)
        except Exception as e:
            log.warning("sofifa todavía no responde (%s). Reintento en 5 min.", e)
            return PokeReturnValue(is_done=False)

    @task.branch(trigger_rule=TriggerRule.ALL_DONE)
    # task.branch es una tarea que permite ejecutar una tarea u otra según una condición, acá retorna una cadena de texto que será interpretada como un nombre de tarea.
    def check_source(**context) -> str:
        """Decide por dónde sigue el DAG, según lo que consiguió el sensor.
        Corre con `ALL_DONE` porque su tarea de arriba puede haber quedado en
        `skipped` -- que es justamente el caso que tiene que manejar. Con la
        regla por defecto, `ALL_SUCCESS`, nunca se ejecutaría cuando más falta
        hace.
        """
        meta = context["ti"].xcom_pull(task_ids="wait_for_source")
        if meta:
            return "has_new_snapshot"
        log.error("sofifa no respondió en 30 minutos. Se usa el respaldo.")
        return "load_frozen"

    @task.short_circuit
    #es una tarea que devuelve un booleano que indica si hay un nuevo snapshot o no.
    def has_new_snapshot(**context) -> bool:
        """¿Hay algo nuevo que bajar? Si no, corta acá y no hace nada.

        sofifa publica un parche cada una o dos semanas. Un DAG diario que baja
        18.936 filas todos los días estaría rehaciendo el mismo trabajo seis de
        cada siete veces.

        La comparación es contra una **Variable** de Airflow, que guarda el
        último roster procesado con éxito. Si devuelve `False`, todo lo que
        está aguas abajo queda en `skipped` y la corrida termina bien: no pasó
        nada porque no había nada que hacer.

        Dos excepciones, las dos deliberadas:
          * `force=True` -- lo pedís explícitamente.
          * `roster` fijado -- estás pidiendo un snapshot puntual, así que
            "el último que vi" no es la pregunta correcta.
        """
        params = context["params"]
        meta = context["ti"].xcom_pull(task_ids="wait_for_source") or {}
        actual = meta.get("roster")

        if params["force"]:
            log.info("force=True: se baja aunque no haya cambiado.")
            return True
        if params["roster"] is not None:
            log.info("roster fijado en %s: se baja ese snapshot.", params["roster"])
            return True

        ultimo = Variable.get(VAR_ULTIMO_ROSTER, default=None)
        if str(actual) == str(ultimo):
            log.info("Sin novedad: sofifa sigue en el roster %s, el mismo de la "
                     "última corrida. No hay nada que hacer.", actual)
            return False

        log.info("Snapshot nuevo: %s (el anterior era %s). Se baja.",
                 actual, ultimo)
        return True

    @task
    #discover_leagues()devuelve una lista de diccionarios, donde cada diccionario es una liga y cuántas páginas tiene.
    #Es la fase de planificación del pipeline: traduce los metadatos del snapshot y los parámetros de ejecución 
    # en una lista estructurada que determina qué se va a descargar y cómo se distribuirá el trabajo entre las 
    # tareas de Airflow.
    def discover_leagues(**context) -> list[dict]:
        """Catálogo de ligas + cuántas páginas pedir de cada una.

        sofifa publica el conteo de jugadores por liga, así que no hace falta
        tantear: sabemos de antemano el tamaño exacto del trabajo.

        El catálogo se pide para el mismo snapshot que después se scrapea: la
        lista de ligas cambia entre actualizaciones.
        """
        params = context["params"]
        meta = context["ti"].xcom_pull(task_ids="wait_for_source") or {}

        # El roster se resuelve acá, una sola vez, y viaja con cada liga.
        # Es la clave de partición del bronce, así que no puede quedar en
        # "el más reciente": dos ligas bajadas con minutos de diferencia
        # podrían caer en snapshots distintos y mezclarse en la misma carpeta.
        roster = params["roster"] or meta.get("roster")
        if roster is None:
            raise ValueError(
                "No se pudo determinar el snapshot de sofifa. El bronce se "
                "particiona por roster, así que necesita uno concreto.")

        #Trae el catalogo de ligas
        # `fetch_catalog` llama a la API de Sofifa y devuelve una lista de dicts,
        # cada uno con al menos: league_id, league_name, n_pages.
        catalogo = fetch_catalog(engine=params["engine"], roster=roster)
        # Si `mode` es "subset", limita a 1 página por liga (útil para pruebas).
        tope = 1 if params["mode"] == "subset" else None

        #Crea las tareas de bronce, cada tarea representa una liga que bajar
        tareas = []
        for lg in catalogo:
            # n es el número de páginas a bajar de cada liga. Si tope es None, se baja todas las páginas.
            n = lg["n_pages"] if tope is None else min(tope, lg["n_pages"])
            # Cada diccionario combina la información del catálogo + los
            # parámetros que deben propagarse a la siguiente fase (scraping).
            tareas.append({**lg, # Trae todo lo que tiene lg
                        "n_pages": n, "roster": roster,
                        "engine": params["engine"],
                        "force": params["force"]})
        #Calcula el total de páginas a procesar
        total = sum(t["n_pages"] for t in tareas)
        log.info("modo %s, snapshot %s: %s ligas, %s páginas",
                 params["mode"], roster, len(tareas), total)
        # Con @task, el return se pushea automáticamente a XCom y será consumido
        # por la siguiente etapa del DAG (normalmente una *dynamic task mapping* que
        # crea una tarea `land_bronze` por cada liga).
        return tareas

    @task(map_index_template="{{ task.op_kwargs['league']['league_name'] }}",
          retries=2, retry_delay=pendulum.duration(seconds=30))
    def land_bronze(league: dict) -> dict:
        """**Capa bronce**: guarda el HTML crudo, sin interpretarlo.

        Es la única tarea de todo el DAG que toca sofifa. No parsea, no
        limpia, no valida: deja el byte tal como vino y anota dónde quedó.

        La regla del bronce es **append-only**: una página ya bajada no se
        vuelve a pedir. De ahí salen las dos propiedades que lo justifican:

          * La segunda corrida sobre el mismo snapshot no genera **ni una
            sola request**. La fuente se toca una vez por dato, no una vez
            por corrida.
          * Un error en el parseo se arregla **sin volver a la fuente**. Se
            corrige `refine_silver` y se reprocesa lo que ya está en disco.

        Sin esta capa, el HTML se pierde apenas se convierte en filas y
        cualquiera de las dos cosas obliga a scrapear todo de nuevo.

        En la UI cada instancia aparece con el nombre de su liga, así que el
        grafo se lee solo: 'Premier League' en verde, 'Serie A' corriendo.
        """
        paginas, pedidas, reusadas = [], 0, 0
        
        #Recorre las páginas de la liga. Cada iteración es una página.
        for i in range(league["n_pages"]):
            # offset sirve para determinar desde que fila se empieza a traer los datos.
            offset = i * PAGE_SIZE
            # destino es la ruta donde se va a guardar cada página.
            destino = bronze_path(league["roster"], league["league_id"], offset)

            # comprueba si el archivo ya existe, si sí reutilizamos
            if destino.exists() and not league["force"]:
                paginas.append(str(destino))
                reusadas += 1
                continue

            # llama a la función page_url para obtener la URL de la página
            url = page_url(league_id=league["league_id"], offset=offset,
                           roster=league["roster"])
            # intenta traer los datos de la página
            try:
                html = fetch(url, engine=league["engine"])
            # si la página no existe, rompe el bucle
            except EndOfLeague:
                break                      # el conteo estaba desactualizado
            # escribe los datos en el archivo.
            bronze_write(destino, html)
            paginas.append(str(destino))
            pedidas += 1
            time.sleep(0.25)               # no golpear la fuente

        log.info("%s: %s páginas en bronce (%s pedidas a sofifa, %s reusadas)",
                 league["league_name"], len(paginas), pedidas, reusadas)

        # Devuelve rutas, no HTML: XCom es para metadatos chicos. Pasar
        # megabytes de página por XCom satura la base de Airflow, y es de
        # los errores más comunes al empezar con la herramienta.
        return {"league": league, "pages": paginas}

    @task(map_index_template="{{ task.op_kwargs['lote']['league']['league_name'] }}")
    def refine_silver(lote: dict) -> str:
        """**Capa plata**: del HTML crudo a filas tipadas, una por jugador.

        Acá pasa todo lo que el bronce no hace: parsear la tabla, convertir
        alturas a centímetros y valores a euros, quedarse con las 90 columnas
        del esquema de la cátedra y descartar el resto.

        **No toca la red.** Todo lo que necesita ya está en disco. Es la
        propiedad que hace valiosa la separación: esta tarea se puede correr
        cien veces mientras se depura el parseo, y sofifa ni se entera.
        """
        import pandas as pd

        league = lote["league"]
        filas, meta = [], None

        for ruta in lote["pages"]:
            html = bronze_read(Path(ruta))
            if meta is None:
                meta = snapshot_meta(html)
            lote_filas = parse_page(html)
            if not lote_filas:
                break
            filas.extend(lote_filas)

        registros = [to_row(f, meta or {}, league) for f in filas]
        PARTIAL_DIR.mkdir(parents=True, exist_ok=True)
        destino = PARTIAL_DIR / f"liga_{league['league_id']}.csv"
        pd.DataFrame(registros, columns=schema.COLUMNS).to_csv(destino, index=False)

        log.info("%s: %s jugadores desde %s páginas de bronce -> %s",
                 league["league_name"], len(registros), len(lote["pages"]),
                 destino.name)
        return str(destino)

    @task
    def consolidate(rutas: list[str]) -> str:
        """Junta los parciales de plata, deduplica y ordena las columnas.

        Sigue siendo capa plata: una fila por jugador, sin agregar nada. El
        oro -- agregados por liga, features derivadas -- se construye en las
        unidades siguientes, sobre este mismo archivo.
        """
        import pandas as pd

        partes = [pd.read_csv(r, low_memory=False) for r in rutas if r]
        partes = [p for p in partes if len(p)]
        if not partes:
            raise ValueError("ninguna liga devolvió filas")

        df = pd.concat(partes, ignore_index=True)[schema.COLUMNS]
        antes = len(df)
        df = df.drop_duplicates("player_id").reset_index(drop=True)
        log.info("%s filas, %s tras deduplicar", antes, len(df))

        for c in schema.ENTEROS:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        destino = OUTPUT_DIR / "_consolidado.csv"
        df.to_csv(destino, index=False)
        return str(destino)

    @task
    def load_frozen() -> str:
        """Rama de último recurso: el respaldo más fresco que haya en disco.

        Prefiere el de la última corrida completa exitosa; si no existe
        todavía, usa la semilla versionada en el repositorio.

        Ojo con qué es este archivo: es **plata**, no bronce. Ya está
        parseado y tipado, así que sirve para seguir adelante, pero no
        permite reprocesar nada. Un respaldo de plata te salva la corrida de
        hoy; el bronce te salva de un bug en el parser.
        """
        import pandas as pd

        for ruta, origen in ((ULTIMO_OK, "última corrida completa exitosa"),
                             (SEMILLA, "semilla versionada en el repositorio")):
            if not ruta.exists():
                continue
            try:
                cabeza = pd.read_csv(ruta, nrows=1)
                fecha = cabeza["fifa_update_date"].iloc[0]
                filas = sum(1 for _ in open(ruta, encoding="utf-8")) - 1
            except Exception:
                fecha, filas = "desconocida", "?"
            log.warning(
                "sofifa no respondió. Se usa el respaldo (%s): %s filas, "
                "datos del %s. NO son de hoy.", origen, filas, fecha)
            return str(ruta)

        raise FileNotFoundError(
            f"sofifa no responde y no hay ningún respaldo en {FROZEN_DIR}. "
            "Falta la semilla fifa_snapshot.csv del repositorio.")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def validate(desde_fuente: str | None, desde_snapshot: str | None) -> str:
        """Chequeos duros. Si alguno falla, el DAG falla: no se publica basura."""
        import pandas as pd

        ruta = desde_fuente or desde_snapshot
        if ruta is None:
            raise ValueError("ninguna rama produjo un archivo")
        df = pd.read_csv(ruta, low_memory=False)

        problemas = []
        if list(df.columns) != schema.COLUMNS:
            faltan = set(schema.COLUMNS) - set(df.columns)
            sobran = set(df.columns) - set(schema.COLUMNS)
            problemas.append(f"columnas distintas (faltan {faltan}, sobran {sobran})")
        #chequea que no haya menos de 500 filas
        if len(df) < 500:
            problemas.append(f"muy pocas filas: {len(df)}")
        if df["player_id"].duplicated().any():
            problemas.append(f"{df['player_id'].duplicated().sum()} player_id repetidos")
        for c in schema.OBLIGATORIAS:
            if c in df.columns and df[c].isna().any():
                problemas.append(f"{c} tiene {df[c].isna().sum()} nulos")
        if "overall" in df.columns and not df["overall"].between(1, 99).all():
            problemas.append("overall fuera del rango 1-99")

        if problemas:
            raise ValueError("Validación fallida:\n  - " + "\n  - ".join(problemas))

        log.info("Validación OK: %s filas x %s columnas, %s ligas",
                 len(df), len(df.columns), df["league_id"].nunique())
        return ruta

    @task
    def save(ruta: str, **context) -> str:
        """Escribe el entregable fechado con la corrida.

        Ojo con `ds`: sólo existe cuando el DAG tiene schedule y por lo tanto
        intervalo de datos. Este corre a demanda, así que la fecha sale del
        DagRun. Es un tropiezo clásico al pasar de Airflow 2 a 3.
        """
        import shutil

        dag_run = context["dag_run"]
        momento = dag_run.logical_date or dag_run.run_after
        ds = momento.date().isoformat()
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        destino = OUTPUT_DIR / f"fifa_{ds}.csv"
        shutil.copy(ruta, destino)
        log.info("Dataset escrito en %s", destino)

        # Refrescar el respaldo, con dos condiciones:
        #   - los datos vinieron de la fuente, no del respaldo mismo
        #   - la corrida fue completa: un subset es una corrida de desarrollo
        #     y sería un retroceso pisar un respaldo entero con uno parcial
        vino_del_respaldo = Path(ruta).parent == FROZEN_DIR
        modo = context["params"]["mode"]
        if vino_del_respaldo:
            log.info("El respaldo no se toca: estos datos salieron de él.")
        elif modo != "full":
            log.info("El respaldo no se toca: modo '%s'. Sólo lo refresca "
                     "una corrida 'full'.", modo)
        else:
            FROZEN_DIR.mkdir(parents=True, exist_ok=True)
            shutil.copy(destino, ULTIMO_OK)
            log.info("Respaldo actualizado -> %s", ULTIMO_OK)

        # Anotar qué snapshot quedó procesado. Es lo que va a leer
        # has_new_snapshot mañana para decidir si hay trabajo.
        meta = context["ti"].xcom_pull(task_ids="wait_for_source") or {}
        if not vino_del_respaldo and meta.get("roster"):
            Variable.set(VAR_ULTIMO_ROSTER, str(meta["roster"]))
            log.info("Último roster procesado -> %s", meta["roster"])

        return str(destino)

    espera = wait_for_source()
    rama = check_source()
    novedad = has_new_snapshot()
    ligas = discover_leagues()
    # En el TaskFlow API de Airflow, la llamada .expand() expande (crea dinámicamente) una tarea para cada elemento de la colección que le pases.
    bronces = land_bronze.expand(league=ligas)
    parciales = refine_silver.expand(lote=bronces)
    consolidado = consolidate(parciales)
    congelado = load_frozen()

    # El único lugar donde hace falta declarar dependencias a mano: las tareas
    # de decisión no se pasan datos entre sí, sólo ordenan el flujo.
    espera >> rama
    rama >> [novedad, congelado]
    novedad >> ligas
    save(validate(consolidado, congelado))


fifa_ingest()

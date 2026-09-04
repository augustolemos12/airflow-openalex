"""
DAG de Ingesta desde OpenAlex API

Este DAG consulta la API REST de OpenAlex utilizando paginación por cursor,
extrae información sobre publicaciones académicas, procesa la respuesta en formato tabular,
guarda el resultado en CSV y realiza validaciones de calidad de datos.
"""
from __future__ import annotations

import logging
from pathlib import Path
import pandas as pd
import pendulum

from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import Param, dag, task


log = logging.getLogger(__name__)

OUTPUT_DIR = Path("/usr/local/airflow/include/output")
# Directorios basados en el modelo Medallón
BRONCE_DIR = OUTPUT_DIR / "bronze"
PLATA_DIR = OUTPUT_DIR / "silver"

CONN_ID = "openalex_api"


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
            description="OpenAlex pide un mail para darte prioridad en la descarga."
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

    @task(retries=3, retry_delay=pendulum.duration(seconds=15))
    def land_bronze(**context) -> str:
        """Capa Bronce: Descarga y almacena los JSON crudos directamente en disco.

        No interpreta, limpia ni parsea nada. Preserva la respuesta original de la API
        para permitir reprocesamientos sin volver a consultar la red.

        Es la única tarea que toca la red. Si el archivo del día ya existe en disco
        y `force=False`, lo reutiliza sin hacer ninguna request: la segunda corrida
        sobre el mismo snapshot no genera ni una llamada a la API.
        """
        import json

        dag_run = context["dag_run"]
        momento = dag_run.logical_date or dag_run.run_after
        fecha_str = momento.date().isoformat()
        force = context["params"]["force"]

        BRONCE_DIR.mkdir(parents=True, exist_ok=True)
        destino_bronce = BRONCE_DIR / f"openalex_raw_{fecha_str}.json"

        # Idempotencia: si el bronce del día ya existe, no volver a descargar.
        # Mismo patrón que el DAG de FIFA: una página ya bajada no se vuelve a pedir.
        if destino_bronce.exists() and destino_bronce.stat().st_size > 0 and not force:
            log.info(
                "Bronce del %s ya existe (%s bytes). Omitiendo descarga. "
                "Usá force=True para forzar una re-ingesta.",
                fecha_str, destino_bronce.stat().st_size,
            )
            return str(destino_bronce)

        hook = HttpHook(method="GET", http_conn_id=CONN_ID)
        conexion = hook.get_connection(CONN_ID)
        api_key = conexion.password

        filas_objetivo = context["params"]["filas_objetivo"]
        correo = context["params"]["correo_api"]
        # el cursor comienza en "*" para indicar que se quiere empezar desde el principio
        cursor = "*"
        # raw_works = Lista que almacena los JSONs crudos.
        raw_works = []

        # Obtenemos la sesión gestionada por el hook (maneja reintentos, headers y auth)
        # y la usamos con la URL completa para evitar problemas de construcción con urljoin
        BASE_URL = "https://api.openalex.org/works"
        session = hook.get_conn()

        log.info("Capa Bronce: Iniciando descarga masiva desde OpenAlex API...")

        while len(raw_works) < filas_objetivo and cursor is not None:
            parametros = {
                "mailto": correo,
                "per_page": 100,
                "cursor": cursor,
                "filter": "publication_year:2023,has_abstract:true"
            }
            if api_key:
                parametros["api_key"] = api_key

            http_resp = session.get(BASE_URL, params=parametros, timeout=30)
            http_resp.raise_for_status()
            respuesta = http_resp.json()
            resultados = respuesta.get("results", [])
            # agregamos los resultados a la lista
            raw_works.extend(resultados)
            # Actualizamos el cursor con el valor devuelto por la API para la siguiente iteración
            cursor = respuesta.get("meta", {}).get("next_cursor")
            log.info("Registros acumulados en Bronce: %s", len(raw_works))

        with open(destino_bronce, "w", encoding="utf-8") as f:
            # json.dump guarda los datos en formato JSON en el archivo.
            json.dump(raw_works, f, ensure_ascii=False)

        log.info("Capa Bronce guardada con éxito en %s", destino_bronce)
        return str(destino_bronce)

    @task
    def refine_silver(ruta_bronce: str) -> str:
        """Capa Plata: Lee los datos crudos del bronce, tipa, limpia y genera el CSV estructurado.

        No toca la red. Todo lo lee del archivo guardado en disco por la capa Bronce.
        Procesa el JSON en streaming para evitar cargar todo el archivo en memoria.
        """
        import csv
        import ijson

        COLUMNAS = [
            "id", "title", "publication_year", "cited_by_count",
            "is_oa", "authors_count", "referenced_works_count",
            "institutions_count", "countries_count", "type",
        ]
        CHUNK_SIZE = 500   # filas por chunk antes de vaciar el buffer

        PLATA_DIR.mkdir(parents=True, exist_ok=True)
        destino_plata = PLATA_DIR / "openalex_silver.csv"
        ids_vistos = set()  # para deduplicación incremental
        total_filas = 0

        with open(ruta_bronce, "rb") as f_in, \
             open(destino_plata, "w", newline="", encoding="utf-8") as f_out:

            writer = csv.DictWriter(f_out, fieldnames=COLUMNAS)
            writer.writeheader()
            buffer = []

            # ijson itera sobre el array raíz del JSON elemento a elemento
            for work in ijson.items(f_in, "item"):
                work_id = work.get("id")
                if work_id in ids_vistos:
                    continue  # deduplicación
                ids_vistos.add(work_id)

                buffer.append({
                    "id": work_id,
                    "title": work.get("title"),
                    "publication_year": work.get("publication_year"),
                    "cited_by_count": work.get("cited_by_count"),
                    "is_oa": work.get("open_access", {}).get("is_oa"),
                    "authors_count": len(work.get("authorships", [])),
                    "referenced_works_count": work.get("referenced_works_count"),
                    "institutions_count": len(work.get("institutions_distinct", [])),
                    "countries_count": work.get("countries_distinct_count", 0),
                    "type": work.get("type"),
                })

                if len(buffer) >= CHUNK_SIZE:
                    writer.writerows(buffer)
                    total_filas += len(buffer)
                    buffer.clear()

            # escribir el último chunk parcial
            if buffer:
                writer.writerows(buffer)
                total_filas += len(buffer)

        log.info("Capa Plata procesada: %s filas -> %s", total_filas, destino_plata)
        return str(destino_plata)

    @task
    def validate(ruta_plata: str) -> str:
        """Validación: Chequeos rigurosos antes de publicar el entregable."""
        df = pd.read_csv(ruta_plata)
        
        problemas = []
        if len(df) < 1000:
            problemas.append(f"Volumen insuficiente: solo {len(df)} filas.")
        
        if df["cited_by_count"].isna().any():
            problemas.append(f"Target 'cited_by_count' tiene {df['cited_by_count'].isna().sum()} nulos.")
            
        if df["id"].duplicated().any():
            problemas.append(f"Existen {df['id'].duplicated().sum()} IDs duplicados.")

        if problemas:
            raise ValueError("Validación fallida en capa Plata:\n  - " + "\n  - ".join(problemas))
            
        log.info("Validación OK: %s filas superaron todos los chequeos.", len(df))
        return ruta_plata

    @task
    def save(ruta_validada: str, **context) -> str:
        """Genera el entregable final fechado con la ejecución del DagRun."""
        import shutil

        dag_run = context["dag_run"]
        momento = dag_run.logical_date or dag_run.run_after
        fecha_str = momento.date().isoformat()
        
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        destino_final = OUTPUT_DIR / f"openalex_{fecha_str}.csv"
        shutil.copy(ruta_validada, destino_final)
        
        log.info("Entregable final escrito exitosamente en %s", destino_final)
        return str(destino_final)

    # Flujo / Grafo
    bronce = land_bronze()
    plata = refine_silver(bronce)
    validado = validate(plata)
    save(validado)


openalex_ingest()
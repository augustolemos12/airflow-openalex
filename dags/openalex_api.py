from __future__ import annotations

import logging
from pathlib import Path
import pandas as pd
import pendulum

from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import Param, dag, task

log = logging.getLogger(__name__)

# Directorio de salida tal como lo indica el ejemplo de clase
OUTPUT_DIR = Path("/usr/local/airflow/include/output")

# Esta conexión la deberás crear en Airflow (ej. base URL: https://api.openalex.org)
CONN_ID = "openalex_api"

@dag(
    dag_id="openalex_ingest",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    tags=["ciencia-de-datos", "proyecto-integrador", "openalex"],
    params={
        "filas_objetivo": Param(
            12000, type="integer", title="Cantidad aproximada de papers",
        ),
        "correo_api": Param(
            "tu_correo@utn.edu.ar", type="string", title="Polite Pool Email",
            description="OpenAlex pide un mail para darte prioridad de descarga."
        )
    },
)
def openalex_ingest():

    # Usamos HttpHook directamente porque necesitamos la lógica propia del cursor (bucle while)
    @task(retries=3, retry_delay=pendulum.duration(seconds=15))
    def extraer_y_guardar_openalex(**context) -> str:
        
        # 1. Instanciamos el hook con la conexión
        hook = HttpHook(method="GET", http_conn_id=CONN_ID)
        
        filas_objetivo = context["params"]["filas_objetivo"]
        correo = context["params"]["correo_api"]
        
        # Parámetros iniciales de OpenAlex
        cursor = "*"
        salida = []
        
        log.info("Iniciando descarga con paginación por cursor...")

        # 2. Bucle While para la paginación con cursor
        while len(salida) < filas_objetivo and cursor is not None:
            
            # Llamada al endpoint /works
            respuesta = hook.run(
                endpoint="works",
                data={
                    "mailto": correo,
                    "per_page": 100, 
                    "cursor": cursor,
                    # Filtro de ejemplo: papers del año 2023 con al menos 1 autor
                    "filter": "publication_year:2023,has_abstract:true" 
                }
            ).json()
            
            # Parsear los resultados (desanidar el JSON)
            for work in respuesta.get("results", []):
                salida.append({
                    "id": work.get("id"),
                    "title": work.get("title"),
                    "publication_year": work.get("publication_year"),
                    "cited_by_count": work.get("cited_by_count"), # Variable Target
                    "is_oa": work.get("open_access", {}).get("is_oa"),
                    "authors_count": len(work.get("authorships", [])),
                    "referenced_works_count": work.get("referenced_works_count"),
                    "institutions_count": len(work.get("institutions_distinct", [])),
                    "type": work.get("type"),
                })
            
            # Actualizamos el cursor para la próxima iteración
            cursor = respuesta.get("meta", {}).get("next_cursor")
            log.info(f"Descargados {len(salida)} registros. Próximo cursor: {cursor}")

        # 3. Guardar directamente en disco (evitamos saturar XCom)
        df = pd.DataFrame(salida)
        
        dag_run = context["dag_run"]
        momento = dag_run.logical_date or dag_run.run_after
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        destino = OUTPUT_DIR / f"openalex_{momento.date().isoformat()}.csv"
        
        df.to_csv(destino, index=False)
        log.info("%s filas x %s columnas -> %s", len(df), len(df.columns), destino)
        
        return str(destino)

    # Invocamos la tarea
    extraer_y_guardar_openalex()

openalex_ingest()
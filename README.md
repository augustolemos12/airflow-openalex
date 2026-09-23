# Pipeline de Ingesta y Procesamiento de OpenAlex

Pipeline de ingesta y procesamiento de datos desarrollado en **Apache Airflow** para el Proyecto Integrador de Ciencia de Datos.

El proyecto implementa la arquitectura del **Modelo Medallón**, utilizando las capas **Bronce** y **Plata**, junto con una estrategia de resiliencia basada en sensores de disponibilidad, ramificación dinámica y respaldos congelados.

El objetivo es mantener el pipeline operativo incluso ante caídas temporales de la API de **OpenAlex**, evitando fallos catastróficos y garantizando la disponibilidad de un dataset de contingencia.

---

## 1. Pregunta de Investigación y Dataset

### Pregunta de investigación

> **¿Qué factores de un artículo científico (cantidad de autores, referencias, temática y si es gratuito) permiten predecir si será de alto impacto en citas?**

### Unidad de análisis

La unidad de análisis es **1 artículo científico**, denominado *work* en OpenAlex.

### Variable objetivo

- `cited_by_count`: cantidad acumulada de citas recibidas por el artículo.

### Variables utilizadas

### Población

Artículos (`type:article`) publicados en 2023, con abstract y no retractados. El filtro se aplica directamente en la API de OpenAlex.

| Grupo | Variable | Descripción |
|---|---|---|
| Identificación | `id` | ID de OpenAlex (ej. `W4385245566`) |
| | `title` | Título (sólo referencia, no es predictor) |
| **Target** | `cited_by_count` | Cantidad de citas recibidas |
| | `is_top10_percent` | Si está en el 10% más citado de su campo y año (`citation_normalized_percentile`) |
| Autores | `authors_count` | Cantidad de autores |
| | `institutions_count` | Cantidad de instituciones distintas |
| | `countries_count` | Cantidad de países distintos |
| Referencias | `referenced_works_count` | Cantidad de trabajos referenciados |
| Temática | `domain` | Dominio de la temática principal: Health, Life, Physical o Social Sciences |
| | `field` | Campo de la temática principal (ej. Medicine, Computer Science) |
| Acceso abierto | `is_oa` | Si el artículo es de acceso abierto |
| | `oa_status` | Tipo de acceso: gold, green, hybrid, bronze, diamond o closed |
| | `apc_paid` | Si se pagó un cargo por publicación (APC) |
| Fuente | `source_type` | Tipo de fuente (journal, repository, conference, ...) |
| | `source_is_core` | Si la fuente pertenece al conjunto "core" de OpenAlex |
| | `is_in_doaj` | Si la fuente está indexada en el Directory of Open Access Journals |
| Controles | `funders_count` | Cantidad de financiadores |
| | `language` | Idioma (código ISO 639-1 en minúsculas) |
| | `publication_month` | Mes de publicación (1-12); controla el tiempo de exposición a citas |
| | `abstract_length` | Cantidad de palabras del abstract |
| Texto | `abstract` | Abstract reconstruido como texto |

Estas son las **columnas derivadas**, pensadas para el análisis. Además, la capa Plata incluye **todos los campos de la API aplanados** (~130 columnas más; ver sección 5).

> **Sobre los dos targets:** `is_top10_percent` está normalizado por campo, así que por construcción cada campo tiene ~10% de artículos en el top. Con ese target la temática no puede predecir el impacto; para evaluarla hay que usar `cited_by_count`.

> **Variables excluidas por fuga de datos:** `fwci`, `cited_by_percentile_year` y `counts_by_year` se calculan a partir de las citas y no deben usarse como predictores.

---

## 2. Arquitectura de Resiliencia

El pipeline implementa **tres niveles de degradación** para evitar que una indisponibilidad de la API de OpenAlex provoque el fallo completo de una ejecución.

### Nivel 1 — Sondeo de la fuente

La tarea `wait_for_source` es un `@task.sensor` que realiza peticiones de prueba a OpenAlex.

Características principales:

- Realiza comprobaciones cada **5 minutos**.
- Espera durante un máximo de **30 minutos**.
- Utiliza `mode="reschedule"` para no mantener ocupado un worker mientras espera.
- Utiliza `soft_fail=True`, por lo que una API indisponible provoca que el sensor quede en estado `skipped` en lugar de generar un fallo.

Cuando OpenAlex está disponible, el sensor devuelve:

```text
PokeReturnValue(is_done=True, xcom_value=True)
```

### Nivel 2 — Branching

La tarea `check_source` es un `@task.branch` que determina qué camino debe seguir el DAG.

La decisión se basa en el resultado recuperado mediante XCom:

- Si `xcom_value=True` → continúa hacia `land_bronze`.
- Si la fuente no estuvo disponible → continúa hacia `load_frozen`.

La regla de trigger utilizada permite que esta decisión se realice aunque el sensor haya quedado en `skipped`.

### Nivel 3 — Respaldo congelado

Cuando OpenAlex no está disponible, `load_frozen` utiliza un dataset previamente congelado.

La estrategia de recuperación prioriza:

1. **ULTIMO_OK**: copia actualizada después de una ejecución exitosa.
2. **SEMILLA**: archivo inicial `openalex_snapshot.csv`, utilizado cuando todavía no existe un respaldo exitoso.

De esta manera, una caída de la API no impide obtener un dataset válido para continuar con las etapas posteriores del pipeline.

---

## 3. Modelo Medallón y Tareas del DAG

| Capa / Tarea | Tipo | Descripción |
|---|---|---|
| `wait_for_source` | `@task.sensor` | Consulta la API de OpenAlex mediante `HttpHook`, utilizando `CONN_ID = "openalex_api"`. Cuando confirma disponibilidad devuelve `PokeReturnValue(is_done=True, xcom_value=True)`. |
| `check_source` | `@task.branch` | Decide el camino del DAG (`land_bronze` o `load_frozen`) según el resultado del sensor recuperado mediante XCom. |
| `land_bronze` | Capa Bronce | Realiza las peticiones HTTP masivas a OpenAlex. Descarga una muestra aleatoria reproducible (`sample` + `seed`) en lotes de hasta 10.000 works y almacena los JSON crudos comprimidos en `.json.gz`. Implementa idempotencia evitando volver a descargar el archivo del día si ya existe. |
| `load_frozen` | Respaldo | Carga el respaldo congelado cuando la API no está disponible, permitiendo que el pipeline continúe utilizando datos de contingencia. |
| `refine_silver` | Capa Plata |No realiza peticiones de red. Lee y descomprime los archivos Bronce de forma iterativa (línea por línea) para optimizar el uso de memoria RAM, calcula las columnas derivadas, aplana todos los campos de la API, elimina IDs duplicados y genera `openalex_silver.csv.gz`. |
| `validate` | Control de Calidad | Utiliza la regla `NONE_FAILED_MIN_ONE_SUCCESS`. Verifica que el dataset tenga todas las columnas de `COLUMNAS_PLATA`, al menos 5.000 filas, que no existan IDs duplicados y que `cited_by_count` no contenga valores nulos. |
| `save` | Publicación | Genera el entregable final con fecha lógica (`openalex_YYYY-MM-DD.csv.gz`) en `OUTPUT_DIR` y actualiza `ULTIMO_OK` en `FROZEN_DIR` cuando los datos provienen de una ejecución exitosa contra la fuente. |

### Flujo general

```text
                    ┌─────────────────────┐
                    │  wait_for_source    │
                    │     @task.sensor     │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │    check_source     │
                    │     @task.branch    │
                    └─────────┬─┬─────────┘
                              │ │
                 API OK ──────┘ └──── API caída
                              │
                              ▼
                 ┌────────────┐   ┌──────────────┐
                 │land_bronze │   │ load_frozen  │
                 │   Bronce   │   │  Respaldo    │
                 └─────┬──────┘   └──────┬───────┘
                       │                 │
                       └────────┬────────┘
                                ▼
                     ┌─────────────────────┐
                     │   refine_silver     │
                     │       Plata         │
                     └──────────┬──────────┘
                                ▼
                     ┌─────────────────────┐
                     │      validate       │
                     │   Calidad de datos  │
                     └──────────┬──────────┘
                                ▼
                     ┌─────────────────────┐
                     │        save         │
                     │     Publicación     │
                     └─────────────────────┘
```

---

## 4. Capa Bronce

La Capa Bronce conserva los datos prácticamente en el estado en que fueron obtenidos desde OpenAlex.

### Características

- Datos crudos provenientes de la API.
- Formato JSONL (json lines).
- Compresión mediante `gzip`.
- Archivos con extensión `.json.gz`.
- Almacenamiento local.
- Muestreo aleatorio reproducible.
- Operación idempotente.
- No se sobrescriben innecesariamente datos ya descargados.

### Muestreo aleatorio

Si no se indica un orden, OpenAlex devuelve los works **ordenados por `cited_by_count` de mayor a menor**. Descargar "los primeros N" resultados produciría una muestra formada sólo por los papers más citados del año, sin casos de bajo impacto contra los cuales comparar.

Por eso el pipeline pide una **muestra aleatoria** mediante los parámetros `sample` y `seed`:

- La misma semilla devuelve siempre la misma muestra, por lo que la descarga es reproducible.
- `sample` admite como máximo **10.000** works y sólo funciona con paginación por página (`page`), no con cursor.
- Para superar ese límite se piden varios lotes con semillas consecutivas (`semilla`, `semilla + 1`, ...). Dos lotes pueden compartir algún work; la Capa Plata elimina los duplicados.

### Compresión

Los JSON crudos se almacenan comprimidos:

```text
.json → .json.gz
```

La compresión permite reducir aproximadamente un **80 % del espacio utilizado**, dependiendo del contenido.

---

## 5. Capa Plata

La Capa Plata transforma los datos crudos de Bronce en un dataset estructurado y preparado para las etapas posteriores de análisis.

Una característica importante es que:

> **La Capa Plata no realiza peticiones a Internet.**

Toda la información utilizada proviene de los archivos almacenados en Bronce o del respaldo congelado.

### Transformaciones principales

1. Lectura del archivo `.json.gz`, línea por línea.
2. Eliminación de IDs duplicados.
3. Cálculo de las **columnas derivadas** (`COLUMNAS_PLATA`, ver sección 1), que van primero en el CSV.
4. **Aplanado de todos los campos de la API** (`aplanar`):
   - Objetos anidados → una columna por hoja, con `_` como separador (`primary_location.source.type` → `primary_location_source_type`).
   - Listas (`authorships`, `topics`, `locations`, `referenced_works`, ...) → texto JSON en la celda, más una columna `<lista>_count` para las de primer nivel.
   - Si una columna aplanada se llama igual que una derivada, se queda la derivada (ej. `id` acortado, `language` normalizado).
5. Reconstrucción del abstract: `abstract_inverted_index` se reemplaza por `abstract` (texto) y `abstract_length`.
6. Escritura en dos pasadas: la primera junta todas las columnas que aparecen en los datos y la segunda escribe las filas, sin cargar el dataset completo en memoria.

Archivo resultante (~150 columnas, ~33 MB comprimido para 12.000 artículos):

```text
openalex_silver.csv.gz
```

Se lee directamente con `pd.read_csv("openalex_silver.csv.gz")`. Para recuperar una lista: `json.loads(df["authorships"][0])`.

> **Cuidado en el análisis:** el archivo incluye campos calculados a partir de las citas (`fwci`, `citation_normalized_percentile_*`, `cited_by_percentile_year_*`, `counts_by_year`). Sirven como target, nunca como predictores. Además, varias columnas derivadas repiten información de la API con un nombre más corto (ej. `is_oa` = `open_access_is_oa`).

---

## 6. Control de Calidad

La tarea `validate` funciona como una barrera de calidad antes de publicar los datos.

Utiliza la regla:

```text
NONE_FAILED_MIN_ONE_SUCCESS
```

### Validaciones

El pipeline comprueba que:

- El archivo tenga **todas las columnas** del esquema `COLUMNAS_PLATA`. Si falta alguna (por ejemplo, un respaldo generado por una versión anterior del DAG), falla sin correr el resto de los chequeos.
- El dataset contenga **al menos 5.000 filas** (`FILAS_MINIMAS`).
- No existan IDs duplicados.
- `cited_by_count` no contenga valores nulos.

Además registra una advertencia, sin fallar, si `is_top10_percent` tiene nulos: OpenAlex no calcula el percentil para algunos artículos.

Si alguna de estas condiciones críticas no se cumple, el pipeline no debería considerar válido el dataset generado.

---

## 7. Publicación y Versionado

La tarea `save` publica el dataset final utilizando la fecha lógica de ejecución.

Formato:

```text
openalex_YYYY-MM-DD.csv.gz
```

(Conserva la extensión de origen: si los datos vienen de la semilla `openalex_snapshot.csv`, el entregable es `.csv`.)

El archivo se almacena en:

```text
OUTPUT_DIR
```

Cuando los datos provienen de una ejecución exitosa contra OpenAlex, también se actualiza la copia de respaldo:

```text
FROZEN_DIR/ultimo_ok.csv.gz
```

Este archivo permite recuperar los datos de la última ejecución válida si OpenAlex deja de estar disponible en una ejecución posterior.

---

## 8. Funciones Auxiliares

El DAG utiliza diferentes funciones helper para separar responsabilidades y mantener organizado el código.

### `bronze_path(fecha_str)`

Construye la ruta canónica del archivo correspondiente a una determinada fecha.

```text
bronze_path("2026-09-04")
```

Permite centralizar la estructura de almacenamiento de los archivos Bronce.

### `bronze_write(destino, works)`

Itera la lista de resultados y serializa cada trabajo como un string JSON independiente seguido de un salto de línea, almacenándolo comprimido mediante gzip.

Su objetivo es:

- Persistir los datos crudos.
- Reducir el espacio utilizado.
- Mantener un formato que pueda ser regenerado posteriormente.

### `fetch_page(session, pagina, tamanio, semilla, correo, api_key)`

Aísla la lógica de comunicación con OpenAlex.

Gestiona parámetros como:

- `mailto`
- `page` y `per_page` (200 works por página)
- `sample` y `seed`
- `filter` (constante `FILTRO`: artículos de 2023 con abstract, no retractados)
- API key

Devuelve la lista de resultados de esa página.

---

## 9. Parámetros de Ejecución

El DAG utiliza parámetros para controlar el comportamiento de cada ejecución.

| Parámetro | Descripción | Valor predeterminado |
|---|---|---|
| `filas_objetivo` | Cantidad de registros que se intenta descargar desde OpenAlex. | `12000` |
| `correo_api` | Dirección de correo institucional utilizada para acceder al *Polite Pool* de OpenAlex. | Configurado en la ejecución |
| `semilla` | Seed de la muestra aleatoria. La misma semilla reproduce la misma muestra. | `42` |
| `force` | Fuerza una nueva ingesta ignorando los archivos Bronce existentes. | `False` |

### `filas_objetivo`

Permite modificar la cantidad de trabajos solicitados sin cambiar el código del DAG.

Ejemplo:

```text
filas_objetivo = 12000
```

### `force`

Cuando `force=False`, el pipeline aprovecha archivos Bronce existentes y evita descargas innecesarias.

Cuando:

```text
force=True
```

se fuerza una nueva ingesta desde OpenAlex.

---

## 10. Idempotencia

El pipeline está diseñado para ser **idempotente**.

Esto significa que ejecutar nuevamente una misma etapa no debería generar datos duplicados ni descargar innecesariamente información que ya se encuentra disponible.

En particular, `land_bronze` comprueba si el archivo correspondiente al día ya existe.

```text
Si existe → reutilizar
Si no existe → descargar
```

El parámetro `force` permite romper este comportamiento deliberadamente cuando se necesita realizar una nueva ingesta.

---

## 11. Resiliencia del Pipeline

La arquitectura permite diferenciar entre un problema temporal de infraestructura y la disponibilidad de datos previamente procesados.

### OpenAlex disponible

```text
OpenAlex
   ↓
land_bronze
   ↓
refine_silver
   ↓
validate
   ↓
save
```

### OpenAlex no disponible, existe ULTIMO_OK

```text
OpenAlex
   ↓
sensor detecta caída
   ↓
load_frozen
   ↓
ULTIMO_OK
   ↓
refine_silver
   ↓
validate
   ↓
save
```

### Primer arranque sin ULTIMO_OK

```text
OpenAlex
   ↓
sensor detecta caída
   ↓
load_frozen
   ↓
SEMILLA
   ↓
refine_silver
   ↓
validate
   ↓
save
```

De esta forma, la disponibilidad de la fuente externa no constituye un punto único de fallo para todo el pipeline.

---

## 12. Tecnologías Utilizadas

- **Python**
- **Apache Airflow**
- **HttpHook**
- **OpenAlex API**
- **JSONL**
- **Gzip**
- **CSV**
- **Modelo Medallón**
- **XCom**
- **Task Sensors**
- **Dynamic Branching**

---

## 13. Resultado Final

El pipeline produce un dataset estructurado que contiene información de trabajos científicos de OpenAlex y sus principales características relacionadas con el impacto académico.

El resultado está preparado para ser utilizado en las siguientes etapas del Proyecto Integrador de Ciencia de Datos, particularmente para el análisis de la relación entre las características de una publicación científica y su cantidad de citas.

La arquitectura garantiza simultáneamente:

- Ingesta desde una fuente externa.
- Persistencia de datos crudos.
- Transformación estructurada.
- Validación de calidad.
- Idempotencia.
- Compresión.
- Recuperación ante indisponibilidad de la API.
- Uso de respaldos congelados.
- Publicación versionada por fecha.

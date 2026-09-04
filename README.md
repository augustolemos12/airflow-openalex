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

| Variable | Descripción |
|---|---|
| `authors_count` | Cantidad de autores |
| `referenced_works_count` | Cantidad de trabajos referenciados |
| `is_oa` | Indica si el artículo es de acceso abierto |
| `publication_year` | Año de publicación |
| `type` | Tipo de publicación |
| `institutions_count` | Cantidad de instituciones asociadas |
| `countries_count` | Cantidad de países asociados |
| `cited_by_count` | Cantidad de citas recibidas (variable target) |

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
| `land_bronze` | Capa Bronce | Realiza las peticiones HTTP masivas a OpenAlex. Utiliza paginación mediante cursor para superar el límite de 10.000 filas y almacena los JSON crudos comprimidos en `.json.gz`. Implementa idempotencia evitando volver a descargar el archivo del día si ya existe. |
| `load_frozen` | Respaldo | Carga el respaldo congelado cuando la API no está disponible, permitiendo que el pipeline continúe utilizando datos de contingencia. |
| `refine_silver` | Capa Plata |No realiza peticiones de red. Lee y descomprime los archivos Bronce de forma iterativa (línea por línea) para optimizar el uso de memoria RAM, desanida las estructuras JSON, tipa las columnas, elimina IDs duplicados y genera `openalex_silver.csv`. |
| `validate` | Control de Calidad | Utiliza la regla `NONE_FAILED_MIN_ONE_SUCCESS`. Verifica que el dataset tenga más de 1.000 filas, que no existan IDs duplicados y que `cited_by_count` no contenga valores nulos. |
| `save` | Publicación | Genera el entregable final con fecha lógica (`openalex_YYYY-MM-DD.csv`) en `OUTPUT_DIR` y actualiza `ULTIMO_OK` en `FROZEN_DIR` cuando los datos provienen de una ejecución exitosa contra la fuente. |

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
- Paginación mediante cursor.
- Operación idempotente.
- No se sobrescriben innecesariamente datos ya descargados.

### Paginación por cursor

OpenAlex limita las consultas paginadas, por lo que el pipeline utiliza el mecanismo de **cursor pagination**.

Esto permite continuar solicitando páginas sucesivas hasta alcanzar la cantidad de registros objetivo, evitando depender de una única consulta limitada a 10.000 resultados.

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

1. Lectura del archivo `.json.gz`.
2. Descompresión del contenido.
3. Desanidamiento de estructuras JSON.
4. Selección de variables relevantes.
5. Conversión de tipos.
6. Eliminación de IDs duplicados.
7. Generación del dataset estructurado.

Archivo resultante:

```text
openalex_silver.csv
```

---

## 6. Control de Calidad

La tarea `validate` funciona como una barrera de calidad antes de publicar los datos.

Utiliza la regla:

```text
NONE_FAILED_MIN_ONE_SUCCESS
```

### Validaciones

El pipeline comprueba que:

- El dataset contenga **más de 1.000 filas**.
- No existan IDs duplicados.
- `cited_by_count` no contenga valores nulos.

Si alguna de estas condiciones críticas no se cumple, el pipeline no debería considerar válido el dataset generado.

---

## 7. Publicación y Versionado

La tarea `save` publica el dataset final utilizando la fecha lógica de ejecución.

Formato:

```text
openalex_YYYY-MM-DD.csv
```

El archivo se almacena en:

```text
OUTPUT_DIR
```

Cuando los datos provienen de una ejecución exitosa contra OpenAlex, también se actualiza la copia de respaldo:

```text
FROZEN_DIR/ULTIMO_OK
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

### `fetch_page(session, cursor, correo, api_key)`

Aísla la lógica de comunicación con OpenAlex.

Gestiona parámetros como:

- `mailto`
- `cursor`
- `filter`
- API key

Devuelve:

- La lista de resultados obtenidos.
- El `next_cursor` necesario para solicitar la siguiente página.

---

## 9. Parámetros de Ejecución

El DAG utiliza parámetros para controlar el comportamiento de cada ejecución.

| Parámetro | Descripción | Valor predeterminado |
|---|---|---|
| `filas_objetivo` | Cantidad de registros que se intenta descargar desde OpenAlex. | `12000` |
| `correo_api` | Dirección de correo institucional utilizada para acceder al *Polite Pool* de OpenAlex. | Configurado en la ejecución |
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

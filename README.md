# Proyecto Integrador — Ciencia de Datos

Proyecto integrador de la materia **Ciencia de Datos**, centrado en la construcción de un proceso de ingeniería de datos a partir de información científica obtenida desde la API pública de OpenAlex.

## 1. Definición del proyecto

### Fuente de datos

Se utiliza la **API pública de OpenAlex**, específicamente el endpoint `/works`.

### Unidad de análisis

Cada fila del dataset representa **un artículo científico o paper**.

### Volumen objetivo

El objetivo es obtener aproximadamente **12.000 registros**, utilizando paginación mediante cursor para superar la limitación de la paginación convencional.

### Pregunta de investigación

> **¿Qué factores de un artículo científico (cantidad de autores, referencias, temática, formato de publicación, etc.) permiten predecir si será de alto impacto en citas?**

---

## 2. Variables del dataset

### Variable objetivo (Target)

El impacto del artículo científico se mide mediante su cantidad de citas:

- `cited_by_count`: cantidad de citas recibidas por el artículo.
- También puede utilizarse una clasificación binaria:
  - `1` = artículo muy citado.
  - `0` = artículo no muy citado.

### Variables explicativas (Features)

| Variable | Descripción |
|---|---|
| `authors_count` | Cantidad de autores del artículo, obtenida a partir de la longitud de la lista `authorships`. |
| `referenced_works_count` | Cantidad de referencias bibliográficas que contiene el artículo. |
| `is_oa` | Indica si el artículo es de Acceso Abierto (`boolean`). Se obtiene de `open_access.is_oa`. |
| `publication_year` | Año de publicación del artículo. |
| `type` | Tipo de publicación, por ejemplo, `journal article`, `proceedings`, etc. |
| `institutions_count` | Cantidad de instituciones distintas asociadas a los autores. |
| `countries_count` | Cantidad de países firmantes distintos. |
| `grants_count` | Cantidad de agencias u organizaciones que financiaron el artículo. |
| `primary_topic` / `concepts` | Tema principal o disciplina a la que pertenece el artículo. |
| `sustainable_development_goals` | Objetivos de Desarrollo Sostenible (ODS) con los que OpenAlex relaciona el artículo. |

---

## 3. Ingeniería de datos

El proyecto utiliza una arquitectura por capas basada en el **Modelo Medallón (Medallion Architecture)**.

El flujo general es:

```text
OpenAlex API
     │
     ▼
┌──────────────┐
│    Bronze    │
│  Datos crudos│
└──────┬───────┘
       │
       ▼
┌──────────────┐
│    Silver    │
│ Datos limpios│
│  y tabulares │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Validate    │
│ Calidad datos│
└──────┬───────┘
       │
       ▼
┌──────────────┐
│     Save     │
│  Entregable  │
└──────────────┘
```

### Capa Bronze — `land_bronze`

**Propósito:** establecer la conexión directa con la fuente y conservar los datos crudos.

**Mecanismo:**

- Consumo de datos mediante `HttpHook`.
- Paginación mediante cursor.
- Recolección de las respuestas de la API en formato JSON.
- El proceso continúa hasta alcanzar el volumen objetivo.

**Almacenamiento:**

Los datos se almacenan en formato JSON dentro del directorio:

```text
/bronze
```

Se conserva la respuesta original sin transformaciones prematuras.

### Capa Silver — `refine_silver`

**Propósito:** transformar los datos crudos en un formato tabular limpio y optimizado.

**Mecanismo:**

1. Lee el JSON almacenado en Bronze.
2. Evita realizar nuevamente la consulta a la API.
3. Desanida las estructuras internas del JSON.
4. Extrae las variables explicativas y la variable objetivo `cited_by_count`.
5. Aplica tipado mediante Pandas.
6. Elimina registros duplicados utilizando el identificador único de publicación.

**Almacenamiento:**

El resultado se exporta en formato CSV dentro de:

```text
/silver
```

---

## 4. Paginación mediante cursor

La API de OpenAlex presenta una limitación de **10.000 resultados** para la paginación convencional basada en parámetros como `page` y `per_page`.

Para obtener el volumen necesario se utiliza **cursor paging**.

El proceso funciona de forma iterativa:

1. Se realiza una petición utilizando `cursor=*`.
2. La API devuelve un bloque de resultados y un `next_cursor`.
3. El script toma ese `next_cursor`.
4. Se utiliza el cursor obtenido en la siguiente petición.
5. Se repite el proceso mediante un bucle `while`.
6. El proceso continúa hasta acumular el volumen objetivo de registros.

De esta manera se pueden recolectar los aproximadamente **12.000 artículos** requeridos.

---

## 5. Procesamiento de datos JSON

Las respuestas obtenidas desde OpenAlex contienen estructuras anidadas.

Por este motivo, durante la transformación se realiza un proceso de **parseo y desanidado de los JSON** para convertir la información relevante en variables tabulares.

Entre los datos extraídos se encuentran:

- Información sobre autores.
- Referencias bibliográficas.
- Acceso abierto.
- Año de publicación.
- Tipo de publicación.
- Instituciones.
- Países.
- Financiamiento.
- Temáticas.
- Objetivos de Desarrollo Sostenible.
- Cantidad de citas.

---

## 6. Controles de calidad y gobernanza

Antes de publicar el dataset procesado se ejecuta una etapa de validación denominada `validate`.

Se realizan los siguientes controles:

### Volumen

Se verifica que el dataset procesado supere el umbral mínimo requerido:

```text
>= 1.000 filas
```

### Variable objetivo

Se controla que no existan valores nulos en la variable objetivo relacionada con la cantidad de citas.

### Unicidad

Se confirma la unicidad de los registros eliminando duplicados según el identificador de publicación.

---

## 7. Persistencia del entregable

Una vez que el dataset supera las validaciones, se ejecuta la etapa `save`.

Esta etapa:

1. Copia el archivo validado.
2. Consolida el resultado en el directorio final.
3. Asigna una nomenclatura basada en la fecha lógica de ejecución del DAG (`DagRun`).

El resultado final se almacena en:

```text
/output
```

---

## 8. Estructura de almacenamiento

La organización conceptual de las capas es:

```text
/
├── bronze/
│   └── datos_crudos.json
│
├── silver/
│   └── dataset_limpio.csv
│
└── output/
    └── dataset_validado.csv
```

Los nombres concretos de los archivos pueden variar según la implementación del DAG.

---

## 9. Tecnologías y herramientas

El proyecto utiliza las siguientes tecnologías mencionadas en la implementación:

- **Python**
- **Pandas**
- **Apache Airflow**
- **API de OpenAlex**
- **JSON**
- **CSV**
- **Modelo Medallón (Bronze / Silver)**

---

## 10. Flujo del proyecto

En términos generales, el pipeline sigue el siguiente proceso:

```text
1. Consumo de OpenAlex
          ↓
2. Paginación mediante cursor
          ↓
3. Almacenamiento de JSON crudo
          ↓
4. Desanidado y transformación
          ↓
5. Tipado con Pandas
          ↓
6. Eliminación de duplicados
          ↓
7. Generación del CSV
          ↓
8. Validación de calidad
          ↓
9. Persistencia del dataset final
```

## 11. Objetivo final

El objetivo de esta etapa del proyecto es construir un dataset estructurado y validado que permita posteriormente analizar la relación entre las características de los artículos científicos y su impacto medido mediante citas.

El dataset resultante constituye la base para las siguientes etapas de análisis y modelado del proyecto de Ciencia de Datos.

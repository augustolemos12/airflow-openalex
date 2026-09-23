# Registro de cambios

## v4: muestra aleatoria, solo artículos y esquema completo en Plata

**Base de comparación:** `7e3cd9a` (DAG v3, 2026-09-04)
**Archivos modificados:** `dags/openalex_ingest.py`, `README.md`

### Resumen

| Aspecto | v3 | v4 |
|---|---|---|
| Selección de works | Primeros N resultados (paginación por cursor) | Muestra aleatoria reproducible (`sample` + `seed`) |
| Población | Works 2023 con abstract, de cualquier tipo | **Solo artículos** 2023 con abstract, no retractados |
| Columnas en Plata | 10 | 21 derivadas + todos los campos de la API (~151 en total) |
| Formato de Plata y entregable | `.csv` | `.csv.gz` |
| Mínimo de filas en `validate` | 1.000 | 5.000 |
| Chequeo de esquema en `validate` | No | Sí |

---

### 1. Corrección del sesgo de muestreo

**Problema detectado en v3:** si no se indica `sort`, la API de OpenAlex devuelve los works **ordenados por `cited_by_count` de mayor a menor**. Como el DAG bajaba los primeros 12.000 resultados, el dataset estaba formado solo por los papers más citados de 2023:

| Posición en el resultado | Citas |
|---|---|
| 1 | 79.061 |
| 100 | 1.770 |
| ~10.000 | 164 |

En la población, en cambio, 4,3 de los 7,66 millones de works tienen 0 citas. Con la muestra de v3 no se podía estudiar qué predice el alto impacto, porque no había casos de bajo impacto con los cuales comparar.

**Solución:** se pide una muestra aleatoria con los parámetros `sample` y `seed` de la API.

- `sample` admite como máximo **10.000** works y no funciona con cursor. Por eso se pasó a paginación por página (`page`, 200 works por página).
- Para superar ese límite, se piden varios lotes con semillas consecutivas (`semilla`, `semilla + 1`, …). Si dos lotes traen el mismo work, Plata lo elimina.
- **Parámetro nuevo del DAG:** `semilla` (valor por defecto `42`). La misma semilla reproduce la misma muestra.
- `fetch_page` cambió de firma: `fetch_page(session, cursor, correo, api_key)` → `fetch_page(session, pagina, tamanio, semilla, correo, api_key)`. Ya no devuelve `next_cursor`.

**Resultado:** la mediana de citas pasó de más de 150 a **1**, y el 41% de los artículos tiene 0 citas, lo que coincide con la población.

### 2. Población: solo artículos

El filtro de la API se centralizó en la constante `FILTRO`, que usan tanto el sensor como la descarga:

```text
publication_year:2023,has_abstract:true,type:article,is_retracted:false
```

- `type:article` deja afuera `conference-paper`, `preprint`, `review`, `book-chapter`, `dataset` y otros tipos. En 2023, los artículos son el 59% de los works con abstract.
- `is_retracted:false` excluye los artículos retractados.
- Además, filtrar por artículos **redujo mucho los datos faltantes**:

| Columna | Faltantes (todos los tipos) | Faltantes (solo artículos) |
|---|---|---|
| `is_top10_percent` | 26% | 0,3% |
| `source_type`, `source_is_core`, `is_in_doaj` | 17% | 0,5% |
| `domain`, `field` | 2% | 0,3% |

> Computer Science queda subrepresentada, porque en esa disciplina se publica mucho en conferencias (`conference-paper`). Para incluir otros tipos basta con modificar `FILTRO`.

### 3. Esquema de la capa Plata

#### 3.1 Columnas derivadas (`COLUMNAS_PLATA`)

Van primero en el CSV y son las pensadas para el análisis. `validate` exige que estén todas.

| Estado | Columnas |
|---|---|
| Se mantienen de v3 | `id`, `title`, `cited_by_count`, `is_oa`, `authors_count`, `referenced_works_count`, `institutions_count`, `countries_count` |
| **Nuevas** | `is_top10_percent`, `domain`, `field`, `oa_status`, `apc_paid`, `source_type`, `source_is_core`, `is_in_doaj`, `funders_count`, `language`, `publication_month`, `abstract_length`, `abstract` |
| **Eliminadas** | `publication_year` y `type` (con el filtro valen lo mismo en todas las filas: 2023 y `article`). Sus valores siguen disponibles entre las columnas aplanadas de la API. |

Detalles:
- `is_top10_percent` es un segundo target: indica si el artículo está en el 10% más citado **de su campo y año**, según `citation_normalized_percentile`.
- `language` se normaliza a ISO 639-1 en minúsculas, porque la API mezcla `en`, `EN` y `ENG`.
- `abstract` es el texto reconstruido a partir de `abstract_inverted_index`.

#### 3.2 Todos los campos de la API

Además de las columnas derivadas, Plata incluye **todos los campos del work** que devuelve la API (función `aplanar`):

- Objetos anidados → una columna por hoja, con `_` como separador. Ejemplo: `primary_location.source.is_core` → `primary_location_source_is_core`.
- Listas → texto JSON en la celda. Las de primer nivel también generan `<lista>_count`, por ejemplo `authorships_count` o `topics_count`. Para recuperar una lista: `json.loads(df["authorships"][0])`.
- `abstract_inverted_index` no se aplana, porque generaría una columna por palabra. Se reemplaza por `abstract` y `abstract_length`.
- Si una columna aplanada se llama igual que una derivada, se queda la derivada.

Como no todos los works traen los mismos campos, `refine_silver` recorre el Bronce dos veces: la primera junta todas las columnas y la segunda escribe las filas. Así el uso de memoria sigue siendo bajo.

> ⚠️ **Fuga de datos:** Plata ahora incluye `fwci`, `citation_normalized_percentile_*`, `cited_by_percentile_year_*` y `counts_by_year`, que se calculan a partir de las citas. Pueden usarse como target, **nunca como predictores**.

#### 3.3 Nuevas funciones helper

| Función | Qué hace |
|---|---|
| `aplanar(obj, prefijo)` | Aplana un work en columnas de CSV |
| `reconstruir_abstract(indice)` | Convierte el índice invertido en texto |
| `fila_plata(work, work_id)` | Arma la fila completa: columnas derivadas + campos de la API aplanados |

### 4. Validación (`validate`)

- **Chequeo de esquema nuevo:** falla si falta alguna columna de `COLUMNAS_PLATA`. Evita publicar, por ejemplo, un respaldo generado por una versión anterior del DAG.
- **Mínimo de filas:** pasó de 1.000 a **5.000** (constante `FILAS_MINIMAS`), según el criterio de la materia.
- **Aviso nuevo, sin fallar:** si `is_top10_percent` tiene nulos, lo registra en el log.
- Para no cargar las ~151 columnas en memoria, lee primero solo el encabezado y después únicamente las columnas que controla.

### 5. Archivos de salida y respaldo

| Archivo | v3 | v4 |
|---|---|---|
| Plata | `silver/openalex_silver.csv` | `silver/openalex_silver.csv.gz` |
| Entregable | `openalex_YYYY-MM-DD.csv` | `openalex_YYYY-MM-DD.csv.gz` (si los datos vienen de la semilla, se mantiene `.csv`) |
| Respaldo `ULTIMO_OK` | `frozen/ultimo_ok.csv` | `frozen/ultimo_ok.csv.gz` |

Los archivos se comprimen porque sin comprimir pesan ~200 MB para 12.000 artículos, y GitHub rechaza archivos de más de 100 MB. Comprimidos pesan ~33 MB. `pd.read_csv` los lee directamente.

`load_frozen` ahora informa en el log el tamaño del respaldo en MB en lugar de la cantidad de filas: el archivo puede estar comprimido y las celdas pueden contener saltos de línea.

### 6. ⚠️ Cambios incompatibles con v3 y pendientes

1. **El respaldo actual no es compatible.** `include/frozen/ultimo_ok.csv` tiene el esquema de v3 (10 columnas) y el DAG ahora busca `ultimo_ok.csv.gz`. Hasta la primera corrida exitosa de v4, si la API está caída el DAG cae a la semilla y `validate` falla.
2. **La semilla `include/frozen/openalex_snapshot.csv` está vacía**: tiene solo el encabezado. En un clon nuevo del repo, el respaldo de último recurso no funciona.
3. **Un Bronce del mismo día ya descargado con v3 se reutiliza.** En la primera corrida de v4 hay que usar `force=True`.

### 7. Verificación

Se probó fuera de Airflow, con la API real, porque Docker no estaba disponible:

- Descarga completa: 12.000 artículos en ~3,5 minutos, en dos lotes (10.000 con seed 42 y 2.000 con seed 43). Todos de tipo `article` y ninguno retractado.
- Plata: 11.996 filas (4 duplicados entre lotes eliminados) × 151 columnas, 33 MB, ~15 s.
- `validate` y `save` corren sin errores y generan el entregable y `ultimo_ok.csv.gz`.

**Falta:** una corrida completa del DAG dentro de Airflow (`astro dev start`).

# Diccionario de columnas del dataset de OpenAlex

Columnas del archivo que produce el DAG openalex_ingest con valor para el análisis exploratorio. Se omiten las columnas constantes, las duplicadas, las URL e identificadores secundarios y los metadatos internos de OpenAlex. "Nulos %" es el porcentaje de celdas vacías sobre 11.813 filas; una lista vacía no cuenta como nulo.

## Identificación y tiempo

| Columna | Tipo | Nulos % | Descripción |
|---|---|---|---|
| id | texto | 0 | ID corto de OpenAlex (por ejemplo, W3045514387). Clave primaria. |
| title | texto | 0 | Título del artículo. Útil para análisis de texto; no es predictor directo. |
| publication_date | texto | 0 | Fecha de publicación (AAAA-MM-DD). El 19,7 % de las filas tiene 2019-01-01, que suele indicar que sólo se conocía el año. |
| publication_month | entero | 0 | Mes de publicación (1 a 12). Controla el tiempo de exposición a citas. El mes 1 está inflado por las fechas imputadas. |

## Variables objetivo e impacto

Todas, salvo los dos targets, se calculan a partir de las citas: sirven para describir el impacto, nunca como predictores.

| Columna | Tipo | Nulos % | Descripción |
|---|---|---|---|
| cited_by_count | entero | 0 | **Target.** Citas acumuladas hasta la descarga (octubre de 2026). Muy asimétrica: mediana 10, media 26,61, máximo 1.722. |
| is_top10_percent | booleano | 0 | **Target.** Si el artículo está en el 10 % más citado de su campo y año. True en el 10,5 % de las filas. |
| fwci | decimal | 0 | *Field-Weighted Citation Impact*: citas del artículo divididas por las esperadas para su campo, tipo y año (1 = promedio). |
| citation_normalized_percentile_value | decimal | 0 | Percentil de citas normalizado por campo y año (0 a 1). |
| counts_by_year | JSON | 0 | Citas recibidas por año. Permite estudiar la trayectoria de citas en el tiempo. |

## Autoría y colaboración

| Columna | Tipo | Nulos % | Descripción |
|---|---|---|---|
| authors_count | entero | 0 | Cantidad de autores. 120 artículos figuran con 0. |
| institutions_count | entero | 0 | Cantidad de instituciones distintas entre todos los autores. |
| countries_count | entero | 0 | Cantidad de países distintos. Un valor mayor a 1 indica colaboración internacional. |
| first_author_country | categórica | 16,7 | País de la primera afiliación del primer autor (código ISO de dos letras). |
| first_author_pre_works | entero | 8,5 | Trabajos del primer autor de 2012 a 2018. |
| first_author_pre_cites | entero | 8,5 | Citas del primer autor de 2012 a 2018. |
| last_author_pre_works | entero | 14,9 | Trabajos del último autor (suele ser el jefe del grupo) de 2012 a 2018. Vacío si hay un solo autor. |
| last_author_pre_cites | entero | 14,9 | Citas del último autor de 2012 a 2018. |
| institution_max_h_index | entero | 15,2 | h-index de la institución con mayor h-index entre las del artículo (valor actual). |
| authorships | JSON | 0 | Detalle de cada autoría: autor, posición, instituciones, países y autor de correspondencia. Base para construir variables nuevas. |

Las métricas "pre" suman sólo años anteriores a 2019 para no incluir citas posteriores al artículo. OpenAlex guarda esa historia desde 2012.

## Revista y editorial

| Columna | Tipo | Nulos % | Descripción |
|---|---|---|---|
| primary_location_source_display_name | categórica | 2,1 | Nombre de la revista o repositorio (2.416 distintos). |
| primary_location_source_host_organization_name | categórica | 12,1 | Editorial (American Chemical Society, Wiley, Royal Society of Chemistry, MDPI, ...). |
| source_type | categórica | 2,1 | Tipo de fuente: journal, repository o book series. |
| source_is_core | booleano | 2,1 | Si la fuente pertenece al conjunto *core* de OpenAlex (revistas de calidad verificada). |
| is_in_doaj | booleano | 2,1 | Si la fuente está indexada en el Directory of Open Access Journals. |
| primary_location_source_is_oa | booleano | 2,1 | Si la revista es completamente de acceso abierto. |
| source_pre_works | entero | 2,1 | Artículos publicados por la revista de 2012 a 2018. |
| source_pre_cites | entero | 2,1 | Citas recibidas por la revista de 2012 a 2018. |
| source_pre_cites_per_work | decimal | 3,1 | Cociente de las dos anteriores: prestigio previo de la revista. |
| primary_location_version | categórica | 4,5 | Versión de referencia: publishedVersion, acceptedVersion o submittedVersion. |
| indexed_in_count | entero | 0 | Cantidad de bases que indexan el artículo (Crossref, PubMed, DOAJ, DataCite). |

## Acceso abierto y costos de publicación

| Columna | Tipo | Nulos % | Descripción |
|---|---|---|---|
| is_oa | booleano | 0 | Si existe alguna versión de acceso abierto. |
| oa_status | categórica | 0 | Tipo de acceso: closed, gold, green, diamond, bronze o hybrid. Más informativa que is_oa. |
| primary_location_license | categórica | 68,3 | Licencia de la versión principal (cc-by, cc-by-nc-nd, ...). |
| locations_count | entero | 0 | Cantidad de copias del artículo (editorial, repositorios, preprints). |
| apc_paid | booleano | 0 | Si OpenAlex registra un cargo por publicación (APC) pagado. |
| apc_paid_value_usd | entero | 79,2 | Monto del APC pagado, en dólares. |
| source_apc_usd | entero | 48,9 | Precio actual de publicar en la revista, en dólares, según el endpoint /sources. |
| apc_list_value_usd | entero | 48,8 | Precio de lista del APC asociado al artículo, en dólares. Difiere de source_apc_usd en la mayoría de las filas con ambos datos. |

## Temática y contenido

| Columna | Tipo | Nulos % | Descripción |
|---|---|---|---|
| primary_topic_subfield_display_name | categórica | 0 | Subcampo del tema principal (8 valores; Materials Chemistry concentra 6.328 filas). |
| primary_topic_display_name | categórica | 0 | Tema principal (123 valores distintos). |
| primary_topic_score | decimal | 0 | Confianza de la asignación del tema (0 a 1). |
| keywords | JSON | 0 | Palabras clave asignadas por OpenAlex, con su score. |
| abstract | texto | 0 | Abstract reconstruido como texto. Incluye 162 abstracts genéricos de fichas del Materials Project y 24 "Abstract not provided.". |
| abstract_length | entero | 0 | Cantidad de palabras del abstract. |
| language | categórica | 0,2 | Idioma detectado, en código ISO 639-1 (en para 10.905 filas). Las 118 filas en lv son títulos en inglés mal detectados. |
| referenced_works_count | entero | 0 | Cantidad de trabajos citados por el artículo. 2.239 filas figuran con 0, en general por metadatos incompletos. |

## Financiamiento

| Columna | Tipo | Nulos % | Descripción |
|---|---|---|---|
| funders_count | entero | 0 | Cantidad de organismos financiadores. |
| awards_count | entero | 0 | Cantidad de subsidios específicos declarados. |

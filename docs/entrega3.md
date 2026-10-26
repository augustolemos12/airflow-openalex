# Propuesta Entrega 3: Modelado

**Pregunta:** ¿qué factores de un artículo científico permiten predecir si será de alto impacto en citas?

**Qué predecimos:** si un artículo de Ciencia de Materiales de 2019 va a estar en el **10 % más citado** de su campo y año (`is_top10_percent`, sí o no). Es un problema de **clasificación**.

---

## 1. Las reglas, antes de entrenar nada

### Métrica: f1 macro

Sólo el 10,5 % de los artículos son "top". Un modelo que diga siempre "no" tendría 90 % de accuracy sin servir para nada, así que la accuracy no sirve.

El **f1** combina dos preguntas sobre una clase:

- **Precisión:** de los que el modelo marcó como "top", ¿cuántos lo eran de verdad?
- **Recall:** de los que de verdad eran "top", ¿cuántos encontró?

El **f1 macro** calcula el f1 de cada clase ("top" y "no top") y los promedia en partes iguales, así la clase chica pesa lo mismo que la grande.

**Qué error cuesta más:** no detectar un artículo que iba a tener mucho impacto (falso negativo). Por eso vamos a probar mover el umbral de decisión.

Como métrica secundaria reportamos *average precision* (área bajo la curva precisión-recall).

### El piso

Un modelo que no mira ninguna columna: `DummyClassifier(strategy="most_frequent")`, que responde siempre "no". Lo medimos con la misma validación que el resto. Debería dar un **f1 macro de alrededor de 0,47**, y ése es el número que hay que superar.

### La validación

Los artículos **no son independientes**: muchos comparten revista, y la revista define columnas como su prestigio previo. Si un artículo de una revista queda en entrenamiento y otro de la misma revista en validación, el número sale inflado.

| Qué | Cómo |
|---|---|
| Test | 20 % separado al principio, **agrupado por revista**. No se mira hasta el final. |
| Validación cruzada | `StratifiedGroupKFold(n_splits=5)` con `groups=revista`, sobre el 80 % restante |
| Control | La misma validación con `StratifiedKFold` al azar, para mostrar cuánto infla el número no agrupar |

No usamos `TimeSeriesSplit` porque todos los artículos son de 2019.

---

## 2. Las columnas (todo dentro de un pipeline)

| Tipo | Columnas | Tratamiento |
|---|---|---|
| Conteos | `authors_count`, `institutions_count`, `countries_count`, `referenced_works_count`, `funders_count`, `abstract_length` | logaritmo (`log1p`) + escalado |
| Historia previa a 2019 | `source_pre_cites_per_work`, `source_pre_works`, `first_author_pre_works`, `first_author_pre_cites`, `last_author_pre_works`, `last_author_pre_cites`, `institution_max_h_index` | imputar con la mediana marcando el faltante + logaritmo + escalado |
| Categóricas | `oa_status`, `source_type`, `primary_topic_subfield_display_name`, `first_author_country` | one-hot, agrupando las categorías raras |
| Booleanas | `source_is_core`, `is_in_doaj`, `apc_paid` | sin cambios |
| Control | `publication_month` | numérica |

**Limpieza:**

- Los `authors_count = 0` y `referenced_works_count = 0` son metadatos faltantes, no ceros reales: se pasan a vacío **dentro** del pipeline.
- Afuera del pipeline sólo van filtros de población decididos de antemano: sacar las 162 fichas del Materials Project y los "Abstract not provided.".

**Quedan afuera por fuga** (se calculan con las citas): `cited_by_count`, `fwci`, `citation_normalized_percentile_value` y `counts_by_year`.

---

## 3. Comparar modelos

Todos con las mismas columnas, la misma validación, y la media y el desvío de los 5 pliegues:

| Modelo | Rol |
|---|---|
| Regresión logística | La referencia simple |
| Árbol de decisión | Familia 2 |
| Random Forest | Familia 3 |
| HistGradientBoosting | Familia 4 |

**Regla:** si un modelo complejo no le gana al simple por más que el desvío, nos quedamos con el simple.

**Ajuste del elegido:** una grilla de hiperparámetros, mostrando la tabla completa y no sólo el ganador. Por ejemplo `learning_rate`, `max_leaf_nodes` y `min_samples_leaf` si gana el boosting, o `C` si gana la logística.

**Clases desparejas:** comparamos tres opciones y anotamos cuánto mueve cada una el f1 macro:

1. Nada
2. `class_weight="balanced"`
3. Mover el umbral (`TunedThresholdClassifierCV`)

**Columnas que probamos y no sirvieron:** medimos el f1 con y sin cada una. Las sospechosas de fuga son:

| Columna | Por qué sospechamos |
|---|---|
| `locations_count`, `indexed_in_count` | Las copias en repositorios se agregan después de publicar, y un artículo muy citado acumula más |
| `oa_status` | El acceso "green" puede aparecer años después. Es el estado de 2026, no el de 2019 |
| `institution_max_h_index` | Es el valor actual, no el de 2019 |
| Historia de autores (bloque) | Para ver cuánto aporta |

---

## 4. Leer el modelo final

1. **Medirlo una sola vez sobre el test.** Todo lo anterior (modelo, hiperparámetros, columnas, umbral) se elige con validación cruzada.
2. **Dónde se equivoca:**
   - matriz de confusión normalizada por fila y por columna;
   - reporte por clase;
   - un error concreto explicado desde el dominio, por ejemplo artículos en revistas de bajo prestigio que igual llegaron al top.
3. **En qué se apoya:** importancia por permutación. Esperamos que la columna que más pese sea `source_pre_cites_per_work`, el prestigio de la revista. **No es fuga** por dos motivos:
   - se calcula sólo con citas de 2012 a 2018;
   - la validación agrupada por revista impide que el modelo memorice revistas.
4. **Azar contra agrupado:** cuánto sube el número si la validación no agrupa por revista. Esa diferencia es la fuga que evitamos.

---

## 5. Expectativa

Un **f1 macro de alrededor de 0,60–0,65** contra 0,47 del piso. Es modesto pero defendible. Si da 0,95, primero buscamos una fuga.

---

## 6. Reparto sugerido

La evaluación es individual y las preguntas van dirigidas: **todos tienen que poder explicar todo**.

| Persona | Parte |
|---|---|
| A | Métrica, piso y validación (sección 1) |
| B | Columnas, pipeline y fugas (sección 2) |
| C | Comparación de modelos y ajuste (sección 3) |
| D | Test, errores e importancia (sección 4) |

---

## 7. Checklist para el día de la entrega

- [ ] Notebook ejecutado de arriba a abajo, sin errores, con salidas guardadas
- [ ] Semilla definida al principio y pasada a todo lo que la acepte
- [ ] Todo lo que se le hace a los datos, adentro de un pipeline
- [ ] Métrica declarada y piso medido
- [ ] Al menos tres modelos comparados con media y desvío
- [ ] Modelo elegido ajustado, con la tabla completa
- [ ] Test mirado una sola vez
- [ ] Matriz de confusión e importancia por permutación
- [ ] Lista de lo que probamos y no anduvo
- [ ] Correcciones de la Entrega 2 resueltas o explicadas

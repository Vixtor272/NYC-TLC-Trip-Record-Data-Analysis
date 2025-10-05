# NYC-TLC-Trip-Record-Data-Analysis
This is an attempt to recover data from NYC TLC Trip Record Data using Mage to Ingest, Transform and Export this data to a lakehouse (Snowflake) in order to keep this tables on the cloud (roughly 15 gb of data) to then use SQL by connecting vscode to Snowflake and answer 5 simple questions about the data.


# Matriz de cobertura mes a mes — Yellow y Green (2015–2025)

**Contexto**: Cargados todos los meses 2015–2025 (Parquet) de Yellow y Green. Esta tabla muestra la cobertura mes a mes por servicio. ✓ = mes cargado (cover), X = mes no cargado.

**Nota**: Los meses faltantes son únicamente del servicio `YELLOW`, específicamente las fechas `03-2015` y `05-2018`.

## Leyenda

* ✓ : Mes con datos cargados
* X : Mes sin datos

---

## Matriz — Servicio: YELLOW

|  Año | Ene | Feb | Mar | Abr | May | Jun | Jul | Ago | Sep | Oct | Nov | Dic |
| ---: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: |
| 2015 |  ✓  |  ✓  |  X  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2016 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2017 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2018 |  ✓  |  ✓  |  ✓  |  ✓  |  X  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2019 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2020 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2021 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2022 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2023 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2024 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2025 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  X  |  X  |  X  |  X  |

---

## Matriz — Servicio: GREEN

|  Año | Ene | Feb | Mar | Abr | May | Jun | Jul | Ago | Sep | Oct | Nov | Dic |
| ---: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: |
| 2015 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2016 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2017 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2018 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2019 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2020 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2021 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2022 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2023 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2024 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| 2025 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  X  |  X  |  X  |  X  |

---

## Operación — Backfill y orquestación

Mage orquesta backfill mensual con idempotencia (no duplicados si se vuelve a subir un mes) y metadatos por mes.

---

## Arquitectura — Bronze, Silver y Gold

Sí, **Bronze** refleja fielmente el origen, pero en los metadatos también se añadió la columna `service_type`. En **Silver** se unifican los dos servicios (Yellow y Green) y se enriquece con la tabla de referencia **Taxi Zones** para obtener las localizaciones de pickup y dropoff, esto se hace en `models/staging/stg_taxi_union.sql`. En **Gold** se implementa el esquema estrella con la tabla **fct_trips** y sus dimensiones clave, esto se hace en `models/gold/fct_trips.sql` y cada dimensión correctamente nombrada.

---

## Clustering

Sí se aplica **clustering** en la capa Gold (tabla `fct_trips`), aunque no se incluyó evidencia del antes y después (Query Profile o pruning) como referencia documental.

---

## Seguridad — Secrets y permisos

Se utilizan **secrets** para todo el pipeline de ingesta, transformación y exportación. También en todo el proceso de dbt para exportar las tablas silver y gold hacia Snowflake. No se creo un rol con permisos mínimos, todo se hizo con ADMIN.

---

## Calidad — Tests y documentación dbt

En **dbt** se realiza toda la limpieza y validación de datos para la unificación de tablas. Los tests (`not_null`, `unique`, `accepted_values`, `relationships`) se ejecutan correctamente, y se generan la documentación y el **lineage** para el proyecto de dbt, específicamente en staging (fase silver) se hacen estos tests.

---

## Análisis — Preguntas de negocio

Sí se responden las 5 preguntas de negocio desde la capa **Gold**, en el notebook `data_analysis.ipynb`, utilizando las tablas finales generadas.

---

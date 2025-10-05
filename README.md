# NYC-TLC-Trip-Record-Data-Analysis
This is an attempt to recover data from NYC TLC Trip Record Data using Mage to Ingest, Transform and Export this data to a lakehouse (Snowflake) in order to keep this tables on the cloud (roughly 15 gb of data) to then use SQL by connecting vscode to Snowflake and answer 5 simple questions about the data.


# Matriz de cobertura mes a mes — Yellow y Green (2015–2025)

**Contexto**: Cargados todos los meses 2015–2025 (Parquet) de Yellow y Green. Esta tabla muestra la cobertura mes a mes por servicio. ✓ = mes cargado (cover), X = mes no cargado.

## Leyenda

* ✓ : Mes con datos cargados
* X : Mes sin datos

---

## Matriz — Servicio: YELLOW

|  Año | Ene | Feb | Mar | Abr | May | Jun | Jul | Ago | Sep | Oct | Nov | Dic |
| ---: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: |
| 2015 |  ✓  |  ✓  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |
| 2016 |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |
| 2017 |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |
| 2018 |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |
| 2019 |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |
| 2020 |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |
| 2021 |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |
| 2022 |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |
| 2023 |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |
| 2024 |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |
| 2025 |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |  X  |

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
| 2025 |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |

---


*Documento generado automáticamente según tu indicación: Yellow — solo Ene/Feb 2015 con ✓; resto X. Green — todos los meses de 2015–2025 con ✓.*


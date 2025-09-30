{{ config(materialized='table') }}

with dates as (
  -- usamos fechas presentes en la tabla silver (pickup y dropoff) para cubrir rango real
  select distinct date_trunc('day', PICKUP_DATETIME) as the_date
  from "NYC_TLC"."SILVER"."STG_TAXI_UNION"
  where PICKUP_DATETIME is not null

  union

  select distinct date_trunc('day', DROPOFF_DATETIME) as the_date
  from "NYC_TLC"."SILVER"."STG_TAXI_UNION"
  where DROPOFF_DATETIME is not null
)

select
  cast(to_char(the_date,'YYYYMMDD') as integer) as date_sk,
  the_date as full_date,
  date_part(year, the_date) as year,
  date_part(month, the_date) as month,
  date_part(day, the_date) as day,
  dayofweek(the_date) as day_of_week,
  case when dayofweek(the_date) in (6,7) then true else false end as is_weekend
from dates
order by the_date

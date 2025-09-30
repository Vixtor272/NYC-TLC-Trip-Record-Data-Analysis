{{ config(materialized='table') }}

with times as (
  select distinct date_part(hour, PICKUP_DATETIME) as hour,
                  date_part(minute, PICKUP_DATETIME) as minute
  from "NYC_TLC"."SILVER"."STG_TAXI_UNION"
  union
  select distinct date_part(hour, DROPOFF_DATETIME) as hour,
                  date_part(minute, DROPOFF_DATETIME) as minute
  from "NYC_TLC"."SILVER"."STG_TAXI_UNION"
)

select
  (hour*100 + minute) as time_sk, -- HHMM
  hour,
  minute,
  case when hour between 5 and 11 then 'morning'
       when hour between 12 and 17 then 'afternoon'
       when hour between 18 and 21 then 'evening'
       else 'night'
  end as day_period
from times
order by hour, minute

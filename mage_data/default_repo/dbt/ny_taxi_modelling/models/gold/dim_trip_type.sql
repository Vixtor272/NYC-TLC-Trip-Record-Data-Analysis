{{ config(materialized='table') }}

select
  md5(coalesce(to_varchar(TRIP_TYPE),'NULL')) as trip_type_sk,
  TRIP_TYPE
from (
  select distinct TRIP_TYPE
  from "NYC_TLC"."SILVER"."STG_TAXI_UNION"
) t

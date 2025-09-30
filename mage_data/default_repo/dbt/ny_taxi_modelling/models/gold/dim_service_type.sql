{{ config(materialized='table') }}

select
  md5(to_varchar(SERVICE_TYPE)) as service_sk,
  SERVICE_TYPE
from (
  select distinct SERVICE_TYPE
  from "NYC_TLC"."SILVER"."STG_TAXI_UNION"
) t

{{ config(materialized='table') }}

select
  md5(to_varchar(vendorid)) as vendor_sk,
  vendorid as vendor_id,
  -- Si tienes una tabla maestra con nombre, joinear aquí; por ahora solo id
  null::varchar as vendor_name
from (
  select distinct vendorid
  from "NYC_TLC"."SILVER"."STG_TAXI_UNION"
) t

{{ config(materialized='table') }}

select
  md5(to_varchar(RATECODEID)) as ratecode_sk,
  RATECODEID as ratecode_id,
  RATECODE_DESC as ratecode_desc
from (
  select distinct RATECODEID, RATECODE_DESC
  from "NYC_TLC"."SILVER"."STG_TAXI_UNION"
) t

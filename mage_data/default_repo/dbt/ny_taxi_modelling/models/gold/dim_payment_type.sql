{{ config(materialized='table') }}

select
  md5(to_varchar(PAYMENT_TYPE)) as payment_sk,
  PAYMENT_TYPE as payment_id,
  PAYMENT_TYPE_DESC as payment_desc
from (
  select distinct PAYMENT_TYPE, PAYMENT_TYPE_DESC
  from "NYC_TLC"."SILVER"."STG_TAXI_UNION"
) t

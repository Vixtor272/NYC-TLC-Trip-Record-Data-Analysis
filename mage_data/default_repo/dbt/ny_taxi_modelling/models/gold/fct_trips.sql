{{ 
  config(
    materialized='incremental',
    unique_key='trip_sk',
    cluster_by=['pickup_date_sk','pu_zone_sk']  
  )
}}

with src as (

  select
    -- Generamos una surrogate key por viaje (determinística)
    lower(md5(
      coalesce(to_varchar(INGEST_RUN_ID),'') || '|' ||
      coalesce(to_varchar(PICKUP_DATETIME),'') || '|' ||
      coalesce(to_varchar(VENDORID),'') || '|' ||
      coalesce(to_varchar(PULOCATIONID),'') || '|' ||
      coalesce(to_varchar(DOLOCATIONID),'') || '|' ||
      coalesce(to_varchar(TO_CHAR(FARE_AMOUNT,'999999999.99')),'')
    )) as trip_sk,

    INGEST_RUN_ID,
    INGEST_TIMESTAMP,
    SERVICE_TYPE,
    YEAR,
    MONTH,
    PICKUP_DATETIME,
    DROPOFF_DATETIME,
    VENDORID,
    PASSENGER_COUNT,
    TRIP_DISTANCE,
    RATECODEID,
    STORE_AND_FWD_FLAG_DESC,
    PULOCATIONID,
    DOLOCATIONID,
    PAYMENT_TYPE,
    PAYMENT_TYPE_DESC,
    FARE_AMOUNT,
    TIP_AMOUNT,
    TOTAL_AMOUNT,
    TRIP_DURATION_MINUTES,
    TRIP_TYPE,
    SIZE,
    SERVICE

  from "NYC_TLC"."SILVER"."STG_TAXI_UNION"

  {% if is_incremental() %}
    -- solo procesamos nuevas filas basadas en ingest_timestamp (heurística)
    where INGEST_TIMESTAMP > (select coalesce(max(INGEST_TIMESTAMP), '2000-01-01') from {{ this }})
  {% endif %}
),

-- join con dimensiones para obtener surrogate keys (coalesce por seguridad)
joined as (
  select
    s.*,
    cast(to_char(date_trunc('day', s.PICKUP_DATETIME),'YYYYMMDD') as integer) as pickup_date_sk,
    cast(to_char(date_trunc('day', s.DROPOFF_DATETIME),'YYYYMMDD') as integer) as dropoff_date_sk,

    -- zone sks: hacemos join con dim_zone por location id
    dzp.zone_sk as pu_zone_sk,
    dzd.zone_sk as do_zone_sk,

    dv.vendor_sk,
    dr.ratecode_sk,
    dp.payment_sk,
    ds.service_sk,
    dtt.trip_type_sk,

    -- time keys
    (date_part('hour', s.PICKUP_DATETIME)*100 + date_part('minute', s.PICKUP_DATETIME)) as pickup_time_sk,
    (date_part('hour', s.DROPOFF_DATETIME)*100 + date_part('minute', s.DROPOFF_DATETIME)) as dropoff_time_sk

  from src s
  left join {{ ref('dim_zone') }} dzp on to_varchar(dzp.location_id) = to_varchar(s.PULOCATIONID)
  left join {{ ref('dim_zone') }} dzd on to_varchar(dzd.location_id) = to_varchar(s.DOLOCATIONID)
  left join {{ ref('dim_vendor') }} dv on dv.vendor_id = s.VENDORID
  left join {{ ref('dim_rate_code') }} dr on dr.ratecode_id = s.RATECODEID
  left join {{ ref('dim_payment_type') }} dp on dp.payment_id = s.PAYMENT_TYPE
  left join {{ ref('dim_service_type') }} ds on ds.SERVICE_TYPE = s.SERVICE_TYPE
  left join {{ ref('dim_trip_type') }} dtt on dtt.TRIP_TYPE = s.TRIP_TYPE
)

select
  trip_sk,
  INGEST_RUN_ID,
  INGEST_TIMESTAMP,
  SERVICE_TYPE,
  YEAR,
  MONTH,
  pickup_date_sk,
  dropoff_date_sk,
  pickup_time_sk,
  dropoff_time_sk,
  pu_zone_sk,
  do_zone_sk,
  vendor_sk,
  ratecode_sk,
  payment_sk,
  service_sk,
  trip_type_sk,
  PASSENGER_COUNT,
  TRIP_DISTANCE,
  FARE_AMOUNT,
  TIP_AMOUNT,
  TOTAL_AMOUNT,
  TRIP_DURATION_MINUTES,
  SIZE,
  SERVICE

from joined

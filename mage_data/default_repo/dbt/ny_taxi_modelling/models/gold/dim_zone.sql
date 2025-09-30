{{ config(materialized='table') }}

-- Conformamos zonas a partir de la tabla de zonas (fuente BRONZE/TAXI_ZONES)
select
  md5(to_varchar(locationid)) as zone_sk,
  locationid as location_id,
  zone,
  borough,
  service_zone
from "NYC_TLC"."BRONZE"."TAXI_ZONES"

-- models/silver/silver_taxi_trips.sql
{{ config(
    materialized='table',
    schema='silver',
) }}

with unioned_trips as (
    -- Tu unión actual de green y yellow
    with green as (
      select
        INGEST_RUN_ID,
        INGEST_TIMESTAMP,
        YEAR,
        MONTH,
        VENDORID,
        PASSENGER_COUNT,
        TRIP_DISTANCE,
        PULOCATIONID,
        DOLOCATIONID,
        RATECODEID,
        STORE_AND_FWD_FLAG,
        PAYMENT_TYPE,
        FARE_AMOUNT,
        TIP_AMOUNT,
        TOTAL_AMOUNT,
        EXTRA,
        MTA_TAX,
        TOLLS_AMOUNT,
        IMPROVEMENT_SURCHARGE,
        CONGESTION_SURCHARGE,
        CBD_CONGESTION_FEE,
        EHAIL_FEE,
        TRIP_TYPE,
        SIZE,
        SERVICE,
        'green' as SERVICE_TYPE,
        LPEP_PICKUP_DATETIME as PICKUP_DATETIME,
        LPEP_DROPOFF_DATETIME as DROPOFF_DATETIME
      from "NYC_TLC"."BRONZE"."GREEN_TRIPS"
    ),

    yellow as (
      select
        INGEST_RUN_ID,
        INGEST_TIMESTAMP,
        YEAR,
        MONTH,
        VENDORID,
        PASSENGER_COUNT,
        TRIP_DISTANCE,
        PULOCATIONID,
        DOLOCATIONID,
        RATECODEID,
        STORE_AND_FWD_FLAG,
        PAYMENT_TYPE,
        FARE_AMOUNT,
        TIP_AMOUNT,
        TOTAL_AMOUNT,
        EXTRA,
        MTA_TAX,
        TOLLS_AMOUNT,
        IMPROVEMENT_SURCHARGE,
        CONGESTION_SURCHARGE,
        CBD_CONGESTION_FEE,
        null as EHAIL_FEE,
        null as TRIP_TYPE,
        SIZE,
        SERVICE,
        'yellow' as SERVICE_TYPE,
        TPEP_PICKUP_DATETIME as PICKUP_DATETIME,
        TPEP_DROPOFF_DATETIME as DROPOFF_DATETIME
      from "NYC_TLC"."BRONZE"."YELLOW_TRIPS"
    )

    select * from green
    union all
    select * from yellow
),

-- Estandarización de zonas horarias y normalización
standardized_trips as (
    select
        *,
        -- Estandarizar timezone a America/New_York
        convert_timezone('UTC', 'America/New_York', PICKUP_DATETIME) as PICKUP_DATETIME_EST,
        convert_timezone('UTC', 'America/New_York', DROPOFF_DATETIME) as DROPOFF_DATETIME_EST,
        
        -- Normalizar payment_type a valores legibles
        case PAYMENT_TYPE
            when 1 then 'Credit card'
            when 2 then 'Cash'
            when 3 then 'No charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            when 6 then 'Voided trip'
            else 'Not specified'
        end as PAYMENT_TYPE_DESC,
        
        -- Normalizar ratecodeid
        case RATECODEID
            when 1 then 'Standard rate'
            when 2 then 'JFK'
            when 3 then 'Newark'
            when 4 then 'Nassau or Westchester'
            when 5 then 'Negotiated fare'
            when 6 then 'Group ride'
            else 'Unknown'
        end as RATECODE_DESC,
        
        -- Normalizar store_and_fwd_flag
        case upper(STORE_AND_FWD_FLAG)
            when 'Y' then 'Yes'
            when 'N' then 'No'
            else 'Unknown'
        end as STORE_AND_FWD_FLAG_DESC,

        -- Reglas de calidad de datos
        case 
            when TRIP_DISTANCE < 0 then 0
            else TRIP_DISTANCE
        end as TRIP_DISTANCE_CLEANED,
        
        case 
            when FARE_AMOUNT < 0 then 0
            else FARE_AMOUNT
        end as FARE_AMOUNT_CLEANED,
        
        case 
            when TIP_AMOUNT < 0 then 0
            else TIP_AMOUNT
        end as TIP_AMOUNT_CLEANED,
        
        -- Validación de fechas
        case 
            when PICKUP_DATETIME > DROPOFF_DATETIME then 1
            else 0
        end as HAS_INVALID_TIMING,
        
        -- Duración del viaje en minutos
        datediff('minute', PICKUP_DATETIME, DROPOFF_DATETIME) as TRIP_DURATION_MINUTES,
        
        -- Validación de datos requeridos
        case 
            when PULOCATIONID is null or DOLOCATIONID is null then 1
            else 0
        end as HAS_MISSING_LOCATION_IDS

    from unioned_trips
),

-- Enriquecer con Taxi Zones
enriched_with_zones as (
    select
        st.*,
        
        -- Información de pickup location
        pz.zone as PICKUP_ZONE,
        pz.borough as PICKUP_BOROUGH,
        pz.service_zone as PICKUP_SERVICE_ZONE,
        
        -- Información de dropoff location  
        dz.zone as DROPOFF_ZONE,
        dz.borough as DROPOFF_BOROUGH,
        dz.service_zone as DROPOFF_SERVICE_ZONE,
        
        -- Flags de calidad basados en zonas
        case 
            when pz.locationid is null then 1
            else 0
        end as HAS_INVALID_PICKUP_ZONE,
        
        case 
            when dz.locationid is null then 1
            else 0
        end as HAS_INVALID_DROPOFF_ZONE

    from standardized_trips st
    left join "NYC_TLC"."BRONZE"."TAXI_ZONES" pz 
        on st.PULOCATIONID = pz.locationid
    left join "NYC_TLC"."BRONZE"."TAXI_ZONES" dz 
        on st.DOLOCATIONID = dz.locationid
),

-- Métricas adicionales y limpieza final
final as (
    select
        -- Identificadores y metadatos
        INGEST_RUN_ID,
        INGEST_TIMESTAMP,
        SERVICE_TYPE,
        
        -- Fechas y tiempos estandarizados
        YEAR,
        MONTH,
        PICKUP_DATETIME_EST as PICKUP_DATETIME,
        DROPOFF_DATETIME_EST as DROPOFF_DATETIME,
        TRIP_DURATION_MINUTES,
        
        -- Datos del viaje
        VENDORID,
        PASSENGER_COUNT,
        TRIP_DISTANCE_CLEANED as TRIP_DISTANCE,
        RATECODEID,
        RATECODE_DESC,
        STORE_AND_FWD_FLAG_DESC,
        
        -- Información de ubicación
        PULOCATIONID,
        PICKUP_ZONE,
        PICKUP_BOROUGH,
        PICKUP_SERVICE_ZONE,
        
        DOLOCATIONID, 
        DROPOFF_ZONE,
        DROPOFF_BOROUGH,
        DROPOFF_SERVICE_ZONE,
        
        -- Información de pago
        PAYMENT_TYPE,
        PAYMENT_TYPE_DESC,
        FARE_AMOUNT_CLEANED as FARE_AMOUNT,
        TIP_AMOUNT_CLEANED as TIP_AMOUNT,
        EXTRA,
        MTA_TAX,
        TOLLS_AMOUNT,
        IMPROVEMENT_SURCHARGE,
        CONGESTION_SURCHARGE,
        CBD_CONGESTION_FEE,
        EHAIL_FEE,
        TOTAL_AMOUNT,
        
        -- Campos específicos de servicio
        TRIP_TYPE,
        SIZE,
        SERVICE,

    from enriched_with_zones
)

select * from final
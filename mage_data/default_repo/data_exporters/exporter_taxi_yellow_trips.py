# export_data_to_snowflake.py
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.data_preparation.shared.secrets import get_secret_value
from mage_ai.io.snowflake import Snowflake
import pandas as pd
import uuid
from datetime import datetime, timezone

@data_exporter
def export_data_to_snowflake(df: pd.DataFrame, *args, **kwargs) -> None:
    # Parámetros de entrada / defaults
    year = int(kwargs.get('year', 2015))
    month = int(kwargs.get('month', 5))
    service = str(kwargs.get('service', 'yellow')).upper()

    # normalizar a MAYÚSCULAS primero
    df.columns = [str(c).upper() for c in df.columns]
    
    # Get secrets using get_secret_value
    snowflake_config = {
        'account': get_secret_value('SNOWFLAKE_ACCOUNT'),
        'user': get_secret_value('SNOWFLAKE_USER'),
        'password': get_secret_value('SNOWFLAKE_PASSWORD'),
        'warehouse': get_secret_value('SNOWFLAKE_WH'),
    }
    
    # Verify all secrets were retrieved
    for key, value in snowflake_config.items():
        if value is None:
            raise ValueError(f"Secret '{key}' not found in Mage secrets")
    
    # Parametrización tabla
    table_name = f"{service}_TRIPS"
    database = "NYC_TLC"
    schema = "BRONZE"
    fecha_extraida = f"{month:02d}-{year}"

    # Export using direct Snowflake connection
    with Snowflake(
        account=snowflake_config['account'],
        user=snowflake_config['user'],
        password=snowflake_config['password'],
        warehouse=snowflake_config['warehouse'],
        database=database,
        schema=schema
    ) as loader:
        result = loader.execute(f"""
            SELECT COUNT(*) 
            FROM {database}.{schema}.{table_name}
            WHERE "YEAR" = {year} AND "MONTH" = {month}
        """)
        if result[0][0] == 0:
            loader.export(
                df,  # Use the modified DataFrame
                table_name,
                database,
                schema,
                if_exists='append',
            )
            print(f"\n Carga realizada: {len(df)} filas insertadas en {database}.{schema}.{table_name} para fecha {fecha_extraida}.")
        else:
            print(f"\n Datos para YEAR={year} y MONTH={month} ya existen. No se realizará la carga.")
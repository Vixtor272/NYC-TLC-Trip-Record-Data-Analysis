import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.data_preparation.shared.secrets import get_secret_value
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


@data_loader
def load_data(*args, **kwargs):
    # URL del CSV (raw)
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    # Leer tal cual
    df = pd.read_csv(url)

    # --- Aquí: poner columnas en MAYÚSCULAS ---
    df.columns = [c.strip().upper() for c in df.columns]

    # Credenciales desde secrets de Mage (crea estos secrets en tu proyecto)
    user = get_secret_value('SNOWFLAKE_USER')
    password = get_secret_value('SNOWFLAKE_PASSWORD')
    account = get_secret_value('SNOWFLAKE_ACCOUNT')
    role = get_secret_value('SNOWFLAKE_ROLE')

    # Conexión mínima a Snowflake (usa la BD/SCHEMA que mencionaste)
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        role=role,
        warehouse='COMPUTE_WH',
        database='NYC_TLC',
        schema='BRONZE',
    )

    # Subir tal cual; write_pandas creará la tabla si no existe
    write_pandas(conn, df, 'TAXI_ZONES', database='NYC_TLC', schema='BRONZE')

    conn.close()
    return df


@test
def test_output(output, *args) -> None:
    # test mínimo obligatorio
    assert output is not None, "Output es None"



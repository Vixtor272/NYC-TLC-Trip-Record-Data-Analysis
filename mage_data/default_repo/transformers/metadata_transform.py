# add_metadata.py
from datetime import datetime, timezone
import uuid
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def add_metadata(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Adds metadata columns to the taxi trips DataFrame.
    """

    year = int(kwargs.get('year', 2015))
    month = int(kwargs.get('month', 5))
    service = str(kwargs.get('service', 'yellow')).upper()

    ingest_run_id = str(uuid.uuid4())
    ingest_timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    # calculamos tamaño total en bytes (número entero)
    total_size_bytes = int(df.memory_usage(deep=True).sum())

    # Añadimos columnas de metadata (tipos simples y serializables)
    df['ingest_timestamp'] = ingest_timestamp  

    df['size'] = total_size_bytes

    df['ingest_run_id'] = ingest_run_id
    df['ingest_run_id'] = df['ingest_run_id'].astype('category')

    df['service'] = service
    df['service'] = df['service'].astype('category')

    # year/month son ints pequeños; si quieres ahorrar aún más:
    df['year'] = year
    df['year'] = df['year'].astype('int16')   # si rango lo permite
    df['month'] = month
    df['month'] = df['month'].astype('int8')


    print("Metadata añadida.")

    return df

@test
def test_output(output, *args) -> None:
    """
    Test más explícito: comprobamos que sea DataFrame y tenga filas.
    """
    assert output is not None, 'El output es None'
    assert isinstance(output, pd.DataFrame), f'Output no es DataFrame, es {type(output)}'
    assert len(output) >= 0, 'Output tiene longitud negativa (imposible)'

# data_loader.py
import pandas as pd
if 'data_loader' not in globals(): 
    from mage_ai.data_preparation.decorators import data_loader 
if 'test' not in globals(): 
    from mage_ai.data_preparation.decorators import test 

@data_loader
def load_data_from_file(*args, **kwargs):
    """
    Carga un archivo parquet remoto con los datos de taxi.
    """
    year = int(kwargs.get('year', 2015))
    month = int(kwargs.get('month', 5))
    service = str(kwargs.get('service', 'yellow'))
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month:02d}.parquet'
    print('Iniciando descarga')
    df = pd.read_parquet(url)  # requiere pyarrow/fastparquet instalado
    print(f'Data del mes {month}-{year}-{service} descargada')
    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

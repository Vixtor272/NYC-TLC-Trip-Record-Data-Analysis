from mage_ai.data_preparation.shared.secrets import get_secret_value
import os
from pathlib import Path
import yaml

# lee secretos
sf_account = get_secret_value('SNOWFLAKE_ACCOUNT')
sf_user = get_secret_value('SNOWFLAKE_USER')
sf_password = get_secret_value('SNOWFLAKE_PASSWORD')
sf_role = get_secret_value('SNOWFLAKE_ROLE')
sf_warehouse = get_secret_value('SNOWFLAKE_WAREHOUSE')
sf_database = get_secret_value('SNOWFLAKE_DATABASE')
sf_schema = get_secret_value('SNOWFLAKE_SCHEMA')

profiles = {
  'ny_scheduler': {
    'outputs': {
      'dev': {
        'type': 'snowflake',
        'account': sf_account,
        'user': sf_user,
        'password': sf_password,
        'role': sf_role,
        'warehouse': sf_warehouse,
        'database': sf_database,
        'schema': sf_schema,
        'threads': 6,
        'client_session_keep_alive': False
      }
    },
    'target': 'dev'
  }
}

# ruta segura para profiles
profiles_dir = Path('/home/src/dbt_profiles')  # ajusta según tu contenedor Mage
profiles_dir.mkdir(parents=True, exist_ok=True)
profiles_file = profiles_dir / 'profiles.yml'

with open(profiles_file, 'w') as f:
    yaml.safe_dump(profiles, f, sort_keys=False)

# exporta variable de entorno para dbt (si necesitas)
os.environ['DBT_PROFILES_DIR'] = str(profiles_dir)

print('profiles.yml escrito en', profiles_file)


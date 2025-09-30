# block: trigger_sequential_controller (Python block in Mage)
from mage_ai.orchestration.triggers.api import trigger_pipeline
from datetime import datetime
import time
import traceback

# CONFIG (ajusta aquí si quieres)
DEST_PIPELINE_UUID = 'd3772e9521df436781187e6d93c2e2ac'   # reemplaza por el UUID del pipeline destino
START_YEAR = 2015
END_YEAR = 2025
END_YEAR_LAST_MONTH = 8  # si END_YEAR tiene que llegar solo hasta agosto (por ejemplo 2025 → 1..8). Pon 12 para todo el año.
SERVICE = 'yellow'       # 'yellow' o 'green'
SLEEP_BETWEEN_TRIGGERS = 10   # segundos entre disparos (después de que termine el run)
POLL_INTERVAL = 10       # segundos entre checks internos de trigger_pipeline
POLL_TIMEOUT = 60 * 30   # timeout para esperar que termine cada run (segundos)

def run_controller(
    dest_pipeline_uuid: str = DEST_PIPELINE_UUID,
    start_year: int = START_YEAR,
    end_year: int = END_YEAR,
    end_year_last_month: int = END_YEAR_LAST_MONTH,
    service: str = SERVICE,
):
    """
    Controlador que dispara el pipeline destino mes-a-mes en el rango especificado.
    Espera a que cada run termine antes de disparar el siguiente (check_status=True).
    """
    if not dest_pipeline_uuid:
        raise ValueError("Define DEST_PIPELINE_UUID con el UUID del pipeline destino.")

    for year in range(start_year, end_year + 1):
        months = range(1, end_year_last_month + 1) if year == end_year else range(1, 13)
        for month in months:
            try:
                print(f"{datetime.now().isoformat()} - Triggering {dest_pipeline_uuid} for {year}-{month:02d} service={service}")
                # trigger_pipeline espera y retorna info del run cuando check_status=True
                info = trigger_pipeline(
                    dest_pipeline_uuid,
                    variables={'year': year, 'month': month, 'service': service},
                    check_status=True,         # esperamos a que termine antes de seguir
                    error_on_failure=True,     # lanza excepción si el run termina en failed
                    poll_interval=POLL_INTERVAL,
                    poll_timeout=POLL_TIMEOUT,
                    verbose=True,
                )
                # info suele contener detalles del pipeline_run; imprime para debug
                print("Trigger result:", info)
            except Exception as e:
                # No detenemos todo: registramos y seguimos con el siguiente mes
                print(f"[ERROR] Al disparar {year}-{month:02d}: {e}")
                traceback.print_exc()
            finally:
                # Pausa corta para no sobrecargar la orquestación (ajusta si quieres)
                time.sleep(SLEEP_BETWEEN_TRIGGERS)

# Si Mage espera una función llamada 'run' en un bloque Python, expónla:
def run(*args, **kwargs):
    # Puedes pasar override vía variables del block en la UI si prefieres
    run_controller(
        dest_pipeline_uuid=kwargs.get('dest_pipeline_uuid') or DEST_PIPELINE_UUID,
        start_year=int(kwargs.get('start_year') or START_YEAR),
        end_year=int(kwargs.get('end_year') or END_YEAR),
        end_year_last_month=int(kwargs.get('end_year_last_month') or END_YEAR_LAST_MONTH),
        service=kwargs.get('service') or SERVICE,
    )


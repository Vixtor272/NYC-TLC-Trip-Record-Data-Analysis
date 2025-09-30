# trigger_sequential_controller_custom_safe.py
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
from mage_ai.orchestration.triggers.api import trigger_pipeline
from datetime import datetime
import time
import traceback

# CONFIG: pon el UUID del pipeline destino (NO el del controller)
DEST_PIPELINE_UUID = 'ny_taxi_pipeline'  
START_YEAR = 2015
END_YEAR = 2025
END_YEAR_LAST_MONTH = 12
SERVICE = 'yellow'
SLEEP_BETWEEN_TRIGGERS = 5
POLL_INTERVAL = 10
POLL_TIMEOUT = 300

@custom
def run(*args, **kwargs):
    # overrides desde UI si quieres
    dest = kwargs.get('dest_pipeline_uuid') or DEST_PIPELINE_UUID
    start = int(kwargs.get('start_year') or START_YEAR)
    end = int(kwargs.get('end_year') or END_YEAR)
    end_month = int(kwargs.get('end_year_last_month') or END_YEAR_LAST_MONTH)
    service = kwargs.get('service') or SERVICE

    # PREVENCIÓN: no disparar el mismo pipeline (auto-trigger)
    current_pipeline_uuid = kwargs.get('current_pipeline_uuid')
    if current_pipeline_uuid and current_pipeline_uuid == dest:
        raise RuntimeError("DEST_PIPELINE_UUID es el mismo que el pipeline controlador. No permitido.")

    try:
        for y in range(start, end + 1):
            months = range(1, end_month + 1) if y == end else range(1, 13)
            for m in months:
                print(f"{datetime.now().isoformat()} - Triggering {dest} for {y}-{m:02d} service={service}")
                try:
                    info = trigger_pipeline(
                        dest,
                        variables={'year': y, 'month': m, 'service': service},
                        check_status=True,
                        error_on_failure=True,
                        poll_interval=POLL_INTERVAL,
                        poll_timeout=POLL_TIMEOUT,
                        verbose=True,
                    )
                    # info suele contener detalles del run; móstralo
                    print("Trigger result:", info)
                except Exception as e:
                    print(f"[ERROR] Al disparar {y}-{m:02d}: {e}")
                    traceback.print_exc()
                    # no abortar todo; continuar con siguiente mes
                finally:
                    time.sleep(SLEEP_BETWEEN_TRIGGERS)
    except KeyboardInterrupt:
        print("Interrumpido por usuario.")

# ─────────────────────────────────────────────────────────────
#  Airflow-style default_args inheritance in Pi-Flow.
#
# Expected resolved values (DAG retries=3, retry_delay_seconds=60):
#   task_inherit      → retries=3, retry_delay_seconds=60  (full inherit)
#   task_override     → retries=1, retry_delay_seconds=60  (partial override)
#   task_zero_retries → retries=0, retry_delay_seconds=60  (explicit zero)
#   task_all_override → retries=5, retry_delay_seconds=120 (full override)
# ─────────────────────────────────────────────────────────────  

from dag_parser.dynamic.dag_context import DAG, PythonOperator
from datetime import datetime

default_args = {
    "retries": 3,
    "retry_delay_seconds": 60,
}

with DAG(
    dag_id="default_args_retry_test_1",
    default_args=default_args,
    schedule_interval=None,
    start_date = datetime(2026,3,30),
    catchup=False,
) as dag:

    # Case 1: No retry args → inherits everything from DAG default_args
    task_inherit = PythonOperator(
        task_id="task_inherit",
        python_callable=lambda: print("Inherits all defaults"),
    )

    # Case 2: Overrides only retries → retry_delay_seconds still inherited
    task_override = PythonOperator(
        task_id="task_override",
        python_callable=lambda: print("Overrides retries only"),
        retries=1,
    )

    # Case 3: Explicitly sets retries=0 → must NOT be overwritten by DAG default
    task_zero_retries = PythonOperator(
        task_id="task_zero_retries",
        python_callable=lambda: print("Explicit zero retries"),
        retries=0,
    )

    # Case 4: Overrides everything → DAG defaults ignored
    task_all_override = PythonOperator(
        task_id="task_all_override",
        python_callable=lambda: print("Full override"),
        retries=5,
        retry_delay_seconds=120,
    )

    task_inherit >> task_override >> task_zero_retries >> task_all_override

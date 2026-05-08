from dag_parser.dynamic.dag_context import DAG, PythonOperator
from datetime import datetime


def always_fail(**context):
    task_name = context["task_id"]
    try_number = context["ti"].try_number
    print(f"[{task_name}] Attempt {try_number} - about to fail")
    raise Exception(f"Intentional failure on attempt {try_number}")


with DAG(
    dag_id="retry_test",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay_seconds": 5,
    },
) as dag:

    task_a = PythonOperator(
        task_id="task_a",
        python_callable=always_fail,
    )

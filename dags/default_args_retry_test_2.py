from dag_parser.dynamic.dag_context import DAG, PythonOperator
from datetime import datetime

default_args={
        "retries": 2,
        "retry_delay_seconds": 5
}
def always_fail(task_name):
    print(f"Executing {task_name}")
    raise Exception("Simulated failure")


with DAG(
    dag_id="default_args_retry_test_2",
    schedule_interval=None,
    start_date=datetime(2026, 2, 13),
    catchup=False,
    default_args=default_args,
) as dag:

    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=always_fail("task_1"),
    )

    task_2 = PythonOperator(
        task_id="task_2",
        python_callable=always_fail("task_2"),
    )

    task_3 = PythonOperator(
        task_id="task_3",
        python_callable=always_fail("task_3"),
    )

    task_1 >> task_2 >> task_3

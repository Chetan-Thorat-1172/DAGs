from dag_parser.dynamic.dag_context import DAG
from dag_parser.dynamic.operators import PythonOperator
from datetime import datetime
import time
import socket
import os


# -----------------------------------------------------
# Generic Task Callable
# -----------------------------------------------------
def heavy_task(task_number: int, **context):
    ti = context["ti"]

    hostname = socket.gethostname()
    pid = os.getpid()

    print("=" * 60)
    print(f"Task {task_number} started")
    print(f"Executed on host: {hostname}")
    print(f"Process PID: {pid}")
    print(f"Task ID: {ti.task_id}")
    print("=" * 60)

    # Simulate workload
    time.sleep(10)

    print(f"Task {task_number} finished")
    print("=" * 60)


# -----------------------------------------------------
# DAG Definition
# -----------------------------------------------------
with DAG(
    dag_id="multi_worker_parallel_test",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="DAG to test multi-worker distribution",
) as dag:

    # Create 30 parallel tasks
    for i in range(1, 31):
        PythonOperator(
            task_id=f"parallel_task_{i}",
            python_callable=heavy_task,
            op_kwargs={"task_number": i},
            trigger_rule="always",   # Critical for parallel scheduling
        )

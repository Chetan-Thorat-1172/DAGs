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
    description="DAG to test multi-worker task distribution",
) as dag:

    t1 = PythonOperator(task_id="parallel_task_1", python_callable=heavy_task, params={"task_number": 1}, trigger_rule="always")
    t2 = PythonOperator(task_id="parallel_task_2", python_callable=heavy_task, params={"task_number": 2}, trigger_rule="always")
    t3 = PythonOperator(task_id="parallel_task_3", python_callable=heavy_task, params={"task_number": 3}, trigger_rule="always")
    t4 = PythonOperator(task_id="parallel_task_4", python_callable=heavy_task, params={"task_number": 4}, trigger_rule="always")
    t5 = PythonOperator(task_id="parallel_task_5", python_callable=heavy_task, params={"task_number": 5}, trigger_rule="always")
    t6 = PythonOperator(task_id="parallel_task_6", python_callable=heavy_task, params={"task_number": 6}, trigger_rule="always")
    t7 = PythonOperator(task_id="parallel_task_7", python_callable=heavy_task, params={"task_number": 7}, trigger_rule="always")
    t8 = PythonOperator(task_id="parallel_task_8", python_callable=heavy_task, params={"task_number": 8}, trigger_rule="always")
    t9 = PythonOperator(task_id="parallel_task_9", python_callable=heavy_task, params={"task_number": 9}, trigger_rule="always")
    t10 = PythonOperator(task_id="parallel_task_10", python_callable=heavy_task, params={"task_number": 10}, trigger_rule="always")

    t11 = PythonOperator(task_id="parallel_task_11", python_callable=heavy_task, params={"task_number": 11}, trigger_rule="always")
    t12 = PythonOperator(task_id="parallel_task_12", python_callable=heavy_task, params={"task_number": 12}, trigger_rule="always")
    t13 = PythonOperator(task_id="parallel_task_13", python_callable=heavy_task, params={"task_number": 13}, trigger_rule="always")
    t14 = PythonOperator(task_id="parallel_task_14", python_callable=heavy_task, params={"task_number": 14}, trigger_rule="always")
    t15 = PythonOperator(task_id="parallel_task_15", python_callable=heavy_task, params={"task_number": 15}, trigger_rule="always")
    t16 = PythonOperator(task_id="parallel_task_16", python_callable=heavy_task, params={"task_number": 16}, trigger_rule="always")
    t17 = PythonOperator(task_id="parallel_task_17", python_callable=heavy_task, params={"task_number": 17}, trigger_rule="always")
    t18 = PythonOperator(task_id="parallel_task_18", python_callable=heavy_task, params={"task_number": 18}, trigger_rule="always")
    t19 = PythonOperator(task_id="parallel_task_19", python_callable=heavy_task, params={"task_number": 19}, trigger_rule="always")
    t20 = PythonOperator(task_id="parallel_task_20", python_callable=heavy_task, params={"task_number": 20}, trigger_rule="always")

from dag_parser.dynamic.dag_context import DAG
from dag_parser.dynamic.operators import SnowflakeOperator
from datetime import datetime
#comment2
default_args = {
    "snowflake_conn_id": "harsh_conn"
}  

with DAG(
    dag_id="test_snowflake_hello",
    schedule_interval=None,
    start_date=datetime(2026, 3, 10),
    catchup=False,
    default_args=default_args,
) as dag:

    task_1 = SnowflakeOperator(
        task_id="say_hello",
        sql="SELECT 'Hello from task 1' AS message;"
    )

    task_2 = SnowflakeOperator(
        task_id="say_world",
        sql="SELECT 'Hello from task 2' AS message;"
    )

    task_3 = SnowflakeOperator(
        task_id="say_done",
        sql="SELECT 'All tasks done!' AS message;"
    )

    task_1 >> task_2 >> task_3

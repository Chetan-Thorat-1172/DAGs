from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, SnowflakeOperator


with DAG(
    dag_id="simple_snowflake_dag",
    schedule_interval="0 12 * * *",  # runs daily at 12:00
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args={
        "snowflake_conn_id": "harsh_conn",
        "retries": 0,
    },
    description="Simple DAG with Snowflake print statements",
) as dag:

    t1 = SnowflakeOperator(
        task_id="print_start",
        sql="""
        SELECT 'Hello from Snowflake - Task 1' AS message;
        """
    )

    t2 = SnowflakeOperator(
        task_id="print_middle",
        sql="""
        SELECT 'Processing step - Task 2' AS message;
        """
    )

    t3 = SnowflakeOperator(
        task_id="print_end",
        sql="""
        SELECT 'Finished execution - Task 3' AS message;
        """
    )

    # Define execution order
    t1 >> t2 >> t3

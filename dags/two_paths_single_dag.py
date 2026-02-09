from dag_parser.dynamic.dag_context import (
    DAG,
    SnowflakeOperator,
)
from datetime import datetime

with DAG(
    dag_id="two_paths_single_dag",
    schedule_interval="30 11 * * *",
    start_date=datetime(2026, 2, 9),
    catchup=False
) as dag:

    # Task a (start path 1)
    a = SnowflakeOperator(
        task_id="a",
        sql="SELECT 'task a executed';",
        connection_id="snowflake_default" 
    )

    # Common task b (join point)
    b = SnowflakeOperator(
        task_id="b",
        sql="SELECT 'task b executed';",
        connection_id="snowflake_default"
    )

    # Downstream task c (branch 1)
    c = SnowflakeOperator(
        task_id="c",
        sql="SELECT 'task c executed';",
        connection_id="snowflake_default"
    )

    # Downstream task f (branch 2)
    f = SnowflakeOperator(
        task_id="f",
        sql="SELECT 'task f executed';",
        connection_id="snowflake_default"
    )

    # ------------------
    # Dependency graph
    # ------------------

    a >> b >> [c,f]


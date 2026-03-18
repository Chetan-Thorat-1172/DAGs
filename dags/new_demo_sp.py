from dag_parser.dynamic.dag_context import DAG
from dag_parser.dynamic.dag_context import SnowflakeOperator
from datetime import datetime

with DAG(
    dag_id="snowflake_etl_dag",
    description="ETL pipeline: Extract → Transform → Load",
    schedule_interval="0 9 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["snowflake", "etl"],
) as dag:

    extract = SnowflakeOperator(
        task_id="extract",
        sql="CALL PI_FLOW.METADATA.SP_EXTRACT();",
        connection_id="snowflake_default",
    )

    transform = SnowflakeOperator(
        task_id="transform",
        sql="CALL PI_FLOW.METADATA.SP_TRANSFORM();",
        connection_id="snowflake_default",
    )

    load = SnowflakeOperator(
        task_id="load",
        sql="CALL PI_FLOW.METADATA.SP_LOAD();",
        connection_id="snowflake_default",
    )

    extract >> transform >> load

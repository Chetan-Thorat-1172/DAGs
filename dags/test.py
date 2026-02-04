from dag_parser.dynamic.dag_context import (
    DAG,
    SnowflakeOperator,
)
from datetime import datetime

with DAG(
    "etl_snowflake_dag",
    schedule_interval="@daily",
    start_date=datetime(2026, 2, 4),  
    catchup=False                 
) as dag:

    extract = SnowflakeOperator(
        task_id="extract",
        sql="CALL PI_FLOW.RAW.EXTRACT_DATA();",
        connection_id="snowflake_default"
    )

    transform = SnowflakeOperator(
        task_id="transform",
        sql="CALL PI_FLOW.RAW.TRANSFORM_DATA();",
        connection_id="snowflake_default"
    )

    load = SnowflakeOperator(
        task_id="load",
        sql="CALL PI_FLOW.RAW.LOAD_DATA();",  
        connection_id="snowflake_default" 
    )

    notify = SnowflakeOperator(
        task_id="notify",
        sql="CALL PI_FLOW.RAW.LOAD_DATA();",  
        connection_id="snowflake_default" 
    )

    # execution order
    extract >> transform >> [ load , notify ]

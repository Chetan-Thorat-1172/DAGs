from dag_parser.dynamic.dag_context import DAG
from dag_parser.dynamic.operators import SnowflakeOperator
from dag_parser.dynamic.params import Param
from datetime import datetime

with DAG(
    dag_id="snowflake_etl_dag",
    description="ETL pipeline: Extract → Transform → Load via Snowflake SPs",
    schedule_interval="0 9 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "processDate": Param(
            default=datetime.now().strftime("%Y-%m-%d"),
            type="string",
            description="Date to process"
        ),
        "appCode": Param(
            default="ETL001",
            type="string",
            description="Application code"
        ),
    },
    tags=["snowflake", "etl"],
) as dag:

    extract = SnowflakeOperator(
        task_id="extract",
        sql="""
        CALL PI_FLOW.METADATA.SP_EXTRACT(
            %(processDate)s,
            %(appCode)s
        );
        """,
        params=["processDate", "appCode"],
        connection_id="snowflake_default",
    )

    transform = SnowflakeOperator(
        task_id="transform",
        sql="""
        CALL PI_FLOW.METADATA.SP_TRANSFORM(
            %(processDate)s,
            %(appCode)s
        );
        """,
        params=["processDate", "appCode"],
        connection_id="snowflake_default",
    )

    load = SnowflakeOperator(
        task_id="load",
        sql="""
        CALL PI_FLOW.METADATA.SP_LOAD(
            %(processDate)s,
            %(appCode)s
        );
        """,
        params=["processDate", "appCode"],
        connection_id="snowflake_default",
    )

    extract >> transform >> load

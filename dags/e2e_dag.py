from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    BranchPythonOperator,
    PythonOperator,
    SnowflakeOperator,
)
from dag_parser.dynamic.params import Param


def validate_params(**context):
    conf = (context.get("dag_run", {}) or {}).get("conf", {})
    print("Trigger params:", conf)
    return conf


def decide_load_mode(**context):
    conf = (context.get("dag_run", {}) or {}).get("conf", {})
    full_load = conf.get("full_load", False)

    print(f"Branch decision - full_load={full_load}")
    if full_load:
        return "stage_orders_full"
    return "stage_orders_incremental"


def join_paths(**_):
    print("Load branch finished. Moving to notebook + dbt.")


with DAG(
    dag_id="pi_flow_sales_sp_notebook_dbt_dag",
    schedule_interval=None,
    start_date=datetime(2026, 4, 20),
    catchup=False,
    description="Branching Sales ETL with Snowflake SPs, Notebook, and DBT project",
    params={
        "business_date": Param(
            type="string",
            default="2026-04-01",
            description="Business date in YYYY-MM-DD",
        ),
        "full_load": Param(
            type="boolean",
            default=False,
            description="If true run full load branch; else incremental branch",
        ),
        "load_window_days": Param(
            type="integer",
            default=3,
            description="Only used by incremental path",
        ),
        "sales_channel": Param(
            type="string",
            default="ALL",
            description="Filter channel (ALL / ONLINE / STORE)",
        ),
    },
    default_args={
        "snowflake_conn_id": "chetan_conn",
        "retries": 1,
        "retry_delay_seconds": 30,
    },
) as dag:
    t01_validate_params = PythonOperator(
        task_id="validate_params",
        python_callable=validate_params,
    )

    t02_decide_load_mode = BranchPythonOperator(
        task_id="decide_load_mode",
        python_callable=decide_load_mode,
    )

    t03_stage_orders_full = SnowflakeOperator(
        task_id="stage_orders_full",
        sql="""
        CALL DAG_TESTING.PI_FLOW_QA.SP_STAGE_ORDERS_FULL(
            TO_DATE(%(business_date)s),
            %(sales_channel)s
        );
        """,
    )

    t04_stage_customers_full = SnowflakeOperator(
        task_id="stage_customers_full",
        sql="""
        CALL DAG_TESTING.PI_FLOW_QA.SP_STAGE_CUSTOMERS_FULL(
            TO_DATE(%(business_date)s)
        );
        """,
    )

    t05_insert_sales_full = SnowflakeOperator(
        task_id="merge_sales_full",
        sql="""
        CALL DAG_TESTING.PI_FLOW_QA.SP_MERGE_SALES_FULL(
            TO_DATE(%(business_date)s)
        );
        """,
    )

    t06_stage_orders_incremental = SnowflakeOperator(
        task_id="stage_orders_incremental",
        sql="""
        CALL DAG_TESTING.PI_FLOW_QA.SP_STAGE_ORDERS_INCREMENTAL(
            TO_DATE(%(business_date)s),
            %(load_window_days)s,
            %(sales_channel)s
        );
        """,
    )

    t07_stage_customers_incremental = SnowflakeOperator(
        task_id="stage_customers_incremental",
        sql="""
        CALL DAG_TESTING.PI_FLOW_QA.SP_STAGE_CUSTOMERS_INCREMENTAL(
            TO_DATE(%(business_date)s),
            %(load_window_days)s
        );
        """,
    )

    t08_insert_sales_incremental = SnowflakeOperator(
        task_id="merge_sales_incremental",
        sql="""
        CALL DAG_TESTING.PI_FLOW_QA.SP_MERGE_SALES_INCREMENTAL(
            TO_DATE(%(business_date)s)
        );
        """,
    )

    t09_join_load_paths = PythonOperator(
        task_id="join_load_paths",
        python_callable=join_paths,
        trigger_rule="none_failed_min_one_success",
    )

    t10_run_snowflake_notebook = SnowflakeOperator(
        task_id="run_snowflake_notebook",
        sql="""
        EXECUTE NOTEBOOK PROJECT DAG_TESTING.PI_FLOW_QA.NB_SALES_QUALITY_CHECKS
        MAIN_FILE = 'nb_sales_quality_checks.ipynb'
        COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
        QUERY_WAREHOUSE = 'COMPUTE_WH'
        RUNTIME = 'V2.2-CPU-PY3.10';
        """,
    )

    t11_run_dbt_project = SnowflakeOperator(
        task_id="run_dbt_project",
        sql=(
            "EXECUTE DBT PROJECT DAG_TESTING.PI_FLOW_QA.PI_FLOW_SALES_DBT ARGS = 'run --target dev --select stg_sales_fact mart_daily_sales mart_channel_sales';"
        ),
    )

    t12_publish_sales_mart = SnowflakeOperator(
        task_id="publish_sales_mart",
        sql="""
        CALL DAG_TESTING.PI_FLOW_QA.SP_PUBLISH_SALES_MART(
            TO_DATE(%(business_date)s)
        );
        """,
    )

    t13_audit_pipeline_run = SnowflakeOperator(
        task_id="audit_pipeline_run",
        sql="""
        CALL DAG_TESTING.PI_FLOW_QA.SP_AUDIT_PIPELINE_RUN(
            'pi_flow_sales_sp_notebook_dbt_dag',
            TO_DATE(%(business_date)s),
            IFF(%(full_load)s, 'FULL', 'INCREMENTAL'),
            'SUCCESS'
        );
        """,
        trigger_rule="none_failed",
    )

    t01_validate_params >> t02_decide_load_mode

    t02_decide_load_mode >> [t03_stage_orders_full, t06_stage_orders_incremental]

    t03_stage_orders_full >> t05_insert_sales_full
    t04_stage_customers_full >> t05_insert_sales_full
    t03_stage_orders_full >> t04_stage_customers_full

    t06_stage_orders_incremental >> t08_insert_sales_incremental
    t07_stage_customers_incremental >> t08_insert_sales_incremental
    t06_stage_orders_incremental >> t07_stage_customers_incremental

    [t05_insert_sales_full, t08_insert_sales_incremental] >> t09_join_load_paths

    t09_join_load_paths >> [t10_run_snowflake_notebook, t11_run_dbt_project]
    [t10_run_snowflake_notebook, t11_run_dbt_project] >> t12_publish_sales_mart
    t12_publish_sales_mart >> t13_audit_pipeline_run

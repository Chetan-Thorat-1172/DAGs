"""
Regression DAG: Snowflake SP + Notebook + DBT E2E

This DAG validates an end-to-end analytics pipeline using:
1. Multiple Snowflake stored procedures
2. Direct Snowflake Notebook execution via EXECUTE NOTEBOOK
3. Direct Snowflake DBT project execution via EXECUTE DBT PROJECT

Reference DDL file:
- dags-repo/dags/regression_suite/sql/reg_e2e_notebook_dbt_ddl.sql   
"""

from datetime import datetime
from dag_parser.dynamic.params import Param
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator, EmailOperator


def bootstrap_context(**context):
    conf = (context.get('dag_run', {}) or {}).get('conf', {})
    run_meta = {
        'business_date': conf.get('business_date', '2026-01-01'),
        'pipeline_id': conf.get('pipeline_id', 'REG_PIPELINE_E2E_01'),
        'dbt_target': conf.get('dbt_target', 'dev'),
    }
    context['ti'].xcom_push(key='run_meta', value=run_meta)
    print('Bootstrap run metadata:', run_meta)
    return run_meta


def capture_post_metrics(**context):
    ti = context['ti']
    run_meta = ti.xcom_pull(task_ids='bootstrap_context', key='run_meta')
    print('Collected post-run metrics for', run_meta)
    return {'metrics_status': 'captured'}


def finalize_pipeline(**context):
    print('E2E Snowflake Notebook + DBT DAG completed')
    return 'done'


with DAG(
    dag_id='e2e_snowflake_notebook_dbt_dag',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        'business_date': Param(type='string', default='2026-01-01', description='Business date YYYY-MM-DD'),
        'pipeline_id': Param(type='string', default='REG_PIPELINE_E2E_01', description='Pipeline identifier'),
        'dbt_target': Param(type='string', default='dev', description='DBT target profile'),
    },
    default_args={
        'retries': 1,
        'retry_delay_seconds': 30,
        "snowflake_conn_id": "chetan_conn"
    },
) as dag:
    t01_bootstrap = PythonOperator(
        task_id='bootstrap_context',
        python_callable=bootstrap_context,
    )

    t02_prepare_objects = SnowflakeOperator(
        task_id='prepare_objects',
        sql='CALL PI_FLOW_QA.SP_PREPARE_OBJECTS();',
    )

    t03_ingest_raw = SnowflakeOperator(
        task_id='ingest_raw_orders',
        sql="CALL PI_FLOW_QA.SP_INGEST_RAW_ORDERS('2026-01-01');",
    )

    t04_cleanse = SnowflakeOperator(
        task_id='cleanse_orders',
        sql='CALL PI_FLOW_QA.SP_CLEANSE_ORDERS();',
    )

    t05_enrich = SnowflakeOperator(
        task_id='enrich_orders',
        sql='CALL PI_FLOW_QA.SP_ENRICH_ORDERS();',
    )

    t06_notebook = SnowflakeOperator(
        task_id='run_snowflake_notebook',
        sql='EXECUTE NOTEBOOK PROJECT DAG_TESTING.PI_FLOW_QA.NB_ORDERS_ANALYTICS();',
    )

    t07_dbt_seed = SnowflakeOperator(
        task_id='run_dbt_seed',
        sql="EXECUTE DBT PROJECT PI_FLOW_QA.DBT_PROJECTS.PI_FLOW_DBT_DEMO ARGS = 'seed --target dev'",
    )

    t08_dbt_staging = SnowflakeOperator(
        task_id='run_dbt_staging_models',
        sql="EXECUTE DBT PROJECT PI_FLOW_QA.DBT_PROJECTS.PI_FLOW_DBT_DEMO ARGS = 'run --select stg_orders stg_customers --target dev';",
    )

    t09_dbt_marts = SnowflakeOperator(
        task_id='run_dbt_marts',
        sql="EXECUTE DBT PROJECT PI_FLOW_QA.DBT_PROJECTS.PI_FLOW_DBT_DEMO ARGS = 'run --select fct_orders mart_revenue --target dev';",
    )

    t10_dq_assertions = SnowflakeOperator(
        task_id='run_dq_assertions',
        sql='CALL PI_FLOW_QA.SP_DQ_ASSERT_ORDERS();',
    )

    t11_publish = SnowflakeOperator(
        task_id='publish_serving_table',
        sql='CALL PI_FLOW_QA.SP_PUBLISH_ORDERS_SERVING();',
    )

    t12_metrics = PythonOperator(
        task_id='capture_post_metrics',
        python_callable=capture_post_metrics,
    )

    t13_notify = EmailOperator(
        task_id='notify_completion',
        to=['qa-alerts@pibythree.com'],
        subject='E2E DAG complete {{ dag_id }} {{ run_id }}',
        html_content='<h3>E2E flow completed</h3><p>state={{ state }}</p>',
        trigger_rule='all_done',
    )

    t14_finalize = PythonOperator(
        task_id='finalize_pipeline',
        python_callable=finalize_pipeline,
        trigger_rule='all_done',
    )

    t01_bootstrap >> t02_prepare_objects >> t03_ingest_raw >> t04_cleanse >> t05_enrich
    t05_enrich >> t06_notebook >> t07_dbt_seed >> t08_dbt_staging >> t09_dbt_marts
    t09_dbt_marts >> t10_dq_assertions >> t11_publish >> t12_metrics >> t13_notify >> t14_finalize

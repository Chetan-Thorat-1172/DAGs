
from datetime import datetime
from dag_parser.dynamic.params import Param
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def log_start(**context):
    print('Starting sales ETL run')


def enrich_batch_meta(**context):
    ti = context['ti']
    ti.xcom_push(key='batch_meta', value={'source': 'sales_api', 'partition': 'daily'})
    return 'meta_ready'


def validate_counts(**context):
    print('Validating staged row counts and duplicates')


def quality_gate(**context):
    print('Quality gate passed')


def finalize(**context):
    ti = context['ti']
    print('Batch meta:', ti.xcom_pull(task_ids='enrich_batch_meta', key='batch_meta'))
    return 'sales_etl_complete'


with DAG(
    dag_id='reg_sales_etl_snowflake_dag',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        'run_date': Param(type='string', required=True, description='Business date YYYY-MM-DD'),
        'batch_id': Param(type='string', default='BATCH_001', description='Batch identifier'),
    },
    default_args={'snowflake_conn_id': 'snowflake_default', 'retries': 1, 'retry_delay_seconds': 30},
) as dag:
    t01_start = PythonOperator(task_id='start', python_callable=log_start)

    t02_stage = SnowflakeOperator(
        task_id='stage_sales',
        sql="CALL PI_FLOW_QA.SP_STAGE_SALES('BATCH_001', CURRENT_DATE());",
    )
    t03_meta = PythonOperator(task_id='enrich_batch_meta', python_callable=enrich_batch_meta)
    t04_dim_customer = SnowflakeOperator(task_id='load_dim_customer', sql='SELECT CURRENT_TIMESTAMP();')
    t05_dim_product = SnowflakeOperator(task_id='load_dim_product', sql='SELECT CURRENT_TIMESTAMP();')
    t06_dim_store = SnowflakeOperator(task_id='load_dim_store', sql='SELECT CURRENT_TIMESTAMP();')
    t07_fact = SnowflakeOperator(
        task_id='merge_fact_sales',
        sql="CALL PI_FLOW_QA.SP_MERGE_SALES('BATCH_001');",
    )
    t08_validate = PythonOperator(task_id='validate_counts', python_callable=validate_counts)
    t09_quality = PythonOperator(task_id='quality_gate', python_callable=quality_gate)
    t10_publish = SnowflakeOperator(task_id='publish_snapshot', sql='SELECT CURRENT_DATE();')
    t11_audit = SnowflakeOperator(task_id='insert_audit', sql="SELECT 'BATCH_001';", trigger_rule='all_done')
    t12_finalize = PythonOperator(task_id='finalize', python_callable=finalize, trigger_rule='none_failed')

    t01_start >> t02_stage >> t03_meta
    t03_meta >> [t04_dim_customer, t05_dim_product, t06_dim_store]
    [t04_dim_customer, t05_dim_product, t06_dim_store] >> t07_fact
    t07_fact >> t08_validate >> t09_quality >> t10_publish >> t11_audit >> t12_finalize

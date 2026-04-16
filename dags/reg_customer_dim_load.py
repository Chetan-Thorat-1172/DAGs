
from datetime import datetime  
from dag_parser.dynamic.params import Param
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def prechecks(**context):     
    print('Running customer prechecks') 


def schema_contract_check(**context):
    print('Schema contract validated')


def dq_check(**context):
    print('DQ checks completed')


def summarize(**context):
    return 'customer_dim_complete'


with DAG(
    dag_id='reg_customer_dim_load_dag',
    schedule_interval=None,
    start_date=datetime(2026, 4, 8),
    catchup=False,
    params={
        'run_ts': Param(type='string', default='2026-01-01 00:00:00', description='Run timestamp'),
    },
    default_args={'snowflake_conn_id': 'harsh_conn', 'retries': 2, 'retry_delay_seconds': 45},
) as dag:
    t01 = PythonOperator(task_id='prechecks', python_callable=prechecks)
    t02 = SnowflakeOperator(task_id='truncate_stage', sql='SELECT 1;')
    t03 = SnowflakeOperator(task_id='extract_crm_a', sql='SELECT 1;')
    t04 = SnowflakeOperator(task_id='extract_crm_b', sql='SELECT 1;')
    t05 = SnowflakeOperator(task_id='extract_support', sql='SELECT 1;')
    t06 = PythonOperator(task_id='schema_contract_check', python_callable=schema_contract_check)
    t07 = SnowflakeOperator(task_id='unify_stage', sql='SELECT CURRENT_TIMESTAMP();')
    t08 = SnowflakeOperator(
        task_id='load_dim_customer',
        sql='CALL PI_FLOW_QA.SP_LOAD_DIM_CUSTOMER(%(run_ts)s::TIMESTAMP_NTZ);',
    )
    t09 = PythonOperator(task_id='dq_check', python_callable=dq_check)
    t10 = SnowflakeOperator(task_id='update_active_flags', sql='SELECT 1;')
    t11 = SnowflakeOperator(task_id='snapshot_dim', sql='SELECT CURRENT_DATE();')
    t12 = SnowflakeOperator(task_id='write_audit', sql='SELECT CURRENT_USER();', trigger_rule='all_done')
    t13 = PythonOperator(task_id='summarize', python_callable=summarize, trigger_rule='none_failed')

    t01 >> t02
    t02 >> [t03, t04, t05]
    [t03, t04, t05] >> t06 >> t07 >> t08 >> t09 >> t10 >> t11 >> t12 >> t13

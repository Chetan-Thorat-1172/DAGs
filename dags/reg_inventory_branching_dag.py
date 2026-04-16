
from datetime import datetime
from dag_parser.dynamic.params import Param
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BranchPythonOperator, SnowflakeOperator 


def choose_path(**context):
    full_load = (context.get('dag_run', {}) or {}).get('conf', {}).get('full_load', False)
    return 'full_extract' if full_load else 'delta_extract'


def mark_rejoin(**context):
    print('Branches rejoined')


def done(**context):
    return 'inventory_done'


with DAG(
    dag_id='reg_inventory_branching_dag',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={'full_load': Param(type='boolean', default=False, description='Run full extraction')},
    default_args={'snowflake_conn_id': 'harsh_conn', 'retries': 1, 'retry_delay_seconds': 20},
) as dag:
    t01 = PythonOperator(task_id='prepare_context', python_callable=lambda **context: 'ctx')
    t02 = BranchPythonOperator(task_id='choose_branch', python_callable=choose_path)

    t03 = SnowflakeOperator(task_id='full_extract', sql='SELECT 1;')
    t04 = SnowflakeOperator(task_id='full_validate', sql='SELECT 1;')
    t05 = SnowflakeOperator(task_id='full_transform', sql='SELECT 1;')

    t06 = SnowflakeOperator(task_id='delta_extract', sql='SELECT 1;')
    t07 = SnowflakeOperator(
        task_id='delta_apply',
        sql='CALL PI_FLOW_QA.SP_APPLY_INVENTORY_DELTA(CURRENT_DATE());',
    )
    t08 = SnowflakeOperator(task_id='delta_validate', sql='SELECT 1;')

    t09 = PythonOperator(task_id='rejoin_marker', python_callable=mark_rejoin, trigger_rule='none_failed_min_one_success')
    t10 = SnowflakeOperator(task_id='aggregate_inventory', sql='SELECT 1;', trigger_rule='none_failed_min_one_success')
    t11 = SnowflakeOperator(task_id='publish_inventory', sql='SELECT 1;')
    t12 = SnowflakeOperator(task_id='audit_inventory', sql='SELECT 1;', trigger_rule='all_done')
    t13 = PythonOperator(task_id='final_done', python_callable=done, trigger_rule='none_failed')

    t01 >> t02
    t02 >> [t03, t06]
    t03 >> t04 >> t05
    t06 >> t07 >> t08
    [t05, t08] >> t09 >> t10 >> t11 >> t12 >> t13

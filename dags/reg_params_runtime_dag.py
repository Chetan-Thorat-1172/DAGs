
from datetime import datetime
from dag_parser.dynamic.params import Param
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def read_params(**context):
    conf = (context.get('dag_run', {}) or {}).get('conf', {})
    print('conf=', conf)
    return conf


def validate_params(**context):
    conf = context['ti'].xcom_pull(task_ids='read_params') or {}
    if not conf.get('country'):
        raise ValueError('country is required in dag_run.conf')
    return 'validated'


def compute_thresholds(**context):
    return {'dq_threshold': 0.95}


def finish(**context):
    return 'params_runtime_done'


with DAG(
    dag_id='reg_params_runtime_dag',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        'country': Param(type='string', default='IN', description='Country code'),
        'run_mode': Param(type='string', default='qa', description='Execution mode'),
        'max_rows': Param(type='integer', default=1000, description='Limit rows'),
        'dry_run': Param(type='boolean', default=False, description='Dry-run mode'),
    },
    default_args={'snowflake_conn_id': 'snowflake_default', 'retries': 1, 'retry_delay_seconds': 15},
) as dag:
    t01 = PythonOperator(task_id='read_params', python_callable=read_params)
    t02 = PythonOperator(task_id='validate_params', python_callable=validate_params)
    t03 = PythonOperator(task_id='compute_thresholds', python_callable=compute_thresholds)
    t04 = SnowflakeOperator(task_id='audit_country', sql="CALL PI_FLOW_QA.SP_AUDIT_PARAM('country', 'IN');")
    t05 = SnowflakeOperator(task_id='audit_run_mode', sql="CALL PI_FLOW_QA.SP_AUDIT_PARAM('run_mode', 'qa');")
    t06 = SnowflakeOperator(task_id='audit_max_rows', sql="CALL PI_FLOW_QA.SP_AUDIT_PARAM('max_rows', '1000');")
    t07 = SnowflakeOperator(task_id='extract_parametrized_data', sql='SELECT 1000;')
    t08 = SnowflakeOperator(task_id='transform_parametrized_data', sql="SELECT 'IN';")
    t09 = SnowflakeOperator(task_id='load_parametrized_data', sql="SELECT 'qa';")
    t10 = SnowflakeOperator(task_id='write_audit', sql='SELECT CURRENT_TIMESTAMP();', trigger_rule='all_done')
    t11 = PythonOperator(task_id='finish', python_callable=finish, trigger_rule='all_done')

    t01 >> t02 >> t03 >> t04 >> t05 >> t06 >> t07 >> t08 >> t09 >> t10 >> t11

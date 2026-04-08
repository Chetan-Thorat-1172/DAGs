
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def child_read_conf(**context):
    conf = (context.get('dag_run', {}) or {}).get('conf', {})
    print('child conf', conf)
    return conf


def child_finalize(**context):
    return 'child_finance_complete'


with DAG(
    dag_id='reg_finance_child_dag',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={'snowflake_conn_id': 'snowflake_default', 'retries': 1, 'retry_delay_seconds': 20},
) as dag:
    t01 = PythonOperator(task_id='child_read_conf', python_callable=child_read_conf)
    t02 = SnowflakeOperator(task_id='child_extract_gl', sql="CALL PI_FLOW_QA.SP_FIN_CHILD_STEP('extract_gl', 'REG_CHILD_RUN');")
    t03 = SnowflakeOperator(task_id='child_extract_ap', sql="CALL PI_FLOW_QA.SP_FIN_CHILD_STEP('extract_ap', 'REG_CHILD_RUN');")
    t04 = SnowflakeOperator(task_id='child_extract_ar', sql="CALL PI_FLOW_QA.SP_FIN_CHILD_STEP('extract_ar', 'REG_CHILD_RUN');")
    t05 = SnowflakeOperator(task_id='child_transform_gl', sql="CALL PI_FLOW_QA.SP_FIN_CHILD_STEP('transform_gl', 'REG_CHILD_RUN');")
    t06 = SnowflakeOperator(task_id='child_transform_ap', sql="CALL PI_FLOW_QA.SP_FIN_CHILD_STEP('transform_ap', 'REG_CHILD_RUN');")
    t07 = SnowflakeOperator(task_id='child_transform_ar', sql="CALL PI_FLOW_QA.SP_FIN_CHILD_STEP('transform_ar', 'REG_CHILD_RUN');")
    t08 = SnowflakeOperator(task_id='child_merge_fact', sql="CALL PI_FLOW_QA.SP_FIN_CHILD_STEP('merge_fact', 'REG_CHILD_RUN');")
    t09 = SnowflakeOperator(task_id='child_quality', sql="CALL PI_FLOW_QA.SP_FIN_CHILD_STEP('quality', 'REG_CHILD_RUN');")
    t10 = SnowflakeOperator(task_id='child_publish', sql="CALL PI_FLOW_QA.SP_FIN_CHILD_STEP('publish', 'REG_CHILD_RUN');")
    t11 = SnowflakeOperator(task_id='child_audit', sql="CALL PI_FLOW_QA.SP_FIN_CHILD_STEP('audit', 'REG_CHILD_RUN');", trigger_rule='all_done')
    t12 = PythonOperator(task_id='child_finalize', python_callable=child_finalize, trigger_rule='all_done')

    t01 >> [t02, t03, t04]
    t02 >> t05
    t03 >> t06
    t04 >> t07
    [t05, t06, t07] >> t08 >> t09 >> t10 >> t11 >> t12

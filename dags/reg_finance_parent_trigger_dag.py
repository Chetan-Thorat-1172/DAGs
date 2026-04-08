
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator, TriggerDagRunOperator


def prep_conf(**context):
    return {'ledger_date': '2026-01-01', 'region': 'APAC'}


def parent_finalize(**context):
    return 'parent_finance_complete'


with DAG(
    dag_id='reg_finance_parent_trigger_dag',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={'snowflake_conn_id': 'snowflake_default', 'retries': 1, 'retry_delay_seconds': 20},
) as dag:
    t01 = PythonOperator(task_id='prepare_parent_context', python_callable=prep_conf)
    t02 = SnowflakeOperator(task_id='parent_extract_calendar', sql="CALL PI_FLOW_QA.SP_FIN_PARENT_STEP('extract_calendar', 'REG_PARENT_RUN');")
    t03 = SnowflakeOperator(task_id='parent_extract_fx', sql="CALL PI_FLOW_QA.SP_FIN_PARENT_STEP('extract_fx', 'REG_PARENT_RUN');")
    t04 = SnowflakeOperator(task_id='parent_prepare_balances', sql="CALL PI_FLOW_QA.SP_FIN_PARENT_STEP('prepare_balances', 'REG_PARENT_RUN');")

    t05 = TriggerDagRunOperator(
        task_id='trigger_finance_child',
        trigger_dag_id='reg_finance_child_dag',
        conf={'region': 'APAC', 'ledger_date': '2026-01-01'},
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    t06 = PythonOperator(task_id='read_child_run_id', python_callable=lambda **context: context['ti'].xcom_pull(task_ids='trigger_finance_child', key='trigger_run_id'))
    t07 = SnowflakeOperator(task_id='parent_post_child_recon', sql="CALL PI_FLOW_QA.SP_FIN_PARENT_STEP('post_child_recon', 'REG_PARENT_RUN');")
    t08 = SnowflakeOperator(task_id='parent_posting', sql="CALL PI_FLOW_QA.SP_FIN_PARENT_STEP('posting', 'REG_PARENT_RUN');")
    t09 = SnowflakeOperator(task_id='parent_publish', sql="CALL PI_FLOW_QA.SP_FIN_PARENT_STEP('publish', 'REG_PARENT_RUN');")
    t10 = SnowflakeOperator(task_id='parent_audit', sql="CALL PI_FLOW_QA.SP_FIN_PARENT_STEP('audit', 'REG_PARENT_RUN');", trigger_rule='all_done')
    t11 = PythonOperator(task_id='parent_finalize', python_callable=parent_finalize, trigger_rule='all_done')

    t01 >> [t02, t03]
    [t02, t03] >> t04 >> t05 >> t06 >> t07 >> t08 >> t09 >> t10 >> t11

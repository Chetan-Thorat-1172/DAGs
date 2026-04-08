from datetime import datetime
from dag_parser.dynamic.params import Param
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BranchPythonOperator, SnowflakeOperator, EmailOperator


def decide_path(**context):
    conf = (context.get('dag_run', {}) or {}).get('conf', {})
    return 'path_a_extract' if conf.get('path', 'A') == 'A' else 'path_b_extract'


def read_ctx(**context):
    return (context.get('dag_run', {}) or {}).get('conf', {})


def final_summary(**context):
    return 'e2e_complete'


with DAG(
    dag_id='reg_end_to_end_feature_mix_dag',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        'path': Param(type='string', default='A', description='Branch path A or B'),
        'run_label': Param(type='string', default='qa_regression', description='Run label'),
    },
    default_args={'snowflake_conn_id': 'snowflake_default', 'retries': 1, 'retry_delay_seconds': 15},
) as dag:
    t01 = PythonOperator(task_id='read_context', python_callable=read_ctx)
    t02 = SnowflakeOperator(task_id='phase_ingest', sql="CALL PI_FLOW_QA.SP_E2E_PHASE('ingest', 'REG_E2E_RUN');")
    t03 = BranchPythonOperator(task_id='decide_path', python_callable=decide_path)

    t04 = SnowflakeOperator(task_id='path_a_extract', sql="CALL PI_FLOW_QA.SP_E2E_PHASE('path_a_extract', 'REG_E2E_RUN');")
    t05 = SnowflakeOperator(task_id='path_a_transform', sql="CALL PI_FLOW_QA.SP_E2E_PHASE('path_a_transform', 'REG_E2E_RUN');")

    t06 = SnowflakeOperator(task_id='path_b_extract', sql="CALL PI_FLOW_QA.SP_E2E_PHASE('path_b_extract', 'REG_E2E_RUN');")
    t07 = SnowflakeOperator(task_id='path_b_transform', sql="CALL PI_FLOW_QA.SP_E2E_PHASE('path_b_transform', 'REG_E2E_RUN');")

    t08 = PythonOperator(task_id='join_paths', python_callable=lambda **context: 'joined', trigger_rule='none_failed_min_one_success')
    t09 = SnowflakeOperator(task_id='phase_quality', sql="CALL PI_FLOW_QA.SP_E2E_PHASE('quality', 'REG_E2E_RUN');")
    t10 = SnowflakeOperator(task_id='phase_publish', sql="CALL PI_FLOW_QA.SP_E2E_PHASE('publish', 'REG_E2E_RUN');")
    t11 = EmailOperator(
        task_id='notify_completion',
        to=['qa-alerts@pibythree.com'],
        subject='E2E mix run {{ dag_id }} {{ run_id }}',
        html_content='<p>state={{ state }}</p>',
        trigger_rule='all_done',
    )
    t12 = SnowflakeOperator(task_id='phase_audit', sql="CALL PI_FLOW_QA.SP_E2E_PHASE('audit', 'REG_E2E_RUN');", trigger_rule='all_done')
    t13 = PythonOperator(task_id='final_summary', python_callable=final_summary, trigger_rule='all_done')

    t01 >> t02 >> t03
    t03 >> t04 >> t05
    t03 >> t06 >> t07
    [t05, t07] >> t08 >> t09 >> t10 >> t11 >> t12 >> t13

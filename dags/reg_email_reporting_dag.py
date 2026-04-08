
from datetime import datetime
from dag_parser.dynamic.params import Param
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator, EmailOperator


def prep(**context):
    print('Preparing report context')


def finalize(**context):
    return 'reporting_done'


with DAG(
    dag_id='reg_email_reporting_dag',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        'report_name': Param(type='string', default='daily_ops', description='Report identifier'),
        'run_date': Param(type='string', default='2026-01-01', description='Report run date'),
    },
    default_args={'snowflake_conn_id': 'snowflake_default', 'retries': 1, 'retry_delay_seconds': 20},
) as dag:
    t01 = PythonOperator(task_id='prepare_context', python_callable=prep)
    t02 = SnowflakeOperator(task_id='extract_report_inputs', sql='SELECT 1;')
    t03 = SnowflakeOperator(task_id='aggregate_metrics', sql='SELECT 1;')
    t04 = SnowflakeOperator(task_id='materialize_report', sql='SELECT 1;')
    t05 = SnowflakeOperator(
        task_id='generate_report_artifact',
        sql='CALL PI_FLOW_QA.SP_GENERATE_REPORT(%(report_name)s, %(run_date)s::DATE);',
    )
    t06 = EmailOperator(
        task_id='send_primary_email',
        to=['qa-alerts@pibythree.com'],
        subject='Pi-Flow report {{ dag_id }} {{ run_id }}',
        html_content='<h3>Primary report generated</h3><p>state={{ state }}</p>',
    )
    t07 = EmailOperator(
        task_id='send_secondary_email',
        to=['qa-alerts@pibythree.com'],
        subject='Pi-Flow secondary notice {{ run_id }}',
        html_content='<p>Secondary notification for {{ dag_id }}</p>',
    )
    t08 = SnowflakeOperator(task_id='write_report_audit', sql='SELECT CURRENT_TIMESTAMP();', trigger_rule='all_done')
    t09 = SnowflakeOperator(task_id='publish_report_status', sql='SELECT CURRENT_USER();', trigger_rule='all_done')
    t10 = EmailOperator(
        task_id='send_completion_email',
        to=['qa-alerts@pibythree.com'],
        subject='Completion {{ dag_id }} status={{ state }}',
        html_content='<p>DAG complete for run {{ run_id }}</p>',
        trigger_rule='all_done',
    )
    t11 = PythonOperator(task_id='finalize', python_callable=finalize, trigger_rule='all_done')

    t01 >> t02 >> t03 >> t04 >> t05 >> t06 >> t07 >> t08 >> t09 >> t10 >> t11

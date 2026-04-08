
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, EmailOperator, SnowflakeOperator, SmtpNotifier


retry_notifier = SmtpNotifier(
    to=['qa-alerts@pibythree.com'],
    subject='Retry event {{ dag_id }}.{{ task_id }} try={{ try_number }}',
    html_content='<p>Retry scheduled for {{ task_id }}</p>',
)

failure_notifier = SmtpNotifier(
    to=['qa-alerts@pibythree.com'],
    subject='Failure {{ dag_id }}.{{ task_id }}',
    html_content='<p>Failure state={{ state }} exception={{ exception }}</p>',
)


def flaky_one(**context):
    if context['ti'].try_number < 2:
        raise Exception('flaky_one first try failure')


def flaky_two(**context):
    if context['ti'].try_number < 3:
        raise Exception('flaky_two until third try')


def stable(**context):
    print('stable task complete')


with DAG(
    dag_id='reg_retry_alerts_dag',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay_seconds': 10,
        'on_retry_callback': retry_notifier,
        'on_failure_callback': failure_notifier,
        'snowflake_conn_id': 'snowflake_default',
    },
    on_failure_callback=failure_notifier,
) as dag:
    t01 = PythonOperator(task_id='bootstrap', python_callable=stable)
    t02 = PythonOperator(task_id='flaky_one', python_callable=flaky_one)
    t03 = SnowflakeOperator(task_id='retry_audit_1', sql='SELECT CURRENT_TIMESTAMP();', trigger_rule='all_done')
    t04 = PythonOperator(task_id='flaky_two', python_callable=flaky_two)
    t05 = SnowflakeOperator(task_id='retry_audit_2', sql='SELECT CURRENT_TIMESTAMP();', trigger_rule='all_done')
    t06 = PythonOperator(task_id='stable_1', python_callable=stable)
    t07 = PythonOperator(task_id='stable_2', python_callable=stable)
    t08 = SnowflakeOperator(task_id='final_audit', sql='SELECT CURRENT_USER();', trigger_rule='all_done')
    t09 = EmailOperator(
        task_id='send_retry_summary',
        to=['qa-alerts@pibythree.com'],
        subject='Retry summary {{ dag_id }} {{ run_id }}',
        html_content='<p>Run state {{ state }}</p>',
        trigger_rule='all_done',
    )
    t10 = PythonOperator(task_id='cleanup', python_callable=stable, trigger_rule='all_done')

    t01 >> t02 >> t03 >> t04 >> t05
    t05 >> [t06, t07]
    [t06, t07] >> t08 >> t09 >> t10

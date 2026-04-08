
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def setup(**context):
    return 'setup_complete'


def make_batch_task(n):
    def _task(**context):
        print(f'Executing batch {n}')
        return {'batch': n, 'status': 'ok'}
    return _task


def join_batches(**context):
    print('Joining parallel batches')


def finish(**context):
    return 'parallel_batches_done'


with DAG(
    dag_id='reg_parallel_batches_dag',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={'snowflake_conn_id': 'snowflake_default', 'retries': 1, 'retry_delay_seconds': 15},
) as dag:
    t01 = PythonOperator(task_id='setup', python_callable=setup)
    t02 = SnowflakeOperator(task_id='snapshot_source', sql='SELECT CURRENT_TIMESTAMP();')

    batch_tasks = [
        PythonOperator(task_id=f'batch_task_{i}', python_callable=make_batch_task(i), trigger_rule='always')
        for i in range(1, 8)
    ]

    t10 = PythonOperator(task_id='join_batches', python_callable=join_batches, trigger_rule='none_failed')
    t11 = SnowflakeOperator(task_id='mark_parallel_complete', sql="CALL PI_FLOW_QA.SP_MARK_BATCH_COMPLETE('ALL');")
    t12 = SnowflakeOperator(task_id='write_parallel_audit', sql='SELECT CURRENT_USER();', trigger_rule='all_done')
    t13 = PythonOperator(task_id='finish', python_callable=finish, trigger_rule='all_done')

    t01 >> t02
    for bt in batch_tasks:
        t02 >> bt
        bt >> t10
    t10 >> t11 >> t12 >> t13

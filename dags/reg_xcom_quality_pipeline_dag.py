
from datetime import datetime
from dag_parser.dynamic.params import Param
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def extract_customer(**context):
    return {'dataset': 'customer', 'rows': 1000}


def extract_order(**context):
    return {'dataset': 'order', 'rows': 5000}


def profile_customer(**context):
    ti = context['ti']
    base = ti.xcom_pull(task_ids='extract_customer')
    ti.xcom_push(key='customer_quality', value={'dataset': base['dataset'], 'score': 97.5})
    return 'customer_profiled'


def profile_order(**context):
    ti = context['ti']
    base = ti.xcom_pull(task_ids='extract_order')
    ti.xcom_push(key='order_quality', value={'dataset': base['dataset'], 'score': 96.0})
    return 'order_profiled'


def consolidate(**context):
    ti = context['ti']
    cq = ti.xcom_pull(task_ids='profile_customer', key='customer_quality')
    oq = ti.xcom_pull(task_ids='profile_order', key='order_quality')
    score = (cq['score'] + oq['score']) / 2
    ti.xcom_push(key='final_quality_score', value=score)
    return {'score': score}


def finish(**context):
    print('Quality pipeline completed')


with DAG(
    dag_id='reg_xcom_quality_pipeline_dag',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        'dataset_name': Param(type='string', default='composite_dataset', description='Dataset name'),
    },
    default_args={'snowflake_conn_id': 'snowflake_default', 'retries': 1, 'retry_delay_seconds': 20},
) as dag:
    t01 = PythonOperator(task_id='extract_customer', python_callable=extract_customer)
    t02 = PythonOperator(task_id='extract_order', python_callable=extract_order)
    t03 = PythonOperator(task_id='profile_customer', python_callable=profile_customer)
    t04 = PythonOperator(task_id='profile_order', python_callable=profile_order)
    t05 = SnowflakeOperator(task_id='run_sql_null_checks', sql='SELECT 1;')
    t06 = SnowflakeOperator(task_id='run_sql_dup_checks', sql='SELECT 1;')
    t07 = PythonOperator(task_id='consolidate_scores', python_callable=consolidate)
    t08 = SnowflakeOperator(
        task_id='persist_scorecard',
        sql="CALL PI_FLOW_QA.SP_PERSIST_SCORECARD('REG_RUN', 'composite_dataset', 95.5);",
        trigger_rule='none_failed',
    )
    t09 = SnowflakeOperator(task_id='publish_quality_metrics', sql='SELECT CURRENT_TIMESTAMP();')
    t10 = SnowflakeOperator(task_id='audit_quality', sql='SELECT CURRENT_USER();', trigger_rule='all_done')
    t11 = PythonOperator(task_id='finish', python_callable=finish, trigger_rule='all_done')

    [t01, t02] >> [t03, t04]
    [t03, t04] >> t05 >> t06 >> t07 >> t08 >> t09 >> t10 >> t11

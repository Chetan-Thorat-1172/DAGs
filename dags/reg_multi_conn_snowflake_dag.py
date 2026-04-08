
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, SnowflakeOperator, PythonOperator


def summarize(**context):
    return 'multi_conn_done'


with DAG(
    dag_id='reg_multi_conn_snowflake_dag',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={'snowflake_conn_id': 'snowflake_default', 'retries': 1, 'retry_delay_seconds': 15},
) as dag:
    t01 = SnowflakeOperator(task_id='default_conn_check_1', sql="CALL PI_FLOW_QA.SP_CONN_HEALTHCHECK('snowflake_default');")
    t02 = SnowflakeOperator(task_id='default_conn_check_2', sql='SELECT CURRENT_TIMESTAMP();')
    t03 = SnowflakeOperator(task_id='alt_conn_check_1', sql="CALL PI_FLOW_QA.SP_CONN_HEALTHCHECK('Dipak_Snowflake_Conn');", connection_id='Dipak_Snowflake_Conn')
    t04 = SnowflakeOperator(task_id='alt_conn_check_2', sql='SELECT CURRENT_USER();', connection_id='Dipak_Snowflake_Conn')
    t05 = SnowflakeOperator(task_id='alt_conn_check_3', sql='SELECT CURRENT_ROLE();', connection_id='Dipak_Snowflake_Conn')
    t06 = SnowflakeOperator(task_id='default_conn_check_3', sql='SELECT CURRENT_DATABASE();')
    t07 = SnowflakeOperator(task_id='default_conn_check_4', sql='SELECT CURRENT_SCHEMA();')
    t08 = SnowflakeOperator(task_id='write_conn_audit_1', sql='SELECT 1;', trigger_rule='all_done')
    t09 = SnowflakeOperator(task_id='write_conn_audit_2', sql='SELECT 1;', trigger_rule='all_done')
    t10 = SnowflakeOperator(task_id='write_conn_audit_3', sql='SELECT 1;', trigger_rule='all_done')
    t11 = PythonOperator(task_id='summarize', python_callable=summarize, trigger_rule='all_done')

    t01 >> t02 >> t03 >> t04 >> t05 >> t06 >> t07 >> t08 >> t09 >> t10 >> t11

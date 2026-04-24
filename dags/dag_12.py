from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def marker(name, **context):
    print(f"dag_12 {name}")


with DAG(
    dag_id="dag_12",
    schedule_interval="30 17 * * *",
    start_date=datetime(2026, 4, 24),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 12 with 20 tasks and strong parallel fanout",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="sp_pre_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_12_pre_1');")
    t03 = SnowflakeOperator(task_id="sp_pre_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_12_pre_2');")

    t04 = SnowflakeOperator(task_id="lane_a_sp_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_12_lane_a_1');")
    t05 = SnowflakeOperator(task_id="lane_a_sp_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_12_lane_a_2');")

    t06 = SnowflakeOperator(task_id="lane_b_sp_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_12_lane_b_1');")
    t07 = SnowflakeOperator(task_id="lane_b_sp_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_12_lane_b_2');")

    t08 = SnowflakeOperator(task_id="lane_c_notebook", sql="""EXECUTE NOTEBOOK PROJECT TESTING.PI_FLOW_LOAD_TEST.NB_LT_PROJECT
MAIN_FILE = 'NB_LT_PROJECT.ipynb'
COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
QUERY_WAREHOUSE = 'COMPUTE_WH'
RUNTIME = 'V2.2-CPU-PY3.10'""")
    t09 = SnowflakeOperator(task_id="lane_c_sp", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_12_lane_c_sp');")

    t10 = SnowflakeOperator(task_id="lane_d_dbt", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")
    t11 = SnowflakeOperator(task_id="lane_d_sp", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_12_lane_d_sp');")

    t12 = SnowflakeOperator(task_id="lane_e_sp_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_12_lane_e_1');")
    t13 = SnowflakeOperator(task_id="lane_e_sp_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_12_lane_e_2');")

    t14 = SnowflakeOperator(task_id="join_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_12_join_1');", trigger_rule="none_failed_min_one_success")
    t15 = SnowflakeOperator(task_id="join_branch_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_12_join_branch_a');")
    t16 = SnowflakeOperator(task_id="join_branch_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_12_join_branch_b');")
    t17 = SnowflakeOperator(task_id="join_branch_c", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_12_join_branch_c');")
    t18 = SnowflakeOperator(task_id="join_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_12_join_2');", trigger_rule="none_failed_min_one_success")
    t19 = SnowflakeOperator(task_id="final_audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_12_final_audit');", trigger_rule="all_done")
    t20 = PythonOperator(task_id="finish", python_callable=lambda **c: marker("finish", **c), trigger_rule="all_done")

    t01 >> t02 >> t03
    t03 >> [t04, t06, t08, t10, t12]
    t04 >> t05 >> t14
    t06 >> t07 >> t14
    t08 >> t09 >> t14
    t10 >> t11 >> t14
    t12 >> t13 >> t14
    t14 >> [t15, t16, t17]
    [t15, t16, t17] >> t18 >> t19 >> t20

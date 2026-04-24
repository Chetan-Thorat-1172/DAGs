from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BranchPythonOperator, SnowflakeOperator


def choose_notebook_path(**context):
    run_id = context.get("run_id", "")
    return "nb_path_a_1" if (sum(ord(c) for c in run_id) % 2 == 0) else "nb_path_b_1"


def marker(name, **context):
    print(f"dag_5 {name}")


with DAG(
    dag_id="dag_5",
    schedule_interval="15 11 * * *",
    start_date=datetime(2026, 4, 24),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 5 with 15 tasks and notebook branch topology",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="prepare_inputs", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_5_prepare_inputs');")
    t03 = SnowflakeOperator(task_id="run_notebook", sql="""EXECUTE NOTEBOOK PROJECT TESTING.PI_FLOW_LOAD_TEST.NB_LT_PROJECT
MAIN_FILE = 'NB_LT_PROJECT.ipynb'
COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
QUERY_WAREHOUSE = 'COMPUTE_WH'
RUNTIME = 'V2.2-CPU-PY3.10'""")
    t04 = BranchPythonOperator(task_id="branch_notebook_variant", python_callable=choose_notebook_path)

    t05 = SnowflakeOperator(task_id="nb_path_a_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_5_nb_path_a_1');")
    t06 = SnowflakeOperator(task_id="nb_path_a_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_5_nb_path_a_2');")
    t07 = SnowflakeOperator(task_id="nb_path_a_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_5_nb_path_a_3');")

    t08 = SnowflakeOperator(task_id="nb_path_b_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_5_nb_path_b_1');")
    t09 = SnowflakeOperator(task_id="nb_path_b_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_5_nb_path_b_2');")
    t10 = SnowflakeOperator(task_id="nb_path_b_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_5_nb_path_b_3');")

    t11 = SnowflakeOperator(task_id="join_variant", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_5_join_variant');", trigger_rule="none_failed_min_one_success")
    t12 = SnowflakeOperator(task_id="post_nb_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_5_post_nb_a');")
    t13 = SnowflakeOperator(task_id="post_nb_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_5_post_nb_b');")
    t14 = SnowflakeOperator(task_id="final_nb_audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_5_final_nb_audit');", trigger_rule="all_done")
    t15 = PythonOperator(task_id="end", python_callable=lambda **c: marker("end", **c), trigger_rule="all_done")

    t01 >> t02 >> t03 >> t04
    t04 >> [t05, t08]
    t05 >> t06 >> t07 >> t11
    t08 >> t09 >> t10 >> t11
    t11 >> [t12, t13] >> t14 >> t15


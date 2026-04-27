from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator, SmtpNotifier


retry_notifier = SmtpNotifier(
    to=["nemer33891@hacknapp.com"],
    subject="dag_3 retry {{ dag_id }}.{{ task_id }} try={{ try_number }}",
    html_content="<p>Retry event for dag_3</p>",
)


failure_notifier = SmtpNotifier(
    to=["nemer33891@hacknapp.com"],
    subject="dag_3 failure {{ dag_id }}.{{ task_id }}",
    html_content="<p>Failure event for dag_3</p>",
)


def flaky_once(**context):
    if context["ti"].try_number < 2:
        raise Exception("dag_3 flaky_once failure on first try")
    print("dag_3 flaky_once recovered")


def marker(name, **context):
    print(f"dag_3 {name}")


with DAG(
    dag_id="dag_3",
    schedule_interval="45 16 * * *",
    start_date=datetime(2026, 4, 27),
    catchup=False,
    default_args={
        "snowflake_conn_id": "harsh_conn",
        "retries": 1,
        "retry_delay_seconds": 5,
        "on_retry_callback": retry_notifier,
        "on_failure_callback": failure_notifier,
    },
    on_failure_callback=failure_notifier,
    description="Load test DAG 3 with 15 tasks focused on retries and parallel joins",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="sp_precheck", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_3_precheck');")
    t03 = PythonOperator(task_id="flaky_transform", python_callable=flaky_once)
    t04 = SnowflakeOperator(task_id="retry_audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_3_retry_audit');", trigger_rule="all_done")

    t05 = SnowflakeOperator(task_id="branch_a_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_3_branch_a_1');")
    t06 = SnowflakeOperator(task_id="branch_a_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_3_branch_a_2');")
    t07 = SnowflakeOperator(task_id="branch_a_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_3_branch_a_3');")

    t08 = SnowflakeOperator(task_id="branch_b_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_3_branch_b_1');")
    t09 = SnowflakeOperator(task_id="branch_b_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_3_branch_b_2');")
    t10 = SnowflakeOperator(task_id="branch_b_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_3_branch_b_3');")

    t11 = SnowflakeOperator(task_id="join_parallel", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_3_join');", trigger_rule="none_failed_min_one_success")
    t12 = SnowflakeOperator(task_id="publish_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_3_publish_a');")
    t13 = SnowflakeOperator(task_id="publish_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_3_publish_b');")
    t14 = SnowflakeOperator(task_id="post_audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_3_post_audit');", trigger_rule="all_done")
    t15 = PythonOperator(task_id="finalize", python_callable=lambda **c: marker("finalize", **c), trigger_rule="all_done")

    t01 >> t02 >> t03 >> t04
    t04 >> [t05, t08]
    t05 >> t06 >> t07 >> t11
    t08 >> t09 >> t10 >> t11
    t11 >> [t12, t13] >> t14 >> t15


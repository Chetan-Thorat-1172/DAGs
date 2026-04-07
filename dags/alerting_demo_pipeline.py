""" n
Pi-Flow Alerting Demo DAG 33   

This DAG tests:   

1. Task-level callbacks:
   - on_execute
   - on_retry
   - on_failure
   - on_success

2. DAG-level callbacks:
   - on_success_callback
   - on_failure_callback

3. default_args inheritance

4. EmailOperator execution

Template variables supported:
{{ dag_id }}, {{ task_id }}, {{ run_id }},
{{ try_number }}, {{ state }}, {{ exception }}
"""

from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, EmailOperator , SmtpNotifier

# # ─────────────────────────────────────────────────────────
# # Notifier Instances
# # ─────────────────────────────────────────────────────────

failure_notifier = SmtpNotifier(
    to=["Chetan.Thorat@Pibythree.com"],
    subject="❌ Task Failed: {{ dag_id }}.{{ task_id }}",
    html_content="""
    <h3>Task Failure</h3>
    <p>DAG: {{ dag_id }}</p>
    <p>Task: {{ task_id }}</p>
    <p>Run ID: {{ run_id }}</p>
    <p>Attempt: {{ try_number }}</p>
    <p>Error: {{ exception }}</p>
    """,
)

retry_notifier = SmtpNotifier(
    to=["Chetan.Thorat@Pibythree.com"],
    subject="🔄 Retry Triggered: {{ dag_id }}.{{ task_id }} (Attempt {{ try_number }})",
    html_content="<p>Task is retrying.</p>",
)

success_notifier = SmtpNotifier(
    to=["Chetan.Thorat@Pibythree.com"],
    subject="✅ Task Success: {{ dag_id }}.{{ task_id }}",
    html_content="<p>Task completed successfully.</p>",
)

dag_success_notifier = SmtpNotifier(
    to=["Chetan.Thorat@Pibythree.com"],
    subject="🎉 DAG Completed: {{ dag_id }}",
    html_content="<p>DAG {{ dag_id }} finished successfully.</p>",
)

dag_failure_notifier = SmtpNotifier(
    to=["Chetan.Thorat@Pibythree.com"],
    subject="🚨 DAG Failed: {{ dag_id }}",
    html_content="<p>DAG {{ dag_id }} has failed.</p>",
)


# ─────────────────────────────────────────────────────────
# Test Functions
# ─────────────────────────────────────────────────────────

def extract_task(**context):
    print("Extracting data...")
    print("Simulating success.")


def transform_task(**context):
    print("Transforming data...")
    print("Simulating retry scenario.")

    # Simulate failure for first attempt only
    if context["try_number"] < 2:
        raise Exception("Simulated transformation error")


def load_task(**context):
    print("Loading data...")
    print("Simulating success.")


# ─────────────────────────────────────────────────────────
# default_args (inherit to all tasks)
# ─────────────────────────────────────────────────────────

default_args = {
    "owner": "data-team",
    "retries": 1,   # So transform retries once
    "retry_delay_seconds": 30,
    "on_failure_callback": failure_notifier,
    "on_retry_callback": retry_notifier,
}


# ─────────────────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────────────────

with DAG(
    dag_id="alerting_demo_pipeline",
    schedule_interval=None,
    start_date=datetime(2026, 3, 2),
    catchup=False,
    description="Demo pipeline showcasing Pi-Flow alerting system",
    tags=["alerting", "demo"],
    default_args=default_args,
    on_success_callback=dag_success_notifier,
    on_failure_callback=dag_failure_notifier,
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_task,
        on_success_callback=success_notifier,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_task,
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_task,
        on_success_callback=success_notifier,
    )

    send_report = EmailOperator(
        task_id="send_summary_email",
        to=["Chetan.Thorat@Pibythree.com"],
        subject="📊 DAG {{ dag_id }} Report",
        html_content="""
        <h2>DAG Execution Summary</h2>
        <p>Run ID: {{ run_id }}</p>
        <p>Status: {{ state }}</p>
        """,
    )

    # Dependencies
    extract >> transform >> load >> send_report



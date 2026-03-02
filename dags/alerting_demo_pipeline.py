"""
This DAG showcases all major alerting features:
  1. SmtpNotifier — send emails on task/DAG lifecycle events
  2. Task-level callbacks — on_execute, on_success, on_failure, on_retry
  3. DAG-level callbacks — on_success_callback, on_failure_callback
  4. default_args inheritance — define callbacks once, apply to all tasks
  5. EmailOperator — dedicated task for sending emails

Template variables available in subject/body:
  {{ dag_id }}, {{ task_id }}, {{ run_id }}, {{ execution_date }},
  {{ try_number }}, {{ state }}, {{ exception }}
"""

from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, EmailOperator, SmtpNotifier

# ── Notifier instances ───────────────────────────────────────────────────────

# Alert ops team on any task failure (with retry info)
failure_notifier = SmtpNotifier(
    to="thoratc146@gmail.com",
    subject="❌ Pi-Flow Task Failed: {{ dag_id }}.{{ task_id }}",
    html_content="""
    <h2>Task Failure Alert</h2>
    <table>
      <tr><td><b>DAG</b></td><td>{{ dag_id }}</td></tr>
      <tr><td><b>Task</b></td><td>{{ task_id }}</td></tr>
      <tr><td><b>Run ID</b></td><td>{{ run_id }}</td></tr>
      <tr><td><b>Attempt</b></td><td>{{ try_number }}</td></tr>
      <tr><td><b>State</b></td><td>{{ state }}</td></tr>
      <tr><td><b>Error</b></td><td>{{ exception }}</td></tr>
    </table>
    """,
    cc="chetanthorat146@gmail.com",
)

# Notify on successful completion
success_notifier = SmtpNotifier(
    to="thoratc146@gmail.com",
    subject="✅ Pi-Flow Task Success: {{ dag_id }}.{{ task_id }}",
    html_content="<p>Task <b>{{ task_id }}</b> completed successfully in DAG <b>{{ dag_id }}</b>.</p>",
)

# Notify when a retry is scheduled
retry_notifier = SmtpNotifier(
    to="thoratc146@gmail.com",
    subject="🔄 Pi-Flow Task Retry: {{ dag_id }}.{{ task_id }} (attempt {{ try_number }})",
    html_content="<p>Task <b>{{ task_id }}</b> failed and will be retried. Attempt: {{ try_number }}.</p>",
)

# DAG-level: notify when the entire pipeline succeeds
dag_success_notifier = SmtpNotifier(
    to="thoratc146@gmail.com",
    subject="🎉 Pipeline Complete: {{ dag_id }}",
    html_content="<p>DAG <b>{{ dag_id }}</b> (run {{ run_id }}) completed successfully.</p>",
)

# DAG-level: notify when the pipeline fails
dag_failure_notifier = SmtpNotifier(
    to="thoratc146@gmail.com",
    subject="🚨 Pipeline Failed: {{ dag_id }}",
    html_content="""
    <h2>Pipeline Failure</h2>
    <p>DAG <b>{{ dag_id }}</b> (run {{ run_id }}) has failed.</p>
    <p>Please check the Pi-Flow dashboard for details.</p>
    """,
    cc="chetanthorat146@gmail.com",
)


# ── default_args: callbacks inherited by all tasks ───────────────────────────

default_args = {
    "owner": "data=team",
    "retries": 2,
    "retry_delay_seconds": 60,
    "on_failure_callback": failure_notifier,
    "on_retry_callback": retry_notifier,
}


# ── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="alerting_demo_pipeline",
    schedule_interval=None,           
    start_date=datetime(2026,3,2),
    description="Demo pipeline showcasing Pi-Flow alerting system",
    tags="alerting,demo",
    default_args=default_args,
    # DAG-level callbacks — fired when the entire DAG run completes
    on_success_callback=dag_success_notifier,
    on_failure_callback=dag_failure_notifier,
) as dag:

    # ── Task 1: Extract data (inherits failure + retry callbacks from default_args)
    extract = PythonOperator(
        task_id="extract_data",
        python_callable="extract_from_source",
        op_kwargs={"source": "salesforce", "table": "accounts"},
    )

    # ── Task 2: Transform (adds a per-task success callback)
    transform = PythonOperator(
        task_id="transform_data",
        python_callable="run_transformations",
        op_kwargs={"model": "dim_accounts"},
        on_success_callback=success_notifier,  # Task-specific override
    )

    # ── Task 3: Load into warehouse
    load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable="load_to_snowflake",
        op_kwargs={"target_table": "DIM_ACCOUNTS"},
        on_success_callback=success_notifier,
    )

    # ── Task 4: Send a summary report via EmailOperator
    send_report = EmailOperator(
        task_id="send_daily_report",
        to="stakeholders@company.com",
        subject="📊 Daily Account Sync Report — {{ dag_id }}",
        html_content="""
        <h2>Daily Account Sync Complete</h2>
        <p>The account synchronization pipeline has finished.</p>
        <p>Run ID: {{ run_id }}</p>
        """,
        cc="data-eng@company.com",
    )

    # ── Dependencies ─────────────────────────────────────────────────────────
    extract >> transform >> load >> send_report

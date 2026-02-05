from dag_parser.dynamic.dag_context import (
    DAG,
    SnowflakeOperator,
)
from datetime import datetime

with DAG(
    dag_id="etl_snowflake_complex_dag",
    schedule_interval="@daily",
    start_date=datetime(2026, 2, 4),
    catchup=False
) as dag:

    # Step 1: Extract raw data
    extract_orders = SnowflakeOperator(
        task_id="extract_orders",
        sql="""
        SELECT *
        FROM PI_FLOW.RAW.ORDERS
        WHERE order_date = CURRENT_DATE() - 1;
        """,
        connection_id="snowflake_default"
    )

    # Step 2a: Validate data
    validate_orders = SnowflakeOperator(
        task_id="validate_orders",
        sql="""
        SELECT COUNT(*) AS invalid_orders
        FROM PI_FLOW.RAW.ORDERS
        WHERE order_amount IS NULL
           OR customer_id IS NULL;
        """,
        connection_id="snowflake_default"
    )

    # Step 2b: Enrich data
    enrich_orders = SnowflakeOperator(
        task_id="enrich_orders",
        sql="""
        SELECT
            o.order_id,
            o.customer_id,
            o.order_amount,
            c.customer_segment
        FROM PI_FLOW.RAW.ORDERS o
        JOIN PI_FLOW.RAW.CUSTOMERS c
          ON o.customer_id = c.customer_id;
        """,
        connection_id="snowflake_default"
    )

    # Step 3: Quality check after enrichment
    check_quality = SnowflakeOperator(
        task_id="check_quality",
        sql="""
        SELECT COUNT(*) AS bad_records
        FROM PI_FLOW.RAW.ORDERS
        WHERE order_amount < 0;
        """,
        connection_id="snowflake_default"
    )

    # Step 4: Load curated data
    load_orders = SnowflakeOperator(
        task_id="load_orders",
        sql="""
        INSERT INTO PI_FLOW.CURATED.ORDERS_CLEAN
        SELECT *
        FROM PI_FLOW.RAW.ORDERS
        WHERE order_amount >= 0;
        """,
        connection_id="snowflake_default"
    )

    # Step 5: Aggregate metrics (fan-in)
    aggregate_metrics = SnowflakeOperator(
        task_id="aggregate_metrics",
        sql="""
        SELECT
            COUNT(*) AS total_orders,
            SUM(order_amount) AS total_revenue
        FROM PI_FLOW.CURATED.ORDERS_CLEAN;
        """,
        connection_id="snowflake_default"
    )

    # Step 6: Publish report
    publish_report = SnowflakeOperator(
        task_id="publish_report",
        sql="""
        INSERT INTO PI_FLOW.REPORTING.DAILY_ORDER_SUMMARY
        SELECT
            CURRENT_DATE() AS report_date,
            COUNT(*) AS total_orders,
            SUM(order_amount) AS total_revenue
        FROM PI_FLOW.CURATED.ORDERS_CLEAN;
        """,
        connection_id="snowflake_default"
    )

    # ------------------
    # Dependency graph
    # ------------------

    extract_orders >> [validate_orders, enrich_orders]

    enrich_orders >> check_quality >> load_orders

    [validate_orders, load_orders] >> aggregate_metrics

    aggregate_metrics >> publish_report

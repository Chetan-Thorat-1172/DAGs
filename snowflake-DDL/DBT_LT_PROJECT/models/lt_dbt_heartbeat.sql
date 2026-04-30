SELECT
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS run_ts,
    'dbt_lt_project' AS project_name,
    'OK' AS status

import time
from snowflake.snowpark.context import get_active_session

session = get_active_session()

time.sleep(2)

session.sql("""
    INSERT INTO TESTING.PI_FLOW_LOAD_TEST.LT_INVOCATION_AUDIT(dag_task_name, note)
    VALUES ('NB_LT_PROJECT', 'notebook invoked')
""").collect()


print('OK: NB_LT_PROJECT')

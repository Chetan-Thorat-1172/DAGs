-- =============================================
-- PREREQUISITE: Set and verify your email
-- =============================================
-- Step 0a: Set email on your user
ALTER USER PI_USER SET EMAIL = 'vasudeva.varna@pibythree.com';

-- Step 0b: After running the above, go to Snowsight:
--   Profile (top-left username) → Profile → Verify your email
--   Click the verification link sent to your email.
--   THEN run the remaining steps below.

-- =============================================
-- BUDGET SETUP (run after email is verified)
-- =============================================

-- Step 1: Activate the account budget
USE ROLE ACCOUNTADMIN;

CALL SNOWFLAKE.LOCAL.ACCOUNT_ROOT_BUDGET!ACTIVATE();

-- Step 2: Set spending limit (in credits, not dollars)
CALL SNOWFLAKE.LOCAL.ACCOUNT_ROOT_BUDGET!SET_SPENDING_LIMIT(100);

-- Step 3: Set up email notifications
CALL SNOWFLAKE.LOCAL.ACCOUNT_ROOT_BUDGET!SET_EMAIL_NOTIFICATIONS(
  'vasudeva.varna@pibythree.com'
);

-- Step 4: Set notification threshold at 83% (~$250 of $300)
CALL SNOWFLAKE.LOCAL.ACCOUNT_ROOT_BUDGET!SET_NOTIFICATION_THRESHOLD(95);


-- WE HAVE BUILT BUDGETS FOR IT, WHEN TOTAL CREDITS CONSUMED IN ACCOUNT REACHES 95% OF 100 CREDITS (300$) ALERT GOES TO VASUDEVA OFFICIAL MAIL

-- Add smtp.gmail.com:587 to the existing network rule
ALTER NETWORK RULE PI_FLOW.PUBLIC.PIFLOW_EGRESS_RULE
SET VALUE_LIST = (
  'github.com',
  'api.github.com',
  'codeload.github.com',
  'objects.githubusercontent.com',
  'raw.githubusercontent.com',
  'z5d5d5xjgnejtdouufjunu7pkm.yeqmywd-gxb37208.us-west-2.aws.postgres.snowflake.app:5432',
  '*.snowflakecomputing.com:443',
  'smtp.gmail.com:587'
);

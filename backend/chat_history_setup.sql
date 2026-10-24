-- One-off setup for saved chat history (CHAT_HISTORY_DAYS in .env). Run as SYSADMIN.
--
-- Every viewer signs in with their own Snowflake user, and the app reads and writes
-- history with that user's session. So all users need SELECT/INSERT on the table, and a
-- row access policy makes sure each one only ever sees their own rows (saved answers can
-- hold data that the author's own row access policies allowed).
--
-- The app only ever reads the last CHAT_HISTORY_DAYS days; the purge task is a storage backstop.

CREATE SCHEMA IF NOT EXISTS PHARMA_DEMO.CHAT_APP;

CREATE TABLE IF NOT EXISTS PHARMA_DEMO.CHAT_APP.USER_CHAT_HISTORY (
  MESSAGE_ID      STRING DEFAULT UUID_STRING(),
  USER_ID         STRING,         -- Snowflake user (CURRENT_USER() of the signed-in viewer)
  CONVERSATION_ID STRING,
  AGENT_ID        STRING,         -- e.g. PHARMA / SALES
  DASHBOARD       STRING,         -- Tableau dashboard name, when running as an extension
  QUESTION        STRING,
  ANSWER          STRING,
  SQL_TEXT        STRING,
  RESULT_DATA     VARIANT,        -- capped at the first 200 rows
  CHART_SPEC      VARIANT,
  CREATED_AT      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
) DATA_RETENTION_TIME_IN_DAYS = 1;

-- Each user sees only their own rows. SYSADMIN (the owner, which runs the purge task)
-- sees all rows so the purge can delete them.
CREATE ROW ACCESS POLICY IF NOT EXISTS PHARMA_DEMO.CHAT_APP.OWN_CHAT_HISTORY
  AS (USER_ID STRING) RETURNS BOOLEAN ->
  USER_ID = CURRENT_USER() OR IS_ROLE_IN_SESSION('SYSADMIN');

ALTER TABLE PHARMA_DEMO.CHAT_APP.USER_CHAT_HISTORY
  ADD ROW ACCESS POLICY PHARMA_DEMO.CHAT_APP.OWN_CHAT_HISTORY ON (USER_ID);

-- Every role inherits PUBLIC, so any signed-in user can save and read their own history.
-- Replace PUBLIC with a specific role to limit who can use it.
GRANT USAGE ON DATABASE PHARMA_DEMO TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA PHARMA_DEMO.CHAT_APP TO ROLE PUBLIC;
GRANT SELECT, INSERT ON TABLE PHARMA_DEMO.CHAT_APP.USER_CHAT_HISTORY TO ROLE PUBLIC;

CREATE TASK IF NOT EXISTS PHARMA_DEMO.CHAT_APP.PURGE_USER_CHAT_HISTORY
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS
  DELETE FROM PHARMA_DEMO.CHAT_APP.USER_CHAT_HISTORY
  WHERE CREATED_AT < DATEADD(day, -7, CURRENT_TIMESTAMP());

ALTER TASK PHARMA_DEMO.CHAT_APP.PURGE_USER_CHAT_HISTORY RESUME;

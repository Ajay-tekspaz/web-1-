-- K-11 evidence: the queries of one dbt run and their tags.
-- Set the invocation id from dbt/target/run_results.json -> metadata.invocation_id.
-- ACCOUNT_USAGE lags up to 45 minutes; INFORMATION_SCHEMA.QUERY_HISTORY is immediate (last 7 days).

SET dbt_invocation_id = '<invocation_id>';

SELECT
    query_id,
    start_time,
    warehouse_name,
    execution_status,
    TRY_PARSE_JSON(query_tag):node_id::string AS dbt_node,
    TRY_PARSE_JSON(query_tag):dag_id::string AS dag_id,
    TRY_PARSE_JSON(query_tag):task_id::string AS task_id,
    query_tag
FROM TABLE(DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_DEV.INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 10000))
WHERE TRY_PARSE_JSON(query_tag):invocation_id::string = $dbt_invocation_id
ORDER BY start_time;

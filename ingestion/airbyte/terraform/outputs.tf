output "connection_id" {
  description = "Set as AIRBYTE_CONNECTION_ID for the Airflow DAG"
  value       = airbyte_connection.retail_to_dev_sandbox.connection_id
}

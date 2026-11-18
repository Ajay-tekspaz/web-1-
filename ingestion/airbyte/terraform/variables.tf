variable "airbyte_server_url" {
  type    = string
  default = "http://localhost:8000/api/public/v1/"
}

variable "airbyte_client_id" {
  type      = string
  sensitive = true
}

variable "airbyte_client_secret" {
  type      = string
  sensitive = true
}

variable "workspace_id" {
  description = "Airbyte workspace (abctl creates a default one; see the Airbyte UI URL)"
  type        = string
}

# Source: the Postgres seeded by python -m loaders.postgres_seeder (ingestion/postgres)
variable "source_postgres_host" {
  description = "Host as seen from Airbyte's pods; host.docker.internal reaches the docker-compose Postgres"
  type        = string
  default     = "host.docker.internal"
}

variable "source_postgres_port" {
  type    = number
  default = 5433
}

variable "source_postgres_password" {
  type      = string
  sensitive = true
}

# Destination: Snowflake DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX (raw schema), key-pair auth
variable "snowflake_host" {
  description = "<account>.snowflakecomputing.com"
  type        = string
}

variable "snowflake_user" {
  type    = string
  default = "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_AIRBYTE"
}

variable "snowflake_private_key" {
  description = "PEM private key for snowflake_user"
  type        = string
  sensitive   = true
}

variable "sync_cron" {
  description = "Quartz cron for the sync when Airflow is not driving it; Airflow triggers it before dbt anyway"
  type        = string
  default     = "0 30 1 * * ?"
}

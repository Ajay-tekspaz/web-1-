# K-15: Postgres (retail operational DB) -> Snowflake DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_DEV.DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX, as code.

locals {
  # Facts arrive daily and only grow: incremental append on ingested_at keeps raw faithful
  # (the D2 duplicate is still there for dbt to catch). Small dimensions are reloaded whole.
  incremental_streams  = ["orders_raw", "order_lines_raw", "inventory_raw"]
  full_refresh_streams = ["stores_raw", "products_raw", "promotions_raw"]
}

data "airbyte_connector_configuration" "postgres" {
  connector_name    = "source-postgres"
  connector_version = "3.8.9"

  configuration = {
    host               = var.source_postgres_host
    port               = var.source_postgres_port
    database           = "daily_expense_tracker_fintech_application_source"
    username           = "retail"
    schemas            = ["retail"]
    ssl_mode           = { mode = "disable" }
    tunnel_method      = { tunnel_method = "NO_TUNNEL" }
    replication_method = { method = "Standard" } # user-defined cursor (ingested_at)
  }

  configuration_secrets = {
    password = var.source_postgres_password
  }
}

resource "airbyte_source" "retail_postgres" {
  name          = "daily-expense-tracker-fintech-application: daily_expense_tracker_fintech_application_source (Postgres)"
  workspace_id  = var.workspace_id
  definition_id = data.airbyte_connector_configuration.postgres.definition_id
  configuration = data.airbyte_connector_configuration.postgres.configuration_json
}

data "airbyte_connector_configuration" "snowflake" {
  connector_name    = "destination-snowflake"
  connector_version = "5.2.0"

  configuration = {
    host      = var.snowflake_host
    role      = "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_ROLE"
    warehouse = "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_XS"
    database  = "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_DEV"
    schema    = "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX"
    username  = var.snowflake_user
    # Query tag so TeksOptima attributes ingestion cost to Airbyte
    jdbc_url_params = "query_tag={\"tool\":\"airbyte\",\"project\":\"daily-expense-tracker-fintech-application\",\"environment\":\"dev\"}"
  }

  configuration_secrets = {
    credentials = {
      auth_type   = "Key Pair Authentication"
      private_key = var.snowflake_private_key
    }
  }
}

resource "airbyte_destination" "snowflake_dev_sandbox" {
  name          = "daily-expense-tracker-fintech-application: Snowflake DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX"
  workspace_id  = var.workspace_id
  definition_id = data.airbyte_connector_configuration.snowflake.definition_id
  configuration = data.airbyte_connector_configuration.snowflake.configuration_json
}

resource "airbyte_connection" "retail_to_dev_sandbox" {
  name           = "daily-expense-tracker-fintech-application: daily_expense_tracker_fintech_application_source -> DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX"
  source_id      = airbyte_source.retail_postgres.source_id
  destination_id = airbyte_destination.snowflake_dev_sandbox.destination_id

  # Land tables directly in DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX with the same names the Python loader uses
  namespace_definition                 = "custom_format"
  namespace_format                     = "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX"
  non_breaking_schema_updates_behavior = "propagate_columns" # D4: a renamed column arrives as a new column
  status                               = "active"

  schedule = {
    schedule_type   = "cron"
    cron_expression = var.sync_cron
  }

  configurations = {
    streams = concat(
      [for s in local.incremental_streams : {
        name         = s
        sync_mode    = "incremental_append"
        cursor_field = ["ingested_at"]
      }],
      [for s in local.full_refresh_streams : {
        name      = s
        sync_mode = "full_refresh_overwrite"
      }],
    )
  }
}

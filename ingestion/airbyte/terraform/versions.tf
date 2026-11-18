terraform {
  required_version = ">= 1.6"
  required_providers {
    airbyte = {
      source  = "airbytehq/airbyte"
      version = "~> 1.3"
    }
  }
}

# Airbyte OSS installed with `abctl local install`: API at http://localhost:8000/api/public/v1,
# credentials from `abctl local credentials`.
provider "airbyte" {
  server_url    = var.airbyte_server_url
  client_id     = var.airbyte_client_id
  client_secret = var.airbyte_client_secret
}

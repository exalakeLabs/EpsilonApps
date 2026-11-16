terraform {
  required_version = ">= 1.5"
  required_providers {
    sigma = {
      source  = "civitaspo/sigma"
      version = "~> 0.1"
    }
  }
}

# Two Sigma orgs = two API client credentials. Create one API client per org
# (Administration > Developer Access) and pass them in via variables.
provider "sigma" {
  alias         = "source"
  base_url      = var.source_base_url
  client_id     = var.source_client_id
  client_secret = var.source_client_secret
}

provider "sigma" {
  alias         = "target"
  base_url      = var.target_base_url
  client_id     = var.target_client_id
  client_secret = var.target_client_secret
}

variable "source_base_url" {
  description = "Sigma API base URL for the source org's cloud/environment (see provider docs for per-cloud values)."
  type        = string
}

variable "target_base_url" {
  description = "Sigma API base URL for the target org. Usually identical to source_base_url when both orgs share an environment."
  type        = string
}

variable "source_client_id" {
  type      = string
  sensitive = true
}

variable "source_client_secret" {
  type      = string
  sensitive = true
}

variable "target_client_id" {
  type      = string
  sensitive = true
}

variable "target_client_secret" {
  type      = string
  sensitive = true
}

variable "workspace_include" {
  description = "Only copy workspaces with these names. Empty = all."
  type        = list(string)
  default     = []
}

variable "workspace_exclude" {
  description = "Workspace names to skip."
  type        = list(string)
  default     = []
}

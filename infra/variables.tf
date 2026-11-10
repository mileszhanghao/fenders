variable "region" {
  description = "AWS region for every resource."
  type        = string
  default     = "us-west-2"
}

variable "name" {
  description = "Prefix for resource names. The bucket becomes <name>-<account id>."
  type        = string
  default     = "fenders"
}

variable "github_repo" {
  description = "owner/repo allowed to assume the publisher role (main branch only)."
  type        = string
  default     = "mileszhanghao/fenders"
}

variable "create_github_oidc_provider" {
  description = "Create GitHub's OIDC provider. Set false if the account already has one (only one may exist)."
  type        = bool
  default     = true
}

variable "alert_email" {
  description = "Email that receives pipeline alerts. AWS sends a confirmation link first. Empty = no email."
  type        = string
  default     = ""
}

variable "max_age_hours" {
  description = "Alert if the last publish is older than this."
  type        = number
  default     = 36
}

variable "max_weather_lag_days" {
  description = "Alert if the weather data stops this many days before today."
  type        = number
  default     = 7
}

variable "snapshot_retention_days" {
  description = "Delete dated mart snapshots after this many days."
  type        = number
  default     = 180
}

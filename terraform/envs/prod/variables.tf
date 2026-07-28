variable "region" {
  type    = string
  default = "us-east-1"
}

variable "account_id" {
  description = "Real target AWS account (corrects the 914216784354 placeholder baked into the original repo files)"
  type        = string
  default     = "300617413029"
}

variable "youtube_api_key" {
  description = "The existing YouTube API key from key.txt, kept as-is per project decision. Pass via -var, never commit to a .tfvars file."
  type        = string
  sensitive   = true
}

variable "ingest_image_tag" {
  description = "Git SHA tagging the yt-ingest image to deploy. No default on purpose — CI always passes this explicitly. Independent per Lambda so each has its own release cycle."
  type        = string
}

variable "raw_transform_image_tag" {
  description = "Git SHA tagging the yt-raw-transform image to deploy. No default on purpose — CI always passes this explicitly."
  type        = string
}

variable "dbt_trigger_image_tag" {
  description = "Git SHA tagging the yt-dbt-trigger image to deploy. No default on purpose — CI always passes this explicitly."
  type        = string
}

variable "dbt_image_tag" {
  description = "Git SHA tagging the yt-dbt image (the actual dbt project container run as a Kubernetes Job) to deploy. No default on purpose — CI always passes this explicitly."
  type        = string
}

variable "eks_node_instance_type" {
  # See terraform/modules/eks/variables.tf for why this isn't t3.small anymore.
  type    = string
  default = "t3.medium"
}

variable "allowed_dashboard_cidr" {
  description = "CIDR allowed to reach the dashboard's NodePort (30080) on the EKS nodes. No default on purpose — the dashboard pod carries AWS read credentials via IRSA and must not be silently left open to the internet. Pass your office/VPN CIDR via -var or the DASHBOARD_ALLOWED_CIDR CI variable."
  type        = string
}

variable "alert_email" {
  description = "Email address subscribed to pipeline failure/success SNS alerts. No default on purpose — pass via -var or the ALERT_EMAIL CI variable. Requires clicking a confirmation link AWS sends to this address before alerts actually deliver."
  type        = string
}

variable "quicksight_user_arn" {
  description = "QuickSight user ARN to grant on the Athena data source/datasets. Leave empty to skip QuickSight provisioning (e.g. when QuickSight is not subscribed). Pass via -var or the QUICKSIGHT_USER_ARN CI variable when ready."
  type        = string
  default     = ""
}

variable "lakeformation_admin_arns" {
  description = "IAM principals to register as Lake Formation admins. Must include gha-deploy-role — see terraform/modules/lakeformation's header comment for why this can't be left to apply-time implicit admin. No default on purpose: this is exactly the kind of value that must never silently default to 'whoever is running this apply'."
  type        = list(string)
}

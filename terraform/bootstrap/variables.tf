variable "aws_region" {
  description = "AWS region for the state backend (should match envs/prod)"
  type        = string
  default     = "us-east-1"
}

variable "state_bucket_name" {
  description = "Globally-unique S3 bucket name for Terraform remote state"
  type        = string
  default     = "yt-pipeline-tfstate-us-east-1-300617413029"
}

variable "lock_table_name" {
  description = "DynamoDB table name for Terraform state locking"
  type        = string
  default     = "yt-pipeline-tfstate-lock"
}

variable "github_org" {
  description = "GitHub org/user that owns the repo"
  type        = string
  default     = "Muhammad-Hamza69"
}

variable "github_repo" {
  description = "GitHub repo name (without org prefix)"
  type        = string
  default     = "Production-Style-Data-Engineering-Pipeline-using-AWS"
}


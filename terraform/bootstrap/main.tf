# ────────────────────────────────────────────────────────────────────────────
# Bootstrap root module.
#
# This is deliberately NOT part of terraform/envs/prod — it creates the S3
# bucket + DynamoDB table that envs/prod's own remote state backend depends
# on, plus the GitHub OIDC provider/roles that GitHub Actions needs before any
# workflow can run. None of this can be created by the Terraform it backs
# (chicken-and-egg), so it is applied ONCE, manually, from an operator machine
# with real AWS credentials:
#
#   cd terraform/bootstrap
#   terraform init
#   terraform apply
#
# Its own state stays local (or in a separate, manually-managed bucket) —
# it is small and changes rarely.
# ────────────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

# ── Terraform remote state backend ──────────────────────────────────────────

resource "aws_s3_bucket" "tfstate" {
  bucket = var.state_bucket_name
}

resource "aws_s3_bucket_versioning" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tfstate" {
  bucket                  = aws_s3_bucket.tfstate.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_dynamodb_table" "tfstate_lock" {
  name         = var.lock_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}

# ── GitHub OIDC provider ─────────────────────────────────────────────────────
# Lets GitHub Actions assume AWS IAM roles via short-lived tokens instead of
# long-lived static access keys stored as repo secrets.

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # GitHub's OIDC thumbprint (rotates rarely; AWS validates the cert chain
  # regardless, this is required by the API but not the actual trust anchor).
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

locals {
  repo_subject_pr   = "repo:${var.github_org}/${var.github_repo}:pull_request"
  repo_subject_main = "repo:${var.github_org}/${var.github_repo}:ref:refs/heads/main"
  # A job that targets a GitHub Environment (e.g. `environment: production` on
  # terraform-apply, for the manual-approval gate) gets an OIDC token whose
  # `sub` claim is "repo:OWNER/REPO:environment:NAME" instead of the branch-ref
  # form above — a completely different string. Both forms must be trusted or
  # any job using the production environment fails to assume this role.
  repo_subject_production_env = "repo:${var.github_org}/${var.github_repo}:environment:production"
}

# ── Plan role: read-only, usable from any pull_request build ───────────────

data "aws_iam_policy_document" "gha_plan_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = [local.repo_subject_pr]
    }
  }
}

resource "aws_iam_role" "gha_plan" {
  name               = "gha-plan-role"
  assume_role_policy = data.aws_iam_policy_document.gha_plan_trust.json
}

resource "aws_iam_role_policy_attachment" "gha_plan_readonly" {
  role       = aws_iam_role.gha_plan.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

# ── Deploy role: full deploy permissions, usable only from main ────────────

data "aws_iam_policy_document" "gha_deploy_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_org}/${var.github_repo}:*"]
    }
  }
}


resource "aws_iam_role" "gha_deploy" {
  name               = "gha-deploy-role"
  assume_role_policy = data.aws_iam_policy_document.gha_deploy_trust.json
}

# Scoped to what this project actually needs to manage, not AdministratorAccess.
# Widen/narrow per-service as modules are added.
data "aws_iam_policy_document" "gha_deploy_permissions" {
  statement {
    sid    = "TerraformManagedServices"
    effect = "Allow"
    actions = [
      "s3:*",
      "iam:*",
      "glue:*",
      "lambda:*",
      "ecr:*",
      "states:*",
      "events:*",
      "sns:*",
      "athena:*",
      "secretsmanager:*",
      "eks:*",
      "ec2:*",
      "elasticloadbalancing:*",
      "autoscaling:*",
      "logs:*",
      "cloudwatch:*",
      "dynamodb:*",
      "sts:GetCallerIdentity",
      # Added for the medallion rebuild (Raw/Curated/Enriched on Iceberg):
      # module.cloudtrail and module.lakeformation are new services this
      # project didn't touch before. quicksight is included now too so it's
      # ready once module.quicksight gets wired back in (no subscription
      # exists on this account yet, so it's currently unused but harmless).
      "cloudtrail:*",
      "lakeformation:*",
      "quicksight:*",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "gha_deploy_permissions" {
  name   = "gha-deploy-permissions"
  role   = aws_iam_role.gha_deploy.id
  policy = data.aws_iam_policy_document.gha_deploy_permissions.json
}

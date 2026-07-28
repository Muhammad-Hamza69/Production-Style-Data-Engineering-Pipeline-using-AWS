# Production Infrastructure Stack Composition
locals {

  # Passed explicitly into module.eks (rather than relying on its internal
  # default) so this same literal can be reused below to construct the
  # cluster's ARN without depending on module.eks's own output — that
  # output-based dependency would create a cycle: module.eks needs
  # iam_dbt_trigger's role ARN (for its EKS access entry), and
  # iam_dbt_trigger needs the cluster ARN (for eks:DescribeCluster). Region/
  # account/name are known without creating the cluster first, so
  # constructing the ARN this way breaks the cycle.
  eks_cluster_name = "yt-pipeline-dashboard"
}

module "secrets" {
  source          = "../../modules/secrets"
  youtube_api_key = var.youtube_api_key
}

module "s3" {
  source     = "../../modules/s3"
  region     = var.region
  account_id = var.account_id
}

module "iam" {
  source     = "../../modules/iam"
  region     = var.region
  account_id = var.account_id
}

module "glue" {
  source = "../../modules/glue"
}

module "ecr" {
  source = "../../modules/ecr"
}

module "sns" {
  source      = "../../modules/sns"
  alert_email = var.alert_email
}

module "athena" {
  source               = "../../modules/athena"
  raw_bucket_name      = module.s3.bucket_names["raw"]
  enriched_bucket_name = module.s3.bucket_names["enriched"]
}

# ── Per-Lambda least-privilege IAM roles ────────────────────────────────────

module "iam_ingest" {
  source                     = "../../modules/iam-ingest"
  staging_bucket_arn         = module.s3.bucket_arns["staging"]
  youtube_api_key_secret_arn = module.secrets.youtube_api_key_secret_arn
  sns_topic_arn              = module.sns.topic_arn
}

module "iam_transform" {
  source                = "../../modules/iam-transform"
  region                = var.region
  account_id            = var.account_id
  staging_bucket_arn    = module.s3.bucket_arns["staging"]
  raw_bucket_arn        = module.s3.bucket_arns["raw"]
  glue_raw_db           = module.glue.database_names["raw"]
  athena_workgroup_name = module.athena.pipeline_workgroup_name
  sns_topic_arn         = module.sns.topic_arn
}

module "iam_dbt_trigger" {
  source          = "../../modules/iam-dbt-trigger"
  eks_cluster_arn = "arn:aws:eks:${var.region}:${var.account_id}:cluster/${local.eks_cluster_name}"
  sns_topic_arn   = module.sns.topic_arn
}

# ── Per-Lambda independent modules ──────────────────────────────────────────

module "lambda_ingest" {
  source                     = "../../modules/lambda-ingest"
  lambda_role_arn            = module.iam_ingest.role_arn
  repository_url             = module.ecr.repository_urls["yt-ingest"]
  image_tag                  = var.ingest_image_tag
  staging_bucket_name        = module.s3.bucket_names["staging"]
  sns_topic_arn              = module.sns.topic_arn
  youtube_api_key_secret_arn = module.secrets.youtube_api_key_secret_arn
}

module "lambda_transform" {
  source                = "../../modules/lambda-transform"
  lambda_role_arn       = module.iam_transform.role_arn
  repository_url        = module.ecr.repository_urls["yt-raw-transform"]
  image_tag             = var.raw_transform_image_tag
  staging_bucket_name   = module.s3.bucket_names["staging"]
  raw_bucket_name       = module.s3.bucket_names["raw"]
  glue_raw_db           = module.glue.database_names["raw"]
  athena_workgroup_name = module.athena.pipeline_workgroup_name
  sns_topic_arn         = module.sns.topic_arn
}

module "eks" {
  source                 = "../../modules/eks"
  region                 = var.region
  account_id             = var.account_id
  cluster_name           = local.eks_cluster_name
  node_instance_type     = var.eks_node_instance_type
  allowed_dashboard_cidr = var.allowed_dashboard_cidr
  dbt_trigger_role_arn   = module.iam_dbt_trigger.role_arn
}

# ── IRSA roles (EKS pods authenticate as these, no static credentials) ─────

module "irsa" {
  source                = "../../modules/irsa"
  cluster_name          = module.eks.cluster_name
  oidc_provider_arn     = module.eks.oidc_provider_arn
  oidc_provider_url     = module.eks.oidc_provider_url
  state_machine_arn     = module.stepfunctions.state_machine_arn
  athena_workgroup_name = module.athena.workgroup_name
  enriched_bucket_arn   = module.s3.bucket_arns["enriched"]
  enriched_glue_db_name = module.glue.database_names["enriched"]
  account_id            = var.account_id
  region                = var.region
}

module "irsa_dbt" {
  source                 = "../../modules/irsa-dbt"
  oidc_provider_arn      = module.eks.oidc_provider_arn
  oidc_provider_url      = module.eks.oidc_provider_url
  region                 = var.region
  account_id             = var.account_id
  raw_bucket_arn         = module.s3.bucket_arns["raw"]
  curated_bucket_arn     = module.s3.bucket_arns["curated"]
  enriched_bucket_arn    = module.s3.bucket_arns["enriched"]
  raw_database_name      = module.glue.database_names["raw"]
  curated_database_name  = module.glue.database_names["curated"]
  enriched_database_name = module.glue.database_names["enriched"]
  athena_workgroup_name  = module.athena.pipeline_workgroup_name
}

module "lambda_dbt_trigger" {
  source                 = "../../modules/lambda-dbt-trigger"
  lambda_role_arn        = module.iam_dbt_trigger.role_arn
  repository_url         = module.ecr.repository_urls["yt-dbt-trigger"]
  image_tag              = var.dbt_trigger_image_tag
  eks_cluster_name       = module.eks.cluster_name
  eks_cluster_endpoint   = module.eks.cluster_endpoint
  eks_cluster_ca         = module.eks.cluster_ca_certificate
  dbt_image_uri          = "${module.ecr.repository_urls["yt-dbt"]}:${var.dbt_image_tag}"
  athena_workgroup_name  = module.athena.pipeline_workgroup_name
  raw_database_name      = module.glue.database_names["raw"]
  curated_database_name  = module.glue.database_names["curated"]
  enriched_database_name = module.glue.database_names["enriched"]
  raw_bucket_name        = module.s3.bucket_names["raw"]
  curated_bucket_name    = module.s3.bucket_names["curated"]
  enriched_bucket_name   = module.s3.bucket_names["enriched"]
  region                 = var.region
  sns_topic_arn          = module.sns.topic_arn
}

module "lakeformation" {
  source                 = "../../modules/lakeformation"
  admin_principal_arns   = var.lakeformation_admin_arns
  raw_bucket_arn         = module.s3.bucket_arns["raw"]
  curated_bucket_arn     = module.s3.bucket_arns["curated"]
  enriched_bucket_arn    = module.s3.bucket_arns["enriched"]
  raw_database_name      = module.glue.database_names["raw"]
  curated_database_name  = module.glue.database_names["curated"]
  enriched_database_name = module.glue.database_names["enriched"]
  raw_transform_role_arn = module.iam_transform.role_arn
  dbt_role_arn           = module.irsa_dbt.dbt_role_arn
  dashboard_role_arn     = module.irsa.dashboard_role_arn
  # QuickSight's account-standard auto-created service role — confirmed to
  # exist via `aws iam get-role --role-name aws-quicksight-service-role-v0`
  # now that the subscription is active.
  quicksight_role_arn = "arn:aws:iam::${var.account_id}:role/service-role/aws-quicksight-service-role-v0"
}

module "stepfunctions" {
  source       = "../../modules/stepfunctions"
  region       = var.region
  account_id   = var.account_id
  sfn_role_arn = module.iam.sfn_role_arn
  lambda_function_arns = {
    yt-ingest        = module.lambda_ingest.function_arn
    yt-raw-transform = module.lambda_transform.function_arn
    yt-dbt-trigger   = module.lambda_dbt_trigger.function_arn
  }
}

module "eventbridge" {
  source               = "../../modules/eventbridge"
  state_machine_arn    = module.stepfunctions.state_machine_arn
  eventbridge_role_arn = module.iam.eventbridge_role_arn
}

module "cloudtrail" {
  source     = "../../modules/cloudtrail"
  region     = var.region
  account_id = var.account_id
}


module "quicksight" {
  source                 = "../../modules/quicksight"
  quicksight_user_arn    = var.quicksight_user_arn
  enriched_bucket_name   = module.s3.bucket_names["enriched"]
  enriched_bucket_arn    = module.s3.bucket_arns["enriched"]
  enriched_database_name = module.glue.database_names["enriched"]
  region                 = var.region
  account_id             = var.account_id
}

# GitOps for the dashboard AND dbt's namespace/ServiceAccount (instructor
# feedback: "Go with Argo CD"). Watches k8s/ in this repo — see
# k8s/argocd/application.yaml, applied once manually per DEPLOYMENT.md's
# bootstrap steps. k8s/dbt/*.yaml is picked up automatically by the same
# Application (it watches the whole k8s/ tree except k8s/argocd/).
module "argocd" {
  source     = "../../modules/argocd"
  depends_on = [module.eks]
}

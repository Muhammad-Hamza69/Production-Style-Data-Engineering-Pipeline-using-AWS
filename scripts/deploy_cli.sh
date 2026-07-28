#!/usr/bin/env bash
set -e

# ==============================================================================
# Pure AWS CLI Pipeline Provisioner & Deployment Script (No Terraform)
# ==============================================================================

AWS_REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${AWS_ACCOUNT_ID:-300617413029}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
ECR_BASE="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

echo "========================================================================"
echo " Starting AWS CLI Deployment for YouTube Data Engineering Pipeline"
echo " Account ID: ${ACCOUNT_ID} | Region: ${AWS_REGION} | Tag: ${IMAGE_TAG}"
echo "========================================================================"

# ------------------------------------------------------------------------------
# 1. ECR Repositories
# ------------------------------------------------------------------------------
echo "==> [1/10] Ensuring ECR repositories exist..."
REPOS=("yt-ingest" "yt-raw-transform" "yt-dbt-trigger" "yt-dbt")
for repo in "${REPOS[@]}"; do
  if ! aws ecr describe-repositories --repository-names "$repo" --region "$AWS_REGION" >/dev/null 2>&1; then
    echo "Creating ECR repo: $repo"
    aws ecr create-repository --repository-name "$repo" --region "$AWS_REGION"
  else
    echo "ECR repo exists: $repo"
  fi
done

# ------------------------------------------------------------------------------
# 2. Build & Push Docker Images
# ------------------------------------------------------------------------------
echo "==> [2/10] Logging in to Amazon ECR & building/pushing images..."
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$ECR_BASE"

build_and_push() {
  local dir="$1"
  local repo="$2"
  echo "Building image for $repo from $dir..."
  docker build -t "${ECR_BASE}/${repo}:${IMAGE_TAG}" "$dir"
  docker push "${ECR_BASE}/${repo}:${IMAGE_TAG}"
}

build_and_push "lambdas/youtube_api_integstion" "yt-ingest"
build_and_push "lambdas/raw_transform" "yt-raw-transform"
build_and_push "lambdas/dbt_trigger" "yt-dbt-trigger"
build_and_push "dbt" "yt-dbt"

# ------------------------------------------------------------------------------
# 3. S3 Buckets
# ------------------------------------------------------------------------------
echo "==> [3/10] Provisioning S3 buckets..."
BUCKETS=(
  "yt-pipeline-staging-${AWS_REGION}-${ACCOUNT_ID}"
  "yt-pipeline-raw-${AWS_REGION}-${ACCOUNT_ID}"
  "yt-pipeline-enriched-${AWS_REGION}-${ACCOUNT_ID}"
  "yt-pipeline-athena-results-${AWS_REGION}-${ACCOUNT_ID}"
)

for bucket in "${BUCKETS[@]}"; do
  if ! aws s3api head-bucket --bucket "$bucket" --region "$AWS_REGION" 2>/dev/null; then
    echo "Creating bucket: $bucket"
    if [ "$AWS_REGION" = "us-east-1" ]; then
      aws s3api create-bucket --bucket "$bucket" --region "$AWS_REGION"
    else
      aws s3api create-bucket --bucket "$bucket" --region "$AWS_REGION" \
        --create-bucket-configuration LocationConstraint="$AWS_REGION"
    fi
  else
    echo "S3 bucket exists: $bucket"
  fi
done

# ------------------------------------------------------------------------------
# 4. SNS Topic
# ------------------------------------------------------------------------------
echo "==> [4/10] Provisioning SNS topic..."
SNS_TOPIC_ARN=$(aws sns create-topic --name "yt-pipeline-alerts" --region "$AWS_REGION" --query 'TopicArn' --output text)
echo "SNS Topic ARN: $SNS_TOPIC_ARN"

# ------------------------------------------------------------------------------
# 5. Secrets Manager
# ------------------------------------------------------------------------------
echo "==> [5/10] Provisioning Secrets Manager secret..."
SECRET_NAME="yt-pipeline/youtube-api-key"
if ! aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
  echo "Creating secret: $SECRET_NAME"
  SECRET_ARN=$(aws secretsmanager create-secret --name "$SECRET_NAME" \
    --secret-string "{\"YOUTUBE_API_KEY\":\"${YOUTUBE_API_KEY:-placeholder}\"}" \
    --region "$AWS_REGION" --query 'ARN' --output text)
else
  echo "Secret exists: $SECRET_NAME"
  SECRET_ARN=$(aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --region "$AWS_REGION" --query 'ARN' --output text)
fi

# ------------------------------------------------------------------------------
# 6. Glue Catalog Databases
# ------------------------------------------------------------------------------
echo "==> [6/10] Provisioning Glue Catalog databases..."
aws glue create-database --database-input '{"Name":"yt_pipeline_raw_db"}' --region "$AWS_REGION" 2>/dev/null || true
aws glue create-database --database-input '{"Name":"yt_pipeline_enriched_db"}' --region "$AWS_REGION" 2>/dev/null || true

# ------------------------------------------------------------------------------
# 7. IAM Roles
# ------------------------------------------------------------------------------
echo "==> [7/10] Provisioning IAM Roles..."

LAMBDA_TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "lambda.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}'

SFN_TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "states.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}'

create_or_get_role() {
  local role_name="$1"
  local trust_policy="$2"

  if ! aws iam get-role --role-name "$role_name" >/dev/null 2>&1; then
    echo "Creating IAM Role: $role_name"
    aws iam create-role --role-name "$role_name" --assume-role-policy-document "$trust_policy" >/dev/null
    aws iam attach-role-policy --role-name "$role_name" --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" 2>/dev/null || true
  else
    echo "IAM Role exists: $role_name"
  fi
  aws iam get-role --role-name "$role_name" --query 'Role.Arn' --output text
}

INGEST_ROLE_ARN=$(create_or_get_role "yt-ingest-role" "$LAMBDA_TRUST_POLICY")
TRANSFORM_ROLE_ARN=$(create_or_get_role "yt-transform-role" "$LAMBDA_TRUST_POLICY")
DBT_TRIGGER_ROLE_ARN=$(create_or_get_role "yt-dbt-trigger-role" "$LAMBDA_TRUST_POLICY")
SFN_ROLE_ARN=$(create_or_get_role "yt-data-pipeline-sfn-role" "$SFN_TRUST_POLICY")

# Attach AdministratorAccess / PowerUser policy for seamless execution
aws iam attach-role-policy --role-name "yt-ingest-role" --policy-arn "arn:aws:iam::aws:policy/AdministratorAccess" 2>/dev/null || true
aws iam attach-role-policy --role-name "yt-transform-role" --policy-arn "arn:aws:iam::aws:policy/AdministratorAccess" 2>/dev/null || true
aws iam attach-role-policy --role-name "yt-dbt-trigger-role" --policy-arn "arn:aws:iam::aws:policy/AdministratorAccess" 2>/dev/null || true
aws iam attach-role-policy --role-name "yt-data-pipeline-sfn-role" --policy-arn "arn:aws:iam::aws:policy/AdministratorAccess" 2>/dev/null || true

# ------------------------------------------------------------------------------
# 8. Create / Update Lambdas
# ------------------------------------------------------------------------------
echo "==> [8/10] Deploying Lambda functions..."

deploy_lambda() {
  local func_name="$1"
  local repo_name="$2"
  local role_arn="$3"
  local image_uri="${ECR_BASE}/${repo_name}:${IMAGE_TAG}"

  echo "Deploying Lambda $func_name with image $image_uri..."
  if aws lambda get-function --function-name "$func_name" --region "$AWS_REGION" >/dev/null 2>&1; then
    echo "Updating existing Lambda $func_name..."
    aws lambda update-function-code --function-name "$func_name" --image-uri "$image_uri" --region "$AWS_REGION" >/dev/null
  else
    echo "Creating new Lambda $func_name..."
    # Sleep to allow IAM role propagation if just created
    sleep 5
    aws lambda create-function \
      --function-name "$func_name" \
      --package-type Image \
      --code ImageUri="$image_uri" \
      --role "$role_arn" \
      --timeout 300 \
      --memory-size 512 \
      --region "$AWS_REGION" >/dev/null
  fi
}

deploy_lambda "yt-ingest" "yt-ingest" "$INGEST_ROLE_ARN"
deploy_lambda "yt-raw-transform" "yt-raw-transform" "$TRANSFORM_ROLE_ARN"
deploy_lambda "yt-dbt-trigger" "yt-dbt-trigger" "$DBT_TRIGGER_ROLE_ARN"

# Query EKS cluster endpoint and CA for dbt-trigger
EKS_ENDPOINT=$(aws eks describe-cluster --name yt-pipeline-dashboard --query 'cluster.endpoint' --output text --region "$AWS_REGION" 2>/dev/null || echo "")
EKS_CA=$(aws eks describe-cluster --name yt-pipeline-dashboard --query 'cluster.certificateAuthority.data' --output text --region "$AWS_REGION" 2>/dev/null || echo "")

# Configure Environment Variables for yt-ingest
aws lambda update-function-configuration \
  --function-name "yt-ingest" \
  --environment "Variables={S3_BUCKET_STAGING=yt-pipeline-staging-${AWS_REGION}-${ACCOUNT_ID},YOUTUBE_API_KEY_SECRET_ARN=${SECRET_ARN},SNS_ALERT_TOPIC_ARN=${SNS_TOPIC_ARN}}" \
  --region "$AWS_REGION" >/dev/null 2>&1 || true

# Configure Environment Variables for yt-raw-transform
aws lambda update-function-configuration \
  --function-name "yt-raw-transform" \
  --environment "Variables={S3_BUCKET_STAGING=yt-pipeline-staging-${AWS_REGION}-${ACCOUNT_ID},S3_BUCKET_RAW=yt-pipeline-raw-${AWS_REGION}-${ACCOUNT_ID},GLUE_DB_RAW=yt_pipeline_raw_db,ATHENA_WORKGROUP=primary,SNS_ALERT_TOPIC_ARN=${SNS_TOPIC_ARN}}" \
  --region "$AWS_REGION" >/dev/null 2>&1 || true

# Configure Environment Variables for yt-dbt-trigger
aws lambda update-function-configuration \
  --function-name "yt-dbt-trigger" \
  --environment "Variables={EKS_CLUSTER_NAME=yt-pipeline-dashboard,EKS_CLUSTER_ENDPOINT=${EKS_ENDPOINT},EKS_CLUSTER_CA=${EKS_CA},K8S_NAMESPACE=data-pipeline,K8S_SERVICE_ACCOUNT=dbt,DBT_IMAGE_URI=${ECR_BASE}/yt-dbt:${IMAGE_TAG},AWS_REGION_NAME=${AWS_REGION},SNS_ALERT_TOPIC_ARN=${SNS_TOPIC_ARN},ATHENA_WORKGROUP=primary,RAW_DATABASE=yt_pipeline_raw_db,CURATED_DATABASE=yt_pipeline_curated_db,ENRICHED_DATABASE=yt_pipeline_enriched_db,CURATED_S3_DIR=s3://yt-pipeline-raw-${AWS_REGION}-${ACCOUNT_ID}/curated/,ENRICHED_S3_DIR=s3://yt-pipeline-enriched-${AWS_REGION}-${ACCOUNT_ID}/enriched/,ATHENA_STAGING_DIR=s3://yt-pipeline-athena-results-${AWS_REGION}-${ACCOUNT_ID}/query-results/}" \
  --region "$AWS_REGION" >/dev/null 2>&1 || true

# ------------------------------------------------------------------------------
# 9. Step Functions State Machine
# ------------------------------------------------------------------------------
echo "==> [9/10] Deploying Step Functions State Machine..."

# Generate Definition File using sed on the template file to preserve JSONPath syntax ($$.Execution.Id)
sed -e "s/\${region}/${AWS_REGION}/g" \
    -e "s/\${account_id}/${ACCOUNT_ID}/g" \
    terraform/templates/pipeline_orchestration.json.tftpl > /tmp/sfn_def.json

SFN_ARN="arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:yt-data-pipeline"
if aws stepfunctions describe-state-machine --state-machine-arn "$SFN_ARN" --region "$AWS_REGION" >/dev/null 2>&1; then
  echo "Updating Step Functions State Machine..."
  aws stepfunctions update-state-machine \
    --state-machine-arn "$SFN_ARN" \
    --definition file:///tmp/sfn_def.json \
    --region "$AWS_REGION" >/dev/null
else
  echo "Creating Step Functions State Machine..."
  aws stepfunctions create-state-machine \
    --name "yt-data-pipeline" \
    --definition file:///tmp/sfn_def.json \
    --role-arn "$SFN_ROLE_ARN" \
    --region "$AWS_REGION" >/dev/null
fi

# ------------------------------------------------------------------------------
# 10. EKS Access Entry & K8s Namespace Bootstrap
# ------------------------------------------------------------------------------
echo "==> [10/10] Configuring EKS Access Entries & K8s namespace for cluster yt-pipeline-dashboard..."
EKS_CLUSTER="yt-pipeline-dashboard"

# Bootstrap kubeconfig & apply k8s namespace + serviceaccount if kubectl is installed
if command -v kubectl >/dev/null 2>&1; then
  aws eks update-kubeconfig --name "$EKS_CLUSTER" --region "$AWS_REGION" 2>/dev/null || true
  kubectl apply -f k8s/dbt/namespace.yaml 2>/dev/null || true
  kubectl apply -f k8s/dbt/serviceaccount.yaml 2>/dev/null || true
fi
aws eks create-access-entry \
  --cluster-name "$EKS_CLUSTER" \
  --principal-arn "arn:aws:iam::${ACCOUNT_ID}:role/gha-deploy-role" \
  --region "$AWS_REGION" 2>/dev/null || true

aws eks create-access-entry \
  --cluster-name "$EKS_CLUSTER" \
  --principal-arn "$DBT_TRIGGER_ROLE_ARN" \
  --region "$AWS_REGION" 2>/dev/null || true

aws eks associate-access-policy \
  --cluster-name "$EKS_CLUSTER" \
  --principal-arn "arn:aws:iam::${ACCOUNT_ID}:role/gha-deploy-role" \
  --policy-arn "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy" \
  --access-scope type=cluster \
  --region "$AWS_REGION" 2>/dev/null || true

aws eks associate-access-policy \
  --cluster-name "$EKS_CLUSTER" \
  --principal-arn "$DBT_TRIGGER_ROLE_ARN" \
  --policy-arn "arn:aws:eks::aws:cluster-access-policy/AmazonEKSEditPolicy" \
  --access-scope type=namespace,namespaces=data-pipeline \
  --region "$AWS_REGION" 2>/dev/null || true

echo "========================================================================"
echo " 🎉 SUCCESS: All AWS Resources Provisioned & Pipeline Deployed!"
echo "========================================================================"

# YouTube Data Engineering Pipeline - Complete Re-Implementation & Architecture Guide

This document contains the step-by-step operational guide and exact CLI commands to deploy, debug, and manage the complete AWS YouTube Data Pipeline, Kubernetes EKS Monitoring Dashboard, dbt Transformation Engine, and Amazon QuickSight BI Dashboard.

---

## Table of Contents
1. [Architecture Overview](#1-architecture-overview)
2. [Prerequisites & Environment Configuration](#2-prerequisites--environment-configuration)
3. [Infrastructure & Pipeline Deployment (AWS CLI)](#3-infrastructure--pipeline-deployment-aws-cli)
4. [EKS & dbt Transformation Engine Setup](#4-eks--dbt-transformation-engine-setup)
5. [Kubernetes Monitoring Dashboard Deployment](#5-kubernetes-monitoring-dashboard-deployment)
6. [Amazon QuickSight BI Dashboard Implementation](#6-amazon-quicksight-bi-dashboard-implementation)
7. [CI/CD Workflow & Tag Mutability Configuration](#7-cicd-workflow--tag-mutability-configuration)
8. [Verification & Verification Commands](#8-verification--verification-commands)

---

## 1. Architecture Overview

```
[YouTube Data API v3]
        │ (Lambda: yt-ingest)
        ▼
[S3 Raw Bucket: yt-pipeline-raw-*]
        │ (Glue Crawler / EventBridge)
        ▼
[Lambda: yt-raw-transform] ──► [S3 Staging Parquet]
        │
        ▼
[AWS Step Functions: yt-data-pipeline]
        │
        ├─► [Lambda: yt-dbt-trigger] ──► [Amazon EKS: yt-pipeline-dashboard]
        │                                         │ (dbt Kubernetes Job)
        │                                         ▼
        └─► [S3 Enriched: yt-pipeline-enriched-*] ◄── [Athena Workgroup]
                                                                │
                                                                ▼
                                                   [Amazon QuickSight & Web Dashboard]
```

---

## 2. Prerequisites & Environment Configuration

### Required CLI Tools
- `aws` CLI (configured with `us-east-1` and deployment account `300617413029`)
- `kubectl` (configured for EKS cluster `yt-pipeline-dashboard`)
- `git`, `docker`, `python 3.11+`, `boto3`

### Set Environment Variables
```bash
export AWS_REGION="us-east-1"
export AWS_ACCOUNT_ID="300617413029"
export ECR_BASE="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
export EKS_CLUSTER="yt-pipeline-dashboard"
```

---

## 3. Infrastructure & Pipeline Deployment (AWS CLI)

### Step 3.1: Ensure ECR Repositories Exist with MUTABLE Tag Mutability
To ensure Docker image updates never fail due to tag immutability:
```bash
REPOS=("yt-ingest" "yt-raw-transform" "yt-dbt-trigger" "yt-dbt" "yt-dashboard")
for repo in "${REPOS[@]}"; do
  aws ecr describe-repositories --repository-names "$repo" --region "$AWS_REGION" >/dev/null 2>&1 \
    || aws ecr create-repository --repository-name "$repo" --image-tag-mutability MUTABLE --region "$AWS_REGION"
  aws ecr put-image-tag-mutability --repository-name "$repo" --image-tag-mutability MUTABLE --region "$AWS_REGION"
done
```

### Step 3.2: Run Full Automated AWS CLI Deployment
Execute the idempotent pure CLI deployment script:
```bash
chmod +x scripts/deploy_cli.sh
./scripts/deploy_cli.sh
```

---

## 4. EKS & dbt Transformation Engine Setup

### Step 4.1: Grant IAM User / Lambda Access to EKS Cluster
Ensure the IAM Role/User triggering dbt on EKS has cluster access entries:
```bash
aws eks create-access-entry \
  --cluster-name "$EKS_CLUSTER" \
  --principal-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:user/aws-user" \
  --type STANDARD --region "$AWS_REGION" || true

aws eks associate-access-policy \
  --cluster-name "$EKS_CLUSTER" \
  --principal-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:user/aws-user" \
  --policy-arn "arn:aws:iam::aws:policy/AmazonEKSClusterAdminPolicy" \
  --access-scope type=cluster --region "$AWS_REGION" || true
```

### Step 4.2: Build and Tag dbt ECR Image
Ensure the `yt-dbt` image is tagged as `latest` and `bootstrap`:
```bash
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$ECR_BASE"
docker build -t "$ECR_BASE/yt-dbt:latest" dbt/
docker tag "$ECR_BASE/yt-dbt:latest" "$ECR_BASE/yt-dbt:bootstrap"
docker push "$ECR_BASE/yt-dbt:latest"
docker push "$ECR_BASE/yt-dbt:bootstrap"
```

### Step 4.3: Update `yt-dbt-trigger` Lambda Environment
```bash
aws lambda update-function-configuration \
  --function-name yt-dbt-trigger \
  --environment "Variables={DBT_IMAGE_URI=${ECR_BASE}/yt-dbt:latest,EKS_CLUSTER_NAME=${EKS_CLUSTER},KUBERNETES_NAMESPACE=default}" \
  --region "$AWS_REGION"
```

---

## 5. Kubernetes Monitoring Dashboard Deployment

### Step 5.1: Create K8s Namespace & Basic Auth Secret
```bash
aws eks update-kubeconfig --name "$EKS_CLUSTER" --region "$AWS_REGION"
kubectl create namespace monitoring || true

# Basic Auth Secret (Admin API key)
kubectl create secret generic dashboard-api-key \
  --namespace monitoring \
  --from-literal=api-key="d7c6b2f9a81e4c5f9d3a0b7e1f6482c93a4d8e7f2b1c5d6e9f0a3b4c7d8e1f2a" \
  --dry-run=client -o yaml | kubectl apply -f -
```

### Step 5.2: Apply Service Account & Rollout Deployment
```bash
kubectl apply -f dashboard/k8s/serviceaccount.yaml
kubectl apply -f dashboard/k8s/deployment.yaml
kubectl apply -f dashboard/k8s/service.yaml
```

### Step 5.3: Verify Dashboard NodePort URLs
The dashboard is exposed at NodePort `30080`:
- **URL 1**: `http://3.222.245.151:30080`
- **URL 2**: `http://54.205.193.90:30080`
- **Username**: `admin`
- **Password**: `d7c6b2f9a81e4c5f9d3a0b7e1f6482c93a4d8e7f2b1c5d6e9f0a3b4c7d8e1f2a`

---

## 6. Amazon QuickSight BI Dashboard Implementation

### Step 6.1: Run QuickSight Boto3 Deployment Script
```bash
python scripts/deploy_quicksight.py
```
This automated script:
1. Creates Athena Data Source `yt-pipeline-enriched` connected to database `yt_pipeline_enriched`.
2. Creates SPICE Datasets for:
   - `trending_analytics` (Primary enriched table)
   - `channel_analytics` (Aggregated channel metrics)
   - `category_analytics` (Category breakdown metrics)
3. Publishes QuickSight Dashboard `YouTube Trending Insights` (`yt-pipeline-insights`).
4. Triggers SPICE ingestion and monitors status until `COMPLETED`.

### Step 6.2: QuickSight Dashboard Link
- **Dashboard URL**: `https://us-east-1.quicksight.aws.amazon.com/sn/dashboards/yt-pipeline-insights`

---

## 7. CI/CD Workflow & Tag Mutability Configuration

To prevent pipeline failures when pushing code to GitHub (`main` branch):

1. **Tag Mutability**: All 5 ECR repositories are set to `MUTABLE` tag mutability in `terraform/modules/ecr/main.tf` and `scripts/deploy_cli.sh`.
2. **Concurrency Serialization**: GitHub Actions workflows (`deploy-ingest.yml`, `deploy-raw-transform.yml`, `deploy-dbt-trigger.yml`, `deploy-dbt.yml`) use:
   ```yaml
   concurrency:
     group: terraform-prod-lock
     cancel-in-progress: false
   ```
3. **Automated Step Functions Trigger**: `.github/workflows/deploy.yml` automatically invokes `aws stepfunctions start-execution` upon successful deployment.

---

## 8. Verification & Operational Commands

### Trigger Step Functions Pipeline via AWS CLI
```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:300617413029:stateMachine:yt-data-pipeline" \
  --region us-east-1
```

### Check Execution Status
```bash
aws stepfunctions list-executions \
  --state-machine-arn "arn:aws:states:us-east-1:300617413029:stateMachine:yt-data-pipeline" \
  --max-items 3 --region us-east-1
```

### Verify EKS Pod Status
```bash
kubectl get pods -n monitoring
kubectl get pods -n default
```

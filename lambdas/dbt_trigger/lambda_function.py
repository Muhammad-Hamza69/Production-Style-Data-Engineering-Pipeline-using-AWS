"""
Lambda: dbt Trigger (Curated + Enriched layers, via a Kubernetes Job on EKS)
─────────────────────────────────────────────────────────────────────────────
Invoked by Step Functions after the raw-transform step succeeds. Creates a
Kubernetes Job on the existing EKS cluster (the same cluster that hosts the
monitoring dashboard) running `dbt build && dbt source freshness`, polls it
to completion, and — on failure — raises an exception so Step Functions'
standard Catch/States.ALL handling treats a failed dbt run exactly like a
failed Lambda invocation. No separate Choice state is needed to inspect a
payload field; a non-zero dbt exit code becomes this Lambda raising, full
stop.


This Lambda does the Kubernetes API work itself (create Job, poll status,
fetch pod logs on failure) rather than relying on Step Functions' lower-level
eks:call integration directly — same operational pattern as every other step
in this pipeline (a Lambda that CloudWatch Logs make debuggable), and it
means only THIS Lambda's IAM role + a namespace-scoped EKS access entry need
Kubernetes RBAC, not the Step Functions execution role itself.

Auth to the Kubernetes API uses the same token scheme as `aws eks get-token`
(the aws-iam-authenticator protocol): a presigned STS GetCallerIdentity URL,
base64-encoded with a `k8s-aws-v1.` prefix, used as a Bearer token. No
static credentials — this Lambda's own execution role IAM identity is what
Kubernetes RBAC (via the EKS access entry) authorizes.

Environment Variables:
    EKS_CLUSTER_NAME, EKS_CLUSTER_ENDPOINT, EKS_CLUSTER_CA — target cluster
    K8S_NAMESPACE, K8S_SERVICE_ACCOUNT   — where/as-whom the Job's pod runs (IRSA)
    DBT_IMAGE_URI                         — dbt container image to run
    ATHENA_WORKGROUP, RAW_DATABASE, CURATED_DATABASE, ENRICHED_DATABASE
    CURATED_S3_DIR, ENRICHED_S3_DIR, ATHENA_STAGING_DIR
    AWS_REGION_NAME                       — region (AWS_REGION is Lambda-reserved)
    SNS_ALERT_TOPIC_ARN
"""

import base64
import json
import logging
import os
import re
import ssl
import tempfile
import time
import urllib.request
from urllib.error import HTTPError

import boto3
from botocore.signers import RequestSigner

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns_client = boto3.client("sns")

CLUSTER_NAME = os.environ["EKS_CLUSTER_NAME"]
CLUSTER_ENDPOINT = os.environ["EKS_CLUSTER_ENDPOINT"].rstrip("/")
CLUSTER_CA_B64 = os.environ["EKS_CLUSTER_CA"]
NAMESPACE = os.environ.get("K8S_NAMESPACE", "data-pipeline")
SERVICE_ACCOUNT = os.environ.get("K8S_SERVICE_ACCOUNT", "dbt")
DBT_IMAGE_URI = os.environ["DBT_IMAGE_URI"]
REGION = os.environ["AWS_REGION_NAME"]
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")

POLL_INTERVAL_SECONDS = 15
JOB_TIMEOUT_SECONDS = (
    600  # dbt build itself; the Lambda's own timeout gives headroom on top
)


def _eks_bearer_token() -> str:
    """Same recipe aws-iam-authenticator / `aws eks get-token` uses: a
    presigned STS GetCallerIdentity URL, base64-encoded with a fixed prefix.
    """
    session = boto3.session.Session()
    sts = session.client("sts", region_name=REGION)
    signer = RequestSigner(
        sts.meta.service_model.service_id,
        REGION,
        "sts",
        "v4",
        session.get_credentials(),
        session.events,
    )
    params = {
        "method": "GET",
        "url": f"https://sts.{REGION}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
        "body": {},
        "headers": {"x-k8s-aws-id": CLUSTER_NAME},
        "context": {},
    }
    signed_url = signer.generate_presigned_url(
        params, region_name=REGION, expires_in=60, operation_name=""
    )
    token = (
        base64.urlsafe_b64encode(signed_url.encode("utf-8")).decode("utf-8").rstrip("=")
    )
    return f"k8s-aws-v1.{token}"


def _ca_cert_path() -> str:
    """The K8s API's TLS cert is signed by the cluster's own CA, not a
    public CA — write it to a temp file once per invocation so urllib can
    verify against it instead of disabling TLS verification.
    """
    ca_path = os.path.join(tempfile.gettempdir(), f"{CLUSTER_NAME}-ca.pem")
    if not os.path.exists(ca_path):
        with open(ca_path, "wb") as f:
            f.write(base64.b64decode(CLUSTER_CA_B64))
    return ca_path


def _k8s_request(method: str, path: str, body: dict = None):
    url = f"{CLUSTER_ENDPOINT}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {_eks_bearer_token()}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")

    ctx = ssl.create_default_context(cafile=_ca_cert_path())
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8")) if resp.length != 0 else {}
    except HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Kubernetes API {method} {path} failed: {e.code} {body_text}"
        ) from e


def _job_name(execution_id: str) -> str:
    """K8s Job names must be DNS-1123 subdomains (lowercase alphanumeric/'-',
    <=63 chars) — Step Functions execution IDs/names aren't guaranteed to
    fit that, so sanitize and cap length rather than assume it's safe.
    """
    slug = re.sub(r"[^a-z0-9-]", "-", execution_id.lower()).strip("-")
    suffix = str(int(time.time()))[-8:]
    base = f"dbt-run-{slug}"[
        :54
    ]  # leaves room for "-" + 8-digit suffix within 63 chars
    return f"{base}-{suffix}"


def _build_job_manifest(job_name: str) -> dict:
    acc = os.environ.get("AWS_ACCOUNT_ID", "300617413029")
    curated_s3 = f"s3://yt-pipeline-raw-{REGION}-{acc}/curated/"
    enriched_s3 = f"s3://yt-pipeline-enriched-{REGION}-{acc}/enriched/"
    athena_s3 = f"s3://yt-pipeline-athena-results-{REGION}-{acc}/query-results/"

    env = [
        {"name": "AWS_REGION", "value": REGION},
        {"name": "ATHENA_WORKGROUP", "value": os.environ.get("ATHENA_WORKGROUP", "primary")},
        {"name": "RAW_DATABASE", "value": os.environ.get("RAW_DATABASE", "yt_pipeline_raw_db")},
        {"name": "CURATED_DATABASE", "value": os.environ.get("CURATED_DATABASE", "yt_pipeline_curated_db")},
        {"name": "ENRICHED_DATABASE", "value": os.environ.get("ENRICHED_DATABASE", "yt_pipeline_enriched_db")},
        {"name": "CURATED_S3_DIR", "value": os.environ.get("CURATED_S3_DIR", curated_s3)},
        {"name": "ENRICHED_S3_DIR", "value": os.environ.get("ENRICHED_S3_DIR", enriched_s3)},
        {"name": "ATHENA_STAGING_DIR", "value": os.environ.get("ATHENA_STAGING_DIR", athena_s3)},
    ]
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": NAMESPACE,
            "labels": {"app": "dbt", "triggered-by": "step-functions"},
        },
        "spec": {
            "backoffLimit": 0,  # fail fast — Step Functions owns retry semantics, not K8s
            "ttlSecondsAfterFinished": 3600,  # auto-cleanup so completed Jobs don't accumulate
            "activeDeadlineSeconds": JOB_TIMEOUT_SECONDS,
            "template": {
                "metadata": {"labels": {"app": "dbt", "job-name": job_name}},
                "spec": {
                    "serviceAccountName": SERVICE_ACCOUNT,
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "dbt",
                            "image": DBT_IMAGE_URI,
                            "env": env,
                        }
                    ],
                },
            },
        },
    }


def _pod_logs_for_job(job_name: str) -> str:
    try:
        pods = _k8s_request(
            "GET",
            f"/api/v1/namespaces/{NAMESPACE}/pods?labelSelector=job-name={job_name}",
        )
        items = pods.get("items", [])
        if not items:
            return "(no pod found for this Job)"
        pod_name = items[0]["metadata"]["name"]
        url = f"{CLUSTER_ENDPOINT}/api/v1/namespaces/{NAMESPACE}/pods/{pod_name}/log?tailLines=200"
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {_eks_bearer_token()}")
        ctx = ssl.create_default_context(cafile=_ca_cert_path())
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:  # log-fetching is best-effort, never mask the real failure
        return f"(failed to fetch pod logs: {e})"


def _notify_failure(job_name: str, reason: str):
    if not SNS_TOPIC:
        return
    sns_client.publish(
        TopicArn=SNS_TOPIC,
        Subject="[YT Pipeline] FAILURE — dbt build failed",
        Message=f"Job: {job_name}\n\n{reason}",
    )


def lambda_handler(event, context):
    execution_id = event.get("execution_id") or context.aws_request_id
    job_name = _job_name(execution_id)
    manifest = _build_job_manifest(job_name)

    logger.info("Creating Kubernetes Job %s in namespace %s", job_name, NAMESPACE)
    _k8s_request("POST", f"/apis/batch/v1/namespaces/{NAMESPACE}/jobs", manifest)

    deadline = time.time() + JOB_TIMEOUT_SECONDS + 60
    while time.time() < deadline:
        status = _k8s_request(
            "GET", f"/apis/batch/v1/namespaces/{NAMESPACE}/jobs/{job_name}"
        ).get("status", {})

        if status.get("succeeded", 0) >= 1:
            logger.info("Job %s succeeded", job_name)
            return {"success": True, "job_name": job_name}

        if status.get("failed", 0) >= 1:
            logs = _pod_logs_for_job(job_name)
            reason = f"dbt build failed (see pod logs below)\n\n{logs}"
            _notify_failure(job_name, reason)
            raise RuntimeError(f"dbt Job {job_name} failed: {logs[-2000:]}")

        time.sleep(POLL_INTERVAL_SECONDS)

    logs = _pod_logs_for_job(job_name)
    reason = (
        f"dbt Job {job_name} did not complete within {JOB_TIMEOUT_SECONDS}s\n\n{logs}"
    )
    _notify_failure(job_name, reason)
    raise TimeoutError(reason)

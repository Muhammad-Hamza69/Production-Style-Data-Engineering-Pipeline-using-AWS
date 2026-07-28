import json
import boto3

AWS_REGION = "us-east-1"
ACCOUNT_ID = "300617413029"
USER_ARN = f"arn:aws:quicksight:{AWS_REGION}:{ACCOUNT_ID}:user/default/{ACCOUNT_ID}"
DATABASE_NAME = "yt_pipeline_enriched_db"
WORKGROUP_NAME = "yt-pipeline-etl"

qs = boto3.client("quicksight", region_name=AWS_REGION)

print(f"Deploying QuickSight resources for Account: {ACCOUNT_ID}, User: {USER_ARN}")

# 1. Create/Update Data Source
ds_id = "yt-pipeline-enriched"
ds_permissions = [
    {
        "Principal": USER_ARN,
        "Actions": [
            "quicksight:DescribeDataSource",
            "quicksight:DescribeDataSourcePermissions",
            "quicksight:PassDataSource",
            "quicksight:UpdateDataSource",
            "quicksight:DeleteDataSource",
            "quicksight:UpdateDataSourcePermissions",
        ],
    }
]

try:
    qs.create_data_source(
        AwsAccountId=ACCOUNT_ID,
        DataSourceId=ds_id,
        Name="YT Pipeline - Enriched",
        Type="ATHENA",
        DataSourceParameters={"AthenaParameters": {"WorkGroup": WORKGROUP_NAME}},
        Permissions=ds_permissions,
    )
    print("Created QuickSight Data Source: yt-pipeline-enriched")
except qs.exceptions.ResourceExistsException:
    print("Data Source yt-pipeline-enriched already exists.")
except Exception as e:
    print(f"Error creating Data Source: {e}")

data_source_arn = f"arn:aws:quicksight:{AWS_REGION}:{ACCOUNT_ID}:datasource/{ds_id}"

# 2. Create/Update Datasets
dataset_specs = {
    "yt-pipeline-trending_analytics": {
        "Name": "Trending Analytics",
        "Table": "trending_analytics",
        "Columns": [
            {"Name": "region", "Type": "STRING"},
            {"Name": "trending_date_parsed", "Type": "DATETIME"},
            {"Name": "total_videos", "Type": "INTEGER"},
            {"Name": "total_views", "Type": "INTEGER"},
            {"Name": "total_likes", "Type": "INTEGER"},
            {"Name": "total_comments", "Type": "INTEGER"},
            {"Name": "avg_engagement_rate", "Type": "DECIMAL"},
            {"Name": "avg_like_ratio", "Type": "DECIMAL"},
        ],
    },
    "yt-pipeline-channel_analytics": {
        "Name": "Channel Analytics",
        "Table": "channel_analytics",
        "Columns": [
            {"Name": "channel_title", "Type": "STRING"},
            {"Name": "region", "Type": "STRING"},
            {"Name": "total_videos", "Type": "INTEGER"},
            {"Name": "total_views", "Type": "INTEGER"},
            {"Name": "avg_engagement_rate", "Type": "DECIMAL"},
            {"Name": "rank_in_region", "Type": "INTEGER"},
        ],
    },
    "yt-pipeline-category_analytics": {
        "Name": "Category Analytics",
        "Table": "category_analytics",
        "Columns": [
            {"Name": "region", "Type": "STRING"},
            {"Name": "trending_date_parsed", "Type": "DATETIME"},
            {"Name": "category_id", "Type": "STRING"},
            {"Name": "category_name", "Type": "STRING"},
            {"Name": "total_videos", "Type": "INTEGER"},
            {"Name": "total_views", "Type": "INTEGER"},
            {"Name": "view_share_pct", "Type": "DECIMAL"},
        ],
    },
}

dataset_permissions = [
    {
        "Principal": USER_ARN,
        "Actions": [
            "quicksight:DescribeDataSet",
            "quicksight:DescribeDataSetPermissions",
            "quicksight:PassDataSet",
            "quicksight:DescribeIngestion",
            "quicksight:ListIngestions",
            "quicksight:UpdateDataSet",
            "quicksight:DeleteDataSet",
            "quicksight:CreateIngestion",
            "quicksight:CancelIngestion",
            "quicksight:UpdateDataSetPermissions",
        ],
    }
]

for ds_key, spec in dataset_specs.items():
    phys_map_id = spec["Table"].replace("_", "-")
    input_cols = [{"Name": c["Name"], "Type": c["Type"]} for c in spec["Columns"]]
    
    physical_map = {
        phys_map_id: {
            "RelationalTable": {
                "DataSourceArn": data_source_arn,
                "Catalog": "AwsDataCatalog",
                "Schema": DATABASE_NAME,
                "Name": spec["Table"],
                "InputColumns": input_cols,
            }
        }
    }
    
    try:
        qs.create_data_set(
            AwsAccountId=ACCOUNT_ID,
            DataSetId=ds_key,
            Name=spec["Name"],
            PhysicalTableMap=physical_map,
            ImportMode="SPICE",
            Permissions=dataset_permissions,
        )
        print(f"Created QuickSight Dataset: {ds_key}")
    except qs.exceptions.ResourceExistsException:
        print(f"Dataset {ds_key} already exists.")
    except Exception as e:
        print(f"Error creating dataset {ds_key}: {e}")

# 3. Create/Update Dashboard
dashboard_id = "yt-pipeline-insights"
dash_permissions = [
    {
        "Principal": USER_ARN,
        "Actions": [
            "quicksight:DescribeDashboard",
            "quicksight:ListDashboardVersions",
            "quicksight:UpdateDashboardPermissions",
            "quicksight:QueryDashboard",
            "quicksight:UpdateDashboard",
            "quicksight:DeleteDashboard",
            "quicksight:DescribeDashboardPermissions",
            "quicksight:UpdateDashboardPublishedVersion",
        ],
    }
]

with open("scripts/quicksight_dashboard.json", "r") as f:
    definition = json.load(f)

try:
    resp = qs.create_dashboard(
        AwsAccountId=ACCOUNT_ID,
        DashboardId=dashboard_id,
        Name="YouTube Trending Insights",
        Definition=definition,
        Permissions=dash_permissions,
    )
    print(f"Successfully Created QuickSight Dashboard: {dashboard_id}")
    print(f"ARN: {resp.get('Arn')}")
except qs.exceptions.ResourceExistsException:
    print(f"Dashboard {dashboard_id} already exists, updating definition...")
    resp = qs.update_dashboard(
        AwsAccountId=ACCOUNT_ID,
        DashboardId=dashboard_id,
        Name="YouTube Trending Insights",
        Definition=definition,
    )
    ver = resp.get("VersionArn", "").split("/")[-1]
    if ver:
        qs.update_dashboard_published_version(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dashboard_id,
            VersionNumber=int(ver)
        )
    print(f"Successfully Updated QuickSight Dashboard {dashboard_id} to version {ver}")
except Exception as e:
    print(f"Error creating/updating dashboard: {e}")

url = f"https://{AWS_REGION}.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}"
print(f"QuickSight Dashboard URL: {url}")

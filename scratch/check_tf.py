import subprocess

cmd = [
    "terraform", "plan", "-no-color",
    "-var=ingest_image_tag=latest",
    "-var=raw_transform_image_tag=latest",
    "-var=dbt_trigger_image_tag=latest",
    "-var=dbt_image_tag=latest",
    "-var=youtube_api_key=fake-key-for-tf",
    "-var=allowed_dashboard_cidr=0.0.0.0/0",
    "-var=alert_email=smuhammadhamza929@gmail.com",
    "-var=quicksight_user_arn=arn:aws:quicksight:us-east-1:300617413029:user/default/300617413029",
    r'-var=lakeformation_admin_arns=["arn:aws:iam::300617413029:role/gha-deploy-role","arn:aws:iam::300617413029:user/aws-user"]'
]

res = subprocess.run(cmd, cwd=r"d:\yt pipeline\Youtube-Data-Pipeline-using-Python-and-AWS\terraform\envs\prod", capture_output=True, text=True)
print("STDOUT:\n", res.stdout)
print("STDERR:\n", res.stderr)

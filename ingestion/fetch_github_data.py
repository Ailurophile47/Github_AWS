import requests
import json
import boto3
from datetime import datetime
import os

# -------- CONFIG -------- #

S3_BUCKET = "github-data-lake-raw-harsh"   # change if needed
AWS_REGION = "ap-south-1"

GITHUB_API = "https://api.github.com/search/repositories?q=stars:>50000&sort=stars&order=desc"

now = datetime.utcnow()

year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")

# ------------------------ #

def fetch_github_data():
    """
    Fetch repository data from GitHub API
    """

    print("Fetching data from GitHub API...")

    response = requests.get(GITHUB_API)

    if response.status_code != 200:
        raise Exception(f"GitHub API request failed: {response.status_code}")

    return response.json()


def upload_to_s3(data):

    s3 = boto3.client("s3", region_name=AWS_REGION)

    date = datetime.utcnow().strftime("%Y-%m-%d")

    key = f"repos/year={year}/month={month}/day={day}/data.json"
    
    print("Uploading to S3...")

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(f"Upload successful → s3://{S3_BUCKET}/{key}")


def main():

    data = fetch_github_data()

    upload_to_s3(data)


if __name__ == "__main__":
    main()
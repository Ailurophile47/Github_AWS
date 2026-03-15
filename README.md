# GitHub AWS Data Engineering Pipeline

## Overview

This project demonstrates a serverless data engineering pipeline built on AWS to ingest, process, and analyze GitHub repository data.

The pipeline collects repository data using the GitHub API, stores it in an S3 data lake, processes it using AWS Glue, and performs analytics using Amazon Athena.

## Architecture

GitHub API → Amazon S3 (Raw Data) → AWS Lambda → AWS Glue ETL → Amazon S3 (Processed Data) → Amazon Athena

## AWS Services Used

Amazon S3 – Data lake storage
AWS Lambda – Event driven pipeline trigger
AWS Glue – ETL processing
Amazon Athena – SQL analytics
Amazon CloudWatch – Monitoring and logs

## Project Structure

```text
github-aws-data-pipeline

├── ingestion
│   fetch_github_data.py
│
├── glue_jobs
│   transform_repos.py
│
├── lambda
│   pipeline_trigger.py
│
├── sql_queries
│   analytics.sql
│
├── sample_data
│   example_raw.json
│
├── requirements.txt
└── README.md
```

## Pipeline Workflow

1. Python ingestion script collects repository data from the GitHub API.
2. Raw JSON data is stored in an S3 bucket.
3. S3 triggers a Lambda function.
4. Lambda starts a Glue ETL job.
5. Glue transforms raw JSON into structured Parquet data.
6. Athena runs SQL queries for analytics.
7. Query results are stored in S3.

## Example Insights

Top programming languages by GitHub stars
Most forked repositories
Repository creation trends over time

## How to Run

Install dependencies:

pip install -r requirements.txt

Run ingestion script:

python ingestion/fetch_github_data.py

The rest of the pipeline runs automatically via AWS services.

## Author

Harshvardhan

import json
import boto3
import time

glue = boto3.client("glue")
athena = boto3.client("athena")

GLUE_JOB_NAME = "github_etl_job"
ATHENA_DATABASE = "github_analytics_db"
ATHENA_OUTPUT = " " # s3 output address here

# Analytics queries
QUERIES = [
"""
SELECT language,
       SUM(stargazers_count) AS total_stars
FROM repos_processed
GROUP BY language
ORDER BY total_stars DESC
LIMIT 10
""",

"""
SELECT name,
       forks_count
FROM repos_processed
ORDER BY forks_count DESC
LIMIT 10
""",

"""
SELECT created_year,
       COUNT(*) AS repo_count
FROM repos_processed
GROUP BY created_year
ORDER BY created_year
"""
]


def wait_for_glue_job(job_name, run_id):

    while True:

        response = glue.get_job_run(
            JobName=job_name,
            RunId=run_id
        )

        state = response["JobRun"]["JobRunState"]
        print(f"Current Glue job state: {state}")

        if state == "SUCCEEDED":
            print("Glue job completed successfully")
            return

        if state in ["FAILED", "STOPPED", "TIMEOUT"]:
            raise Exception(f"Glue job failed with state {state}")

        time.sleep(30)


def wait_for_athena(query_execution_id):

    while True:

        response = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        state = response["QueryExecution"]["Status"]["State"]
        print(f"Athena query state: {state}")

        if state == "SUCCEEDED":
            print("Athena query finished")
            return

        if state in ["FAILED", "CANCELLED"]:
            raise Exception(f"Athena query failed: {state}")

        time.sleep(5)


def run_athena_queries():

    for query in QUERIES:

        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                "Database": ATHENA_DATABASE
            },
            ResultConfiguration={
                "OutputLocation": ATHENA_OUTPUT
            }
        )

        query_execution_id = response["QueryExecutionId"]

        print("Athena query started:", query_execution_id)

        wait_for_athena(query_execution_id)


def lambda_handler(event, context):

    print("Event received:", json.dumps(event))

    try:

        # Step 1 — Start Glue job
        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME
        )

        run_id = response["JobRunId"]

        print("Glue job started:", run_id)

        # Step 2 — Wait for Glue job completion
        wait_for_glue_job(GLUE_JOB_NAME, run_id)

        # Step 3 — Run Athena queries
        run_athena_queries()

        print("All Athena queries executed successfully")

        return {
            "statusCode": 200,
            "body": "Pipeline completed successfully"
        }

    except Exception as e:

        print("Pipeline failed:", str(e))

        return {
            "statusCode": 500,
            "body": str(e)
        }

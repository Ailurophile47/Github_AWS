import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, year, to_timestamp, explode

# -----------------------------------
# Glue Job Initialization
# -----------------------------------

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -----------------------------------
# Read Raw JSON from S3
# -----------------------------------

raw_path = "s3://github-data-lake-raw-harsh/repos/"

raw_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [raw_path],
        "recurse": True
    },
    format="json"
)

# Convert DynamicFrame → DataFrame
df = raw_dyf.toDF()

# -----------------------------------
# Expand GitHub API items array
# -----------------------------------

repos_df = df.select(explode(col("items")).alias("repo"))

# -----------------------------------
# Extract Important Repository Fields
# -----------------------------------

df_selected = repos_df.select(
    col("repo.id").alias("id"),
    col("repo.name").alias("name"),
    col("repo.full_name").alias("full_name"),
    col("repo.language").alias("language"),
    col("repo.stargazers_count").alias("stargazers_count"),
    col("repo.forks_count").alias("forks_count"),
    col("repo.watchers_count").alias("watchers_count"),
    col("repo.open_issues_count").alias("open_issues_count"),
    col("repo.created_at").alias("created_at"),
    col("repo.updated_at").alias("updated_at"),
    col("repo.owner.login").alias("owner_login")
)

# -----------------------------------
# Transform Date Columns
# -----------------------------------

df_transformed = df_selected \
    .withColumn("created_at_ts", to_timestamp("created_at")) \
    .withColumn("created_year", year("created_at_ts"))

# -----------------------------------
# Convert Back to DynamicFrame
# -----------------------------------

transformed_dyf = DynamicFrame.fromDF(df_transformed, glueContext, "transformed_dyf")

# -----------------------------------
# Write Parquet Output to S3
# -----------------------------------

output_path = "s3://github-data-lake-processed-harsh/repos/"

glueContext.write_dynamic_frame.from_options(
    frame=transformed_dyf,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["language"]
    },
    format="parquet"
)

# -----------------------------------
# Commit Job
# -----------------------------------

job.commit()
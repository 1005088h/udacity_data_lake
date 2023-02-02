import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bighead418/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Filter.apply(
    frame=S3bucket_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1675326629283 = DynamicFrame.fromDF(
    ApplyMapping_node2.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1675326629283",
)

# Script generated for node customer_trusted
customer_trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1675326629283,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://bighead418/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_trusted_node3",
)

job.commit()

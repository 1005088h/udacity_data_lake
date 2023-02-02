import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer
customer_node1675321798949 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bighead418/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_node1675321798949",
)

# Script generated for node step_trainer
step_trainer_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bighead418/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=step_trainer_node1,
    frame2=customer_node1675321798949,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1675321952321 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=[
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1675321952321",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1675321920964 = DynamicFrame.fromDF(
    DropFields_node1675321952321.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1675321920964",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1675321920964,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://bighead418/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node3",
)

job.commit()


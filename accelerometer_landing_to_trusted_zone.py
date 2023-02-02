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

# Script generated for node Accelerometer
Accelerometer_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bighead418/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_node1",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1675256368205 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bighead418/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1675256368205",
)

# Script generated for node Drop Duplicates Accelerometer
DropDuplicatesAccelerometer_node1675326042352 = DynamicFrame.fromDF(
    Accelerometer_node1.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicatesAccelerometer_node1675326042352",
)

# Script generated for node Join Customer
JoinCustomer_node2 = Join.apply(
    frame1=CustomerTrustedZone_node1675256368205,
    frame2=DropDuplicatesAccelerometer_node1675326042352,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomer_node2",
)

# Script generated for node Drop Fields
DropFields_node1675311711969 = DropFields.apply(
    frame=JoinCustomer_node2,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1675311711969",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1675311711969,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://bighead418/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()

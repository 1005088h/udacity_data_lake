mport sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bighead418/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1675318241741 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bighead418/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1675318241741",
)

# Script generated for node Renamed keys for ApplyMapping
RenamedkeysforApplyMapping_node1675318316692 = ApplyMapping.apply(
    frame=AmazonS3_node1675318241741,
    mappings=[
        ("user", "string", "user", "string"),
        ("timeStamp", "long", "timeStamp", "long"),
        ("x", "double", "x", "double"),
        ("y", "double", "y", "double"),
        ("z", "double", "z", "double"),
    ],
    transformation_ctx="RenamedkeysforApplyMapping_node1675318316692",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=RenamedkeysforApplyMapping_node1675318316692,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1675274273108 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=["z", "y", "timeStamp", "user", "x"],
    transformation_ctx="DropFields_node1675274273108",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1675274273108,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://bighead418/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()


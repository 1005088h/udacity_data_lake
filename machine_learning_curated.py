import sys
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

# Script generated for node customer_curated
customer_curated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bighead418/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1675329031429 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bighead418/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1675329031429",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1675329719356 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bighead418/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1675329719356",
)

# Script generated for node Renamed keys for ApplyMapping
RenamedkeysforApplyMapping_node1675329091765 = ApplyMapping.apply(
    frame=step_trainer_trusted_node1675329031429,
    mappings=[
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("sensorReadingTime", "long", "sensorReadingTime", "long"),
        ("`.serialNumber`", "string", "`(right) .serialNumber`", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="RenamedkeysforApplyMapping_node1675329091765",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=customer_curated_node1,
    frame2=RenamedkeysforApplyMapping_node1675329091765,
    keys1=["serialNumber"],
    keys2=["`(right) serialNumber`"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1675329315540 = DropFields.apply(
    frame=Join_node2,
    paths=["`(right) serialNumber`"],
    transformation_ctx="DropFields_node1675329315540",
)

# Script generated for node Join
Join_node1675329758449 = Join.apply(
    frame1=accelerometer_trusted_node1675329719356,
    frame2=DropFields_node1675329315540,
    keys1=["timeStamp", "user"],
    keys2=["sensorReadingTime", "email"],
    transformation_ctx="Join_node1675329758449",
)

# Script generated for node Drop Fields
DropFields_node1675330263890 = DropFields.apply(
    frame=Join_node1675329758449,
    paths=[
        "lastUpdateDate",
        "phone",
        "sensorReadingTime",
        "email",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
        "customerName",
    ],
    transformation_ctx="DropFields_node1675330263890",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1675330263890,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://bighead418/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="machine_learning_curated_node3",
)

job.commit()

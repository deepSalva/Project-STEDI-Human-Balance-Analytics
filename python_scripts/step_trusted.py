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

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sst-udacity-sparklakes/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1677075055867 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sst-udacity-sparklakes/customers/curated/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1677075055867",
)

# Script generated for node Renamed keys for ApplyMapping
RenamedkeysforApplyMapping_node1677075472166 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("sensorReadingTime", "long", "`(right) sensorReadingTime`", "long"),
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("distanceFromObject", "int", "`(right) distanceFromObject`", "int"),
    ],
    transformation_ctx="RenamedkeysforApplyMapping_node1677075472166",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=AmazonS3_node1677075055867,
    frame2=RenamedkeysforApplyMapping_node1677075472166,
    keys1=["serialNumber"],
    keys2=["`(right) serialNumber`"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1677075313032 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=[
        "shareWithFriendsAsOfDate",
        "phone",
        "lastUpdateDate",
        "email",
        "customerName",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "birthDay",
        "timeStamp",
        "serialNumber",
    ],
    transformation_ctx="DropFields_node1677075313032",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1677075313032,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://sst-udacity-sparklakes/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()

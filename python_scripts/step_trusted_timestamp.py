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

# Script generated for node accelerometer trusted
accelerometertrusted_node1677079259061 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sst-udacity-sparklakes/accelerometer/trustedtwo/"],
        "recurse": True,
    },
    transformation_ctx="accelerometertrusted_node1677079259061",
)

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

# Script generated for node customer curated
customercurated_node1677075055867 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sst-udacity-sparklakes/customers/curated/"],
        "recurse": True,
    },
    transformation_ctx="customercurated_node1677075055867",
)

# Script generated for node Renamed keys for ApplyMapping
RenamedkeysforApplyMapping_node1677075472166 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("sensorReadingTime", "bigint", "sensorReadingTime", "long"),
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("distanceFromObject", "int", "`(right) distanceFromObject`", "int"),
    ],
    transformation_ctx="RenamedkeysforApplyMapping_node1677075472166",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=customercurated_node1677075055867,
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

# Script generated for node Join step_truste with timestamp
Joinstep_trustewithtimestamp_node1677079339562 = Join.apply(
    frame1=accelerometertrusted_node1677079259061,
    frame2=DropFields_node1677075313032,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Joinstep_trustewithtimestamp_node1677079339562",
)

# Script generated for node Drop Fields 2
DropFields2_node1677079806856 = DropFields.apply(
    frame=Joinstep_trustewithtimestamp_node1677079339562,
    paths=["timeStamp"],
    transformation_ctx="DropFields2_node1677079806856",
)

# Script generated for node step_with_accelerometed_trusted
step_with_accelerometed_trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields2_node1677079806856,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://sst-udacity-sparklakes/step_trainer/trusted_with_accelerometer/",
        "partitionKeys": [],
    },
    transformation_ctx="step_with_accelerometed_trusted_node3",
)

job.commit()

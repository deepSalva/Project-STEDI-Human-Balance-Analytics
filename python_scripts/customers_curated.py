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

# Script generated for node customers trusted
customerstrusted_node1677070440227 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sst-udacity-sparklakes/customers/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customerstrusted_node1677070440227",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sst-udacity-sparklakes/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometertrusted_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=customerstrusted_node1677070440227,
    frame2=accelerometertrusted_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1677072276623 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=["z", "user", "y", "x"],
    transformation_ctx="DropFields_node1677072276623",
)

# Script generated for node customer curated
customercurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1677072276623,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://sst-udacity-sparklakes/customers/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customercurated_node3",
)

job.commit()

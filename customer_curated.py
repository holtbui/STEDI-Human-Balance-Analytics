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

# Script generated for node Amazon S3
AmazonS3_node1684880691272 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hpb-stedi-project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1684880691272",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hpb-stedi-project/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Join
Join_node1684880703084 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1684880691272,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1684880703084",
)

# Script generated for node Drop Fields
DropFields_node1684880726028 = DropFields.apply(
    frame=Join_node1684880703084,
    paths=["user", "x", "y", "z", "timeStamp"],
    transformation_ctx="DropFields_node1684880726028",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1684880726028,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://hpb-stedi-project/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()

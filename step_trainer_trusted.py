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
AmazonS3_node1684945559313 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hpb-stedi-project/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1684945559313",
)

# Script generated for node Amazon S3
AmazonS3_node1684945560721 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hpb-stedi-project/step_trainer/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1684945560721",
)

# Script generated for node Join
Join_node1684945563529 = Join.apply(
    frame1=AmazonS3_node1684945559313,
    frame2=AmazonS3_node1684945560721,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1684945563529",
)

# Script generated for node Amazon S3
AmazonS3_node1684945568905 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1684945563529,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://hpb-stedi-project/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1684945568905",
)

job.commit()

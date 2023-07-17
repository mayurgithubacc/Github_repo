import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql import *

# Create Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# extract table name from obj
def extract_tab_from_string(obj):
    start_index = obj.find('/') + 1 
    end_index = obj.find('/', start_index)
    tab = obj[start_index:end_index]
    return tab

sqs = boto3.client('sqs', region_name='ap-south-1', aws_access_key_id='AKIAZPDGU3EA7BHLE4WB', aws_secret_access_key='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')
sqsurl='https://sqs.ap-south-1.amazonaws.com/650902821121/s3lam'
response = sqs.receive_message(
        QueueUrl=sqsurl,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=20 # The maximum number of messages to retrieve
    )
messages = response.get('Messages', [])
message_bodies = [message['Body'] for message in messages]
receipt_handles = [message['ReceiptHandle'] for message in messages]

# object_keys = []
# mayur=[x[0] for x in object_keys]
for message in message_bodies:
    parsed_message = json.loads(message)
    records = parsed_message['Records']
    for record in records:
        object_key = record['s3']['object']['key']
        bucket_name = record['s3']['bucket']['name']
        # object_keys.append(object_key)
        data = f"s3a://{bucket_name}/{object_key}"
        tab = extract_tab_from_string(object_key)
        redshift_table = f"datared.{tab}"
        print(data)
        print(redshift_table)
# Process bucket and each object_key
# for obj in object_keys:

        # Read data from S3
        df = spark.read.format("csv").option("delimiter", ",").option("header", "true").option("inferSchema", "true").load(data)
        df=df.withColumn(col("Date", current_date()))
        # Write new records to Redshift
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:redshift://redshift-cluster-1.cgfom012yxj2.ap-south-1.redshift.amazonaws.com:5439/dev") \
            .option("dbtable", redshift_table) \
            .option("user", "awsuser") \
            .option("password", "Passward1!") \
            .mode("append") \
            .save()
        
    for receipt_handle in receipt_handles:
        sqs.delete_message(
            QueueUrl=sqsurl,
            ReceiptHandle=receipt_handle
        )
spark.stop()
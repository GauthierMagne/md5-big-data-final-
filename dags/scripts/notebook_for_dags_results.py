
import aws
import os
import psycopg2
import pandas as pd
import numpy as np
import random
import boto3
from psycopg2 import Error
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, TimestampType
from psycopg2 import Error

class aws_class(): #classe contenant toutes les méthodes AWS
    def __init__(self, s3_boto, s3_client):
        self.s3 = s3_boto
        self.s3_client = s3_client

    def upload_file_bucket(self, file_to_upload, bucket_name, bucket_local_name_file):
        self.s3.meta.client.upload_file(file_to_upload, bucket_name, bucket_local_name_file)
    
    def print_content_bucket(self, bucket_name, return_item=False):
        list_item=[]
        my_bucket = self.s3.Bucket(bucket_name)
        for file in my_bucket.objects.filter(Prefix='.csv'):
            print(file.key)

    def download_file_bucket(self, bucket_name, bucket_local_name_file, local_file_name):
        self.s3.meta.client.download_file(bucket_name, bucket_local_name_file ,local_file_name)
        #delet directory_to_remove

    def object_to_delete_from_bucket(self, bucket_name, object_to_remove):
        self.s3.Object(bucket_name, object_to_remove).delete()

    def file_to_delete_from_bucket(self, bucket_name, file_to_remove):
        self.s3.Object(bucket_name,file_to_remove).delete()

    def upload_local_image_to_bucket(self,local_path, bucket_name):
        ## UPLOAD IMAGE FROM deepnote INTO bucket
        for path, dirs, files in os.walk(local_path):
            for filename in files:
                self.upload_file_bucket(f"{local_path}/{filename}", bucket_name, f"img/{filename}")
    
    def dl_file_from_dir_bucket(self, bucket, prefix, csv_local_path ):
        prefix = prefix
        bucket = bucket
        result = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        i=0
        for item in result['Contents']:
            i+=1
            if(item['Key']!= prefix) :
                cle, fichier = item['Key'].split("/",1)
                aws_instance.download_file_bucket(bucket, item['Key'],f"{csv_local_path}{fichier}")
                print(fichier)

############################################## MAIN  #######################################
# Initialize Amazon Credetial ## THIS IS THE MAIN
# MAIN instance#
s3 = boto3.resource(
    's3',
    aws_access_key_id="AKIA2GUR44TUQFSZHMV4",
    aws_secret_access_key="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",
)
s3_client = boto3.client(
    's3',
    aws_access_key_id="AKIA2GUR44TUQFSZHMV4",
    aws_secret_access_key="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",
)
aws_instance = aws_class(s3,s3_client)
bucket_name = 'hetic-bigdata'

################################ ANALYSE DATA ###############################################

# Create SparkSession
spark = SparkSession.builder.master("local[*]") \
                    .appName('FinalTP') \
                    .getOrCreate()

# Extract SparkContext
sc = spark.sparkContext

print('Spark CPU usage :', sc.defaultParallelism)

aws_instance.download_file_bucket('hetic-bigdata', 'reporting/global/df_corelation.csv', '/usr/local/spark/resources/data/df_corelation.csv') # 2ieme argument = bucket file path - 3ieme=local file csv path

df_corelation = pd.read_csv('/usr/local/spark/resources/data/df_corelation.csv')

from sqlalchemy import create_engine 
engine = create_engine('postgresql://airflow:airflow@0.0.0.0:5432/airflow') 
df_corelation.to_sql('table_project', engine)

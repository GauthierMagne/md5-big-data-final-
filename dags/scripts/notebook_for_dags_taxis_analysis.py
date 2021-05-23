
import aws
import os
import psycopg2
import pandas as pd
import numpy as np
import random
import boto3
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
        print("APRES INITIALISATION CLIENT ")
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
bucket_directory = 'taxi/'

csv_local_path="/usr/local/spark/resources/data/"  #=====>#  local csv filepath
aws_instance.dl_file_from_dir_bucket(bucket_name, bucket_directory, csv_local_path)



################################ ANALYSE DATA ###############################################

# Create SparkSession
spark = SparkSession.builder.master("local[*]") \
                    .appName('FinalTP') \
                    .getOrCreate()

# Extract SparkContext
sc = spark.sparkContext

print('Spark CPU usage :', sc.defaultParallelism)

print("FINIS")
green_taxi_schema = StructType() \
      .add("VendorID",IntegerType(),True) \
      .add("lpep_pickup_datetime",TimestampType(),True) \
      .add("lpep_dropoff_datetime",TimestampType(),True) \
      .add("store_and_fwd_flag",StringType(),True) \
      .add("RatecodeID",IntegerType(),True) \
      .add("PULocationID",IntegerType(),True) \
      .add("DOLocationID",IntegerType(),True) \
      .add("passenger_count",IntegerType(),True) \
      .add("trip_distance",DoubleType(),True) \
      .add("fare_amount",DoubleType(),True) \
      .add("extra",DoubleType(),True) \
      .add("mta_tax",DoubleType(),True) \
      .add("tip_amount",DoubleType(),True) \
      .add("tolls_amount",DoubleType(),True) \
      .add("ehail_fee",DoubleType(),True) \
      .add("improvement_surcharge",DoubleType(),True) \
      .add("total_amount",DoubleType(),True) \
      .add("payment_type",DoubleType(),True) \
      .add("trip_type",DoubleType(),True) \
      .add("congestion_surcharge",DoubleType(),True)

df_green = spark.read.option("header",True) \
     .schema(green_taxi_schema) \
     .csv("/usr/local/spark/resources/data/green_tripdata_taxi_2020-07.csv")


schema_location = StructType() \
      .add("LocationID",IntegerType(),True) \
      .add("Borough",StringType(),True) \
      .add("Zone",StringType(),True) \
      .add("service_zone",StringType(),True)

df_location = spark.read.option("header",True) \
     .schema(schema_location) \
     .csv("/usr/local/spark/resources/data/taxi_zone_lookup.csv")


# Join to get the location of all trip
print("JOINTURE")
# 1 - Start location
df_green = df_green.join(df_location,df_green['PULocationID'] ==  df_location["LocationID"],"inner").select(df_green["*"],
                                                                                                            F.col('Borough').alias("start_location"),
                                                                                                            F.col('service_zone').alias("start_zone"))

# 2 - End location
df_green = df_green.join(df_location,df_green['DOLocationID'] ==  df_location["LocationID"],"inner").select(df_green["*"],
                                                                                                            F.col('Borough').alias("end_location"),
                                                                                                            F.col('service_zone').alias("end_zone"))

# Adding new column 'trip_duration' / Time in second
df_green = df_green.withColumn('trip_duration', df_green['lpep_dropoff_datetime'].cast("long") - df_green['lpep_pickup_datetime'].cast("long"))


# Adding new columns: split pick-up timestamp in day, hour
df_green = df_green.withColumn('pick-up_day', F.dayofweek(df_green['lpep_pickup_datetime'])) \
                   .withColumn('pick-up_day_name', F.date_format(df_green['lpep_pickup_datetime'], 'EEEE')) \
                   .withColumn('pick-up_hour', F.hour(df_green['lpep_pickup_datetime']))


# Drop columns
df_green = df_green.drop('ehail_fee')

# Drop NaN rows
df_green = df_green.na.drop()

# Number of rows left
print(df_green.count())

# Transform our Dataframe into a Panda DataFrame
df_green_pandas = df_green.toPandas()




df_green_focus_hour = df_green_pandas.groupby('pick-up_hour') \
                                   .agg({
                                       'total_amount' : 'mean',
                                       'tip_amount' : 'mean',
                                       'trip_duration' : 'mean',
                                       'trip_distance' : 'mean',
                                       'passenger_count' : 'mean',
                                       'lpep_pickup_datetime' : 'count'
                                   }) \
                                   .rename(columns={
                                       'total_amount' : 'total_amount_mean',
                                       'tip_amount' : 'tip_amount_mean',
                                       'trip_duration' : 'trip_duration_mean',
                                       'trip_distance' : 'trip_distance_mean',
                                       'passenger_count': 'passenger_count_mean',
                                       'lpep_pickup_datetime' : 'count'
                                   }) \
                                   .reset_index()




df_green_focus_dayow = df_green_pandas.groupby('pick-up_day_name') \
                                   .agg({
                                       'total_amount' : 'mean',
                                       'tip_amount' : 'mean',
                                       'trip_duration' : 'mean',
                                       'trip_distance' : 'mean',
                                       'passenger_count' : 'mean',
                                       'lpep_pickup_datetime' : 'count'
                                   }) \
                                   .rename(columns={
                                       'total_amount' : 'total_amount_mean',
                                       'tip_amount' : 'tip_amount_mean',
                                       'trip_duration' : 'trip_duration_mean',
                                       'trip_distance' : 'trip_distance_mean',
                                       'passenger_count': 'passenger_count_mean',
                                       'lpep_pickup_datetime' : 'count'
                                   }) \
                                   .reset_index()



df_green_focus_location = df_green_pandas.groupby('start_location') \
                                   .agg({
                                       'total_amount' : 'mean',
                                       'tip_amount' : 'mean',
                                       'trip_duration' : 'mean',
                                       'trip_distance' : 'mean',
                                       'passenger_count' : 'mean',
                                       'lpep_pickup_datetime' : 'count'
                                   }) \
                                   .rename(columns={
                                       'total_amount' : 'total_amount_mean',
                                       'tip_amount' : 'tip_amount_mean',
                                       'trip_duration' : 'trip_duration_mean',
                                       'trip_distance' : 'trip_distance_mean',
                                       'passenger_count': 'passenger_count_mean',
                                       'lpep_pickup_datetime' : 'count'
                                   }) \
                                   .reset_index()
             
#Pour transformer en csv les 
df_green_focus_hour.to_csv('/usr/local/spark/resources/data/df_green_focus_hour.csv', index=False)
df_green_focus_dayow.to_csv('/usr/local/spark/resources/data/df_green_focus_dayow.csv', index=False)
df_green_focus_location.to_csv('/usr/local/spark/resources/data/df_green_focus_location.csv', index=False)

bucket_directory = 'reporting/taxi/'

file_local_path = '/usr/local/spark/resources/data/'
file_local_name = "df_green_focus_hour.csv"
file_bucket_local_name ="taxi_focus_hour.csv"
aws_instance.upload_file_bucket(f"{file_local_path}{file_local_name}", bucket_name, f"{bucket_directory}{file_bucket_local_name}")

file_local_path = '/usr/local/spark/resources/data/'
file_local_name = "df_green_focus_dayow.csv"
file_bucket_local_name ="taxis_focus_dayow.csv"
aws_instance.upload_file_bucket(f"{file_local_path}{file_local_name}", bucket_name, f"{bucket_directory}{file_bucket_local_name}")

file_local_path = '/usr/local/spark/resources/data/'
file_local_name = "df_green_focus_location.csv"
file_bucket_local_name ="green_focus_location.csv"
aws_instance.upload_file_bucket(f"{file_local_path}{file_local_name}", bucket_name, f"{bucket_directory}{file_bucket_local_name}")
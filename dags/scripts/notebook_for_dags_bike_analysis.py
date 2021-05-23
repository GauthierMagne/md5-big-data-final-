import matplotlib.pyplot as plt
import aws
import os
import psycopg2
import pandas as pd
import numpy as np
import random
import boto3
import seaborn as sns
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
bucket_directory = 'bicycle/'

csv_local_path="/usr/local/spark/resources/data/"  #=====> Mettre chemin vers fichiers
aws_instance.dl_file_from_dir_bucket(bucket_name, bucket_directory, csv_local_path) #3A1 extraire des CSV sous jacent que vous enverrez dans votre bucket dans le répertoire “reporting/bicycle”

################################ ANALYSE DATA ###############################################

# Create SparkSession
spark = SparkSession.builder.master("local[*]") \
                    .appName('FinalTP') \
                    .getOrCreate()

# Extract SparkContext
sc = spark.sparkContext

print('Spark CPU usage :', sc.defaultParallelism)




# Bike
aws_instance.download_file_bucket('hetic-bigdata', 'bicycle/202007-citibike-tripdata.csv', '/usr/local/spark/resources/data/202007-citibike-tripdata.csv') # 2ieme argument = chemin in bucket et #3ieme=#Chemin du fichier csv

bike_schema = StructType() \
      .add("tripduration",IntegerType(),True) \
      .add("starttime",TimestampType(),True) \
      .add("stoptime",TimestampType(),True) \
      .add("start station id",IntegerType(),True) \
      .add("start station name",StringType(),True) \
      .add("start station latitude",DoubleType(),True) \
      .add("start station longitude",DoubleType(),True) \
      .add("end station id",IntegerType(),True) \
      .add("end station name",StringType(),True) \
      .add("end station latitude",DoubleType(),True) \
      .add("end station longitude",DoubleType(),True) \
      .add("bikeid",IntegerType(),True) \
      .add("usertype",StringType(),True) \
      .add("birth year",IntegerType(),True) \
      .add("gender",IntegerType(),True)

df_bike = spark.read.option("header",True) \
     .schema(bike_schema) \
     .csv("/usr/local/spark/resources/data/202007-citibike-tripdata.csv") #Chemin du fichier csv

df_bike.show(5)

# Looking for missing values
for col in df_bike.columns:
  print(col, "\t", "with null values: ", df_bike.filter(df_bike[col].isNull()).count())
  
df_bike = df_bike.withColumn('starttime_day', F.dayofweek(df_bike['starttime'])) \
                   .withColumn('starttime_day_name', F.date_format(df_bike['starttime'], 'EEEE')) \
                   .withColumn('starttime_hour', F.hour(df_bike['starttime']))

# Drop columns
df_bike = df_bike.drop('starttime', 'stoptime', 'start station id', 'start station name', 'end station id', 'end station name')

# Drop NaN rows
df_bike = df_bike.na.drop()

# Number of rows left
print(df_bike.count())
from geopy.distance import geodesic
from geopy.geocoders import Nominatim

def get_distance(source_lat, source_long, dest_lat, dest_long):
    # Returns the distance in Miles between the source and the destination.
    
    distance = geodesic((source_lat, source_long), 
                        (dest_lat, dest_long)).miles
    return distance

get_distance_udf = F.udf(get_distance)

# def get_city(reverse, source_lat, source_long):
#     location = reverse((source_lat, source_long))
#     return location.address.split(",")[3]

# Adding new column: trip distance in miles
df_bike = df_bike.withColumn('trip_distance', get_distance_udf(df_bike['start station latitude'], df_bike['start station longitude'], df_bike['end station latitude'], df_bike['end station longitude']))
# Adding a limit because it take too much time // (Pour l'exemple)
df_bike_limit = df_bike.limit(200)

print("DERNIERE ETAPE ")
###################### SAI TRAI LONT #######################
# Converting to Pandas DataFrame
df_bike_pandas = df_bike_limit.toPandas()
from tqdm import tqdm
from tqdm._tqdm_notebook import tqdm_notebook
from geopy.extra.rate_limiter import RateLimiter


def location_split(full_location):
    split_location = full_location[0].split(',')[3].strip()
    if 'Manhattan' in split_location:
        return split_location.split(' ')[0]
    return split_location

tqdm.pandas()

df_bike_pandas['coord'] = df_bike_pandas['start station latitude'].map(str) + ',' + df_bike_pandas['start station longitude'].map(str)

locator = Nominatim(user_agent="google", timeout=10)
rgeocode = RateLimiter(locator.reverse, min_delay_seconds=1/50)

df_bike_pandas['start_location'] = df_bike_pandas['coord'].progress_apply(rgeocode)
df_bike_pandas['start_location'] = df_bike_pandas['start_location'].apply(lambda x: location_split(x))
plt.xticks(rotation=45)
sns.set(rc={'figure.figsize':(15,12)})
sns.countplot(x="start_location", data=df_bike_pandas)
df_bike_pandas['trip_distance'] = pd.to_numeric(df_bike_pandas['trip_distance'])

df_bike_pandas_location = df_bike_pandas.groupby('start_location') \
                                   .agg({
                                       'trip_distance' : 'mean',
                                       'tripduration' : 'mean',
                                       'bikeid' : 'count'
                                   }) \
                                   .rename(columns={
                                       'trip_distance' : 'trip_distance_mean',
                                       'tripduration' : 'trip_duration_mean',
                                       'bikeid' : 'trip_count'
                                   }) \
                                   .reset_index()
plt.xticks(rotation=45)
plt.xticks(rotation=45)
df_bike_pandas_location.head()

df_bike_pandas_location.to_csv('/usr/local/spark/resources/data/df_bike_pandas_location.csv', index=False)

bucket_directory = 'reporting/bicycle/'

file_local_path = '/usr/local/spark/resources/data/'
file_local_name = "df_bike_pandas_location.csv"
file_bucket_local_name ="bike_pandas_location.csv"
aws_instance.upload_file_bucket(f"{file_local_path}{file_local_name}", bucket_name, f"{bucket_directory}{file_bucket_local_name}")


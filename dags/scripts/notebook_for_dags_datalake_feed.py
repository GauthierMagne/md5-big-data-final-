
import aws
import os
import psycopg2
import pandas as pd
import numpy as np
import random
import boto3
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
        
    def downl_dir_from_bucket(self, bucket_name, dir_path):
        list_item=[]
        client = boto3.client('s3')
        my_bucket = self.s3.Bucket(bucket_name)
        startAfter = dir_path
        theobjects = client.list_objects_v2(Bucket=my_bucket, StartAfter=startAfter)

    def download_file_bucket(self, bucket_name, bucket_local_name_file, local_file_name):
        self.s3.meta.client.download_file(bucket_name, bucket_local_name_file ,local_file_name)

    def object_to_delete_from_bucket(self, bucket_name, object_to_remove):
        self.s3.Object(bucket_name, object_to_remove).delete()

    def file_to_delete_from_bucket(self, bucket_name, file_to_remove):
        self.s3.Object(bucket_name,file_to_remove).delete()

    def upload_local_image_to_bucket(self,local_path, bucket_name):
        ## UPLOAD IMAGE FROM deepnote INTO bucket
        for path, dirs, files in os.walk(local_path):
            for filename in files:
                self.upload_file_bucket(f"{local_path}/{filename}", bucket_name, f"img/{filename}")
    
    def dl_file_from_dir_bucket(self, bucket, prefix):
        prefix = prefix
        bucket = bucket
        result = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        i=0
        for item in result['Contents']:
            i+=1
            if(item['Key']!= prefix) :
                cle, fichier = item['Key'].split("/",1)
                aws_instance.download_file_bucket('hetic-bigdata',item['Key'],fichier)
            

# Initialize Amazon Credetial
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

#PARTI 2 
#1:
#Tous les CSV contenant le mot “trip” seront envoyé dans votre bucket dans le répertoire “taxi”
#Tous les CSV contenant le mot “citibike” seront envoyé dans votre bucket dans le répertoire “bicycle” 

def count_nb_file(local_path, word_researched):
    entries = os.listdir(local_path)
    print(entries)
    list_file = find_word(entries, word_researched)
    return list_file

def find_word(list_word_to_analyse, extension):
    file_list=[]
    if len(list_word_to_analyse) > 0:
        for file_analysed in list_word_to_analyse:
            if extension in file_analysed:
                file_list.append(file_analysed)
        return file_list
    else:
        print("Nothing in List ")
csv_local_path="/usr/local/spark/resources/data"  #=====> Mettre chemin vers fichiers
taxi_csv_file = count_nb_file(csv_local_path,"taxi")
bicycle_csv_file = count_nb_file(csv_local_path,"bicycle")
for item in taxi_csv_file:
    print(f"LES ITEMS {item}")
    aws_instance.upload_file_bucket(f"{csv_local_path}/{item}",'hetic-bigdata',f'taxi/{item}')
    
for item in bicycle_csv_file:
    print(f"LES ITEMS {item}")
    aws_instance.upload_file_bucket(f"{csv_local_path}/{item}",'hetic-bigdata',f'bicycle/{item}')

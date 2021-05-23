
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
        #for obj in theobjects['Contents']:
        #    print(obj['Key'])
    #    for file in my_bucket.objects.all():
    #        if return_item:
    #            list_item.append(file.key)
    #    print(file.key)
    #    if len(list_item) > 0:
    #        return list_item
        
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


##Main methode for aws instance##
#aws_instance.download_file_bucket('kickoffthib',"dont_watch.txt",'recupe_fichier.txt') #download a file from the bucket to local file
#aws_instance.upload_file_bucket('.csv/users.csv', 'kickoffthib', 'csv/users.csv') #upload local file to the bucket
#aws_instance.print_content_bucket('kickoffthib') ## print content bucket
## delete file_to_remove
#aws_instance.object_to_delete_from_bucket('kickoffthib', 'file_to_remove/')
#aws_instance.file_to_delete_from_bucket('kickoffthib','hello.txt')

#Partie 2
#aws_instance.upload_local_image_to_bucket("/work/img",'kickoffthib')
#aws_instance.download_file_bucket('kickoffthib','csv/users.csv', './csvFromBucket/users.csv')
#aws_instance.download_file_bucket('kickoffthib','csv/users.csv', './csvFromBucket/users.csv')
#bucket_local_name_file,bucket_name ,local_file_name



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

#.downl_dir_from_bucket('hetic-bigdata', "/csv_file/")

#Tous les CSV contenant le mot “trip” seront envoyé dans votre bucket dans le répertoire “taxi”
#Tous les CSV contenant le mot “citibike” seront envoyé dans votre bucket dans le répertoire “bicycle” 

# <a style='text-decoration:none;line-height:16px;display:flex;color:#5B5B62;padding:10px;justify-content:end;' href='https://deepnote.com?utm_source=created-in-deepnote-cell&projectId=2f1d4b6b-d47e-44da-8e1b-719b861c4345' target="_blank">
# <img alt='Created in deepnote.com' style='display:inline;max-height:16px;margin:0px;margin-right:7.5px;' src='data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPHN2ZyB3aWR0aD0iODBweCIgaGVpZ2h0PSI4MHB4IiB2aWV3Qm94PSIwIDAgODAgODAiIHZlcnNpb249IjEuMSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIiB4bWxuczp4bGluaz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94bGluayI+CiAgICA8IS0tIEdlbmVyYXRvcjogU2tldGNoIDU0LjEgKDc2NDkwKSAtIGh0dHBzOi8vc2tldGNoYXBwLmNvbSAtLT4KICAgIDx0aXRsZT5Hcm91cCAzPC90aXRsZT4KICAgIDxkZXNjPkNyZWF0ZWQgd2l0aCBTa2V0Y2guPC9kZXNjPgogICAgPGcgaWQ9IkxhbmRpbmciIHN0cm9rZT0ibm9uZSIgc3Ryb2tlLXdpZHRoPSIxIiBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPgogICAgICAgIDxnIGlkPSJBcnRib2FyZCIgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoLTEyMzUuMDAwMDAwLCAtNzkuMDAwMDAwKSI+CiAgICAgICAgICAgIDxnIGlkPSJHcm91cC0zIiB0cmFuc2Zvcm09InRyYW5zbGF0ZSgxMjM1LjAwMDAwMCwgNzkuMDAwMDAwKSI+CiAgICAgICAgICAgICAgICA8cG9seWdvbiBpZD0iUGF0aC0yMCIgZmlsbD0iIzAyNjVCNCIgcG9pbnRzPSIyLjM3NjIzNzYyIDgwIDM4LjA0NzY2NjcgODAgNTcuODIxNzgyMiA3My44MDU3NTkyIDU3LjgyMTc4MjIgMzIuNzU5MjczOSAzOS4xNDAyMjc4IDMxLjY4MzE2ODMiPjwvcG9seWdvbj4KICAgICAgICAgICAgICAgIDxwYXRoIGQ9Ik0zNS4wMDc3MTgsODAgQzQyLjkwNjIwMDcsNzYuNDU0OTM1OCA0Ny41NjQ5MTY3LDcxLjU0MjI2NzEgNDguOTgzODY2LDY1LjI2MTk5MzkgQzUxLjExMjI4OTksNTUuODQxNTg0MiA0MS42NzcxNzk1LDQ5LjIxMjIyODQgMjUuNjIzOTg0Niw0OS4yMTIyMjg0IEMyNS40ODQ5Mjg5LDQ5LjEyNjg0NDggMjkuODI2MTI5Niw0My4yODM4MjQ4IDM4LjY0NzU4NjksMzEuNjgzMTY4MyBMNzIuODcxMjg3MSwzMi41NTQ0MjUgTDY1LjI4MDk3Myw2Ny42NzYzNDIxIEw1MS4xMTIyODk5LDc3LjM3NjE0NCBMMzUuMDA3NzE4LDgwIFoiIGlkPSJQYXRoLTIyIiBmaWxsPSIjMDAyODY4Ij48L3BhdGg+CiAgICAgICAgICAgICAgICA8cGF0aCBkPSJNMCwzNy43MzA0NDA1IEwyNy4xMTQ1MzcsMC4yNTcxMTE0MzYgQzYyLjM3MTUxMjMsLTEuOTkwNzE3MDEgODAsMTAuNTAwMzkyNyA4MCwzNy43MzA0NDA1IEM4MCw2NC45NjA0ODgyIDY0Ljc3NjUwMzgsNzkuMDUwMzQxNCAzNC4zMjk1MTEzLDgwIEM0Ny4wNTUzNDg5LDc3LjU2NzA4MDggNTMuNDE4MjY3Nyw3MC4zMTM2MTAzIDUzLjQxODI2NzcsNTguMjM5NTg4NSBDNTMuNDE4MjY3Nyw0MC4xMjg1NTU3IDM2LjMwMzk1NDQsMzcuNzMwNDQwNSAyNS4yMjc0MTcsMzcuNzMwNDQwNSBDMTcuODQzMDU4NiwzNy43MzA0NDA1IDkuNDMzOTE5NjYsMzcuNzMwNDQwNSAwLDM3LjczMDQ0MDUgWiIgaWQ9IlBhdGgtMTkiIGZpbGw9IiMzNzkzRUYiPjwvcGF0aD4KICAgICAgICAgICAgPC9nPgogICAgICAgIDwvZz4KICAgIDwvZz4KPC9zdmc+' > </img>
# Created in <span style='font-weight:600;margin-left:4px;'>Deepnote</span></a>

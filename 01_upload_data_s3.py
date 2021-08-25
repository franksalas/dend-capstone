import pandas as pd
import numpy as np
import sys
import os
import re
import glob
import boto3
import awswrangler as wr
from botocore.exceptions import ClientError
import time

# helper functions


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def return_path(path, year):
    '''return path'''
    return f'{path}{year}/'


def list_files(path,):
    '''list files in a given path'''
    return list(glob.glob(f'{path}/*'))


def list_files_match(path, file_type):
    '''list the files in a given path given file type'''
    return list(glob.glob(f'{path}/*{file_type}'))


def upload_list_of_files_S3(file_list, bucket_name, subfolder_path):
    '''function to upload multiple files in a path to S3
    param file_list: list of paths where the files are located
    param bucket_name: bucket to upload to
    param: subfoder_path: where in the bucket the files will go to'''
    try:
        for file in file_list:
            file_name = file.rsplit('/', 1)[1]  # name of file
            upload_file(file, bucket_name,
                        object_name=f"{subfolder_path}/{file_name}")
    except ClientError as e:
        logging.error(e)
        return False
    return True


def main(bucket_name):
    # upload crime data for given years
    years = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
    for i in years:
        print(f'uploading:{i}')
        bucket_subfolders = f'capstone/raw-data/crime-data/{i}'
        path = f'data/raw/crime_data/{i}/csv'
        file_list = list_files(path)
        upload_list_of_files_S3(file_list, bucket_name, bucket_subfolders)

    # upload weather data
    print(f'uploading:weather data')
    bucket_subfolders = 'capstone/raw-data/weather-data'
    path = 'data/raw/weather_data'
    file_list = list_files(path)
    upload_list_of_files_S3(file_list, bucket_name, bucket_subfolders)


if __name__ == "__main__":
    start_time = time.time()
    # name of your bucket to upload data
    bucket_name = "dend-data"
    print(f"bucket-name:{bucket_name}")
    main(bucket_name)
    print("--- %s seconds ---" % (time.time() - start_time))

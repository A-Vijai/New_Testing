from io import StringIO
import boto3
import pandas as pd
import os
from datetime import date, datetime
import json

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = os.getenv('sns_arn')

def lambda_handler(event, context):
    # TODO implement
    print(event)
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_file_key = event["Records"][0]["s3"]["object"]["key"]
    print(bucket_name)
    print(s3_file_key)
    resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
    body = resp['Body'].read()
    json_dicts = body.decode('utf-8').split('\r\n')
    json_df = pd.DataFrame(columns = ['id','status','amount','date'])

    for line in json_dicts:
        texts=json.loads(line)
        if texts["status"] =='delivered':
            json_df.loc[texts["id"]] = texts
    json_df.to_csv('/tmp/test.csv')

    try:
        date_var = str(date.today())
        file_name ='processed_data/{}_processed_data.csv'.format(date_var)
    except:
        file_name = 'processed_data/processed_data.csv'
    lambda_path = '/tmp/test.csv'
    bucket_name = 'target-zone-project'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.upload_file('/tmp/test.csv', file_name)

    respone=sns_client.publish(TopicArn=sns_arn,Message="Doordash File {} has been processed succesfuly !!".format("s3://"+bucket_name+"/"+s3_file_key))

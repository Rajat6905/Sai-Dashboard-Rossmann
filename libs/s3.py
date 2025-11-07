import boto3
import json
from botocore.exceptions import ClientError
from fastapi import HTTPException
from cachetools import cached, TTLCache

from config import config

BUCKET_NAME = config.BUCKET_NAME
AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
S3_SECRET_NAME = config.S3_SECRET_NAME
AWS_REGION = config.AWS_REGION

s3_credentials_cache = TTLCache(maxsize=100, ttl=60)


@cached(cache=s3_credentials_cache)
def get_s3_credentials(S3_SECRET_NAME):
    session = boto3.session.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID, 
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY, 
        region_name='eu-west-2'
    )
    client = session.client('secretsmanager')
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=S3_SECRET_NAME
        )
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise e

    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
        credentials = json.loads(secret)
        return credentials
    else:
        raise ValueError("Secret does not contain a string value.")

def create_presigned_url(object_name,expires_in):
    credentials = get_s3_credentials(S3_SECRET_NAME)
    
    aws_access_key_id = credentials.get("AWS-access-key")
    aws_secret_access_key = credentials.get("AWS-secret-key")

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id, 
        aws_secret_access_key=aws_secret_access_key, 
        region_name=AWS_REGION
    )

    try:
        response = s3_client.generate_presigned_url(
            'get_object', 
            Params={'Bucket': BUCKET_NAME, 'Key': object_name}, 
            ExpiresIn=expires_in
        )
    except ClientError as e:
        print(e)
        raise HTTPException(status_code=500, detail="Error generating presigned URL")
    
    return response

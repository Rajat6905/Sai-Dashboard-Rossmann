import boto3
from botocore.exceptions import ClientError
from fastapi import HTTPException
from config import config

BUCKET_NAME = config.BUCKET_NAME
AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
AWS_REGION = config.AWS_REGION

def create_presigned_url(object_name,expires_in):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID, 
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY, 
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

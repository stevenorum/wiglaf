import boto3
from calvin import json

def body(bucket, key=None):
    bucket = bucket if not bucket.startswith("s3://") else bucket[5:]
    if not key:
        if "/" in bucket:
            key = "/".join(bucket.split("/")[1:])
            bucket = bucket.split("/")[0]
        else:
            raise RuntimeError("Must specify either a full file path or a bucket and key!")
    response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    return response["Body"]

def readb(*args, **kwargs):
    return body(*args, **kwargs).read()

def read(*args, **kwargs):
    return readb(*args, **kwargs).decode("utf-8")

def load(*args, **kwargs):
    return json.loads(read(*args, **kwargs))

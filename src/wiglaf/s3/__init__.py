import boto3
from calvin import json

def bucket_and_key(bucket, key, *args, **kwargs):
    bucket = bucket if not bucket.startswith("s3://") else bucket[5:]
    if not key:
        if "/" in bucket:
            key = "/".join(bucket.split("/")[1:])
            bucket = bucket.split("/")[0]
        else:
            raise RuntimeError("Must specify either a full file path or a bucket and key!")
    return bucket, key

def body(*args, **kwargs):
    bucket, key = bucket_and_key(*args, **kwargs)
    response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    return response["Body"]

def readb(*args, **kwargs):
    return body(*args, **kwargs).read()

def read(*args, **kwargs):
    return readb(*args, **kwargs).decode("utf-8")

def load(*args, **kwargs):
    return json.loads(read(*args, **kwargs))

def upload_file(*args, **kwargs):
    bucket, key = bucket_and_key(*args, **kwargs)
    # Check if file already in S3.
    # If no, or if force=True, upload. (Even if the file is the same, uploading re-triggers the lambda watching the bucket.)
    # If yes, see if it has an etag or MD5 tag.
    # If so, take MD5 of the file on disk.  If it matches either S3 object tag, skip the upload.
    # If they don't match, upload the new file and attach the MD5 tag.
    pass

def delete_file(*args, **kwargs):
    bucket, key = bucket_and_key(*args, **kwargs)
    pass

def delete_file(bucket, prefix, *args, **kwargs):
    pass

def list_bucket(bucket, prefix, *args, **kwargs):
    pass

def download_results():
    pass

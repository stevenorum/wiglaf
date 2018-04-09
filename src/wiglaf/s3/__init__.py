import boto3
from calvin import files, json

def bucket_and_key(bucket, key, *args, **kwargs):
    bucket = bucket if not bucket.startswith("s3://") else bucket[5:]
    if not key:
        if "/" in bucket:
            key = "/".join(bucket.split("/")[1:])
            bucket = bucket.split("/")[0]
        else:
            raise RuntimeError("Must specify either a full file path or a bucket and key!")
    return bucket, key

def remove_prefix(filename, prefix):
    prefix = prefix.strip("/")
    filename = filename.strip("/")
    if len(prefix) >= len(filename):
        return ""
    return filename[len(prefix):].strip("/")

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

def list_files(bucket, prefix=""):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=1000,
    )
    objects = response.get('Contents', [])
    token = response.get('NextContinuationToken', None)
    while token:
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1000,
            ContinuationToken=token
        )
        objects.extend(response['Contents'])
        token = response.get('NextContinuationToken', None)
    return objects

def download_files(bucket, prefix, directory):
    for fileobj in list_files(bucket=bucket, prefix=prefix):
        files.writeb(os.path.join(directory, remove_prefix(filename=fileobj["Key"], prefix=prefix)),
                     body(bucket=bucket, key=fileobj["Key"]).read())

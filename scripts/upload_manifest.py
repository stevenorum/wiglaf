#!/usr/bin/env python3

import argparse
import boto3
import os

from calvin import json

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile",
                        help='AWS profile to use.',
                        required=True)
    parser.add_argument("--region",
                        help='AWS region to use.',
                        default="us-east-1")
    parser.add_argument("--name",
                        help='Name for the cluster.',
                        required=True)
    parser.add_argument("--filepath",
                        help='Path of the manifest file on disk.',
                        required=True)
    parser.add_argument("--upload_resources",
                        help='If this flag is provided, the FilesToDownload specified in the manifest will also be uploaded.',
                        action="store_true",
                        default=False,
                        dest="upload_resources")
    return parser.parse_args()

def get_data_bucket(stack_name):
    cf = boto3.client("cloudformation")
    stack = cf.describe_stacks(StackName=stack_name)["Stacks"][0]
    outputs = {op["OutputKey"]:op["OutputValue"] for op in stack["Outputs"]}
    bucket = outputs["DataBucket"]
    return bucket

def upload_file(filename, bucket, key):
    s3 = boto3.client("s3")
    print("Uploading {} to s3://{}/{}...".format(filename, bucket, key))
    with open(filename, "rb") as f:
        s3.put_object(Bucket=bucket, Key=key, Body=f)

def main():
    args = parse_args()
    boto3.setup_default_session(region_name=args.region, profile_name=args.profile)
    bucket = get_data_bucket(args.name)
    if args.upload_resources:
        manifest = json.loadf(args.filepath)
        for filename in manifest["FilesToDownload"]:
            key = "jobs/{job_name}/resources/{filename}".format(job_name=manifest["JobName"], filename=filename)
            upload_file(os.path.join(manifest["LocalDirectory"], filename), bucket, key)
    upload_file(args.filepath, bucket, "manifest.json")
    pass

if __name__ == "__main__":
    main()

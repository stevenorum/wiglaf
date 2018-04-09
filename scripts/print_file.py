#!/usr/bin/env python3

import argparse
import boto3
import copy
from datetime import datetime, timedelta, timezone
import time
import traceback

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
    parser.add_argument("--key",
                        help='Key of the file to print.',
                        required=True)
    return parser.parse_args()

def print_file(bucket_name, key):
    s3 = boto3.client('s3')
    manifest_object = s3.get_object(Bucket=bucket_name, Key=key)
    print(manifest_object["Body"].read().decode("utf-8"))
    
def get_bucket_name(stack_name):
    cf = boto3.client("cloudformation")
    stack = cf.describe_stacks(StackName=stack_name)["Stacks"][0]
    outputs = {op["OutputKey"]:op["OutputValue"] for op in stack["Outputs"]}
    bucket = outputs["DataBucket"]
    return bucket

def main():
    args = parse_args()
    boto3.setup_default_session(region_name=args.region, profile_name=args.profile)
    bucket_name = get_bucket_name(args.name)
    print_file(bucket_name, args.key)
    pass

if __name__ == "__main__":
    main()

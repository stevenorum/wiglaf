#!/usr/bin/env python3

import argparse
import boto3
import copy
import time
import traceback

from calvin import json
from calvin.files import readb

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
    return parser.parse_args()

def upload_manifest(stack_name, filepath):
    cf = boto3.client("cloudformation")
    s3 = boto3.client("s3")
    stack = cf.describe_stacks(StackName=stack_name)["Stacks"][0]
    outputs = {op["OutputKey"]:op["OutputValue"] for op in stack["Outputs"]}
    bucket = outputs["DataBucket"]
    key = "manifest.json"
    s3.put_object(Bucket=bucket, Key=key, Body=readb(filepath))

def main():
    args = parse_args()
    boto3.setup_default_session(region_name=args.region, profile_name=args.profile)
    upload_manifest(stack_name=args.name, filepath=args.filepath)
    pass

if __name__ == "__main__":
    main()

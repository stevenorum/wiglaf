#!/usr/bin/env python3

import argparse
import boto3
import copy
import time
import traceback

from calvin.aws.lambda_deployment import create_zipfile
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
    return parser.parse_args()

def clear_buckets_for_stack(name):
    cf = boto3.client("cloudformation")
    s3 = boto3.client("s3")
    stack = cf.describe_stacks(StackName=name)["Stacks"][0]
    outputs = {op["OutputKey"]:op["OutputValue"] for op in stack["Outputs"]}
    # Should update this to look through the stack resources and grab all buckets, but this'll work for now.
    buckets = list(outputs.values())
    for bucket in buckets:
        response = s3.list_objects_v2(
            Bucket=bucket,
            MaxKeys=1000,
        )
        batches = [[obj["Key"] for obj in response['Contents']]]
        token = response.get('NextContinuationToken', None)
        while token:
            response = s3.list_objects_v2(
                Bucket=bucket,
                MaxKeys=1000,
                ContinuationToken=token
            )
            batches.append([obj["Key"] for obj in response['Contents']])
            token = response.get('NextContinuationToken', None)
        for batch in batches:
            s3.delete_objects(
                Bucket=bucket,
                Delete={
                    "Objects": [{"Key":k} for k in batch],
                    "Quiet":True
                }
            )

def main():
    args = parse_args()
    boto3.setup_default_session(region_name=args.region, profile_name=args.profile)
    clear_buckets_for_stack(args.name)
    pass

if __name__ == "__main__":
    main()

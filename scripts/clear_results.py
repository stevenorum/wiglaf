#!/usr/bin/env python3

import argparse
import boto3
import copy
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
    parser.add_argument("--job",
                        help='Name of the job whose results you want to clear.',
                        required=True)
    return parser.parse_args()

def clear_results(stack_name, job_name):
    cf = boto3.client("cloudformation")
    s3 = boto3.client("s3")
    stack = cf.describe_stacks(StackName=stack_name)["Stacks"][0]
    outputs = {op["OutputKey"]:op["OutputValue"] for op in stack["Outputs"]}
    # Should update this to look through the stack resources and grab all buckets, but this'll work for now.
    bucket = outputs["DataBucket"]
    prefix = "jobs/{}/results/".format(job_name)
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=1000,
    )
    batches = [[obj["Key"] for obj in response['Contents']]]
    token = response.get('NextContinuationToken', None)
    while token:
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1000,
            ContinuationToken=token
        )
        batches.append([obj["Key"] for obj in response['Contents']])
        token = response.get('NextContinuationToken', None)
    for batch in batches:
        if batch:
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
    clear_results(stack_name=args.name, job_name=args.job)
    pass

if __name__ == "__main__":
    main()

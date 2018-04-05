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
                        help='Name of the job whose results you want to list.',
                        required=True)
    return parser.parse_args()

def list_all(s3, bucket, prefix):
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

BYTE_LEVELS="BKMGTPE"

def format_object(obj):
    # "{'Key': 'logs/testjob00/i-0f0e17205c232e20c/syslog', 'LastModified': datetime.datetime(2018, 4, 5, 8, 36, 50, tzinfo=tzutc()), 'ETag': '49b6ef2a07508d15e37fa9aef1a3a72a', 'Size': 1196868, 'StorageClass': 'STANDARD'}"
    # "102B Apr  4 10:33 src"
    size = float(obj["Size"])
    adj = 0
    while size >= 1000:
        adj += 1
        size /= 1024.0
    if size < 10:
        sizestr = "{:1.1f}".format(size)
    else:
        sizestr = "{:3.0f}".format(size)
    sizestr = "{}{}".format(sizestr, BYTE_LEVELS[adj])
    day = obj["LastModified"].strftime("%d")
    day = day if int(day) < 10 else " {}".format(day[-1])
    datestr = obj["LastModified"].strftime("%a {day} %H:%M").format(day=day)
    pretty = "{} {} {}".format(sizestr, datestr, obj["Key"])
    return pretty

def list_results(stack_name, job_name):
    cf = boto3.client("cloudformation")
    s3 = boto3.client("s3")
    stack = cf.describe_stacks(StackName=stack_name)["Stacks"][0]
    outputs = {op["OutputKey"]:op["OutputValue"] for op in stack["Outputs"]}
    bucket = outputs["DataBucket"]
    prefix = "results/{}/".format(job_name)
    results = list_all(s3, bucket, "results/{}/".format(job_name))
    resources = list_all(s3, bucket, "resources/{}/".format(job_name))
    logs = list_all(s3, bucket, "logs/{}/".format(job_name))
    fobjs = lambda x: [format_object(o) for o in x]
    print("\n".join(fobjs(resources)))
    print("\n".join(fobjs(results)))
    print("\n".join(fobjs(logs)))

def main():
    args = parse_args()
    boto3.setup_default_session(region_name=args.region, profile_name=args.profile)
    list_results(stack_name=args.name, job_name=args.job)
    pass

if __name__ == "__main__":
    main()

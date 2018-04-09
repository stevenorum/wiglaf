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
    parser.add_argument("--job",
                        help='Name of the job whose results you want to list.',
                        required=True)
    return parser.parse_args()

def generate(bucket_name, job_name):
    s3 = boto3.client('s3')
    manifest_key = "jobs/{job_name}/resources/wiglaf_manifest.json".format(job_name=job_name)
    manifest_object = s3.get_object(Bucket=bucket_name, Key=manifest_key)
    results_prefix = 'jobs/{job_name}/results/'.format(job_name=job_name)
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        MaxKeys=1000,
        Prefix=results_prefix,
    )
    objects = response['Contents']
    token = response.get('NextContinuationToken', None)
    while token:
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            MaxKeys=1000,
            Prefix=results_prefix,
            ContinuationToken=token
        )
        objects.extend(response['Contents'])
        token = response.get('NextContinuationToken', None)
    links = [s3.generate_presigned_url(ClientMethod='get_object',Params={'Bucket':bucket_name,'Key':obj['Key']},ExpiresIn=60*60*24) for obj in objects]

    start_dt = manifest_object["LastModified"]
    end_dt = datetime.now(timezone.utc)
    message_lines = [
        "Job '{}' finished".format(job_name),
        "",
        "Started: {}".format(start_dt.strftime("%a %d %H:%M:%S UTC")),
        "Ended: {}".format(end_dt.strftime("%a %d %H:%M:%S UTC")),
        "Duration: {}".format(pretty_delta(end_dt - start_dt)),
        "",
        "Links to output files:",
        ""
    ]
    message_lines.extend(links)
    print("\n".join(message_lines))

def pretty_delta(td):
    MINUTE = 60
    HOUR = MINUTE * 60
    DAY = HOUR * 24
    days, drem = divmod(td.seconds, DAY)
    hours, hrem = divmod(drem, HOUR)
    minutes, mrem = divmod(hrem, MINUTE)
    seconds = mrem
    pretty = ""
    if days:
        pretty += "{} day{}{}".format(days, "" if days == 1 else "s", "" if drem == 0 else ", ")
    if hours:
        pretty += "{} hour{}{}".format(hours, "" if hours == 1 else "s", "" if hrem == 0 else ", ")
    if minutes:
        pretty += "{} minute{}{}".format(minutes, "" if minutes == 1 else "s", "" if mrem == 0 else ", ")
    if seconds:
        pretty += "{} second{}".format(seconds, "" if seconds == 1 else "s")
    return "{1} ({0})".format(str(td), pretty)
    
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
    generate(bucket_name, args.job)
    pass

if __name__ == "__main__":
    main()

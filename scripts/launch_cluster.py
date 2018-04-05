#!/usr/bin/env python3

import argparse
import boto3
import copy
import hashlib
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
    parser.add_argument("--email",
                        help='Email address to notify of completed jobs.',
                        default="")
    return parser.parse_args()

def wait_for_stack(cf, name):
    stack = cf.describe_stacks(StackName=name)["Stacks"][0]
    status = stack["StackStatus"]
    while status.endswith("_IN_PROGRESS"):
        print("Stack '{}' in transitional state '{}'.  Sleeping for 5 seconds...".format(name, status))
        time.sleep(5)
        stack = cf.describe_stacks(StackName=name)["Stacks"][0]
        status = stack["StackStatus"]
    print("Stack '{}' in stable state '{}'.".format(name, status))
    if status in ["CREATE_COMPLETE","UPDATE_COMPLETE"]:
        return {op["OutputKey"]:op["OutputValue"] for op in stack["Outputs"]}
    else:
        raise RuntimeError("Stack {} is in unexpected state {}!".format(name, status))

def create_stack(name, template, params):
    cf = boto3.client("cloudformation")
    try:
        print("Creating stack '{}'".format(name))
        cf.create_stack(
            StackName=name,
            TemplateBody=json.dumps(template),
            Parameters=[{"ParameterKey":k,"ParameterValue":params[k]} for k in params],
            Capabilities=['CAPABILITY_IAM'],
            OnFailure='ROLLBACK'
        )
    except Exception as e:
        traceback.print_exc()
        pass
    return wait_for_stack(cf, name)

def update_stack(name, template, params):
    cf = boto3.client("cloudformation")
    wait_for_stack(cf, name)
    print("Updating stack '{}'".format(name))
    cf.update_stack(
        StackName=name,
        TemplateBody=json.dumps(template),
        Parameters=[{"ParameterKey":k,"ParameterValue":params[k]} for k in params],
        Capabilities=['CAPABILITY_IAM']
    )
    return wait_for_stack(cf, name)

def launch_cluster_stack(name, email):
    template = json.loadf("cluster.cf.json")
    params = {
        "ClusterName":name,
        "EmailAddress":email
    }
    transitional_template = copy.deepcopy(template)
    del transitional_template["Resources"]["DataBucket"]["Properties"]["NotificationConfiguration"]
    print("Creating initial stack.")
    outputs = create_stack(name, transitional_template, params)
    print("Uploading lambda function code to lambda bucket.")
    body = create_zipfile("lambda/")
    lambda_key = "lambda.{}.zip".format(hashlib.md5(body).hexdigest())
    boto3.client("s3").put_object(Bucket=outputs["LambdaBucket"], Key=lambda_key, Body=body)
    params["LambdaS3Key"] = lambda_key
    print("Updating stack to include Lambda function and permissions.")
    outputs = update_stack(name, transitional_template, params)
    print("Updating stack to include S3 data bucket notifications.")
    outputs = update_stack(name, template, params)
    print("Data bucket: {}".format(outputs["DataBucket"]))

def main():
    args = parse_args()
    boto3.setup_default_session(region_name=args.region, profile_name=args.profile)
    launch_cluster_stack(args.name, args.email)
    pass

if __name__ == "__main__":
    main()

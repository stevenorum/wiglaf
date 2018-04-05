import boto3
import copy
import hashlib
import logging
import time
import traceback

from calvin import json
from calvin.aws.lambda_deployment import create_zipfile
from .templates import WIGLAF_TEMPLATE

def wait_for_stack(cluster_name, target_states=["CREATE_COMPLETE","UPDATE_COMPLETE"], **kwargs):
    cf = boto3.client("cloudformation")
    stack = cf.describe_stacks(StackName=cluster_name)["Stacks"][0]
    status = stack["StackStatus"]
    while status.endswith("_IN_PROGRESS"):
        logging.debug("Stack '{}' in transitional state '{}'.  Sleeping for 5 seconds...".format(cluster_name, status))
        time.sleep(5)
        stack = cf.describe_stacks(StackName=cluster_name)["Stacks"][0]
        status = stack["StackStatus"]
    logging.debug("Stack '{}' in stable state '{}'.".format(cluster_name, status))
    if status in ["CREATE_COMPLETE","UPDATE_COMPLETE"]:
        return {op["OutputKey"]:op["OutputValue"] for op in stack["Outputs"]}
    else:
        raise RuntimeError("Stack {} is in unexpected state {}!".format(cluster_name, status))

def create_stack(cluster_name, template, params, **kwargs):
    cf = boto3.client("cloudformation")
    try:
        logging.debug("Creating stack '{}'".format(cluster_name))
        cf.create_stack(
            StackName=cluster_name,
            TemplateBody=json.dumps(template),
            Parameters=[{"ParameterKey":k,"ParameterValue":params[k]} for k in params if params[k]],
            Capabilities=['CAPABILITY_IAM'],
            OnFailure='ROLLBACK'
        )
    except Exception as e:
        logging.warn(traceback.format_exc())
        pass
    return wait_for_stack(cluster_name)

def update_stack(cluster_name, template, params, **kwargs):
    cf = boto3.client("cloudformation")
    try:
        logging.debug("Updating stack '{}'".format(cluster_name))
        cf.update_stack(
            StackName=cluster_name,
            TemplateBody=json.dumps(template),
            Parameters=[{"ParameterKey":k,"ParameterValue":params[k]} for k in params if params[k]],
            Capabilities=['CAPABILITY_IAM']
        )
    except Exception as e:
        logging.warn(traceback.format_exc())
        if 'No updates are to be performed' in str(e):
            pass
        else:
            raise e
    return wait_for_stack(cluster_name)

def launch_cluster_stack(cluster_name,
                         image_id=None,
                         instance_type=None,
                         instance_count=None,
                         email_address=None,
                         key_name=None,
                         skip_create=False,
                         **kwargs):
    template = copy.deepcopy(WIGLAF_TEMPLATE)
    params = {
        "ClusterName":cluster_name,
        "EmailAddress":email_address,
        "KeyName":key_name,
        "ImageId":image_id,
        "InstanceType":instance_type,
        "InstanceCount":instance_count,
    }
    transitional_template = copy.deepcopy(template)
    del transitional_template["Resources"]["DataBucket"]["Properties"]["NotificationConfiguration"]
    if skip_create:
        outputs = wait_for_stack(cluster_name)
    else:
        logging.info("Creating initial stack.")
        outputs = create_stack(cluster_name, transitional_template, params)
    logging.info("Uploading lambda function code to lambda bucket.")
    body = create_zipfile("lambda/")
    lambda_key = "lambda.{}.zip".format(hashlib.md5(body).hexdigest())
    boto3.client("s3").put_object(Bucket=outputs["LambdaBucket"], Key=lambda_key, Body=body)
    params["LambdaS3Key"] = lambda_key
    logging.info("Updating stack to include Lambda function and permissions.")
    outputs = update_stack(cluster_name, transitional_template, params)
    logging.info("Updating stack to include S3 data bucket notifications.")
    outputs = update_stack(cluster_name, template, params)
    logging.info("Data bucket: {}".format(outputs["DataBucket"]))

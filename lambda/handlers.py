#!/usr/bin/env python3

print("Function loading.")

import boto3
import json
import os
import traceback

def lambda_handler(event, context):
    print("Received event:")
    print(json.dumps(event))
    for record in event["Records"]:
        try:
            obj_key = record["s3"]["object"]["key"]
            bucket_name = record["s3"]["bucket"]["name"]
            print("Handling {}/{}".format(bucket_name, obj_key))
            if obj_key == "manifest.json":
                print("Processing manifest.json file")
                return process_manifest(bucket_name, obj_key)
            elif obj_key.startswith("results/") and not obj_key.endswith("/wiglaf_manifest.json"):
                job_name = obj_key.split("/")[1]
                print("Processing result for job {}".format(job_name))
                return process_result(bucket_name, job_name)
            elif obj_key.startswith("terminate/"):
                instance_id = obj_key.split("/")[-1]
                print("Terminating instance {}".format(instance_id))
                boto3.client('ec2').terminate_instances(InstanceIds=[instance_id])
                boto3.client('s3').delete_object(Bucket=bucket_name, Key=obj_key)
                pass
            else:
                print("We don't care about this event.")
                pass
        except Exception as e:
            traceback.print_exc()

def process_manifest(bucket_name, obj_key):
    s3 = boto3.client('s3')
    manifest_body = s3.get_object(Bucket=bucket_name, Key=obj_key)["Body"].read()
    manifest = json.loads(manifest_body.decode("utf-8"))
    script_lines = [
        "#!/bin/bash",
        "",
        "Bucket={}".format(bucket_name),
        "JobName={}".format(manifest["JobName"]),
        "InstanceId=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)",
        # "sudo apt-get install -y python3",
        # "wget -O /tmp/get-pip.py 'https://bootstrap.pypa.io/get-pip.py'",
        # "sudo python3 /tmp/get-pip.py",
        # "sudo pip install awscli --upgrade",
        "mkdir -p /tmp/wiglaf",
        "cd /tmp/wiglaf",
        ""]
    for filename in manifest["FilesToDownload"]:
        script_lines.append("aws s3 cp s3://$Bucket/resources/$JobName/{filename} /tmp/wiglaf/{filename}".format(filename=filename))
    if "InstallCommands" in manifest:
        script_lines.append("echo \"Starting installation of dependencies at $(date)\"")
        for command in manifest["InstallCommands"]:
            script_lines.append("echo Executing command \'{}\'".format(json.dumps(command)))
            script_lines.append(command)
        script_lines.append("echo \"Finished installation of dependencies at $(date)\"")
    for iteration in range(manifest.get("RunsPerNode",1)):
        script_lines.append("echo \"Starting execution #{} of commands at $(date)\"".format(iteration+1))
        for command in manifest["CommandsToRun"]:
            script_lines.append("echo Executing command \'{}\'".format(json.dumps(command)))
            script_lines.append(command)
        script_lines.append("echo \"Finishedexecution #{} of commands at $(date)\"".format(iteration+1))
        for filename in manifest["FilesToUpload"]:
            script_lines.append("aws s3 cp /tmp/wiglaf/{filename} s3://$Bucket/results/$JobName/{filename}.{iteration}.$InstanceId".format(filename=filename, iteration=iteration))
        for logfile in ["cloud-init.log","cloud-init-output.log","syslog"]:
            script_lines.append("aws s3 cp /var/log/{logfile} s3://$Bucket/logs/$JobName/$InstanceId/{logfile}".format(logfile=logfile))
    script_lines.append("AZ=$(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone)")
    script_lines.append("REGION=v${AZ::-1}")
    script_lines.append("aws --region $REGION ec2 terminate-instances --instance-ids $InstanceId")
    startup_script = "\n".join(script_lines)
    s3.put_object(Bucket=bucket_name, Key="do_stuff.sh", Body=startup_script.encode("utf-8"))
    s3.put_object(Bucket=bucket_name, Key="resources/{job_name}/wiglaf_manifest.json".format(job_name=manifest["JobName"]), Body=manifest_body)
    start_cluster()

def start_cluster(*args, **kwargs):
    asg_name = get_asg_name()
    asg = boto3.client('autoscaling')
    response = asg.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    max_size = response["AutoScalingGroups"][0]["MaxSize"]
    boto3.client('autoscaling').update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=max_size)

def stop_cluster(*args, **kwargs):
    asg_name = get_asg_name()
    asg = boto3.client('autoscaling')
    boto3.client('autoscaling').update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=0)

def get_asg_name():
    return boto3.client('autoscaling').describe_tags(Filters=[
        {
            "Name":"key",
            "Values":["aws:cloudformation:stack-name"]
        },
        {
            "Name":"value",
            "Values":[os.environ['STACK_NAME']]
        }
    ])["Tags"][0]['ResourceId']

def process_result(bucket_name, job_name, *args, **kwargs):
    asg_name = get_asg_name()
    batch_count = int(os.environ['BATCH_COUNT'])
    sns_topic = os.getenv('SNS_TOPIC',None)
    s3 = boto3.client('s3')

    manifest_key = "resources/{job_name}/wiglaf_manifest.json".format(job_name=job_name)
    manifest = json.loads(s3.get_object(Bucket=bucket_name, Key=manifest_key)["Body"].read().decode("utf-8"))

    batch_count = manifest["NumberOfBatches"]

    result_aggregate_key = 'results/{job_name}/'.format(job_name=job_name)
    try:
        obj = s3.get_object(
            Bucket=bucket_name,
            Key=result_aggregate_key
        )
        json.loads(obj['Body'].read().decode('utf-8'))
        # If we get to this point, it means that the results.json file already exists, which means we're done.
        return
    except:
        pass

    results_prefix = 'results/{job_name}/'.format(job_name=job_name)
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
    if len(objects) >= batch_count:
        stop_cluster()
        links = [s3.generate_presigned_url(ClientMethod='get_object',Params={'Bucket':bucket_name,'Key':obj['Key']},ExpiresIn=60*60*24) for obj in objects[:batch_count]]
        body = json.dumps(links, indent=2, sort_keys=True).encode('utf-8')
        s3.put_object(Body=body, Bucket=bucket_name, Key=result_aggregate_key)
        agg_link = s3.generate_presigned_url(ClientMethod='get_object',Params={'Bucket':bucket_name,'Key':result_aggregate_key},ExpiresIn=60*60*24)
        if sns_topic:
            message = "Job '{}' done.  Output file:\n\n{}".format(job_name, agg_link)
            subject = "Job '{}' done".format(job_name)
            sns.publish(TopicArn=sns_topic, Message=message, Subject=subject)
    pass

# def old_lambda_handler(event, context):
#     topic_arn = "arn:aws:sns:us-east-1:190692641744:text-me"
#     print(json.dumps(event, sort_keys=True))
#     obj_key = event["Records"][0]["s3"]["object"]["key"]
#     obj_bucket = event["Records"][0]["s3"]["bucket"]["name"]
#     subject = "S3 upload"
#     message = "File '{}' uploaded to bucket '{}'".format(obj_key, obj_bucket)
#     print("Publishing the following message to SNS:")
#     print(message)
#     sns = boto3.client("sns")
#     sns.publish(TopicArn=topic_arn, Message=message, Subject=subject)
#     return ''

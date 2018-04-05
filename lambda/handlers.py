#!/usr/bin/env python3

print("Function loading.")

import boto3
from datetime import datetime, timedelta, timezone
import json
import os
import traceback

def cluster_name():
    return os.getenv("CLUSTER_NAME", os.getenv("STACK_NAME"))

def lambda_handler(event, context):
    for record in event["Records"]:
        try:
            obj_key = record["s3"]["object"]["key"]
            bucket_name = record["s3"]["bucket"]["name"]
            print("Handling {}/{}".format(bucket_name, obj_key))
            if obj_key == "manifest.json":
                print("Processing manifest.json file")
                return process_manifest(bucket_name, obj_key)
            elif obj_key.startswith("jobs/"):
                if "/results/" in obj_key and not obj_key.endswith("/wiglaf_manifest.json"):
                    # Example: jobs/foo/results/file.Rdata
                    job_name = obj_key.split("/")[1] # foo
                    print("Processing result for job {}".format(job_name))
                    return process_result(bucket_name, job_name)
                elif "/terminate/" in obj_key:
                    # Example: jobs/foo/terminate/i-12345678
                    instance_id = obj_key.split("/")[-1]
                    print("Terminating instance {}".format(instance_id))
                    boto3.client('ec2').terminate_instances(InstanceIds=[instance_id])
                    boto3.client('s3').delete_object(Bucket=bucket_name, Key=obj_key)
                    pass
                else:
                    print("We don't care about this object.")
                    pass
            else:
                print("We don't care about this object.")
                pass
        except Exception as e:
            print("ERROR CAUGHT")
            traceback.print_exc()

def process_manifest(bucket_name, obj_key):
    s3 = boto3.client('s3')
    manifest_body = s3.get_object(Bucket=bucket_name, Key=obj_key)["Body"].read()
    manifest = json.loads(manifest_body.decode("utf-8"))
    job_name = manifest["JobName"]
    script_lines = [
        "#!/bin/bash",
        "",
        "InstanceId=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)",
        "mkdir -p /tmp/wiglaf",
        "cd /tmp/wiglaf",
        "echo \"$InstanceId\" > /tmp/$InstanceId",
        ""]
    checkpoints = {"__NEXT__":0}
    def checkpoint(name,step=""):
        if name not in checkpoints:
            checkpoints[name] = checkpoints["__NEXT__"]
            checkpoints["__NEXT__"] += 1
        index = checkpoints[name]
        key = "jobs/$JobName/checkpoints/$InstanceId.{index}-{name}-{step}".format(index=index, step=step, name=name)
        return "date > /tmp/checkpoint && aws s3 cp /tmp/checkpoint s3://$Bucket/{key}".format(key=key)
    script_lines.append(checkpoint("DownloadingFiles","begin"))
    for filename in manifest["FilesToDownload"]:
        script_lines.append("aws s3 cp s3://$Bucket/jobs/$JobName/resources/{filename} /tmp/wiglaf/{filename}".format(filename=filename))
    script_lines.append(checkpoint("DownloadingFiles","end"))
    if "InstallCommands" in manifest:
        script_lines.append("echo \"Starting installation of dependencies at $(date)\"")
        script_lines.append(checkpoint("InstallCommands","begin"))
        for command in manifest["InstallCommands"]:
            script_lines.append("echo Executing command \'{}\'".format(json.dumps(command)))
            script_lines.append(command)
        script_lines.append(checkpoint("InstallCommands","end"))
        script_lines.append("echo \"Finished installation of dependencies at $(date)\"")
    for iteration in range(manifest.get("RunsPerNode",1)):
        script_lines.append("echo \"Starting execution #{} of commands at $(date)\"".format(iteration+1))
        script_lines.append(checkpoint("CommandsToRun.{}".format(iteration),"begin"))
        for command in manifest["CommandsToRun"]:
            script_lines.append("echo Executing command \'{}\'".format(json.dumps(command)))
            script_lines.append(command)
        script_lines.append(checkpoint("CommandsToRun.{}".format(iteration),"end"))
        script_lines.append("echo \"Finished execution #{} of commands at $(date)\"".format(iteration+1))
        for filename in manifest["FilesToUpload"]:
            script_lines.append("aws s3 cp /tmp/wiglaf/{filename} s3://$Bucket/jobs/$JobName/results/{filename}.{iteration}.$InstanceId".format(filename=filename, iteration=iteration))
        for logfile in ["cloud-init.log","cloud-init-output.log","syslog"]:
            script_lines.append("aws s3 cp /var/log/{logfile} s3://$Bucket/jobs/$JobName/logs/$InstanceId.{logfile}".format(logfile=logfile))
    script_lines.append("AZ=$(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone)")
    script_lines.append("REGION=v${AZ::-1}")
    script_lines.append("aws s3 cp /tmp/$InstanceId s3://$Bucket/jobs/$JobName/terminate/$InstanceId")
    script_lines.append("aws --region $REGION ec2 terminate-instances --instance-ids $InstanceId")
    startup_script = "\n".join(script_lines)
    startup_script = startup_script.replace("$Bucket",bucket_name).replace("$JobName",job_name)
    s3.put_object(Bucket=bucket_name, Key="do_stuff.sh", Body=startup_script.encode("utf-8"))
    s3.put_object(Bucket=bucket_name, Key="jobs/{job_name}/resources/wiglaf_manifest.json".format(job_name=job_name), Body=manifest_body)
    stop_cluster()
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
    sns_topic = os.getenv('SNS_TOPIC',None)
    s3 = boto3.client('s3')

    manifest_key = "jobs/{job_name}/resources/wiglaf_manifest.json".format(job_name=job_name)
    manifest_object = s3.get_object(Bucket=bucket_name, Key=manifest_key)
    manifest = json.loads(manifest_object["Body"].read().decode("utf-8"))

    batch_count = manifest["NumberOfBatches"]

    result_aggregate_key = 'jobs/{job_name}/results/wiglaf_results.json'.format(job_name=job_name)
    try:
        obj = s3.get_object(
            Bucket=bucket_name,
            Key=result_aggregate_key
        )
        json.loads(obj['Body'].read().decode('utf-8'))
        # If we get to this point, it means that the results.json file already exists, which means we're done.
        stop_cluster()
        return
    except:
        pass

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
    print("{} batches retrieved towards a goal of {}".format(len(objects), batch_count))
    if len(objects) < batch_count:
        print("Not finished.  Keep cluster alive.")
        pass
    else:
        print("Goal met.  Tearing down cluster.")
        stop_cluster()
        links = [s3.generate_presigned_url(ClientMethod='get_object',Params={'Bucket':bucket_name,'Key':obj['Key']},ExpiresIn=60*60*24*7) for obj in objects]
        body = json.dumps(links, indent=2, sort_keys=True).encode('utf-8')
        s3.put_object(Body=body, Bucket=bucket_name, Key=result_aggregate_key)
        if sns_topic:
            start_dt = manifest_object["LastModified"]
            end_dt = datetime.now(timezone.utc)
            message_lines = [
                "Job '{}' finished".format(job_name),
                "",
                "Cluster: {}".format(cluster_name()),
                "",
                "Started: {}".format(start_dt.strftime("%a %d %H:%M:%S UTC")),
                "Ended: {}".format(end_dt.strftime("%a %d %H:%M:%S UTC")),
                "Duration: {}".format(pretty_delta(end_dt - start_dt)),
                "",
                "Links to output files:",
                ""
            ] + [l+"\n" for l in links] + ["The above links are valid for ~7 days after when the job finished."]
            message = '\n'.join(message_lines)
            subject = "Job '{}' done".format(job_name)
            boto3.client("sns").publish(TopicArn=sns_topic, Message=message, Subject=subject)
    pass

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
    return "{} ({})".format(pretty, str(td))

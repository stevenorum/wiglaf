from calvin import json

WIGLAF_TEMPLATE = {
    "Parameters":{
        "ClusterName":{
            "Type":"String",
            "Default":"Wiglaf"
        },
        "ImageId":{
            "Type":"String",
            "Default":"ami-aab1e9d0"
        },
        "InstanceType":{
            "Type":"String",
            "Default":"c5.xlarge"
        },
        "MaxInstanceCount":{
            "Type":"Number",
            "Default":"5"
        },
        "EmailAddress":{
            "Type":"String",
            "Default":""
        },
        "KeyName":{
            "Type":"String",
            "Default":""
        },
        "LambdaS3Key":{
            "Type":"String",
            "Default":""
        }
    },
    "Conditions":{
        "KeyNameProvided":{
            "Fn::Not":[{"Fn::Equals":["",{"Ref":"KeyName"}]}]
        },
        "EmailAddressProvided":{
            "Fn::Not":[{"Fn::Equals":["",{"Ref":"EmailAddress"}]}]
        },
        "LambdaS3KeyProvided":{
            "Fn::Not":[{"Fn::Equals":["",{"Ref":"LambdaS3Key"}]}]
        }
    },
    "Resources":{
        "LambdaBucket":{
            "Type":"AWS::S3::Bucket",
            "Properties":{
                "AccessControl": "BucketOwnerFullControl"
            }
        },
        "DataBucket":{
            "Type":"AWS::S3::Bucket",
            "Properties":{
                "AccessControl": "BucketOwnerFullControl",
                "NotificationConfiguration": {
                    "Fn::If":["LambdaS3KeyProvided",{
                        "LambdaConfigurations":[
                            {
                                "Event":"s3:ObjectCreated:*",
                                "Function":{ "Fn::GetAtt": ["Function", "Arn"] }
                            }
                        ]
                    },{"Ref":"AWS::NoValue"}]}
            }
        },
        "VPC":{
            "Type":"AWS::EC2::VPC",
            "Properties":{
                "CidrBlock":"10.0.0.0/16"
            }
        },
        "Subnet":{
            "Type":"AWS::EC2::Subnet",
            "Properties":{
                "VpcId":{"Ref":"VPC"},
                "MapPublicIpOnLaunch":"true",
                "CidrBlock":"10.0.0.0/24"
            }
        },
        "InternetGateway":{
            "Type":"AWS::EC2::InternetGateway"
        },
        "VPCGatewayAttachment":{
            "Type":"AWS::EC2::VPCGatewayAttachment",
            "Properties":{
                "VpcId":{"Ref":"VPC"},
                "InternetGatewayId":{"Ref":"InternetGateway"}
            }
        },
        "RouteTable":{
            "Type":"AWS::EC2::RouteTable",
            "Properties":{
                "VpcId":{"Ref":"VPC"}
            }
        },
        "Route":{
            "Type":"AWS::EC2::Route",
            "DependsOn":"VPCGatewayAttachment",
            "Properties":{
                "GatewayId":{"Ref":"InternetGateway"},
                "DestinationCidrBlock":"0.0.0.0/0",
                "RouteTableId":{"Ref":"RouteTable"}
            }
        },
        "SubnetRouteTableAssociation":{
            "Type":"AWS::EC2::SubnetRouteTableAssociation",
            "Properties":{
                "SubnetId":{"Ref":"Subnet"},
                "RouteTableId":{"Ref":"RouteTable"}
            }
        },
        "EC2Role":{
            "Type":"AWS::IAM::Role",
            "Properties":{
                "AssumeRolePolicyDocument":{
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": [
                                    "ec2.amazonaws.com"
                                ]
                            },
                            "Action": [
                                "sts:AssumeRole"
                            ]
                        }
                    ]
                },
                "Path":"/",
                "ManagedPolicyArns":[{"Fn::Sub":"arn:${AWS::Partition}:iam::aws:policy/AdministratorAccess"}]
            }
        },
        "InstanceProfile":{
            "Type":"AWS::IAM::InstanceProfile",
            "Properties":{
                "Path":"/",
                "Roles":[
                    {"Ref":"EC2Role"}
                ]
            }
        },
        "LambdaRole":{
            "Type":"AWS::IAM::Role",
            "Properties":{
                "AssumeRolePolicyDocument":{
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": [
                                    "lambda.amazonaws.com"
                                ]
                            },
                            "Action": [
                                "sts:AssumeRole"
                            ]
                        }
                    ]
                },
                "Path":"/",
                "ManagedPolicyArns":[{"Fn::Sub":"arn:${AWS::Partition}:iam::aws:policy/AdministratorAccess"}]
            }
        },
        "LaunchConfiguration":{
            "Type":"AWS::AutoScaling::LaunchConfiguration",
            "DependsOn":"Route",
            "Properties":{
                "IamInstanceProfile":{"Ref":"InstanceProfile"},
                "ImageId":{"Ref":"ImageId"},
                "InstanceType":{"Ref":"InstanceType"},
                "KeyName":{"Fn::If":["KeyNameProvided",{"Ref":"KeyName"},{"Ref":"AWS::NoValue"}]},
                "UserData":{
                    "Fn::Base64": {
                        "Fn::Join":[
                            "\n",[
                                "#!/bin/bash",
                                "sudo apt-get install -y python3",
                                "wget -O /tmp/get-pip.py 'https://bootstrap.pypa.io/get-pip.py'",
                                "sudo python3 /tmp/get-pip.py",
                                "sudo pip install awscli --upgrade",
                                {"Fn::Sub":"aws s3 cp s3://${DataBucket}/do_stuff.sh /tmp/do_stuff.sh"},
                                "chmod +x /tmp/do_stuff.sh",
                                "/tmp/do_stuff.sh",
                                ""
                            ]
                        ]
                    }
                }
            }
        },
        "AutoScalingGroup":{
            "Type":"AWS::AutoScaling::AutoScalingGroup",
            "Properties":{
                "LaunchConfigurationName":{"Ref":"LaunchConfiguration"},
                "MinSize":"0",
                "MaxSize":{"Ref":"MaxInstanceCount"},
                "DesiredCapacity":"0",
                "VPCZoneIdentifier":[{"Ref":"Subnet"}]
            }
        },
        "Topic":{
            "Type":"AWS::SNS::Topic",
            "Condition":"EmailAddressProvided",
            "Properties":{
                "DisplayName":{"Ref":"ClusterName"},
                "Subscription":[
                    {
                        "Endpoint":{"Ref":"EmailAddress"},
                        "Protocol":"email"
                    }
                ]
            }
        },
        "Function":{
            "Type":"AWS::Lambda::Function",
            "Condition":"LambdaS3KeyProvided",
            "Properties":{
                "Code":{
                    "S3Bucket":{"Ref":"LambdaBucket"},
                    "S3Key":{"Ref":"LambdaS3Key"}
                },
                "Environment":{
                    "Variables":{
                        "SNS_TOPIC":{"Fn::If":["EmailAddressProvided",{"Ref":"Topic"},{"Ref":"AWS::NoValue"}]},
                        "STACK_NAME":{"Ref":"AWS::StackName"},
                        "CLUSTER_NAME":{"Ref":"ClusterName"}
                    }
                },
                "Handler":"handlers.lambda_handler",
                "MemorySize":"128",
                "Role":{"Fn::GetAtt":["LambdaRole","Arn"]},
                "Runtime":"python3.6",
                "Timeout":"10"
            }
        },
        "Permission":{
            "Type":"AWS::Lambda::Permission",
            "Condition":"LambdaS3KeyProvided",
            "Properties":{
                "FunctionName": { "Ref": "Function" },
                "Action": "lambda:InvokeFunction",
                "Principal": "s3.amazonaws.com",
                "SourceArn": {"Fn::Sub":"arn:${AWS::Partition}:s3:::*"}
            }
        }
    },
    "Outputs":{
        "LambdaBucket":{
            "Value":{"Ref":"LambdaBucket"}
        },
        "DataBucket":{
            "Value":{"Ref":"DataBucket"}
        }
    }
}

WIGLAF_TEMPLATE_BODY = json.dumps(WIGLAF_TEMPLATE, separators=(',',':'))


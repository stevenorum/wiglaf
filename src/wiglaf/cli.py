#!/usr/bin/env python3

from calvin.cli import Argument, CLIDispatcher
from wiglaf.dispatch import Wiglaf

class WiglafDispatcher(CLIDispatcher):
    description = 'Interact with Wiglaf from the command line.'

    shared_args = [
        Argument('--config', default=None, help='The path to the Wiglaf cluster config file.'),
        Argument('--cluster-name', default=None, help='The name of the cluster.  May be specified in the config file instead.'),
        Argument('--image-id', default=None, help='The base EC2 AMI to use for the nodes.  May be specified in the config file instead.'),
        Argument('--instance-type', default=None, help='The EC2 instance type for each node.  May be specified in the config file instead.'),
        Argument('--max-size', default=None, help='The maximum number of nodes at one time.  May be specified in the config file instead.'),
        Argument('--email-address', default=None, help='An email address to notify when jobs finish.  May be specified in the config file instead.'),
        Argument('--key-name', default=None, help='The name of an EC2 SSH keypair.  May be specified in the config file instead.'),
        Argument('--profile', default=None, help='The AWS credential profile to use.  May be specified in the config file instead.'),
        Argument('--region', default=None, help='The AWS region.  May be specified in the config file instead.'),
        Argument("-v", "--verbosity", dest="verbosity", action="count", default=0, help='How verbose this CLI should be.  More -v\'s, more verbose.')
    ]

    operation_info={
        'create':{
            'help':'Create a new Wiglaf cluster.'
        },
        'update':{
            'help':'Update an existing Wiglaf cluster.'
        },
        'describe':{
            'help':'Print information about an existing Wiglaf cluster.'
        },
        'generate-manifest':{
            'help':'Generate a basic manifest.json file to help get you started.'
        },
        'erase-job':{
            'help':'Erase all files for a job from S3.'
        },
        'generate-report':{
            'help':'Generate a report with general information about a job.'
        },
        'clear-results':{
            'help':'Erase the results of a job from S3.'
        },
        'list-results':{
            'help':'List the results from a job.'
        },
        'download-results':{
            'help':'Download the results from a job.'
        },
        'start-job':{
            'help':'Start a new job.'
        },
        'abort-job':{
            'help':'End an in-progress job and erase the contents from S3.'
        },
    }

    @classmethod
    def execute(cls, action, **kwargs):
        wig = Wiglaf(**kwargs)
        method = getattr(wig, action)
        response = method(**kwargs)
        if response:
            print(response)

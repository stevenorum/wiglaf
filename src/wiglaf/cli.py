#!/usr/bin/env python3

from calvin.cli import Argument, CLIDispatcher
from wiglaf.dispatch import Wiglaf

class WiglafDispatcher(CLIDispatcher):
    description = 'Interact with Wiglaf from the command line.'

    shared_args = [
        Argument('--config', default=None, help='The path to the Wiglaf cluster config file.'),
        Argument('--manifest', default=None, help='The path to the Wiglaf job manifest file.'),
        Argument('--cluster-name', default=None, help='The name of the cluster.  May be specified in the config file instead.'),
        Argument('--image-id', default=None, help='The base EC2 AMI to use for the nodes.  May be specified in the config file instead.'),
        Argument('--instance-type', default=None, help='The EC2 instance type for each node.  May be specified in the config file instead.'),
        Argument('--max-size', default=None, help='The maximum number of nodes at one time.  May be specified in the config file instead.'),
        Argument('--email-address', default=None, help='An email address to notify when jobs finish.  May be specified in the config file instead.'),
        Argument('--key-name', default=None, help='The name of an EC2 SSH keypair.  May be specified in the config file instead.'),
        Argument('--profile', default=None, help='The AWS credential profile to use.  May be specified in the config file instead.'),
        Argument('--region', default=None, help='The AWS region.  May be specified in the config file instead.'),
        Argument('--results-directory', default=None, help='Directory to which to download the results for the job.'),
        Argument('--job-name', default=None, help='Override the job name given in the manifest.'),
        Argument("-v", "--verbosity", dest="verbosity", action="count", default=0, help='How verbose this CLI should be.  More -v\'s, more verbose.')
    ]

    operation_info={
        'create-cluster':{
            'help':'Create a new Wiglaf cluster.'
        },
        'update-cluster':{
            'help':'Update an existing Wiglaf cluster.'
        },
        'delete-cluster':{
            'help':'Fully delete a Wiglaf cluster, including all data and results currently stored in the cloud.'
        },
        'describe-cluster':{
            'help':'Print information about an existing Wiglaf cluster.'
        },
        'stop-cluster':{
            'help':'Stop all nodes.  Any work that is being performed but has not yet been uploaded to the cloud will be lost.'
        },
        'start-cluster':{
            'help':'Start all nodes.  The work from the most recent job will be restarted.'
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
        'erase-data':{
            'help':'Erase all data currently stored in the cloud.  This must be done before deleting a cluster.'
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
        'describe-job':{
            'help':'Print information about a run, such as current status.'
        },
        'abort-job':{
            'help':'End an in-progress job and erase the contents from S3.'
        },
        'bake-image':{
            'help':'Create a new base node image from a job manifest.  This will download all of the data and will then run the install steps, but not any of the compute or upload steps.'
        },
    }

    @classmethod
    def execute(cls, action, **kwargs):
        wig = Wiglaf(**kwargs)
        method = getattr(wig, action)
        response = method(**kwargs)
        if response:
            print(response)

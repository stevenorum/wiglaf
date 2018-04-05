#!/usr/bin/env python3

import argparse
import os

from calvin import json

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job",
                        help='Name of the job.  Must be unique for the cluster; an easy way to accomplish this is to append a timestamp.',
                        required=True)
    parser.add_argument("--directory",
                        help='Directory containing the files to be uploaded for download onto the cluster nodes.',
                        required=True)
    parser.add_argument("--command",
                        help="Command to run on the cluster.  Should be a script that's among the files uploaded.  If you need to run multiple commands, it's easy to add those to the manifest file before uploading.",
                        required=True)
    parser.add_argument("--result",
                        help="Name of the file containing the results of the operation.",
                        required=True)
    parser.add_argument("--number",
                        help="How many copies need to be performed.",
                        type=int,
                        default=10,
                        required=True)
    return parser.parse_args()

def main():
    args = parse_args()
    manifest = {
        "JobName":args.job,
        "LocalDirectory":args.directory,
        "FilesToDownload":os.listdir(args.directory),
        "CommandsToRun":[args.command],
        "FilesToUpload":[args.result],
        "NumberOfBatches":args.number,
        "RunsPerNode":args.number,
        "InstallCommands":["echo 'Commands that only need run once per node go here (e.g., installing libraries).'"]
    }
    fname = "manifest.{}.json".format(args.job)
    json.dumpf(manifest, fname, indent=4, sort_keys=True)
    print("manifest.json shell written to {}".format(fname))
    pass

if __name__ == "__main__":
    main()

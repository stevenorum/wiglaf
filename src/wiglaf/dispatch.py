#!/usr/bin/env python3

import boto3
import copy
import logging
import sys
import wiglaf.cloudformation

from calvin import json

defaults = {
    "region":"us-east-1"
}

required = [
    "region","profile","cluster_name"
]

def load_config(**kwargs):
    print(kwargs)
    conf = copy.deepcopy(defaults)
    if kwargs.get("config"):
        conf.update(json.loadf(kwargs["config"]))
    for k in kwargs:
        if kwargs[k]:
            conf[k] = kwargs[k]
    missing = [r for r in required if not conf.get(r)]
    if missing:
        raise RuntimeError("Required argument(s) missing: {}".format(", ".join(missing)))
    return conf

class Wiglaf(object):

    def __init__(self, **kwargs):
        self.config = load_config(**kwargs)
        root_logger = logging.getLogger()
        if self.config.get("verbosity"):
            root_logger.setLevel(logging.DEBUG)
        else:
            root_logger.setLevel(logging.INFO)
        boto3.setup_default_session(region_name=self.config["region"], profile_name=self.config["profile"])
        for noisy in ('botocore', 'boto3', 'requests'):
            logging.getLogger(noisy).level = logging.WARN
            pass

    def create(self, *args, **kwargs):
        print("create called")
        return wiglaf.cloudformation.launch_cluster_stack(skip_create=False, **self.config)

    def update(self, *args, **kwargs):
        print("update called")
        return wiglaf.cloudformation.launch_cluster_stack(skip_create=True, **self.config)

    def describe(self, *args, **kwargs):
        print("describe called; currently a no-op")
        pass

    def generate_manifest(self, *args, **kwargs):
        print("generate_manifest called")
        pass

    def erase_job(self, *args, **kwargs):
        print("erase_job called")
        pass

    def generate_report(self, *args, **kwargs):
        print("generate_report called")
        pass

    def clear_results(self, *args, **kwargs):
        print("clear_results called")
        pass

    def list_results(self, *args, **kwargs):
        print("list_results called")
        pass

    def download_results(self, *args, **kwargs):
        print("download_results called")
        pass

    def start_job(self, *args, **kwargs):
        print("start_job called")
        pass

    def abort_job(self, *args, **kwargs):
        print("abort_job called")
        pass

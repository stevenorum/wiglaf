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

    @property
    def _data_bucket(self):
        return wiglaf.cloudformation.describe_stack(**self.config)["Outputs"]["DataBucket"]

    @property
    def _cluster_name(self):
        return self.config.get("cluster_name")

    @property
    def _job_name(self):
        return self.config.get("job_name")

    def create_cluster(self, *args, **kwargs):
        return wiglaf.cloudformation.launch_cluster_stack(skip_create=False, **self.config)

    def update_cluster(self, *args, **kwargs):
        return wiglaf.cloudformation.launch_cluster_stack(skip_create=True, **self.config)

    def describe_cluster(self, *args, **kwargs):
        print(json.dumps(wiglaf.cloudformation.get_stack_info(**self.config), indent=4, sort_keys=True))

    def delete_cluster(self, *args, **kwargs):
        print("delete_cluster not yet implemented")
        pass

    def start_cluster(self, *args, **kwargs):
        print("start_cluster not yet implemented")
        pass

    def stop_cluster(self, *args, **kwargs):
        print("stop_cluster not yet implemented")
        pass

    def generate_manifest(self, *args, **kwargs):
        print("generate_manifest not yet implemented")
        pass

    def erase_job(self, *args, **kwargs):
        print("erase_job not yet implemented")
        pass

    def erase_data(self, *args, **kwargs):
        print("erase_data not yet implemented")
        pass

    def generate_report(self, *args, **kwargs):
        print("generate_report not yet implemented")
        pass

    def clear_results(self, *args, **kwargs):
        print("clear_results not yet implemented")
        pass

    def list_results(self, *args, **kwargs):
        print("list_results not yet implemented")
        pass

    def download_results(self, *args, **kwargs):
        print("download_results not yet implemented")
        prefix = "/".join(["jobs", self._job_name, "results"])
        return wiglaf.s3.download_files(bucket=self._data_bucket, prefix=prefix, directory=self.config.get("results_directory"))

    def start_job(self, *args, **kwargs):
        print("start_job not yet implemented")
        pass

    def abort_job(self, *args, **kwargs):
        print("abort_job not yet implemented")
        pass

    def describe_job(self, *args, **kwargs):
        print("describe_job not yet implemented")
        pass

    def bake_image(self, *args, **kwargs):
        print("bake_image not yet implemented")
        pass

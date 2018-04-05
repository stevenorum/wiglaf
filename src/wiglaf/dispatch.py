#!/usr/bin/env python3

import boto3
from calvin import json

def load_config(**kwargs):
    conf = {}
    if kwargs.get("config"):
        conf = json.loadf(kwargs["config"])
    conf.update(kwargs)
    return conf

class Wiglaf(object):

    def __init__(self, **kwargs):
        self.config = load_config(**kwargs)

    def create(self, *args, **kwargs):
        print("create called")
        pass

    def update(self, *args, **kwargs):
        print("update called")
        pass

    def describe(self, *args, **kwargs):
        print("describe called")
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

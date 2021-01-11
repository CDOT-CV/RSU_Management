#!/usr/bin/python3
# -*- coding: utf-8 -*-
#pylint:disable=E1101

"""
This script handles the handling of data between the raw ingest, the data lake, and the
data warehouse. The raw ingest and data lake at present are located in their respective
buckets in the GCP. The data warehouse is currently a Pub/Sub topic to which cleaned 
data will be published in byte string form.
"""

import json
import ndjson
import requests
import os
import pytest
from collections import defaultdict
from google.cloud import bigquery   # google cloud - bigquery / dataset-table access
from google.cloud.storage import Client    # google cloud - storage / bucket access
from google.cloud.pubsub_v1 import PublisherClient  # google cloud - pub/sub topic access
# below: for data cleaning
import pandas as pd
import numpy as np
# below: convert date time to 10 digit
import time
import datetime
import pprint
# below: to facilitate comms between Cloud Function, Pub/Sub, and Scheduler
import logging
from string import Template
import config

def is_json_clean(rsu_data):
    """
    -----------------------------------------------------------------------
    Returns TRUE if json_file is clean. FALSE otherwise.

    Param: rsu_data --> RSU data as a list of dicts from raw ingest to be checked
    -----------------------------------------------------------------------
    """
    # checking duplicate values in the RSU list
    print(type(rsu_data))
    print(type(rsu_data[0]))
    print(rsu_data[14:16])
    
    """
    seen = set()
    new_rsu_data = []
    for d in rsu_data:
        t = tuple(d.items())
        if t not in seen:
            seen.add(t)
            new_rsu_data.append(d)
    """
    return True

def rsu_raw_bucket(client, filename, filepath, bucket_name):
    """
    -----------------------------------------------------------------------
    Executes the transfer of raw data from the roadside unit to the 
    rsu_raw-ingest bucket in the GCP which stores raw, unclean data.

    Param: client --> object referencing GCP Storage/Bucket Client
    -----------------------------------------------------------------------
    """
    print("Beginning of bucket #1.")
    raw_bucket = client.get_bucket(bucket_name)
    raw_blob = raw_bucket.blob(filename)
    raw_blob.upload_from_filename(filename=filepath)
    
    # logging raw ingest upload message
    current_time = datetime.datetime.utcnow()                   
    log_message = Template('Raw ingest updated with new file at $time')
    logging.info(log_message.safe_substitute(time=current_time))
    
    print("End of bucket #1.")

def rsu_data_lake_bucket(client, r_bucket, l_bucket):
    """
    -----------------------------------------------------------------------
    Executes the transfer of raw data from the rsu_raw-ingest bucket
    to the data lake bucket in the GCP to be cleaned/filtered/aggregated/etc.

    Param: client --> object referencing GCP Storage/Bucket Client
    -----------------------------------------------------------------------
    """
    print("Beginning of bucket #2.")
    raw_bucket = client.get_bucket(r_bucket)            # source bucket
    lake_bucket = client.get_bucket(l_bucket)           # destination bucket
    
    raw_blobs = client.list_blobs(raw_bucket)
    for blob in raw_blobs:                                      # copying each RSU raw data file to the data lake
        data_string = blob.download_as_string()                 # data pulled as a BYTE string
        # check if JSON string is valid
        try:                                                    
            json_data = ndjson.loads(data_string)
            # IF DATA IS CLEAN: copy the blob to the data lake
            if is_json_clean(json_data) is True:  
                raw_bucket.copy_blob(blob, lake_bucket)
        except Exception as error:  # find more specific exceptions
            log_message = Template('Invalid JSON string from raw ingest: $message.')
            logging.error(log_message.safe_substitute(message=error))       

    print("End of bucket #2.") 

def help_warehouse(list_blobs, client, topic):
    """
    -----------------------------------------------------------------------
    Helper function for the rsu_data_warehouse_bucket() function which
    publishes data as a byte string to the Pub/Sub topic.

    Param: client --> object referencing GCP Pub/Sub Client
    -----------------------------------------------------------------------
    """
    for blob in list_blobs:
        data_string = blob.download_as_string()                 # data MUST be a byte string
        future = client.publish(topic,data_string)       # when message is published, the client returns a "future"
        print(future.result())       

def rsu_data_warehouse_bucket(pub_client, storage_client, topic, bucket):
    """
    -----------------------------------------------------------------------
    Will push data from the data lake to the designated Pub/Sub topic which
    will serve as a form of short-term data warehouse.

    Param: b_client --> object referencing GCP Storage/Bucket Client
    Param: p_client --> object referencing GCP Publisher Client
    -----------------------------------------------------------------------
    """
    print("Beginning of bucket #3.")
    lake_bucket = storage_client.get_bucket(bucket)
    lake_blobs = storage_client.list_blobs(lake_bucket)          # retrieving data lake bucket
    help_warehouse(lake_blobs, pub_client, topic)

    # logging publication message
    current_time = datetime.datetime.utcnow()    
    log_message = Template('Published message to the warehouse pub/sub topic at $time')
    logging.info(log_message.safe_substitute(time=current_time))

    print("End of bucket #3.")

def main(data, context):
    """
    -----------------------------------------------------------------------
    Main function.
    Triggered from a message on a Cloud Pub/Sub topic.

    Param - data: (dict) Event payload.
    Param - context: (google.cloud.functions.Context) Event metadata.
    -----------------------------------------------------------------------
    """

    # setting the Google Application Credentials to JSON with service account key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\divav\Desktop\CDOT\gcp_test\CDOT CV ODE Dev-4d9416c81201.json"
    print("\nLoaded Google App credentials.")

    json_file = 'RSU-ND.json'

    topic_path = PublisherClient().topic_path('cdot-cv-ode-dev','transfer_pubsub')
    raw_ingest_bucket = 'rsu_raw-ingest'
    data_lake_bucket = 'rsu_data-lake'
    
    try:
        current_time = datetime.datetime.utcnow()
        log_message = Template('Cloud Function was triggered on $time')
        logging.info(log_message.safe_substitute(time=current_time))

        try:
            print("Begin filling buckets . . .")
            rsu_raw_bucket(Client(), "json_obj1", json_file, 'rsu_raw-ingest')
            rsu_data_lake_bucket(Client(), raw_ingest_bucket, data_lake_bucket)
            #rsu_data_warehouse_bucket(PublisherClient(), Client(), topic_path, data_lake_bucket)
        
        except Exception as error:
            log_message = Template('Data transfer failed due to $message.')
            logging.error(log_message.safe_substitute(message=error))
        
    except Exception as error:
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message)
    
if __name__ == '__main__':
    main('data', 'context')
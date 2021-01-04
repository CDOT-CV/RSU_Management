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
from google.cloud import bigquery   # google cloud - bigquery / dataset-table access
from google.cloud import storage    # google cloud - storage / bucket access
from google.cloud import pubsub_v1  # google cloud - pub/sub topic access
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

    Parameter: rsu_data --> RSU data as a list from raw ingest to be checked
    -----------------------------------------------------------------------
    """
    # checking duplicate values in the RSU list
    if len(rsu_data) == len(set(rsu_data)):
        return True

def rsu_raw_bucket(client):
    """
    -----------------------------------------------------------------------
    Executes the transfer of raw data from the roadside unit to the 
    rsu_raw-ingest bucket in the GCP which stores raw, unclean data.

    Parameter: client --> object referencing GCP Storage/Bucket Client
    -----------------------------------------------------------------------
    """
    
    raw_bucket = client.get_bucket('rsu_raw-ingest')
    json_file = r'gcp_test\RSU-ND.json'
    raw_blob = raw_bucket.blob(json_file)
    raw_blob.upload_from_filename(filename=json_file)
    
    # logging raw ingest upload message
    current_time = datetime.datetime.utcnow()                   
    log_message = Template('Raw ingest updated with new file at $time')
    logging.info(log_message.safe_substitute(time=current_time))       

def rsu_data_lake_bucket(client):
    """
    -----------------------------------------------------------------------
    Executes the transfer of raw data from the rsu_raw-ingest bucket
    to the data lake bucket in the GCP to be cleaned/filtered/aggregated/etc.

    Parameter: client --> object referencing GCP Storage/Bucket Client
    -----------------------------------------------------------------------
    """
    
    raw_bucket = client.get_bucket('rsu_raw-ingest')            # source bucket
    data_lake_bucket = client.get_bucket('rsu_data-lake')       # destination bucket
    
    raw_blobs = client.list_blobs(raw_bucket)
    for blob in raw_blobs:                                      # copying each RSU raw data file to the data lake
        data_string = blob.download_as_string()                 # data pulled as a BYTE string
        # check if JSON string is valid
        try:                                                    
            json_data = ndjson.loads(data_string)
            # IF DATA IS CLEAN: copy the blob to the data lake
            if is_json_clean(json_data) is True:  
                raw_bucket.copy_blob(blob, data_lake_bucket)
        except Exception as error:
            log_message = Template('Invalid JSON string from raw ingest: $message.')
            logging.error(log_message.safe_substitute(message=error))        

def rsu_data_warehouse_bucket(b_client, p_client):
    """
    -----------------------------------------------------------------------
    Will push data from the data lake to the designated Pub/Sub topic which
    will serve as a form of short-term data warehouse.

    Parameter: b_client --> object referencing GCP Storage/Bucket Client
    Parameter: p_client --> object referencing GCP Publisher Client
    -----------------------------------------------------------------------
    """
    topic_path = p_client.topic_path('cdot-cv-ode-dev','rsu_data_warehouse')
    data_lake_bucket = b_client.get_bucket('rsu_data-lake')      
    lake_blobs = b_client.list_blobs(data_lake_bucket)          # retrieving data lake bucket
    
    for blob in lake_blobs:
        data_string = blob.download_as_string()                 # data MUST be a byte string
        future = p_client.publish(topic_path,data_string)       # when message is published, the client returns a "future"
        print(future.result())       

    # logging publication message
    current_time = datetime.datetime.utcnow()    
    log_message = Template('Published message to the warehouse pub/sub topic at $time')
    logging.info(log_message.safe_substitute(time=current_time))

def send_to_table(client):
    """
    -----------------------------------------------------------------------
    Executes the transfer of data from the data source file to the 
    specified GCP table.

    Parameter: client --> object referencing a BigQuery Client
    -----------------------------------------------------------------------
    """
    
    job_config = bigquery.LoadJobConfig(source_format='NEWLINE_DELIMITED_JSON')

    # transfer to GCP table via LOCAL JSON FILE
    print("Sending to GCP.")
    with open('gcp_test-ND.json', 'rb') as data_source:
        job = client.load_table_from_file(data_source, 'cdot-cv-ode-dev.rsu_sample.gcp_test_table', job_config=job_config)

    job.result() # waits for the data transfer to complete
    print("Sent to GCP table!")
    current_time = datetime.datetime.utcnow()
    log_message = Template('RSU file #(insert no.) sent to the GCP table at $time.')
    logging.info(log_message.safe_substitute(time=current_time))

def main(data, context):
    """
    -----------------------------------------------------------------------
    Main function.
    Triggered from a message on a Cloud Pub/Sub topic.

    Parameter - data: (dict) Event payload.
    Parameter - context: (google.cloud.functions.Context) Event metadata.
    -----------------------------------------------------------------------
    """

    print("1. Loading Google App credentials.")
    # setting the Google Application Credentials to JSON with service account key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\divav\Desktop\CDOT\gcp_test\CDOT CV ODE Dev-4d9416c81201.json"
    
    #bq_client = bigquery.Client()                  # GCP BigQuery client object for dataset/table use!
    bucket_client = storage.Client()                # GCP storage client object for bucket use!
    pubsub_client = pubsub_v1.PublisherClient()     # GCP publisher client for pub/sub topic
    print("2. Established bucket and pubsub client!")
    
    try:
        current_time = datetime.datetime.utcnow()
        log_message = Template('Cloud Function was triggered on $time')
        logging.info(log_message.safe_substitute(time=current_time))

        try:
            #send_to_table(bq_client)
            print("3. Begin filling buckets . . .")
            rsu_raw_bucket(bucket_client)
            print("4. Filled bucket #1: RAW INGEST.")
            rsu_data_lake_bucket(bucket_client)
            print("5. Filled bucket #2: DATA LAKE.")
            rsu_data_warehouse_bucket(bucket_client, pubsub_client)
            print("6. Filled bucket #3: DATA WAREHOUSE.")
        
        except Exception as error:
            log_message = Template('Data transfer failed due to $message.')
            logging.error(log_message.safe_substitute(message=error))
        
    except Exception as error:
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message)
    

if __name__ == '__main__':
    main('data', 'context')

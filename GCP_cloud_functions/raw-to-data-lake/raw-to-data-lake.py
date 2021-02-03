from google.cloud import storage
import ndjson
import datetime
import logging
from string import Template
import config

def is_json_clean(rsu_data):
    """
    Returns TRUE if json_file is clean. FALSE otherwise.
    Args: 
        rsu_data --> RSU data as a list of dicts from raw ingest to be checked
    """
    check = True
    
    #check 1: duplicate records
    check1 = True
    unique = []
    for d in rsu_data:
        if d not in unique:
            unique.append(d)
    if len(unique) != len(rsu_data):
        check1 = False

    #check 2: empty records based on timeReceived key
    check2 = True
    for d in rsu_data:
        if len(d["timeReceived"]) == 0:
            check2 = False
            break

    # have any checks failed?
    if (check1 == False) or (check2 == False):
        check = False

    return check

def raw_to_data_lake(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    # logging cloud function trigger
    current_time = datetime.datetime.now()
    log_message = Template('Cloud Function "raw-to-data-lake" was triggered at $time')
    logging.info(log_message.safe_substitute(time=current_time))
    
    client = storage.Client()
    raw_bucket = client.get_bucket(config.config_vars['raw_ingest_id'])           
    lake_bucket = client.get_bucket(config.config_vars['data_lake_id'])

    # retrieving the latest blob upload to the bucket
    blob = client.raw_bucket.get_blob(event['name'])
    data_string = blob.download_as_bytes()
    json_data = ndjson.loads(data_string)
    if is_json_clean(json_data) is True:
        raw_bucket.copy_blob(blob, lake_bucket)
        current_time = datetime.datetime.now()
        log_message = Template('Data lake updated at $time')
        logging.info(log_message.safe_substitute(time=current_time))

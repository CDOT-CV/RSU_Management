from google.cloud import storage
import ndjson
import datetime
import logging
from string import Template
from GCP_cloud_functions import config

def is_json_clean(rsu_data):
    """
    Returns TRUE if json_file is clean. FALSE otherwise.
    Args: 
        rsu_data (list): RSU data as a list of dicts from raw ingest to be checked
    """
    isJSONclean = True
    
    #check 1: duplicate records
    isDuplicateFree = True
    unique = []
    for data in rsu_data:
        if data not in unique:
            unique.append(data)
    if len(unique) != len(rsu_data):
        isDuplicateFree = False
        return isDuplicateFree

    #check 2: empty records based on timeReceived key
    isEmpty = True
    for data in rsu_data:
        if len(data["timeReceived"]) == 0:
            isEmpty = False
            break

    # have any checks failed?
    if (isDuplicateFree == False) or (isEmpty == False):
        isJSONclean = False

    return isJSONclean

def raw_to_data_lake(event, context):
    """Triggered by new upload to the raw ingest storage bucket.
    Function retrieves this new upload and checks for cleanliness
    before sending clean data to the data lake bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    client = storage.Client()
    
    # logging cloud function trigger
    current_time = datetime.datetime.now()
    log_message = Template('Cloud Function "raw-to-data-lake" was triggered at $time')
    logging.info(log_message.safe_substitute(time=current_time))
        
    try:
        raw_bucket = client.get_bucket(config.config_vars['raw_ingest_id'])           
        lake_bucket = client.get_bucket(config.config_vars['data_lake_id'])
        blob = raw_bucket.get_blob(event['name'])
        data_string = blob.download_as_bytes()
        json_data = ndjson.loads(data_string)
        if is_json_clean(json_data) is True:
            raw_bucket.copy_blob(blob, lake_bucket)
            current_time = datetime.datetime.now()
            log_message = Template('Data lake updated at $time')
            logging.info(log_message.safe_substitute(time=current_time))

    except Exception as error:
        log_message = Template('Failed to perform operations on raw and/or data lake storage buckets due to $message')
        logging.error(log_message.safe_substitute(message=error))
  


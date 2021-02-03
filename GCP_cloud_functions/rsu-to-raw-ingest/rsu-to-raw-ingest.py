from google.cloud import storage
import datetime
import logging
from string import Template
import config

def rsu_to_raw_ingest(event, context):
    """Triggered by a Pub/Sub message published by the Cloud Scheduler.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    
    # logging cloud function trigger
    current_time = datetime.datetime.now()
    log_message = Template('Cloud Function "rsu-to-raw ingest" was triggered at $time')
    logging.info(log_message.safe_substitute(time=current_time))

    client = storage.Client()
    raw_bucket = client.get_bucket(config.config_vars['raw_ingest_id'])
    raw_blob = raw_bucket.blob(str(datetime.datetime.now()))
    raw_blob.upload_from_filename(filename='RSU-ND-clean.json')

    # logging raw ingest upload
    current_time = datetime.datetime.now()
    log_message = Template('Raw ingest updated with new data at $time')
    logging.info(log_message.safe_substitute(time=current_time))

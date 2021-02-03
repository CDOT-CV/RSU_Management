import json
from google.cloud import storage 
from google.cloud import pubsub_v1
import time
import datetime
import pprint
import logging
from string import Template
import config

def rsu_data_warehouse_bucket(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    
    publisher = pubsub_v1.PublisherClient()
    client = storage.Client()

    topic = publisher.topic_path(config.config_vars['project_id'], config.config_vars['data_warehouse_id'])
    blob = client.get_bucket(event['bucket']).get_blob(event['name'])
    data_string = blob.download_as_string()
    future = publisher.publish(topic,data_string)
    print(future.result())

    # logging publication message
    current_time = datetime.datetime.utcnow()    
    log_message = Template('Published message to the warehouse pub/sub topic at $time')
    logging.info(log_message.safe_substitute(time=current_time))

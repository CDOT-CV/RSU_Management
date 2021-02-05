from google.cloud import storage 
from google.cloud import pubsub_v1
import datetime
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

    try:
        # logging cloud function trigger
        current_time = datetime.datetime.now()
        log_message = Template('Cloud Function "lake-to-data-warehouse" was triggered at $time')
        logging.info(log_message.safe_substitute(time=current_time))
        
        try:
            topic = publisher.topic_path(config.config_vars['project_id'], config.config_vars['data_warehouse_id'])
            blob = client.get_bucket(event['bucket']).get_blob(event['name'])
            data_string = blob.download_as_string()
            future = publisher.publish(topic,data_string)
            print(future.result())
            
            # logging publication message
            current_time = datetime.datetime.utcnow()    
            log_message = Template('Published message to the warehouse pub/sub topic at $time')
            logging.info(log_message.safe_substitute(time=current_time))

        except Exception as error:
            log_message = Template('Failed to perform actions with data warehouse Pub/Sub topic due to $message')
            logging.error(log_message.safe_substitute(message=error))
    
    except Exception as error:
         log_message = Template('$error').substitute(error=error)
         logging.error(log_message)

    

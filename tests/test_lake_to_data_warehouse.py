from GCP_cloud_functions.lake_to_data_warehouse import lake_to_data_warehouse
import mock
import pytest

@mock.patch("google.cloud.pubsub_v1.PublisherClient")
@mock.patch("google.cloud.storage.Client")
def test_Success(client, publish_client):

    event = {
        'bucket': 'rsu_data-lake',
        'name': 'test',
        'metageneration': 'some-metageneration',
        'timeCreated': '0',
        'updated': '0'
    }
    context = mock.MagicMock()
    context.event_id = 'some-id'
    context.event_type = 'gcs-event'

    lake_bucket = 'rsu_lake-bucket'
    lake_bucketOBJ = client().get_bucket(lake_bucket)
    lake_blob = lake_bucketOBJ.blob(event['name'])
    lake_blob.upload_from_string('{"timeReceived": "2020-05-14T11:37:06Z", "year": "2020", "month": "05", "day": "14", "hour": "11", "version": "1.1.0", "type": "bsm"}')
    topic_path = publish_client().topic_path('cdot-cv-ode-dev','rsu_data_warehouse')

    lake_to_data_warehouse.rsu_data_warehouse_bucket(event, context)
    
    publish_client().publish.assert_called_with(topic_path, client().get_bucket().get_blob().download_as_bytes())

    assert True

@mock.patch("google.cloud.storage.Client")
@mock.patch("google.cloud.pubsub_v1.PublisherClient")
def test_ExceptionRaised_InvalidJSON(client, publish_client):
    
    event = {
        'bucket': 'rsu_data-lake',
        'name': 'test',
        'metageneration': 'some-metageneration',
        'timeCreated': '0',
        'updated': '0'
    }
    context = mock.MagicMock()
    context.event_id = 'some-id'
    context.event_type = 'gcs-event'

    lake_bucketOBJ = client().get_bucket
    lake_to_data_warehouse.rsu_data_warehouse_bucket(event, context)

    lake_blob = lake_bucketOBJ.blob
    blob_name = event['name']

    with pytest.raises(Exception):
        lake_blob.assert_called_with(blob_name)
    

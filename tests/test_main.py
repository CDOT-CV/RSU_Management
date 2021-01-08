import sys
sys.path.append("..")
from data_manager.source_code import main
import mock
import os
import pytest
from google.cloud.storage import Client
from google.cloud.pubsub_v1 import PublisherClient

"""
This script handles the unit testing for main.py.
"""

#def test_is_json_clean():
#    mock_json = '{"column":1, "status":"yes"}'
#    assert is_json_clean(mock_json) is True

@mock.patch("google.cloud.storage.Client")
def test_rsu_raw_bucket(client):
    
    print("test raw ingest: begin.")
    
    raw_bucket = client().get_bucket
    file_name = 'json_test1'
    file_path = 'gcp_test/RSU-ND.json'
    main.rsu_raw_bucket(client(), file_name,file_path,'rsu_raw-ingest')
    
    blob = raw_bucket().blob
    blob.assert_called_with(file_name)
    blob().upload_from_filename.assert_called_with(filename=file_path)
    print("test raw ingest: complete!")
    assert True

@mock.patch("google.cloud.storage.Client")
def test_rsu_data_lake_bucket(client):
    
    print("test data lake: begin.")

    raw_bucket = 'rsu_raw-ingest'
    raw_bucketOBJ = client().get_bucket(raw_bucket)
    data_lake_bucket = 'rsu_data-lake-bucket'

    main.rsu_data_lake_bucket(client(), raw_bucket, data_lake_bucket)

    client().list_blobs.assert_called_with(raw_bucketOBJ)
    print("test data lake: complete!")
    assert True

@mock.patch("google.cloud.pubsub_v1.PublisherClient")
@mock.patch("google.cloud.storage.Client")
def test_rsu_data_warehouse_bucket(client, publish_client):
    
    print("test warehouse: begin.")
    
    data_lake_bucket = 'rsu_data-lake-bucket'
    lake_bucketOBJ = client().get_bucket(data_lake_bucket)

    topic_path = publish_client().topic_path('cdot-cv-ode-dev','rsu_data_warehouse')
    main.rsu_data_warehouse_bucket(publish_client(), client(), topic_path, data_lake_bucket)

    client().list_blobs.assert_called_with(lake_bucketOBJ)
    print("test raw ingest: complete!")
    assert True

@mock.patch("google.cloud.pubsub_v1.PublisherClient")
@mock.patch("google.cloud.storage.Client")
def test_help_warehouse(client, publish_client):
    print("test help-warehouse: begin.")
    data_lake_bucket = 'rsu_data-lake-bucket'
    lake_bucketOBJ = client().get_bucket(data_lake_bucket)
    lake_blob = lake_bucketOBJ.blob("test1")
    lake_blob.upload_from_string('{"column":1, "status":"yes"}')

    topic_path = publish_client().topic_path('cdot-cv-ode-dev','rsu_data_warehouse')

    main.help_warehouse([lake_blob], publish_client(),topic_path)
    publish_client().publish.assert_called_with(topic_path, client().get_bucket().blob().download_as_string())
    print("test help-warehouse: complete!")
    assert True

def test_main():
    print("test main (from main.py): begin")
    
    print("test main (from main.py): complete!")
    
def main():
    test_rsu_raw_bucket()
    test_rsu_data_lake_bucket()
    test_rsu_data_warehouse_bucket()
    test_help_warehouse()

if __name__ == '__main__':
    main()

import sys
sys.path.append("..")
from GCP_cloud_functions.rsu_to_raw_ingest import rsu_to_raw_ingest
import mock
import base64
import datetime
from unittest.mock import patch, mock_open
from unittest import TestCase
from google.cloud.storage import Client
from google.cloud.pubsub_v1 import PublisherClient
import pytest

"""
This script handles the unit testing for the rsu-to-raw-ingest Cloud Function.
"""

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

@mock.patch("google.cloud.storage.Client")
def test_PubSubmitted(client):
    
    name = 'Testing GCP automation, 1-2-3!'
    data = {'data': base64.b64encode(name.encode())}
    raw_bucket = client().get_bucket
    rsu_to_raw_ingest.rsu_to_raw_ingest(data, mock_context)
    blob = raw_bucket().blob
    blob_name = (str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
    file_name = 'RSU-ND-clean.json'
    blob.assert_called_with(blob_name)
    blob().upload_from_filename.assert_called_with(filename=file_name)

    assert True


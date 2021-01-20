
# RSU Data Manager

| Build       | Quality Gate     | Code Coverage     |
| :------------- | :----------: | -----------: |
|  [![Build Status](https://travis-ci.com/CDOT-CV/RSU_Management.svg?branch=CV-29)](https://travis-ci.com/CDOT-CV/RSU_Management) | [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=CDOT-CV_RSU_Management&metric=alert_status)](https://sonarcloud.io/dashboard?id=CDOT-CV_RSU_Management)   | [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=CDOT-CV_RSU_Management&metric=coverage)](https://sonarcloud.io/dashboard?id=CDOT-CV_RSU_Management)    |

## Project Description

This project is an open-source, proof-of-concept for the roadside unit (RSU) data manager with the integration of Google Cloud Storage (GCS) functions. RSU data is assumed to take the form of JSON strings. The data will be passed through three "containers". First, all data will be placed in the raw ingest (a GCS bucket). From there, each data is checked for cleanliness: if the check passes, the clean data is placed in the data lake (another GCS bucket). From there, data is pushed as byte string messages to the short-term data warehouse (a Google Cloud Pub/Sub topic).

## Guidelines
- Issues
  - Create issues using the SMART goals outline (Specific, Measurable, Actionable, Realistic and Time-Aware)
- PR (Pull Requests)
  - Create all pull requests from the master branch. 
  - Create small, narrowly focused pull requests.
  - Maintain a clean commit history so that they are easier to review.

## Prerequisites and Set-Up

This project supports Python >= 3.5. Refer to the requirements.txt document to [pip](https://pip.pypa.io/en/stable/) install the necessary packages. The Google Cloud Storage Python packages, for example, would be installed like so:

```bash
pip install google-cloud-bigquery
pip install google-cloud-storage
pip install google-cloud-pubsub
```

## How to Run

The integration of RSUs into this script is yet to come. At present, the script (main.py) uses the RSU sample file 'RSU-ND-clean.json' (found in the def main() function of the main.py script). This sample file accompanies the main.py script in data_manager/source_code. When running locally, ensure that main.py and the sample script are located in the same folder.
 
To run this code:

```
python3 main.py
```

## Testing

The test for the main.py script is the test_main.py script, which can be found in the /tests directory. 

To run the test script:

```
python -m pytest test_main.py
```


## Contributors
For any questions, contact Dhivahari Vivek at dhivahari.vivekanandasarma@state.co.us.

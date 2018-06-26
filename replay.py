# Author: Richard Ortega
# Description: Point to a Storage container and replay folder of data.
# Inspired from: https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-capture-python
# Install dependencies via: pip install -r requirements.txt
# Assumes a blob storage pattern of: {Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}
#
## Replay architecture ##
#
###################################
#   ----------      ----------    #
#  |          |    |          |   #
#  |  Replay  | -> | EventHub |   #
#  |          |    |          |   #
#   ----------      ----------    #
#       ^                         #
#       |                         #
#   ----------                    #
#  |          |                   #
#  |   Blob   |                   #
#  |  (JSON)  |                   #
#  |          |                   #
#   ----------                    #
###################################

import os
import sys
import string
import json
import uuid
import datetime
import random
import pdb
import time
from azure.storage.blob import BlockBlobService
from azure.servicebus import ServiceBusService

STORAGE_ACCOUNT_NAME = os.environ.get("STORAGE_ACCOUNT_NAME")
STORAGE_SAS_KEY = os.environ.get("STORAGE_SAS_KEY")
STORAGE_CONTAINER_NAME = os.environ.get("STORAGE_CONTAINER_NAME")
EVENT_HUB_NAMESPACE = os.environ.get("EVENT_HUB_NAMESPACE")
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME")
EVENT_HUB_SAS_NAME = os.environ.get("EVENT_HUB_SAS_NAME")
EVENT_HUB_SAS_KEY = os.environ.get("EVENT_HUB_SAS_KEY")

# Determines how fast messages are sent after downloading a blob and separating it.
# Default value is 100ms
MESSAGE_INTERVAL_DEFAULT_MS = 100
MESSAGE_INTERVAL_MS = os.environ.get("MESSAGE_INTERVAL_MS") or MESSAGE_INTERVAL_DEFAULT_MS
MESSAGE_INTERVAL_MS = int(MESSAGE_INTERVAL_MS)
MESSAGE_INTERVAL_S = MESSAGE_INTERVAL_MS / 1000.0

# Filters the results to return only blobs whose names begin with the specified prefix.
# If nothing is provide, None will be passed in and search will start at root of the folder.
PATH_PREFIX = os.environ.get("PATH_PREFIX")

# Validate env vars
if all([STORAGE_ACCOUNT_NAME,
        STORAGE_SAS_KEY,
        STORAGE_CONTAINER_NAME,
        EVENT_HUB_NAMESPACE,
        EVENT_HUB_NAME,
        EVENT_HUB_SAS_NAME,
        EVENT_HUB_SAS_KEY]):
    pass
else:
    print("Storage or Event Hub information is not defined.")
    sys.exit()

if not MESSAGE_INTERVAL_MS:
    print("MESSAGE_INTERVAL_MS environment variable not defined")
    print(f"MESSAGE_INTERVAL_MS set to default: {MESSAGE_INTERVAL_MS}")

if not PATH_PREFIX:
    print("PATH_PREFIX environment variable not defined")
    print(f"PATH_PREFIX will search at Root of container")

def start_processing():
    print(f"Connecting to Event Hub: {EVENT_HUB_NAME} within Event Hub Namespace {EVENT_HUB_NAMESPACE}")

    # Create Event Hub client
    sbs = ServiceBusService(service_namespace=EVENT_HUB_NAMESPACE,
                            shared_access_key_name=EVENT_HUB_SAS_NAME, 
                            shared_access_key_value=EVENT_HUB_SAS_KEY)

    # Create blob access client
    block_blob_service = BlockBlobService(
        account_name=STORAGE_ACCOUNT_NAME, 
        account_key=STORAGE_SAS_KEY)

    print(f"Connecting to Blob Storage: {STORAGE_ACCOUNT_NAME}")
    print(f"Using path prefix: {PATH_PREFIX}")

    # List all blobs in container
    blobs = block_blob_service.list_blobs(STORAGE_CONTAINER_NAME,
                                          prefix=PATH_PREFIX)

    print(f"Send message interval (ms): {MESSAGE_INTERVAL_S}")

    # Iterate over each blob and send to EventHub
    for blob in blobs:

        # Verify file has a .json extension
        # TODO: Support "avro" later
        if '.json' in blob.name:
            
            # content_length == 508 is an empty file, so only process content_length > 508 (skip empty files)
            if blob.properties.content_length > 508:

                # Ensure file is JSON
                # ['2017/06/28/15/0_248b3c7cb64342418a302475921f6665_1', 'json']
                if blob.name.split('.')[1] == 'json':
                    print('Downloading blob: ' + blob.name)
                    print('This may take a while depending on filesize.')

                    # Download blob
                    # blob = block_blob_service.get_blob_to_text(STORAGE_CONTAINER_NAME, blob.name)
                    blob = block_blob_service.get_blob_to_bytes(STORAGE_CONTAINER_NAME, blob.name)
                    print('Downloaded blob: ' + blob.name)

                    try:
                        blob.state = "downloaded"

                        # Parse blob
                        messages = json.loads(blob.content)

                        blob.state = "parsed"

                        messages_count = len(messages)
                        print(f'Sending {messages_count} messages for {blob.name}')

                        blob.state = "sending"
                        
                        # Split blob into individual messags
                        for index, message in enumerate(messages):
                            formatted_message = json.dumps(message)

                            # Send to Event Hub (Add +1 for clarity on message number)
                            current_message_num = (index + 1)
                            print(f"Found Message {current_message_num} out of {messages_count} for {blob.name}")
                            
                            # Send blob data to Event Hub
                            sbs.send_event(EVENT_HUB_NAME, formatted_message)

                            # Uncomment this line if you'd like message sending logging
                            # print(f"Sending message: {formatted_message}")

                            # TODO: This does not account for any throttling Event Hub may do if data exceeds plan throughput
                            # Sleep after every sent message
                            time.sleep(MESSAGE_INTERVAL_S)
                    except:
                        print(f'Error parsing: {blob.name} while in state: {blob.state}')
                        print(f'Skipping: {blob.name}')

                    print('Finished blob: ' + blob.name)

start_processing()

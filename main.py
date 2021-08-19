import base64
import json
import pandas as pd
import logging
from google.cloud import storage


def hello_pubsub(event, context):
    Args:
         event(dict): Event payload.
         context(google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    json_data = json.loads(pubsub_message)
    # final_jsob_data = {"id":json_data['id'], "text":json_data['text']}
    df = pd.json_normalize(json_data)
    storage_client = storage.Client()
    # df = self.convert_message_to_dataframe()
    bucket = storage_client.get_bucket('facebook-bucket1')
    blob = bucket.blob('facebook_data}.csv'.format(context.timestamp))
    blob.upload_from_string(data = df.to_csv(
        index = False), content_type = 'text/csv')

import json
import requests
import pandas as pd

from config import *

def query_news_trends():
    
    query = {
        # "query": {
        #     "match": {
        #     "title": "부동산"
        #     }
        # },
        "size": 0,
        "aggs": {
            "group_by_date": {
                "date_histogram": {
                    "field": "created_at",
                    "interval": "day"
                }
            }
        }
    }

    headers = {
        'Content-Type': 'application/json'
    }

    resp = requests.get(
        f'{ELASTIC_SEARCH_URL}/news/_search',
        headers = headers,
        data = json.dumps(query),
        auth=ELASTIC_SEARCH_AUTH,
    )

    #print(resp.status_code)

    results = resp.json()

    buckets = results['aggregations']['group_by_date']['buckets']

    df = pd.DataFrame(buckets)

    df['date'] = df['key_as_string'].str[:10]
    df = df[['date', 'doc_count']]

    return df.to_dict(orient='records')

def main(event, context):

    trends = query_news_trends()

    body = {
        'trends' : trends
    }

    response = {
        "statusCode": 200, # status 200은 성공이고, 그 내용이 body
        "body": json.dumps(body)
    }

    return response

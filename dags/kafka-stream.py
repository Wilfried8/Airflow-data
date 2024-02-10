from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'wilfried',
    'start_date':datetime(2024, 2, 10, 00)
}

def get_data():
    import requests
    import json

    response = requests.get("https://randomuser.me/api/")
    response = response.json()
    response = response['results'][0]
    #print(json.dumps(response, indent=2))

    return response

def format_data(response):
    data = {}
    location = response['location']
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    data['location'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"

    data['post_code'] = location['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']

    return data

def stream_data():
    import json
    import time
    import logging
    response = get_data()
    response = format_data(response=response)
    print(json.dumps(response, indent=2))

stream_data()
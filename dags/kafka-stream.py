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
    from kafka import KafkaProducer


    #print(json.dumps(response, indent=2))
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    current_time = time.time()

    while True:
        # 30 seconde
        if time.time() > current_time + 20:
            break

        try:
            response = get_data()
            response = format_data(response=response)

            producer.send('usercreated', json.dumps(response).encode('utf-8'))

        except Exception as e:
            logging.error(f'error is : {e}')
            continue

def after_stream_data():
    print('its ok .........')

with DAG('user_automation',
        default_args=default_args,
        schedule='@daily',
        catchup=False

         ) as dag:
    streaming_task = PythonOperator(
        task_id="stream_data_from_API",
        python_callable=stream_data
    )

    after_streaming_task = PythonOperator(
        task_id="after_stream_data_task",
        python_callable=after_stream_data
    )

    streaming_task >> after_streaming_task


#stream_data()

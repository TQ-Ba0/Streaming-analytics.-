# import library
import requests
from datetime import datetime
import json
import time
from airflow import DAG
from airflow.operators.python import PythonOperator


KAFKA_HOST_IP="192.168.1.6"
KAFKA_PORT="29092"
TOPIC='new_topic'

# get data from web  by api
def Get_data():
    api_result = requests.get('https://api.coincap.io/v2/assets', headers={'Authorization': 'TOK:10d4871e-9a75-41f2-bb69-dc5a503f7a9e'})
    api_response = api_result.json()
    data=api_response['data']
    Time_Update=api_response['timestamp']
    Time_Update=str(datetime.fromtimestamp(Time_Update/1000))
    return data,Time_Update,api_response['timestamp']

#
dag = DAG(
        dag_id="stream_coin_data", 
        start_date=datetime(2023, 10, 1), 
        schedule_interval='*/1 * * * *',   # every minute
        catchup=False,
        tags=['production'])

#
def serializer(message):
    return json.dumps(message).encode('utf-8')

# Create json schema based on data 
def Create_messeage(data,timestamp,time):
    coin_data=[]
    coin_data.append({
            "schema": {
                "name": "coin_data",
                "type": "struct",
                "fields": [
                    {"field": "id", "type": "int64"},
                    {"field": "rank", "type": "int64"},
                    {"field": "symbol", "type": "string"},
                    {"field": "name", "type": "string"},
                    {"field": "supply", "type": "float"},
                    {"field": "maxSupply", "type": "float"},
                    {"field": "marketCapUsd", "type": "float"},
                    {"field": "volumeUsd24Hr", "type": "float"},
                    {"field": "priceUsd", "type": "float"},
                    {"field": "changePercent24Hr", "type": "float"},
                    {"field": "vwap24Hr", "type": "float"},
                    {"field": "update", "type": "string"},
                ]
            },
            "payload": {
                "id": timestamp,
                "rank": data["rank"],
                "symbol": data["symbol"],
                "name": data["name"],
                "supply":data["supply"],
                "maxSupply":data["maxSupply"],
                "marketCapUsd" :data["marketCapUsd"],
                "volumeUsd24Hr":data["volumeUsd24Hr"],
                "priceUsd":data["priceUsd"],
                "changePercent24Hr":data["changePercent24Hr"],
                "vwap24Hr":data["vwap24Hr"],
                "update":time
            }
        })
        
    return coin_data[0]

# Fill Null in dict 
def dict_clean(items):
    result = {}
    for key, value in items:
        if value is None:
            value = '0'
        result[key] = value
    return result

# send message to kafka 
def send_message():
    from kafka import KafkaProducer
    data,time,time_stamp=Get_data()
    producer = KafkaProducer(
        bootstrap_servers=[f'{KAFKA_HOST_IP}:{KAFKA_PORT}'], value_serializer=serializer)
    for i in data:
        dict_str = json.dumps(i)
        my_dict = json.loads(dict_str, object_pairs_hook=dict_clean)
        my_dict['rank']=int(my_dict['rank'])
        my_dict['supply']=float(my_dict['supply'])
        my_dict['maxSupply']=float(my_dict['maxSupply'])
        my_dict['marketCapUsd']=float(my_dict['marketCapUsd'])
        my_dict['volumeUsd24Hr']=float(my_dict['volumeUsd24Hr'])
        my_dict['priceUsd']=float(my_dict['priceUsd'])
        my_dict['changePercent24Hr']=float(my_dict['changePercent24Hr'])
        my_dict['vwap24Hr']=float(my_dict['vwap24Hr'])
        message=Create_messeage(my_dict,time_stamp,time)
        producer.send(TOPIC, value=message)
    print('send succesfull message')

#
producer_task = PythonOperator(
    task_id="producer",
    provide_context=True,
    python_callable=send_message,
    dag=dag
)
producer_task 


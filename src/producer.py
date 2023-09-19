import requests
import time
import json
from kafka import KafkaProducer
import threading
# import credentials as cred

# key = cred.login['private_key']
key = '033da8185c74452e9c3e837ee3d3504a'

def send_Mumbai():
    url = 'https://api.weatherbit.io/v2.0/history/subhourly?&city=Mumbai&country=IN&start_date=2021-09-24&end_date=2021-09-26&key={}'.format(key)
    data = requests.get(url).json()
    city_name = data['city_name']

    for dataval in data['data']:
        dataval["city"] = city_name
        del dataval["weather"]
        del dataval["timestamp_utc"]
        del dataval["timestamp_local"]
        output = json.dumps(dataval)
        producer.send('weather', output)
        producer.flush()
        time.sleep(4)

def send_Chennai():
    url = 'https://api.weatherbit.io/v2.0/history/subhourly?&city=Chennai&country=IN&start_date=2021-09-24&end_date=2021-09-26&key={}'.format(key)
    data = requests.get(url).json()
    city_name = data['city_name']

    for dataval in data['data']:
        dataval["city"] = city_name
        del dataval["weather"]
        del dataval["timestamp_utc"]
        del dataval["timestamp_local"]
        output = json.dumps(dataval)
        producer.send('weather', output)
        producer.flush()
        time.sleep(4)

def send_Banglore():
    url = 'https://api.weatherbit.io/v2.0/history/subhourly?&city=Banglore&country=IN&start_date=2021-09-24&end_date=2021-09-26&key={}'.format(key)
    data = requests.get(url).json()
    city_name = data['city_name']

    for dataval in data['data']:
        dataval["city"] = city_name
        del dataval["weather"]
        del dataval["timestamp_utc"]
        del dataval["timestamp_local"]
        output = json.dumps(dataval)
        producer.send('weather', output)
        producer.flush()
        time.sleep(4)

def send_Hyderabad():
    url = 'https://api.weatherbit.io/v2.0/history/subhourly?&city=Hyderabad&country=IN&start_date=2021-09-24&end_date=2021-09-26&key={}'.format(key)
    data = requests.get(url).json()
    city_name = data['city_name']

    for dataval in data['data']:
        dataval["city"] = city_name
        del dataval["weather"]
        del dataval["timestamp_utc"]
        del dataval["timestamp_local"]
        output = json.dumps(dataval)
        producer.send('weather', output)
        producer.flush()
        time.sleep(4)

producer = KafkaProducer(value_serializer = lambda x: str(x).encode("utf-8"), bootstrap_servers = ['localhost:9092'])

threading.Thread(target=send_Mumbai).start()
threading.Thread(target=send_Chennai).start()
threading.Thread(target=send_Banglore).start()
threading.Thread(target=send_Hyderabad).start()

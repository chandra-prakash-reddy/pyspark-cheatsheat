from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['35.212.182.225:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

message = {'hello':'world'}


for e in range(1000):
    data = {'number': e}
    producer.send('publication-ops-databricks', value=message)
    print('message sent :  {}'.format(data))
    sleep(100)

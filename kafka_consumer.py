from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    bootstrap_servers=['35.212.182.225:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

consumer.subscribe(topics=['expired-dataset'])
print("subscribed")

for message in consumer:
    message = message.value
    print('message recieved :  {}'.format(message))

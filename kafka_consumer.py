from kafka import KafkaConsumer
from json import loads


import ssl

sasl_mechanism = 'SCRAM-SHA-256'
security_protocol = 'SASL_SSL'

# Create a new context using system defaults, disable all but TLS1.2
context = ssl.create_default_context()
context.options &= ssl.OP_NO_TLSv1
context.options &= ssl.OP_NO_TLSv1_1

broker_list=[']

username=""
password=""
topic_name=""
group_name=""

consumer = KafkaConsumer(
    bootstrap_servers= broker_list,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=group_name,
    sasl_plain_username=username,
    sasl_plain_password=password,
    security_protocol=security_protocol,
    ssl_context=context,
    sasl_mechanism=sasl_mechanism,
    value_deserializer=lambda x: loads(x.decode('utf-8')))

consumer.subscribe(topics=[topic_name])
print("subscribed")

for message in consumer:
    message = message.value
    print('message recieved :  {}'.format(message))

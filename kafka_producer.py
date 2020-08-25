from time import sleep
from json import dumps
from kafka import KafkaProducer

import ssl

sasl_mechanism = 'SCRAM-SHA-256'
security_protocol = 'SASL_SSL'

# Create a new context using system defaults, disable all but TLS1.2
context = ssl.create_default_context()
context.options &= ssl.OP_NO_TLSv1
context.options &= ssl.OP_NO_TLSv1_1

broker_list=['']

username=""
password=""
topic_name=""


producer = KafkaProducer(bootstrap_servers = broker_list,
                         sasl_plain_username = username,
                         sasl_plain_password = password,
                         security_protocol = security_protocol,
                         ssl_context = context,
                         sasl_mechanism = sasl_mechanism,
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))


message = {'hello':'world'}


for e in range(1000):
    data = {'number': e}
    producer.send(topic_name, value=message)
    print('message sent :  {}'.format(data))
    sleep(100)

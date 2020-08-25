from json import dumps

from kafka import KafkaProducer

from publication.error.publication_errors import InvalidConfigurationError
from common.timeout import timeout
from kazoo.client import KazooClient
import kafka

_bootstrap_server = 'bootstrap.servers'


class KafkaClient:

    def __init__(self, kafka_url):
        self.kafka_url = kafka_url
        pass

    @timeout(10.0)
    def is_kafka_topic_exists(self, topic):
        try:
            consumer = kafka.KafkaConsumer(group_id='test', bootstrap_servers=[self.kafka_url])
            if topic in set(consumer.topics()):
                return True
            else:
                return False
        except Exception as e:
            raise InvalidConfigurationError(
                "kafka_broker({}), topic({}) validation error : {}".format(self.kafka_url, topic,
                                                                           getattr(e, 'message', str(e))))
        pass

    @timeout(10.0)
    def is_zookeeper_exists(self, zookeper):
        try:
            zk = KazooClient(hosts=zookeper)
            zk.start()
            if zk.state == 'CONNECTED':
                zk.stop()
                return True
            else:
                return False
        except Exception as e:
            raise InvalidConfigurationError(
                "zookeeper validation error : {}".format(zookeper, getattr(e, 'message', str(e))))

    def send_json_message(self, topic, message):
        producer = KafkaProducer(bootstrap_servers=[self.kafka_url],
                                 value_serializer=lambda x:
                                 dumps(x).encode('utf-8'))
        producer.send(topic=topic, value=message)

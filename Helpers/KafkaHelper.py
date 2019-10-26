from kafka import KafkaConsumer, KafkaProducer
import json
import Config as cfg


class KafkaHelper:

    def __init__(self):
        try:
            self.consumer = None
            self._producer = None

        except Exception as e:
            print(e)

    def get_consumer(self, topic):
        try:
            self.consumer = KafkaConsumer(
                topic,  # topic name
                bootstrap_servers=cfg.kafka_bootstrap_servers,
                auto_offset_reset='earliest',
                group_id=cfg.consumer_group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
            print("Kafka Consumer is UP!")
        except Exception as e:
            print("Kafka Consumer can not be started! The exception was: %s" % str(e))
            exit(1)

    def get_producer(self):
        try:
            self._producer = KafkaProducer(
                client_id=cfg.producer_group_id,
                bootstrap_servers=cfg.kafka_bootstrap_servers,
            )
            print("Kafka Producer is UP!")
        except Exception as e:
            print(str(e.args[0]))
            print("Kafka Producer can not be started! The exception was: %s" % str(e))
            exit(1)

    def set_thread_manager(self, _thread_manager):
        self.listener_thread = _thread_manager

    def publish_message(self, key, value, topic):
        try:
            key_bytes = bytes(key, encoding='utf-8')
            value_bytes = bytes(value, encoding='utf-8')
            self._producer.send(topic, key=key_bytes, value=value_bytes)
            self._producer.flush()
            print("Message published successfully. Key:%s, Value:%s" % (key, value))
        except Exception as ex:
            print(str(ex.args[0]))
            print("Exception in publishing message. Key:%s, Value:%s" % (key, value))


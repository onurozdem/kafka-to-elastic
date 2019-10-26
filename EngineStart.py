import threading
from Helpers import Config as eng_cfg
from KafkaToElastic import Kafka2Elastic


class EngineStart:

    def __init__(self):
        threads = list()
        for i in range(len(eng_cfg.kafka_topics_names)):
            process_cls = Kafka2Elastic()
            x = threading.Thread(target=process_cls.consume_message, args=(eng_cfg.kafka_topics_names[i],))
            print("thread {} starting".format(i))
            threads.append(x)
            x.start()

        for i in threads:
            i.join()

EngineStart()
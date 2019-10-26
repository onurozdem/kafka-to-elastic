from Helpers.KafkaHelper import KafkaHelper
from Helpers.ElasticHelper import IndexHelper


class Kafka2Elastic:
    consumer_no = 1

    def __init__(self):
        self.elastic = IndexHelper()

        self.consumer_id = self.consumer_no
        Kafka2Elastic.consumer_no += 1
        print(str(self.consumer_id))

    def consume_message(self, consumer):
        self.kh = KafkaHelper()
        self.kh.get_consumer(consumer)

        for message in self.kh.consumer:
            try:
                for i in message.value:
                    data = i.copy()
                    print(self.consumer_id, "th consumer consumed a message")

                    index_name = str(consumer).lower()
                    doc_type_name = "from_kafka"

                    self.elastic.data_insert_index(index_name=index_name, doc_type_name=doc_type_name, data=data)
            except Exception as e:
                print("Error on {} consumer and error: {}".format(self.consumer_id, e.args[0]))

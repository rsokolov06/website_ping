import json

from lib.kafka_actors import Producer, Consumer

config = json.load(open('config.json'))


class TestKafkaClient:
    test_topic = 'metric_events'

    def test_message_e2e(self):
        message = 'hello world test message'.encode('utf-8')

        # This also tests Kafka broker connection
        client = Producer(config['kafka']['server'], self.test_topic)
        client.produce(message)
        client.producer.flush()

        consumer = Consumer(config['kafka']['server'], self.test_topic)
        messages = consumer.consume()

        # NOTE: Only check most recent value consumed
        assert messages[-1].value == message

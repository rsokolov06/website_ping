import json

from lib.kafka_actors import Consumer
from lib.dbclient import DbClient


class Writer:
    """
    Consumes messages from Kafka and writes stats to PostgreSQL DB.
    """

    def __init__(self, config: dict):
        self.consumer = Consumer(
                config['kafka']['server'],
                config['kafka']['topic']
        )
        self.db_client = DbClient(
                config['postgres']['host'],
                config['postgres']['db'],
                config['postgres']['port'],
                config['postgres']['username'],
                config['postgres']['table']
        )

    def receive_insert(self):
        while True:
            messages = self.consumer.consume()
            for raw_msg in messages:
                message = json.loads(raw_msg.value.decode('utf-8'))

                url = message['site_url']
                http_status = message['http_status']

                print(f'Consumer: got message for {url} with {http_status}')

                page_body = message['response_text']
                search_pattern = message['search_pattern']

                # Perform simple check for pattern.
                if search_pattern in page_body:
                    match_result = f'{search_pattern} found!'
                else:
                    match_result = f'{search_pattern} NOT found!'

                self.db_client.insert(
                        url,
                        http_status,
                        message['response_time'],
                        match_result
                )

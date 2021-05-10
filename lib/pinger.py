import asyncio
import json
import time
from typing import List
import aiohttp
from lib.kafka_actors import Producer

# Inspired by:
# https://realpython.com/async-io-python/#a-full-program-asynchronous-requests
##

class Pinger:

    def __init__(self, config: dict):
        self.interval = config['monitor_interval']
        self.producer = Producer(
                config['kafka']['server'],
                config['kafka']['topic']
        )

    async def ping_send(self, websites: dict):
        """
        Ping websites and send to Kafka.
        """

        print(f'Monitoring sites {list(websites.keys())}')
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2)) as session:
            while True:
                start = time.time()
                tasks = []
                for site in websites:
                    tasks.append(self._get_website(session, site, websites[site]))
                await asyncio.gather(*tasks)

                end_time = time.time() - start
                sleep_time = max(0, int(self.interval - end_time))
                print(
                        'Producer: Queried %d sites in %.2f seconds. '
                        'Sleeping for %d seconds...' %
                        (len(websites), end_time, sleep_time)
                )
                time.sleep(sleep_time)

    async def _get_website(self,
                           session: aiohttp.ClientSession,
                           site_url: str,
                           search_pattern: str):
        """
        Async method to get website and produce stats to Kafka.
        """

        start = time.time()
        response = await session.get(site_url)
        delta = int((time.time() - start) * 1000)
        print(f'Producer: {site_url} returned {response.status}')

        # TODO: Timeout handling.
        body = await response.text(encoding='ISO-8859-1')
        # prefix = body[0:100]
        # print(f'Check this one: {prefix}')

        message = {
            'site_url': site_url,
            'http_status': response.status,
            'response_time': delta,
            'response_text': body,
            'search_pattern': search_pattern
        }
        self.producer.produce(json.dumps(message).encode('utf-8'))

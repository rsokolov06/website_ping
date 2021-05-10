#!/usr/bin/env python

import asyncio
import json
import threading
import sys

from lib.pinger import Pinger
from lib.writer import Writer

config = json.load(open('config.json'))

#
# Asynchronous approach used to build this pinger.
# Link -> https://realpython.com/async-io-python/#a-full-program-asynchronous-requests
#


def run_pinger():
    pinger = Pinger(config)

    # This is a workaround to call asyncio for Windows
    # Link -> https://github.com/encode/httpx/issues/914#issuecomment-622586610
    if (sys.version_info[0] == 3 and
    sys.version_info[1] >= 8 and
    sys.platform.startswith('win')):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(pinger.ping_send(config['websites']))


def run_consumer():
    writer = Writer(config)
    writer.receive_insert()


if __name__ == '__main__':
    try:
        pinger_thread = threading.Thread(target=run_pinger, daemon=True)
        consumer_thread = threading.Thread(target=run_consumer, daemon=True)

        pinger_thread.start()
        consumer_thread.start()

        # Join temporarily in loop to allow exiting via Ctrl+C from command line
        while pinger_thread.is_alive():
            pinger_thread.join(1)
        while consumer_thread.is_alive():
            consumer_thread.join(1)
    except KeyboardInterrupt:
        print('Ctrl+C pressed.  Exiting...')
        # TODO: close connections to Kafka and PostgreSQL
        sys.exit()

# request API and save message to Kafka
import argparse
# release resource (thread pool, database connection,
# network connection) once error occurs
import atexit
import json
import logging
import requests
import schedule
import time
import cbpro
import dateutil.parser

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger_format = "%(asctime)s - %(message)s"
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)  # elaborate more details at debug level

API_BASE = 'https://api.gdax.com'
WSS_URL = 'wss://ws-feed.pro.coinbase.com'
CHANNEL = ['ticker']


class CoinbaseWsClient(cbpro.WebsocketClient):
    def on_message(self, msg):
        if 'type' in msg and msg['type'] == 'ticker':
            print(msg)
            try:
                price = msg['price']
                symbol = msg['product_id']
                timestamp = dateutil.parser.parse(msg['time']).timestamp()

                payload = {
                    'exchange': 'Coinbase',
                    'symbol': str(symbol),
                    'price': str(price),
                    'ts': str(timestamp)
                }

                logger.debug('Retrieved %s info %s', symbol, payload)

                # serialization json => str, default string format is ASCII
                producer.send(topic=topic_name,
                              value=json.dumps(payload).encode('utf-8'))
                logger.info('Sent price for %s to kafka', symbol)
            except Exception as e:
                logger.error('Failed to fetch price: %s', e)


def check_symbol(symbols):
    """
    Helper method checks if the symbol exists in coinbase API.
    """
    logger.debug('Checking symbol.')
    try:
        response = requests.get(API_BASE + '/products')
        product_ids = [product['id'] for product in response.json()]
        for s in symbols:
            if s not in product_ids:
                logger.warn(
                    'symbol %s not supported. The list of supported symbols: %s',
                    s.product_ids)
                exit()
    except Exception as e:
        logger.error('Failed to fetch products: %s', e)


def shutdown_hook(producer, ws_client):
    try:
        producer.flush(
            10)  # forcely write buffer into disk, discard after 10 sec
    except KafkaError as kafka_error:
        logger.warn(
            'Failed to Failed to flush pending messages to kafka, caused by: %s',
            kafka_error.message)
    finally:
        try:
            producer.close(10)
        except Exception as e:
            logger.error('Failed to close kafka connection, cased by %s',
                        e.message)
        ws_client.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--symbols', nargs='+', help='the symbol you want to pull.')
    parser.add_argument('-t', '--topic-name', help='the kafka topic push to.')
    parser.add_argument('-b', '--broker', help='the location of kafka broker.')

    # Parse arguments.
    args = parser.parse_args()
    symbols = args.symbols
    topic_name = args.topic_name
    kafka_broker = args.broker

    # Check if the symbol is supported
    check_symbol(symbols)
    print(symbols)
    # Intantiate a simple kafka producer.
    # https://github.com/dpkp/kafka-python
    producer = KafkaProducer(bootstrap_servers=kafka_broker)

    # Schedule and run the fetch_price function every one second
    # mark the calendar
    # schedule.every(1).seconds.do(fetch_price, symbol, producer, topic_name)

    ws_client = CoinbaseWsClient(url=WSS_URL,
                                 channels=CHANNEL,
                                 products=symbols)

    # Setup proper shutdown hook
    atexit.register(shutdown_hook, producer, ws_client)

    ws_client.start()
    while True:

        # otherwise, schedule always check whether 1 second is reached
        # see usage part at: https://github.com/dbader/schedule
        # check calendar
        time.sleep(1)

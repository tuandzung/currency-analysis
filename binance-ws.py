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
import dateutil.parser

from kafka import KafkaProducer
from kafka.errors import KafkaError

from binance.client import Client
from binance.websockets import BinanceSocketManager
from pprint import pprint


logger_format = "%(asctime)s - %(message)s"
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)  # elaborate more details at debug level


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

    client = Client()
    # Intantiate a simple kafka producer.
    # https://github.com/dpkp/kafka-python
    producer = KafkaProducer(bootstrap_servers=kafka_broker)

    def shutdown_hook(producer, bm, conn_key):
        try:
            producer.flush(
                10)  # forcely write buffer into disk, discard after 10 sec
        except KafkaError as kafka_error:
            logger.error(
                'Failed to Failed to flush pending messages to kafka, caused by: %s',
                kafka_error.message)
        finally:
            try:
                producer.close(10)
            except Exception as e:
                logger.error('Failed to close kafka connection, cased by %s',
                            e.message)
            bm.stop_socket(conn_key)
            bm.close()
    
    def check_symbol(symbols):
        """
        Helper method checks if the symbol exists in coinbase API.
        """
        logger.debug('Checking symbol.')
        try:
            exchange_info = client.get_exchange_info()
            symbols_info = {
                product['symbol']: f'{product["baseAsset"]}-{product["quoteAsset"]}' \
                for product in exchange_info['symbols']
            }
            for s in symbols:
                if s not in symbols_info.keys():
                    logger.error(
                        'symbol %s not supported. The list of supported symbols: %s',
                        s, symbols_info.keys())
                    exit()
            return symbols_info
        except Exception as e:
            logger.error('Failed to fetch products: %s', e)


    def process_message(msg):
        print(msg['data'])
        # do something

    # pprint(client.get_exchange_info())
    symbols_info = check_symbol(symbols)
    sockets = [f'{s.lower()}@aggTrade' for s in symbols]
    bm = BinanceSocketManager(client)
    conn_key = bm.start_multiplex_socket(sockets, process_message)

    # Setup proper shutdown hook
    atexit.register(shutdown_hook, producer, bm, conn_key)

    bm.start()

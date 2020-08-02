# read data from Hbase to Kafka
import argparse
import atexit
import json
import happybase
import logging
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-storage-reader')
logger.setLevel(logging.DEBUG)


def shutdown_hook(producer):
    """
    a shutdown hook to be called before the shutdown
    """
    try:
        logger.info('Closing Kafka producer')
        producer.flush(10)  # 10 sec
        producer.close()
        logger.info('Kafka producer closed')
    except KafkaError as kafka_error:
        logger.warn('Failed to close Kafka producer, caused by: %s',
                    kafka_error.message)
    finally:
        logger.info('Exiting program')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name')
    parser.add_argument('kafka_broker')

    # Parse arguments
    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    # Initiate a simple kafka producer.
    # https://stackoverflow.com/a/1419159
    # bootstrap is the "master" server
    kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)
    
    payload = {
        "schema": {
            "type": "struct", 
            "name": "coins.ohlc",
            "fields": [
                {"field": "exchange", "type": "string", "optional": False},
                {"field": "symbol", "type": "string", "optional": False},
                {"field": "time", "type": "string", "optional": False},
                {"field": "open", "type": "float", "optional": False},
                {"field": "high", "type": "float", "optional": False},
                {"field": "low", "type": "float", "optional": False},
                {"field": "close", "type": "float", "optional": False}]
        },
        "payload": {
            "exchange": "Coinbase",
            "symbol": "BTC-USD",
            "time": "1596171720",
            "open": 11068.55,
            "high": 11068.55,
            "low": 11068.55,
            "close": 11068.55
        }
    }
    print(json.dumps(payload))
    kafka_producer.send(topic=topic_name,
                        value=json.dumps(payload).encode('utf-8'))
    time.sleep(5)
    # Setup proper shutdown hook
    atexit.register(shutdown_hook, kafka_producer)
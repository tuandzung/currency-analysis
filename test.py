# read data from Hbase to Kafka
import argparse
import atexit
import json
import happybase
import logging
import time

from kafka import KafkaProducer

logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-storage-reader')
logger.setLevel(logging.DEBUG)


def shutdown_hook(producer, connection):
    """
    a shutdown hook to be called before the shutdown
    """
    try:
        logger.info('Closing Kafka producer')
        producer.flush(10)  # 10 sec
        producer.close()
        logger.info('Kafka producer closed')
        logger.info('Closing Hbase Connection')
        connection.close()
        logger.info('Hbase Connection closed')
    except KafkaError as kafka_error:
        logger.warn('Failed to close Kafka producer, caused by: %s',
                    kafka_error.message)
    finally:
        logger.info('Existing program')


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
            "name": "User",
            "fields": [
                {"field": "firstName", "type": "string"},
                {"field": "lastName", "type": "string"},
                {"field": "age","type": "int32"},
                {"field":"salary","type":"int64"}
            ]
        },
        "payload": {
            "firstName": "John",
            "lastName": "Smith",
            "age":30,
            "salary": 4830
        }
    }
    print(json.dumps(payload))
    kafka_producer.send(topic=topic_name,
                        value=json.dumps(payload).encode('utf-8'))
    time.sleep(5)
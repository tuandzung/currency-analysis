import json
import math

from unittest.mock import MagicMock, call
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext

import time
from time import sleep

data_stream_module = __import__("data-stream")

topic = 'test_topic'

# 42000 / 3 = 14000
test_input = [
    json.dumps({
        'ts': '1526900000001',
        'symbol': 'BTC-USD',
        'price': '10000',
        'base_vol': '0.1'
    }),
    json.dumps({
        'ts': '1526900000001',
        'symbol': 'BTC-USD',
        'price': '12000',
        'base_vol': '0.2'
    }),
    json.dumps({
        'ts': '1526900000001',
        'symbol': 'BTC-USD',
        'price': '20000',
        'base_vol': '0.15'
    }),
    json.dumps({
        'ts': '1526900000001',
        'symbol': 'ETH-USD',
        'price': '300',
        'base_vol': '0.15'
    }),
]


def _make_dstream_helper(sc, ssc, test_input):
    input_rdds = [sc.parallelize(test_input, 1)]
    input_stream = ssc.queueStream(input_rdds)
    return input_stream


def test_data_stream(sc, sqlc, ssc, topic, batch_duration):
    input_stream = _make_dstream_helper(sc, ssc, test_input)
    
    print(input_stream.pprint())
    mock_kafka_producer = MagicMock()

    data_stream_module.process_stream(input_stream, sc, sqlc, mock_kafka_producer, topic, batch_duration)

    ssc.start()
    sleep(5)
    ssc.stop()

    assert mock_kafka_producer.send.call_count == 2  # test whether send() function has been called only once
    (first_args, first_kwargs), (second_args, second_kwargs) = mock_kafka_producer.send.call_args_list  # list all arguments when calling send()
    print(first_kwargs)
    print(second_kwargs)

    assert math.isclose(json.loads(first_kwargs['value'])['price'],
                        14000.0,
                        rel_tol=1e-10)
    assert math.isclose(json.loads(first_kwargs['value'])['volume'],
                        0.45 * 14000.0,
                        rel_tol=1e-10)
    assert math.isclose(json.loads(second_kwargs['value'])['price'],
                        300.0,
                        rel_tol=1e-10)
    assert math.isclose(json.loads(second_kwargs['value'])['volume'],
                        0.15 * 300.0,
                        rel_tol=1e-10)
    print('test_data_stream passed!')


if __name__ == '__main__':
    sc = SparkContext('local[2]', 'local-testing')
    sqlc = SQLContext(sc)
    ssc = StreamingContext(sc, 1)
    batch_duration = 10

    test_data_stream(sc, sqlc, ssc, topic, batch_duration)

from confluent_kafka import Producer
import json

data = {
    'tag': 'order-1',
    'name': 'Name-1',
    'id': 1,
    'items': {
        'product1': 100,
        'product2': 200
    }
}


def read_cloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf


producer = Producer(read_cloud_config('/Users/pavanmantha/Pavans/PracticeExamples/DataScience_Practice/spark-handson/spark-kafka/client.properties'))
producer.produce(topic="orders", value=json.dumps(data).encode('utf-8'))
producer.flush()


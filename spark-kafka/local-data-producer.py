from kafka import KafkaProducer
import json

kafka_server = "localhost:9092"
producer = KafkaProducer(bootstrap_servers=[kafka_server], value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         acks='all', retries=3)


def product():
    try:
        for i in range(10):
            data = {
                'tag': 'order-'+str(i),
                'name': 'Name'+str(i),
                'id': i,
                'items': {
                    'product1': 100,
                    'product2': 200
                }
            }

            producer.send(topic='orders', value=data)
            producer.flush()
            print("order {} streamed to topic".format(i))
    except Exception as e:
        print(e.__cause__)


if __name__ == '__main__':
    product()

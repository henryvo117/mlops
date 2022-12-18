from time import sleep
from json import dumps
from kafka import KafkaProducer
from sensor import SensorEntry


class StreamProducer:
    def __init__(self, filename, topic):
        self.filename = filename
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                      value_serializer=lambda x:
                                      dumps(x).encode('utf-8'))

    def start_stream(self):
        json_file = open(self.filename)
        data = json_file.read()
        data = SensorEntry.schema().loads(data, many=True)

        for sensor_entry in data:
            print(sensor_entry.to_json())
            self.producer.send(self.topic, value=sensor_entry.to_json())  # object of 3 fields
            sleep(0.01)

        json_file.close()


if __name__ == '__main__':
    topic = 'numtest'
    producer = StreamProducer('data/parking.json', topic)
    producer.start_stream()


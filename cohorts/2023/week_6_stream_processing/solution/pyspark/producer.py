from typing import Tuple
import csv
from time import sleep
from typing import Dict
from kafka import KafkaProducer

from settings import BOOTSTRAP_SERVERS, INPUT_GREEN_PATH, INPUT_FHV_PATH, \
    GREEN_KAFKA_TOPIC, FHV_KAFKA_TOPIC


class RideCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        # self.producer = Producer(producer_props)

    @staticmethod
    def read_records(resource_path: str, type='green'):
        records, ride_keys = [], []
        i = 0
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            if type == 'green':
                for row in reader:
                    # vendor_id, pu_location_id
                    records.append(f'{row[0]}, {row[5]}')
                    ride_keys.append(str(row[0]))
                    i += 1
            elif type == 'fhv':
                for row in reader:
                    # dispatching_base_num, pu_location_id
                    records.append(f'{row[0]}, {row[3]}')
                    ride_keys.append(str(row[0]))
                    i += 1
        return zip(ride_keys, records)

    def publish(self, topic: str, records: Tuple[str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)


if __name__ == "__main__":
    config = {
        'bootstrap_servers': [BOOTSTRAP_SERVERS],
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8')
    }
    producer = RideCSVProducer(props=config)
    print("Producing Green Taxi Records")
    green_records = producer.read_records(resource_path=INPUT_GREEN_PATH, type='green')
    producer.publish(topic=GREEN_KAFKA_TOPIC, records=green_records)

    print('Producing FHV Records')
    fhv_records = producer.read_records(resource_path=INPUT_FHV_PATH, type='fhv')
    producer.publish(topic=FHV_KAFKA_TOPIC, records=fhv_records)


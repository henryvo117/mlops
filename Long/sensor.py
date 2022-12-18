from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config
from datetime import date, datetime, timedelta
import numpy as np
from marshmallow import fields
from numpy import ceil


@dataclass_json
@dataclass
class SensorEntry:
    date: datetime = field(
        metadata=config(
            encoder=datetime.isoformat,
            decoder=datetime.fromisoformat,
            mm_field=fields.DateTime(format='iso')
        )
    )
    id: int
    time: float


class DataGeneration:
    def __init__(self, number_hours, number_sensors):
        self.number_hours = number_hours
        self.number_sensors = number_sensors
        self.parking_data = []

    def generate(self):
        """
        Script to generate amount of time (1 car at a time) for each parking sensor during the amount of time
        Returns: list of date and time, sensor_id, and time parked per car
        """
        starting_datetime = datetime(2019, 10, 31, 00, 00, 00)

        for sensor_id in range(self.number_sensors):
            transaction_time = starting_datetime
            # 150 hours of transactions
            for hours in range(self.number_hours):
                # Random number of time per car parked
                h = 60
                while h > 0:
                    minute = round(np.random.uniform(5, 50), 3)
                    h -= ceil(minute)
                    transaction_time += timedelta(minutes=minute)
                    sensor_entry = SensorEntry(transaction_time, sensor_id, minute)
                    self.parking_data.append(sensor_entry)

    def to_json(self, filename='data.json'):
        """
        Creates json from list
        """
        json_data = SensorEntry.schema().dumps(self.parking_data, many=True)
        with open(filename, "w+") as f:
            f.write(str(json_data))

    def parking_simulate(self, filename='data.json'):
        self.generate()
        self.to_json(filename)


if __name__ == '__main__':
    number_hours = 100
    number_sensors = 200
    generator = DataGeneration(number_hours, number_sensors)
    generator.parking_simulate('data/parking.json')

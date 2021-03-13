import os
from pathlib import Path
from confluent_kafka import avro

# key_schema = avro.load(
#     f"{Path(__file__).parents[0]}/producers/models/schemas/arrival_key.json"
# )

# print(type(key_schema))
# print(key_schema)

from enum import IntEnum


class Train:
    """Defines CTA Train Model"""

    status = IntEnum("status", "out_of_service in_service broken_down", start=0)

    def __init__(self, train_id, status):
        self.train_id = train_id
        self.status = status
        if self.status is None:
            self.status = Train.status.out_of_service

    def __str__(self):
        return f"Train ID {self.train_id} is {self.status.name.replace('_', ' ')}"

    def __repr__(self):
        return str(self)

    def broken(self):
        return self.status == Train.status.broken_down


train = Train(3, Train.status.in_service)
print(train)
print(train.status)
print(type(train.status.name))
print(train.status.name)

print()
for stat in Train.status:
    print(stat.name, stat.value)

value1 = '{"id": "8PX2adtkjmzmyNcFmhCXjD", "v": "12", "q": true, "t": 1693004182012}'

# Received data=b'{"id":"271dc4b5-2b06-46d8-a7df-5171bae9b95d","v":48829.10475631259,"q":true,"t":1659006375584}'
# Received data=b'{"id":"271dc4b5-2b06-46d8-a7df-5171bae9b95d","v":27533.85680169882,"q":true,"t":1659006398233}'
# Received data=b'{"id":"271dc4b5-2b06-46d8-a7df-5171bae9b95d","v":30011.40692440489,"q":true,"t":1659006406664}'
# Received data=b'{"id":"271dc4b5-2b06-46d8-a7df-5171bae9b95d","v":37129.41006993877,"q":true,"t":1659006433747}'
# Received data=b'{"id":"271dc4b5-2b06-46d8-a7df-5171bae9b95d","v":52553.738641178854,"q":true,"t":1659006457822}'

import time
import json
import random
import boto3
from botocore.config import Config

STREAM_NAME = "input-stream"
my_config = Config(region_name="eu-central-1")


def get_state_data():
    return {
        "id": "8PX2adtkjmzmyNcFmhCXjD",
        "v": random.choice(["Idle", "Running", "Stopped", "Held"]),
        "q": True,
        "t": int(time.time() * 1000),
    }


def get_speed_data():
    return {"id": "uuidSpeed1234", "v": str(random.random() * 1000), "q": True, "t": int(time.time() * 1000)}


def other_data():
    return {"id": "outofscopeUUID", "v": str(random.random() * 12), "q": True, "t": int(time.time() * 1000)}


def generate(stream_name, kinesis_client):
    while True:
        data = get_state_data()
        time.sleep(1)
        print("Sending data: {}".format(data))
        kinesis_client.put_record(StreamName=stream_name, Data=json.dumps(data), PartitionKey="dummymeasurement")

        for _ in range(random.randint(1, 4)):
            data = get_speed_data()
            time.sleep(1)
            print("Sending data: {}".format(data))
            kinesis_client.put_record(StreamName=stream_name, Data=json.dumps(data), PartitionKey="dummymeasurement")

        data = other_data()
        time.sleep(1)
        print("Sending data: {}".format(data))
        kinesis_client.put_record(StreamName=stream_name, Data=json.dumps(data), PartitionKey="dummymeasurement")


if __name__ == "__main__":
    generate(STREAM_NAME, boto3.client("kinesis", config=my_config))

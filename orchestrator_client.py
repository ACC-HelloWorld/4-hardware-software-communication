# Description: Send commands to HiveMQ and receive sensor data from HiveMQ

# NOTE: for this to work properly, you are expected to be running the
# microcontroller_client.py script on your Pico W

import os
from queue import Queue, Empty
import json
from time import time, sleep
import secrets
import paho.mqtt.client as paho
import threading

course_id = os.environ["COURSE_ID"]
username = os.environ["HIVEMQ_USERNAME"]
password = os.environ["HIVEMQ_PASSWORD"]
host = os.environ["HIVEMQ_HOST"]

# Topics
neopixel_topic = f"{course_id}/neopixel"
as7341_topic = f"{course_id}/as7341"

# Commands for three gemstone colors
commands = [
    {"R": 15, "G": 82, "B": 186},  # sapphire
    {"R": 155, "G": 17, "B": 30},  # ruby
    {"R": 80, "G": 200, "B": 120},  # emerald
]

# Later, this should be populated with the payload dictionaries from the microcontroller
results_dicts = []


# - Set up the on_message and on_connect event handlers for the client.
# - Connect to the MQTT broker and subscribe to the provided topic.
# - Return the configured client instance.
def get_client_and_queue(
    subscribe_topic, host, username, password=None, port=8883, tls=True
):
    client = paho.Client()  # create new instance
    queue = Queue()  # Create queue to store sensor data
    connected_event = threading.Event()  # event to wait for connection

    def on_message(client, userdata, msg):
        print(f"Received message on topic {msg.topic}: {msg.payload}")
        # TODO: Convert msg (a JSON string) into a dictionary
        # TODO: Put the dictionary into the queue
        ...

    def on_connect(client, userdata, flags, rc):
        client.subscribe(subscribe_topic, qos=2)
        connected_event.set()

    client.on_connect = on_connect
    client.on_message = on_message

    # enable TLS for secure connection
    if tls:
        client.tls_set(tls_version=paho.ssl.PROTOCOL_TLS_CLIENT)

    # set username and password
    client.username_pw_set(username, password)

    # connect to HiveMQ Cloud on port 8883 (default for MQTT)
    client.connect(host, port)

    client.subscribe(subscribe_topic, qos=2)

    # wait for connection to be established
    connected_event.wait(timeout=10.0)
    return client, queue


# Function to send a command to the neopixel and wait for sensor data
def run_experiment(
    client, queue, command_topic, payload_dict, queue_timeout=30, function_timeout=300
):
    # TODO: Convert payload_dict into a JSON string
    # TODO: Publish the JSON string to the command_topic with qos=2
    ...

    client.loop_start()

    t0 = time()
    while True:
        if time() - t0 > function_timeout:
            raise TimeoutError(
                f"Function timed out without valid data ({function_timeout} seconds)"
            )
        try:
            results = queue.get(True, timeout=queue_timeout)
        except Empty as e:
            raise Empty(
                f"Sensor data retrieval timed out ({queue_timeout} seconds)"
            ) from e

        # only return the data if it matches the expected experiment id
        if (
            isinstance(results, dict)
            and results["experiment_id"] == payload_dict["experiment_id"]
        ):
            client.loop_stop()
            return results


# Orchestrator subscribes to the sensor data topic
client, queue = get_client_and_queue(as7341_topic, host, username, password=password)

payload_dicts = []

sleep(5.0)

# Run the experiments
for command in commands:
    # random experiment id to keep track where the sensor data is from
    experiment_id = secrets.token_hex(4)  # 4 bytes = 8 characters
    payload_dict = {"command": command, "experiment_id": experiment_id}
    payload_dicts.append(payload_dict)

    print(f"Sending {payload_dict} to {neopixel_topic}")
    results_dict = run_experiment(client, queue, neopixel_topic, payload_dict)

    # results_dict should be of the form:
    # {
    #     "command": {"R": ..., "G": ..., "B": ...},
    #     "sensor_data": {"ch410": ..., "ch440": ..., ..., "ch670": ...},
    #     "experiment_id": "...",
    # }

    results_dicts.append(results_dict)
    sleep(1)

# write the commands with experiment ids to a file (for autograding)
with open("payload_dicts.json", "w") as f:
    json.dump(payload_dicts, f)

# write the results to a file (for autograding)
with open("results.json", "w") as f:
    json.dump(results_dicts, f)

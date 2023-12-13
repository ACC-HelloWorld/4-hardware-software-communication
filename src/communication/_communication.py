import os
from paho.mqtt import client as mqtt_client
import threading

import json

username_key = "HIVEMQ_USERNAME"
password_key = "HIVEMQ_PASSWORD"
host_key = "HIVEMQ_HOST"


def hivemq_communication(outgoing_message, subscribe_topic, publish_topic):
    broker = os.environ[host_key]
    username = os.environ[username_key]
    password = os.environ[password_key]
    # print(f"Connecting to {broker} with username {username} and password {password}")

    received_messages = []  # avoid using nonlocal by using mutable data structure
    message_received_event = threading.Event()
    connected_event = threading.Event()

    def on_connect(client, userdata, flags, rc):
        client.subscribe(subscribe_topic, qos=2)
        connected_event.set()

    def on_message(client, userdata, message):
        received_message = json.loads(message.payload)
        received_messages.append(received_message)
        message_received_event.set()

    client = mqtt_client.Client()
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.on_message = on_message

    client.tls_set(
        tls_version=mqtt_client.ssl.PROTOCOL_TLS_CLIENT
    )  # Enable TLS with specific version
    client.connect(broker, port=8883)  # Connect to the broker on port 8883
    client.loop_start()

    connected_event.wait(timeout=10)  # Wait for the connection to be established

    client.publish(publish_topic, outgoing_message, qos=2)

    message_received_event.wait(timeout=20)  # Wait for the message to be received

    if not message_received_event.is_set():
        raise TimeoutError("No message received within the specified timeout")

    client.loop_stop()

    assert (
        len(received_messages) == 1
    ), f"Expected 1 message, got {len(received_messages)}"
    received_message = received_messages[0]

    return received_message


"""Developer note:

Within a conda environment, you can run the following commands to set
environment variables persistently in a way that can be read by
os.getenv("VAR_NAME"). This helps while developing the repo locally
instead of needing to run it on GitHub Codespaces.

```
conda env config vars set HIVEMQ_USERNAME=your_username
conda env config vars set HIVEMQ_PASSWORD=your_password
conda env config vars set HIVEMQ_HOST=your_host
conda env config vars set COURSE_ID=your_course_id

(or all in one line, e.g., conda env config vars set HIVEMQ_USERNAME=your_username HIVEMQ_PASSWORD=your_password HIVEMQ_HOST=your_host COURSE_ID=your_course_id)
```
"""

# import os
# import paho.mqtt.client as paho
# from paho import mqtt
# import json
# from queue import Queue

# username_key = "HIVEMQ_USERNAME"
# password_key = "HIVEMQ_PASSWORD"
# host_key = "HIVEMQ_HOST"


# data_queue: "Queue[dict]" = Queue()

# def get_paho_client(subscribe_topic, port=8883, tls=True):
#     broker = os.environ[host_key]
#     username = os.environ[username_key]
#     password = os.environ[password_key]

#     client = paho.Client(protocol=paho.MQTTv5)  # create new instance

#     def on_message(client, userdata, msg):
#         data_queue.put(json.loads(msg.payload))

#     # The callback for when the client receives a CONNACK response from the server.
#     def on_connect(client, userdata, flags, rc, properties=None):
#         if rc != 0:
#             print("Connected with result code " + str(rc))
#         # Subscribing in on_connect() means that if we lose the connection and
#         # reconnect then subscriptions will be renewed.
#         client.subscribe(subscribe_topic, qos=1)

#     client.on_connect = on_connect
#     client.on_message = on_message

#     # enable TLS for secure connection
#     if tls:
#         client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
#     # set username and password
#     client.username_pw_set(username, password)
#     # connect to HiveMQ Cloud on port 8883 (default for MQTT)
#     client.connect(broker, port)
#     client.subscribe(subscribe_topic, qos=1)

#     return client


# def send_and_receive(client, command_topic, msg, queue_timeout=30):
#     client.publish(command_topic, msg, qos=2)

#     client.loop_start()
#     while True:
#         data = data_queue.get(True, queue_timeout)
#         client.loop_stop()
#         return data

# def hivemq_communication(outgoing_message, subscribe_topic, publish_topic, queue_timeout=30):
#     client = get_paho_client(subscribe_topic)
#     return send_and_receive(client, publish_topic, outgoing_message, queue_timeout=queue_timeout)

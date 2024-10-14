# Description: Receive commands from HiveMQ and send dummy sensor data to HiveMQ

import json
from mqtt_as import MQTTClient, config
import asyncio
from netman import connectWiFi
try:
    import ussl
except:
    import ssl
import ntptime
from time import time, sleep
import sys
try;
    from uio import StringIO
except:
    from io import StringIO

# NOTE: This differs from the usual CLSLab:Light instructions, in that the file
# is now named `my_secrets.py` instead of `secrets.py`. Calling it secrets.py in
# a MicroPython context is fine, but since we're doing autograding in Python,
# this clashes with Python's stdlib secrets module. Overriding it can cause
# unexpected issues (e.g., breaking numpy and other imports)
from my_secrets import (
    HIVEMQ_HOST,
    HIVEMQ_PASSWORD,
    HIVEMQ_USERNAME,
    COURSE_ID,
    PASSWORD,
    SSID,
)

connectWiFi(SSID, PASSWORD, country="US")

# To validate certificates, a valid time is required
ntptime.timeout = 15  # type: ignore
ntptime.host = "time.google.com"
try:
    ntptime.settime()
except Exception as e:
    print(f"{e} with {ntptime.host}. Trying again after 10 seconds")
    sleep(10)
    try:
        ntptime.settime()
    except Exception as e:
        print(f"{e} with {ntptime.host}. Trying again with pool.ntp.org")
        sleep(10)
        ntptime.host = "pool.ntp.org"
        ntptime.settime()

print("Obtaining CA Certificate from file")
with open("hivemq-com-chain.der", "rb") as f:
    cacert = f.read()
f.close()

# Local configuration
config.update(
    {
        "ssid": SSID,
        "wifi_pw": PASSWORD,
        "server": HIVEMQ_HOST,
        "user": HIVEMQ_USERNAME,
        "password": HIVEMQ_PASSWORD,
        "ssl": True,
        "ssl_params": {
            "server_side": False,
            "key": None,
            "cert": None,
            "cert_reqs": ussl.CERT_REQUIRED,
            "cadata": cacert,
            "server_hostname": HIVEMQ_HOST,
        },
        "keepalive": 30,
    }
)


# Dummy function for running a color experiment
def run_color_experiment(R, G, B):
    """
    Run a color experiment with the specified RGB values.

    Parameters
    ----------
    R : int
        The red component of the color, between 0 and 255.
    G : int
        The green component of the color, between 0 and 255.
    B : int
        The blue component of the color, between 0 and 255.

    Returns
    -------
    dict
        A dictionary with the sensor data from the experiment.

    Examples
    --------
    >>> run_color_experiment(255, 0, 0)
    {'ch410': 25.5, 'ch440': 51.0, 'ch470': 76.5, 'ch510': 102.0, 'ch550': 127.5, 'ch583': 153.0, 'ch620': 229.5, 'ch670': 255.0} # noqa: E501
    """
    wavelengths = [410, 440, 470, 510, 550, 583, 620, 670]
    rw = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.9, 1.0]
    gw = [0.2, 0.4, 0.6, 0.8, 1.0, 0.8, 0.4, 0.2]
    bw = [0.9, 1.0, 0.8, 0.6, 0.4, 0.2, 0.1, 0.0]
    sensor_data = {
        f"ch{wavelength}": rw[i] * R + gw[i] * G + bw[i] * B
        for i, wavelength in enumerate(wavelengths)
    }
    return sensor_data


# MQTT Topics
command_topic = f"{COURSE_ID}/neopixel"
sensor_data_topic = f"{COURSE_ID}/as7341"


async def messages(client):  # Respond to incoming messages
    async for topic, msg, retained in client.queue:
        try:
            topic = topic.decode()
            msg = msg.decode()
            retained = str(retained)
            print((topic, msg, retained))

            if topic == command_topic:
                # TODO: Implement message handling logic to run the experiment
                # and publish a dictionary with the original payload dictionary
                # and the sensor data to the sensor data topic. The dictionary
                # should be of the form:
                # {
                #     "command": {"R": ..., "G": ..., "B": ...},
                #     "sensor_data": {"ch410": ..., "ch440": ..., ..., "ch670": ...},
                #     "experiment_id": "...",
                # }
                ...  # IMPLEMENT
        except Exception as e:
            with StringIO() as f:  # type: ignore
                sys.print_exception(e, f)  # type: ignore
                print(f.getvalue())  # type: ignore


async def up(client):  # Respond to connectivity being (re)established
    while True:
        await client.up.wait()  # Wait on an Event
        client.up.clear()
        await client.subscribe(command_topic, 1)  # renew subscriptions


async def main(client):
    await client.connect()
    for coroutine in (up, messages):
        asyncio.create_task(coroutine(client))

    start_time = time()
    # must have the while True loop to keep the program running
    while True:
        await asyncio.sleep(5)
        elapsed_time = round(time() - start_time)
        print(f"Elapsed: {elapsed_time}s")


config["queue_len"] = 5  # Use event interface with specified queue length
MQTTClient.DEBUG = True  # Optional: print diagnostic messages
client = MQTTClient(config)
try:
    asyncio.run(main(client))
finally:
    client.close()  # Prevent LmacRxBlk:1 errors

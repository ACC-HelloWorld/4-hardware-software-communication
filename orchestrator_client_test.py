import os
from time import time, sleep
import json
import subprocess
import warnings
import paho.mqtt.client as mqtt_client
from pprint import pformat
from pathlib import Path
import threading

username_key = "HIVEMQ_USERNAME"
password_key = "HIVEMQ_PASSWORD"
host_key = "HIVEMQ_HOST"
course_id_key = "COURSE_ID"

sensor_data_fname = "results.json"
payload_dict_fname = "payload_dicts.json"


def flatten_dict(d, parent_key="", sep="_"):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    flattened = dict(items)
    if len(items) != len(set(k for k, v in items)):
        raise ValueError("Overlapping keys encountered.")
    return flattened

def run_color_experiment(R, G, B):
    """Dummy function for receiving R, G, B values and returning sensor data."""
    wavelengths = [410, 440, 470, 510, 550, 583, 620, 670]
    rw = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.9, 1.0]
    gw = [0.2, 0.4, 0.6, 0.8, 1.0, 0.8, 0.4, 0.2]
    bw = [0.9, 1.0, 0.8, 0.6, 0.4, 0.2, 0.1, 0.0]
    sensor_data = {
        f"ch{wavelength}": rw[i] * R + gw[i] * G + bw[i] * B
        for i, wavelength in enumerate(wavelengths)
    }
    return sensor_data


def test_orchestrator_client():
    """Pretend to be the microcontroller"""

    # More of a check to the student than a test
    script_name = "orchestrator_client.py"
    script_content = open(script_name).read()

    if "..." in script_content:
        warnings.warn(
            f"Please complete the '...' sections in {script_name} and remove the '...' from each section"
        )

    course_id = os.getenv(course_id_key)
    assert (
        course_id is not None
    ), f"Please set the COURSE_ID environment variable per the README instructions."  # noqa: E501
    # three commands for three gemstone colors :)
    rgb_values = [
        {"R": 15, "G": 82, "B": 186},  # sapphire
        {"R": 155, "G": 17, "B": 30},  # ruby
        {"R": 80, "G": 200, "B": 120},  # emerald
    ]

    host = os.environ["HIVEMQ_HOST"]
    username = os.environ["HIVEMQ_USERNAME"]
    password = os.environ["HIVEMQ_PASSWORD"]
    course_id = os.environ["COURSE_ID"]

    command_topic = f"{course_id}/neopixel"
    sensor_data_topic = f"{course_id}/as7341"

    # Remove files if they exist
    for filename in [sensor_data_fname, payload_dict_fname]:
        file_path = Path(filename)
        file_path.unlink(missing_ok=True)

    connected_event = threading.Event()

    def on_connect(client, userdata, flags, rc):
        print(f"Connected with result code {rc}")
        client.subscribe(command_topic, qos=2)
        connected_event.set()

    # Create lists to store commands and sensor data
    received_payloads = []
    sent_payload_dicts = []

    # Create queue to store sensor data
    # payload_dict_queue = Queue()

    def on_message(client, userdata, message):
        topic = message.topic
        msg = message.payload.decode()

        print(f"Received message on topic {topic}: {msg}")

        if topic == command_topic:
            print("Topic matches command_topic")
            received_payload_dict = json.loads(msg)
            received_payloads.append(
                received_payload_dict
            )  # Store the received command
            cmd = received_payload_dict["command"]
            sensor_data = run_color_experiment(cmd["R"], cmd["G"], cmd["B"])

            # Join params and sensor_data into the payload
            payload_dict = {**received_payload_dict, "sensor_data": sensor_data}
            payload = json.dumps(payload_dict)

            # slight delay to allow real microcontroller to go first if
            # it's running at the same time
            sleep(1.0)

            client.publish(sensor_data_topic, payload, qos=2)
            sent_payload_dicts.append(payload_dict)  # Store the sent sensor data
            # payload_dict_queue.put(payload_dict)  # Add sensor data to the queue

    client = mqtt_client.Client()
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.on_message = on_message

    client.tls_set(tls_version=mqtt_client.ssl.PROTOCOL_TLS_CLIENT)
    client.connect(host, port=8883)
    client.loop_start()

    # Wait for the client to connect
    connected_event.wait(timeout=10.0)

    orchestrator_client_process = subprocess.Popen(
        ["python", script_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    sleep(5.0)

    try:
        # give the data some time to be sent
        start_time = time()
        timeout = 30  # seconds
        lower_limit = 15  # seconds, to ensure success even when microcontroller is running concurrently

        while (
            not os.path.exists(sensor_data_fname)
            and not os.path.exists(payload_dict_fname)
        ) or time() - start_time < lower_limit:
            sleep(1)
            if time() - start_time > timeout:
                raise TimeoutError(
                    f"{sensor_data_fname} and {payload_dict_fname} not found within {timeout} s. Number of commands received so far: {len(received_payloads)}"  # noqa: E501
                )

        if len(received_payloads) != len(rgb_values):
            raise TimeoutError(
                f"Expected to receive {len(rgb_values)} commands, but got {len(received_payloads)} commands"  # noqa: E501
            )  # noqa: E501

        client.loop_stop()

        def to_sorted_rounded_frozenset_list(dict_list):
            rounded_dict_list = []
            for d in dict_list:
                rounded_dict = {
                    k: round(v, 5) if isinstance(v, float) else v for k, v in d.items()
                }
                rounded_dict_list.append(rounded_dict)
            return sorted(frozenset(d.items()) for d in rounded_dict_list)

        # Check that the experiment_id key is present in all received commands and unique
        assert all(
            "experiment_id" in command for command in received_payloads
        ), f"Received commands {received_payloads} do not contain experiment_id key"  # noqa: E501
        assert len(
            set(command["experiment_id"] for command in received_payloads)
        ) == len(
            received_payloads
        ), f"Received commands {received_payloads} do not have unique experiment_id keys"  # noqa: E501

        with open(sensor_data_fname) as f:
            results_dicts = json.load(f)

        with open(payload_dict_fname) as f:
            payload_dicts_for_microcontroller = json.load(f)

        sent_rgb_values = [
            {k: v for k, v in payload_dict["command"].items() if k in ("R", "G", "B")}
            for payload_dict in payload_dicts_for_microcontroller
        ]

        # Check that rgb_values and sent_rgb_values match, regardless of order
        assert to_sorted_rounded_frozenset_list(
            rgb_values
        ) == to_sorted_rounded_frozenset_list(
            sent_rgb_values
        ), f"rgb_values {rgb_values} do not match sent_rgb_values {sent_rgb_values}"  # noqa: E501

        flat_received = [flatten_dict(d) for d in received_payloads]
        flat_sent = [flatten_dict(d) for d in payload_dicts_for_microcontroller]

        # Check that the original commands and received commands match,
        # regardless of order
        received_frozensets = to_sorted_rounded_frozenset_list(flat_received)
        sent_frozensets = to_sorted_rounded_frozenset_list(flat_sent)
        assert (
            received_frozensets == sent_frozensets
        ), f"Received commands do not match sent commands. Sent: \n{pformat(sent_frozensets)}\n Received: \n{pformat(received_frozensets)}\n"  # noqa: E501

        flat_results = [flatten_dict(d) for d in results_dicts]
        flat_sent_payloads = [flatten_dict(d) for d in sent_payload_dicts]

        # Convert the lists to sorted, rounded frozenset lists
        sent_payload_frozensets = to_sorted_rounded_frozenset_list(flat_sent_payloads)
        received_results_frozensets = to_sorted_rounded_frozenset_list(flat_results)

        # Check that the data received by the orchestrator matches the data sent
        # from here, regardless of order
        assert (
            sent_payload_frozensets == received_results_frozensets
        ), f"Received data do not match sent sensor data. Sent: \n{pformat(sent_payload_frozensets)}\n Received: \n{pformat(received_results_frozensets)}"
    except Exception as e:
        blinded_credentials = {
            username_key: username
            if len(username) < 4
            else username[:2] + "*" * (len(username) - 4) + username[-2:],
            password_key: "*" * len(password),
            host_key: host
            if len(host) < 4
            else host[:2] + "*" * (len(host) - 4) + host[-2:],
            course_id_key: course_id,
            "command_topic": command_topic,
            "sensor_data_topic": sensor_data_topic,
        }
        raise Exception(
            f"{e}. Please check {script_name} and refer back to the README. The following (blinded) credentials were used during this run: \n{pformat(blinded_credentials)}\n"  # noqa: E501
        ) from e
    finally:
        # Stop the orchestrator_client.py process

        print(f"Reading STDOUT and STDERR from {script_name} process")
        stdout, stderr = orchestrator_client_process.communicate()

        print("STDOUT:")
        print(stdout.decode())
        print("STDERR:")
        print(stderr.decode())

        print(f"TERMINATING {script_name} process")
        orchestrator_client_process.terminate()
        orchestrator_client_process.wait()


if __name__ == "__main__":
    test_orchestrator_client()

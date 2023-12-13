import os
import json
from communication import hivemq_communication
import numpy as np
import secrets
from queue import Empty
import warnings
from orchestrator_client_test import flatten_dict

username_key = "HIVEMQ_USERNAME"
password_key = "HIVEMQ_PASSWORD"
host_key = "HIVEMQ_HOST"
course_id_key = "COURSE_ID"


def test_send_and_receive():
    """act as the orchestrator"""

    # More of a check to the student than a test
    script_name = "microcontroller_client.py"
    script_content = open(script_name).read()

    if "..." in script_content:
        warnings.warn(
            f"Please complete the '...' sections in {script_name} and remove the '...' from each section"
        )

    COURSE_ID = os.environ[course_id_key]

    command = {"R": 48, "G": 213, "B": 200}  # turquoise :)

    publish_topic = f"{COURSE_ID}/neopixel"
    subscribe_topic = f"{COURSE_ID}/as7341"

    # Generate a random string of 8 characters (4 bytes)
    payload_dict = {"command": command, "experiment_id": secrets.token_hex(4)}
    payload = json.dumps(payload_dict)
    try:
        payload_data = hivemq_communication(payload, subscribe_topic, publish_topic)
        print(f"Received payload: {payload_data}")
    except (Empty, TimeoutError) as e:
        raise Empty(
            f"Did not receive any data on topic {subscribe_topic} after publishing to {publish_topic}. Refer to troubleshooting checklist in the README."  # noqa: E501
        ) from e

    payload_data_check = {
        **payload_dict,
        "sensor_data": {
            "ch410": 227.4,
            "ch440": 294.8,
            "ch470": 302.2,
            "ch510": 309.6,
            "ch550": 317.0,
            "ch583": 239.2,
            "ch620": 148.4,
            "ch670": 90.6,
        },
    }  # dummy turquoise sensor data

    flat_check = flatten_dict(payload_data_check)
    flat_data = flatten_dict(payload_data)

    # Check that at minimum the keys in the check are in the data
    assert set(flat_check.keys()).issubset(
        set(flat_data.keys())
    ), f"sensor_data_check: {payload_data_check} is not a subset of sensor_data: {payload_data}"  # noqa: E501

    experiment_id_data = payload_data["experiment_id"]
    experiment_id_check = payload_data_check["experiment_id"]

    assert (
        experiment_id_data == experiment_id_check
    ), f"experiment_id: {experiment_id_data} != {experiment_id_check}"

    # There might be slight differences due to differences in floating point
    # precision between the microcontroller and the orchestrator
    same_within_tol = True
    rtol = 1e-4
    for key in flat_check:
        if isinstance(flat_check[key], (int, float)) and isinstance(
            flat_data[key], (int, float)
        ):
            same_within_tol = np.isclose(flat_data[key], flat_check[key], rtol=rtol)
            if not same_within_tol:
                break
    assert (
        same_within_tol
    ), f"sensor_data: {payload_data} != {payload_data_check} within np.isclose relative tolerance {rtol}"  # noqa: E501


if __name__ == "__main__":
    test_send_and_receive()

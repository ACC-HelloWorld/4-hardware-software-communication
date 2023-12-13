import os
import json
from communication import hivemq_communication

username_key = "HIVEMQ_USERNAME"
password_key = "HIVEMQ_PASSWORD"
host_key = "HIVEMQ_HOST"
course_id_key = "COURSE_ID"


def test_env_vars_exist():
    for env_var in [username_key, password_key, host_key, course_id_key]:
        assert env_var in os.environ, f"Environment variable {env_var} does not exist."
    default_broker = "248cc294c37642359297f75b7b023374.s2.eu.hivemq.cloud"
    assert (
        default_broker not in os.environ[host_key]
    ), f"You must create your own HiveMQ instance rather than use the default, which is {default_broker}"


def test_basic_hivemq_communication():
    outgoing_message = "Test message"
    topic = "/test/topic"
    payload = json.dumps(outgoing_message)
    received_message = hivemq_communication(payload, topic, topic)

    assert (
        received_message == outgoing_message
    ), f"Received {received_message} instead of {outgoing_message} on topic {topic}. Check that your HiveMQ instance is set up and that the GitHub secrets are set correctly."  # noqa: E501


if __name__ == "__main__":
    test_env_vars_exist()
    test_basic_hivemq_communication()

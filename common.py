import json
import os
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
COMMAND_TOPIC = 'router.commands'
RESULT_TOPIC = 'router.results'
ROUTER_IP = '192.168.126.130'


def validate_command(command):
    if not isinstance(command, str) or not command.strip().lower().startswith('show '):
        raise ValueError('This demo accepts show commands only')
    command = command.strip()
    if len(command) > 200 or any(ord(c) < 32 or ord(c) == 127 for c in command):
        raise ValueError('Use one command, without control characters')
    return command


def ensure_topics():
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP})
    existing = admin.list_topics(timeout=15).topics
    missing = [NewTopic(t, num_partitions=1, replication_factor=1)
               for t in (COMMAND_TOPIC, RESULT_TOPIC) if t not in existing]
    for topic, future in admin.create_topics(missing).items() if missing else []:
        try:
            future.result()
        except KafkaException as exc:
            from confluent_kafka import KafkaError
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def new_producer():
    return Producer({'bootstrap.servers': BOOTSTRAP,
                     'enable.idempotence': True, 'message.timeout.ms': 15000})


def publish(producer, topic, key, event):
    errors = []
    delivered = []
    def callback(err, msg):
        errors.append(err) if err else delivered.append(True)
    producer.produce(topic, key=key, value=json.dumps(event).encode(), on_delivery=callback)
    remaining = producer.flush(20)
    if remaining or errors or not delivered:
        raise RuntimeError(f'Kafka delivery failed: {errors or "delivery timeout"}')

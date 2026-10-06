"""Consume command -> Cisco SSH -> publish result -> commit command offset."""
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
import paramiko
from confluent_kafka import Consumer, KafkaException
from common import (BOOTSTRAP, COMMAND_TOPIC, RESULT_TOPIC, ROUTER_IP,
                    ensure_topics, new_producer, publish, validate_command)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('worker')
ANSI = re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
INITIAL_PROMPT = re.compile(r'(?:^|\n)([^\s\r\n]+[>#])\s*$')


def read_until_prompt(channel, prompt=None, timeout=20):
    deadline = time.monotonic() + timeout
    output = ''
    while time.monotonic() < deadline:
        if channel.recv_ready():
            raw = channel.recv(65535)
            if not raw:
                raise ConnectionError('Router closed SSH channel')
            output += raw.decode('utf-8', errors='replace')
            if len(output) > 500000:
                raise RuntimeError('Router output exceeds demo size limit')
            clean = ANSI.sub('', output).replace('\r', '')
            pattern = (re.compile(r'(?:^|\n)' + re.escape(prompt) + r'\s*$')
                       if prompt else INITIAL_PROMPT)
            match = pattern.search(clean)
            if match:
                return clean, prompt or match.group(1)
        elif channel.closed or channel.exit_status_ready():
            raise ConnectionError('Router closed SSH channel before returning a prompt')
        else:
            time.sleep(0.05)
    raise TimeoutError('Timed out waiting for Cisco prompt')


def run_cisco(command):
    client = paramiko.SSHClient()
    # Lab setting: accept the GNS3 router host key. Use known_hosts for production.
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(hostname=ROUTER_IP, port=22,
                       username=os.getenv('ROUTER_USER', 'admin'),
                       password=os.environ['ROUTER_PASSWORD'],
                       look_for_keys=False, allow_agent=False,
                       timeout=10, auth_timeout=10, banner_timeout=10)
        # Cisco IOS often works best with an interactive shell rather than exec_command.
        channel = client.invoke_shell(width=200, height=1000)
        _, prompt = read_until_prompt(channel)
        channel.sendall('terminal length 0\n')
        setup, _ = read_until_prompt(channel, prompt)
        if any(marker in setup for marker in ('% Invalid', '% Error', '% Authorization')):
            raise RuntimeError('Unable to disable Cisco output pagination')
        channel.sendall(command + '\n')
        output, _ = read_until_prompt(channel, prompt)
        if any(marker in output for marker in ('% Invalid', '% Incomplete', '% Ambiguous',
                                               '% Authorization', '% Error')):
            raise RuntimeError(output.strip())
        return output.strip()
    finally:
        client.close()


def main():
    if not os.getenv('ROUTER_PASSWORD'):
        raise RuntimeError('Set ROUTER_PASSWORD before starting the worker')
    ensure_topics()
    producer = new_producer()
    consumer = Consumer({'bootstrap.servers': BOOTSTRAP,
                         'group.id': 'cisco-workers',
                         'auto.offset.reset': 'earliest',
                         'enable.auto.commit': False,
                         'enable.auto.offset.store': False,
                         'max.poll.interval.ms': 300000})
    consumer.subscribe([COMMAND_TOPIC])
    log.info('Listening on %s; router=%s', COMMAND_TOPIC, ROUTER_IP)
    try:
        while True:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            event = {}
            result = {'source': {'topic': msg.topic(), 'partition': msg.partition(),
                                 'offset': msg.offset()}}
            try:
                event = json.loads(msg.value())
                if not isinstance(event, dict):
                    raise ValueError('Event must be an object')
                if event.get('router_ip') != ROUTER_IP:
                    raise ValueError('Unexpected router IP')
                command = validate_command(event.get('command'))
                result.update(event_id=event.get('event_id'), router_ip=ROUTER_IP, command=command)
                result.update(status='success', output=run_cisco(command))
            except Exception as exc:
                result.update(status='failed', error=str(exc))
            result['completed_at'] = datetime.now(timezone.utc).isoformat()
            # If result publishing fails, exit WITHOUT committing the command.
            publish(producer, RESULT_TOPIC, ROUTER_IP, result)
            log.info('Result: %s', json.dumps(result))
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        log.info('Stopping worker')
    finally:
        consumer.close()

if __name__ == '__main__':
    main()

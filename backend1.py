"""HTTP backend: POST a command, return 202 after Kafka acknowledges it."""
from datetime import datetime, timezone
from uuid import uuid4
import threading
from flask import Flask, request, jsonify
from common import (ensure_topics, new_producer, publish, validate_command,
                    COMMAND_TOPIC, ROUTER_IP)

app = Flask(__name__)
producer = None
publish_lock = threading.Lock()

@app.post('/commands')
def submit_command():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return jsonify(error='Send a JSON object'), 400
    try:
        command = validate_command(body.get('command', 'show ip interface brief'))
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    event = {'event_id': str(uuid4()), 'router_ip': ROUTER_IP,
             'command': command, 'created_at': datetime.now(timezone.utc).isoformat()}
    try:
        with publish_lock:
            publish(producer, COMMAND_TOPIC, ROUTER_IP, event)
    except Exception:
        app.logger.exception('Unable to publish command')
        return jsonify(error='Kafka unavailable; command delivery is uncertain'), 503
    return jsonify(status='queued', topic=COMMAND_TOPIC, **event), 202

if __name__ == '__main__':
    ensure_topics()
    producer = new_producer()
    app.run(host='0.0.0.0', port=8001, debug=False)

# Router commands through Kafka

backend1 HTTP API -> router.commands -> worker -> Paramiko SSH -> Cisco router
worker -> router.results (output or error)

Target: 192.168.126.130, SSH port 22, username admin. Password is read from .env.
Topics are created automatically at startup when missing. Each command is a new
message in router.commands, not a new topic.

## Run with your existing Kafka containers

1. Start Kafka first:

```bash
cd ~/kafka-docker
docker compose up -d
```

2. Extract this project's folder into your Ubuntu home directory. Then:

```bash
cd ~/router-kafka-demo
cp .env.example .env
docker compose up -d --build
docker compose logs -f worker
```

The included .env.example sets ROUTER_PASSWORD=cisco for this lab. Do not commit .env.
This Compose file joins the existing external network kafka-docker_default and
uses kafka:29092. Check `docker network ls` and update its network name if your
Kafka Compose project uses a different name. It does not start another Kafka broker.

3. In a second Ubuntu terminal, submit a command:

```bash
curl -X POST http://localhost:8001/commands \
  -H 'Content-Type: application/json' \
  -d '{"command":"show ip interface brief"}'
```

HTTP 202 means Kafka acknowledged the message; it does not mean SSH execution
has finished. The returned event_id correlates the eventual result.
Send `{}` to generate the default show ip interface brief command.

4. Read results from Kafka:

```bash
cd ~/kafka-docker
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic router.results \
  --from-beginning
```

Or open Kafka UI -> local -> Topics -> router.results -> Messages.
Result status is success or failed. Output/error is in the same JSON event.

## Run directly on Ubuntu instead of Docker

```bash
cd ~/router-kafka-demo
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export ROUTER_PASSWORD=cisco
python worker.py
```

In a second terminal, activate the same venv and run `python backend1.py`.
These host processes use Ubuntu localhost:9092. Do not use Windows localhost:
your broker is running inside the Ubuntu VM and its external listener is currently
bound to Ubuntu loopback.

## Behavior and limits

- backend1.py exposes POST /commands; worker.py processes events sequentially.
- common.py creates topics and waits for acknowledged producer delivery.
- Only single-line show commands are accepted in this demonstration.
- The worker disables pagination with terminal length 0, then waits for the
  Cisco prompt after the command instead of using a fixed sleep to collect output.
- This does not enter enable mode. Use an account permitted to run your show
  command; privileged commands may fail when the router prompt is R1>.
- The worker publishes a failed result for SSH/validation errors and commits that
  event after publishing. Those failed commands are not automatically retried.
- If result publishing fails, the worker exits without committing the command.
  Restart it with `docker compose up -d worker` after fixing Kafka connectivity.
- A crash after SSH execution but before committing can repeat a show command.
  This is at-least-once processing; event IDs allow consumers to identify duplicates.
- Paramiko 3.5.1 is pinned for compatibility with older GNS3 IOS SSH algorithms.
  It does not read OpenSSH's ~/.ssh/config. If negotiation still fails, inspect
  `docker compose logs worker` for the actual unsupported algorithm.
- The lab worker accepts an unknown SSH host key. A production deployment should
  verify known host keys and use authenticated API/Kafka access.
- The HTTP API uses Flask's development server and binds port 8001 only on Ubuntu
  loopback through Docker. To access it from Windows in your lab, change the
  mapping to 8001:8001 and call http://<Ubuntu-IP>:8001/commands.

## Validation

Python syntax was checked locally. Live Kafka/SSH integration requires your
Ubuntu VM and GNS3 router; those were not accessible from the authoring environment.

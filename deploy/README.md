# deploy

Lab Compose stack for Hopper: `api`, `worker`, MongoDB replica set, RabbitMQ, nginx.

```bash
docker compose -f compose.yaml up -d
docker compose -f compose.yaml down
```

| File | Role |
| :--- | :--- |
| `compose.yaml` | Services and networks |
| `hopper.example.yaml` | Config template (`REPLACE_ME`) |
| `hopper.yaml` | Lab overlay (local only — do not treat as production secrets) |
| `go.Dockerfile` | One image, two commands |
| `nginx.conf` | Edge proxy |
| `rabbitmq.conf` / `rabbitmq-definitions.json` | Classic queues `jobs`, `jobs.delay.*`, `jobs.dlq` |

`backend` is `internal: true` on purpose (lab isolation). Point workers at reachable public targets when testing egress.

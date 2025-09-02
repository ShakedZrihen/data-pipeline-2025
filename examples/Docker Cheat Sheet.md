## Stop all containers

```bash
docker stop $(docker ps -a -q)
```

## restart service in docker compose

```bash
docker-compose restart shopping-chat
```
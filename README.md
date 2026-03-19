# RRR Backend MVP

FastAPI backend for the `Russian Road Rage` lobby and authoritative multiplayer MVP.

## Implemented endpoints

- `POST /api/v1/sessions/guest`
- `GET /api/v1/lobbies`
- `POST /api/v1/lobbies`
- `GET /api/v1/lobbies/{lobby_id}`
- `POST /api/v1/lobbies/{lobby_id}/join`
- `POST /api/v1/lobbies/{lobby_id}/leave`
- `PUT /api/v1/lobbies/{lobby_id}/car-config`
- `GET /api/v1/matches/{match_id}`
- `GET /api/v1/health`
- `GET /api/v1/ws?session_token=...`

## WebSocket messages

Client to server:

- `subscribe_lobby`
- `unsubscribe_lobby`
- `match_loaded`
- `player_input`
- `ping`

Server to client:

- `welcome`
- `lobby_snapshot`
- `lobby_player_joined`
- `lobby_player_left`
- `lobby_starting`
- `match_created`
- `match_started`
- `match_state`
- `player_disconnected`
- `error`

## Local run

```powershell
.\\.venv\\Scripts\\pip.exe install -r requirements.txt
.\\.venv\\Scripts\\uvicorn.exe app.main:app --reload --host 0.0.0.0 --port 8080
```

## Docker run

```bash
cp .env.example .env
docker compose up -d --build
```

## Tests

```powershell
.\\.venv\\Scripts\\pytest.exe
```

## Smoke test

```powershell
.\\.venv\\Scripts\\python.exe .\\scripts\\smoke_test.py --base-url http://127.0.0.1:8080
```

## Deploy

1. Copy [`deploy/.env.production.example`](/C:/Work/RRRBack/deploy/.env.production.example) to `.env` on the server.
2. Run `docker compose up -d --build`.
3. Add nginx config from [`deploy/nginx.rrr-demo.conf`](/C:/Work/RRRBack/deploy/nginx.rrr-demo.conf).
4. Issue a certificate with `certbot` after DNS is pointed to the server.
5. Use [`deploy/redeploy.sh`](/C:/Work/RRRBack/deploy/redeploy.sh) for pull-and-rebuild redeploys.

## GitHub Actions redeploy

Workflow file: [deploy.yml](/C:/Work/RRRBack/.github/workflows/deploy.yml)

Required repository secrets:

- `SSH_HOST`
- `SSH_USER`
- `SSH_PORT`
- `SSH_PRIVATE_KEY`

The workflow runs tests on every push to `main`, then connects to the server over SSH and executes:

```bash
cd /root/rrr-back
sh ./deploy/redeploy.sh
```

## MVP limitations

- Single instance only
- In-memory state only
- Simplified kinematic simulation
- No PostgreSQL, Redis, replay, or progression

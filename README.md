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
- `GET /api/v1/content/vehicles`
- `GET /api/v1/content/vehicles/{vehicle_id}`
- `GET /api/v1/content/vehicles/{vehicle_id}/offers`
- `GET /api/v1/content/bundles/{bundle_id}`
- `GET /api/v1/ws?session_token=...`
- `GET /admin`
- `GET /api/v1/admin/lobbies`
- `GET /api/v1/admin/lobbies/{lobby_id}`
- `GET /api/v1/admin/matches`
- `GET /api/v1/admin/matches/{match_id}`
- `POST /api/v1/admin/content/vehicles/publish`
- `POST /api/v1/admin/content/vehicles/{vehicle_id}/bundle`
- `POST /api/v1/admin/content/vehicles/{vehicle_id}/offers/sync`
- `GET /api/v1/admin/content/vehicles/{vehicle_id}/offers`
- `PUT /api/v1/admin/content/vehicles/{vehicle_id}/offers`
- `GET /api/v1/admin/ws?token=...`

## WebSocket messages

Client to server:

- `subscribe_lobby`
- `unsubscribe_lobby`
- `match_loaded`
- `player_input`
- `player_state` (legacy compatibility)
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

Admin WebSocket events:

- `admin_connected`
- `admin_lobbies_snapshot`
- `admin_lobby_updated`
- `admin_matches_snapshot`
- `admin_match_updated`
- `admin_match_state`
- `error`

## Observer panel

- URL: `/admin`
- Static frontend files: [`app/static/admin/index.html`](/C:/Work/RRRBack/app/static/admin/index.html), [`app/static/admin/app.js`](/C:/Work/RRRBack/app/static/admin/app.js), [`app/static/admin/styles.css`](/C:/Work/RRRBack/app/static/admin/styles.css)
- Admin auth: optional `ADMIN_TOKEN`; if set, pass it as `?token=...` to `/admin`, admin REST endpoints, and `/api/v1/admin/ws`

## Spawn contract

- `GET /api/v1/matches/{match_id}` now returns `players[]` with `spawn_point_id`, `spawn_position`, `spawn_rotation`, `car_config`, and `connection_state`
- `match_created` websocket message now includes the same `players[]` spawn assignment payload
- spawn assignment is server-authoritative and deterministic: players are ordered by `joined_at`, then mapped to configured spawn points for the selected `map_id`

## Vehicle content registry

- public catalog endpoint:
  - `GET /api/v1/content/vehicles`
- public latest manifest endpoint:
  - `GET /api/v1/content/vehicles/{vehicle_id}`
- admin publish endpoint:
  - `POST /api/v1/admin/content/vehicles/publish`
- auth:
  - uses the same optional `ADMIN_TOKEN` as the rest of the admin API
- storage:
  - manifests are persisted under `CONTENT_STORAGE_DIR`
  - latest manifests live in `vehicles/latest`
  - published history is archived in `vehicles/history/{vehicle_id}`

Current intent:

- Unity editor exports semantic vehicle manifests
- backend stores latest published manifest per vehicle
- publish rejects changed content with the same `content_version`
- diff info for added/removed domains and values is returned in the publish response

## Bundle storage

- admin upload endpoint:
  - `POST /api/v1/admin/content/vehicles/{vehicle_id}/bundle`
- public download endpoint:
  - `GET /api/v1/content/bundles/{bundle_id}`
- storage:
  - bundle binaries are stored under `CONTENT_STORAGE_DIR/bundles/files`
  - bundle metadata is stored under `CONTENT_STORAGE_DIR/bundles/meta`

Current intent:

- Unity editor builds one vehicle content bundle per vehicle
- backend stores the uploaded binary and exposes a stable download URL
- manifest publish can reference uploaded `bundle_id`, `bundle_hash`, and `bundle_url`

## Offer registry

- public offers endpoint:
  - `GET /api/v1/content/vehicles/{vehicle_id}/offers`
- admin sync endpoint:
  - `POST /api/v1/admin/content/vehicles/{vehicle_id}/offers/sync`
- admin list endpoint:
  - `GET /api/v1/admin/content/vehicles/{vehicle_id}/offers`
- admin update endpoint:
  - `PUT /api/v1/admin/content/vehicles/{vehicle_id}/offers`

Current intent:

- backend derives generic per-vehicle offers from published manifest `domain_id/value_id`
- new offers are generated automatically during sync
- removed options are marked `deprecated`
- existing prices are preserved across syncs
- public endpoint exposes only `published` offers

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

If Unity dedicated observer runs on the same host outside Docker, set `DIRECT_OBSERVER_URL=http://host.docker.internal:7777`.
If you want vehicle content publishing enabled with persistent storage, also set `CONTENT_STORAGE_DIR`.

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

For direct PurrNet dedicated observer integration, configure `DIRECT_OBSERVER_URL`, `DIRECT_OBSERVER_SECRET`, and `DIRECT_OBSERVER_REQUEST_TIMEOUT_SEC`. When backend runs in Docker on the same machine as Unity dedicated, use `http://host.docker.internal:7777`.

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
- `player_input` is the primary realtime contract
- When `simulation_service_url` is configured, backend pushes inputs into Unity dedicated simulation and broadcasts authoritative snapshots back to clients
- If the simulation service is unavailable, backend degrades to client-state fallback instead of dropping the match
- No PostgreSQL, Redis, replay, or progression

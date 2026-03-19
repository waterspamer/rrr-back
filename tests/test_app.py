from __future__ import annotations

import time

from fastapi.testclient import TestClient

from app.core.config import Settings
from app.main import create_app


def sample_car_config(name: str) -> dict[str, object]:
    return {
        "version": 1,
        "loadout_name": f"{name}_Loadout",
        "loadout_display_name": name,
        "body_set_option_index": 0,
        "engine_index": 0,
        "suspension_index": 0,
        "paint_index": 2,
        "body_set_name": "",
        "engine_name": f"{name}_Engine",
        "suspension_name": f"{name}_Suspension",
        "paint_name": "British Green",
        "has_paint": True,
        "paint": {"r": 0.1, "g": 0.25, "b": 0.14, "a": 1.0},
        "customizations": [{"selector_path": "Bumper/Front", "variant_name": "SetD"}],
    }


def build_client() -> TestClient:
    app = create_app(
        Settings(
            auto_start_countdown_sec=0,
            match_load_timeout_sec=1,
            match_tick_rate=10,
            match_broadcast_rate=10,
            docs_url=None,
            redoc_url=None,
        )
    )
    return TestClient(app)


def test_rest_lobby_lifecycle() -> None:
    with build_client() as client:
        session_1 = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_1001"}).json()
        session_2 = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_1002"}).json()

        create = client.post(
            "/api/v1/lobbies",
            headers={"Authorization": f"Bearer {session_1['session_token']}"},
            json={"name": "Downtown Drift", "map_id": "city_default", "max_players": 2, "car_config": sample_car_config("Cooper")},
        )
        assert create.status_code == 201
        lobby_id = create.json()["lobby_id"]

        listed = client.get("/api/v1/lobbies").json()
        assert listed["total"] == 1
        assert listed["items"][0]["lobby_id"] == lobby_id

        join = client.post(
            f"/api/v1/lobbies/{lobby_id}/join",
            headers={"Authorization": f"Bearer {session_2['session_token']}"},
            json={"car_config": sample_car_config("Mustang")},
        )
        assert join.status_code == 200

        lobby = client.get(f"/api/v1/lobbies/{lobby_id}").json()
        assert lobby["status"] == "waiting"
        assert lobby["current_players"] == 2


def test_websocket_match_flow() -> None:
    with build_client() as client:
        session_1 = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_2001"}).json()
        session_2 = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_2002"}).json()

        with client.websocket_connect(f"/api/v1/ws?session_token={session_1['session_token']}") as ws1, client.websocket_connect(
            f"/api/v1/ws?session_token={session_2['session_token']}"
        ) as ws2:
            assert ws1.receive_json()["type"] == "welcome"
            assert ws2.receive_json()["type"] == "welcome"

            create = client.post(
                "/api/v1/lobbies",
                headers={"Authorization": f"Bearer {session_1['session_token']}"},
                json={"name": "Downtown Drift", "map_id": "city_default", "max_players": 2, "car_config": sample_car_config("Cooper")},
            )
            lobby_id = create.json()["lobby_id"]

            ws1.send_json({"type": "subscribe_lobby", "lobby_id": lobby_id})
            ws2.send_json({"type": "subscribe_lobby", "lobby_id": lobby_id})
            assert ws1.receive_json()["type"] == "lobby_snapshot"
            assert ws2.receive_json()["type"] == "lobby_snapshot"

            join = client.post(
                f"/api/v1/lobbies/{lobby_id}/join",
                headers={"Authorization": f"Bearer {session_2['session_token']}"},
                json={"car_config": sample_car_config("Mustang")},
            )
            assert join.status_code == 200

            match_id = None
            seen_types: set[str] = set()
            deadline = time.time() + 5
            while time.time() < deadline and match_id is None:
                for ws in (ws1, ws2):
                    message = ws.receive_json()
                    seen_types.add(message["type"])
                    if message["type"] == "match_created":
                        match_id = message["match_id"]
                if match_id:
                    break
            assert "lobby_starting" in seen_types
            assert match_id is not None

            ws1.send_json({"type": "match_loaded", "match_id": match_id})
            ws2.send_json({"type": "match_loaded", "match_id": match_id})

            started = False
            deadline = time.time() + 5
            while time.time() < deadline and not started:
                for ws in (ws1, ws2):
                    message = ws.receive_json()
                    if message["type"] == "match_started":
                        started = True
                        break
            assert started

            ws1.send_json(
                {
                    "type": "player_input",
                    "match_id": match_id,
                    "seq": 1,
                    "client_time": int(time.time() * 1000),
                    "input": {"throttle": 1.0, "brake": 0.0, "steer": 0.2, "handbrake": False, "nitro": False},
                }
            )
            ws2.send_json(
                {
                    "type": "player_input",
                    "match_id": match_id,
                    "seq": 1,
                    "client_time": int(time.time() * 1000),
                    "input": {"throttle": 0.8, "brake": 0.0, "steer": -0.1, "handbrake": False, "nitro": True},
                }
            )

            got_match_state = False
            deadline = time.time() + 5
            while time.time() < deadline and not got_match_state:
                for ws in (ws1, ws2):
                    message = ws.receive_json()
                    if message["type"] == "match_state":
                        got_match_state = True
                        assert len(message["players"]) == 2
                        break
            assert got_match_state

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


def updated_car_config(name: str) -> dict[str, object]:
    config = sample_car_config(name)
    config["customizations"] = [
        {"selector_path": "Spoiler", "variant_name": "SetX"},
        {"selector_path": "Skirts", "variant_name": "SetY"},
    ]
    return config


def build_client(**overrides) -> TestClient:
    app = create_app(
        Settings(
            auto_start_countdown_sec=0,
            match_load_timeout_sec=1,
            match_tick_rate=10,
            match_broadcast_rate=10,
            docs_url=None,
            redoc_url=None,
            **overrides,
        )
    )
    return TestClient(app)


def build_client_with_admin(admin_token: str = "", **overrides) -> TestClient:
    app = create_app(
        Settings(
            auto_start_countdown_sec=0,
            match_load_timeout_sec=1,
            match_tick_rate=10,
            match_broadcast_rate=10,
            admin_token=admin_token,
            docs_url=None,
            redoc_url=None,
            **overrides,
        )
    )
    return TestClient(app)


def receive_until(ws, expected_types: set[str], max_messages: int = 32) -> dict[str, object]:
    last_message = None
    for _ in range(max_messages):
        last_message = ws.receive_json()
        if last_message["type"] in expected_types:
            return last_message
    raise AssertionError(f"Did not receive {expected_types}, last_message={last_message}")


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


def test_lobby_rejects_player_count_above_map_spawn_capacity() -> None:
    with build_client() as client:
        session = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_0999"}).json()
        response = client.post(
            "/api/v1/lobbies",
            headers={"Authorization": f"Bearer {session['session_token']}"},
            json={"name": "Too Many", "map_id": "duel_test", "max_players": 3, "car_config": sample_car_config("Cooper")},
        )

        assert response.status_code == 400
        assert response.json()["code"] == "INVALID_REQUEST"
        assert "supports at most 2 players" in response.json()["message"]


def test_waiting_lobby_expires_after_timeout() -> None:
    with build_client(lobby_ttl_seconds=1, maintenance_interval_sec=1) as client:
        session = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_TTL"}).json()

        with client.websocket_connect(f"/api/v1/ws?session_token={session['session_token']}") as ws:
            assert ws.receive_json()["type"] == "welcome"

            create = client.post(
                "/api/v1/lobbies",
                headers={"Authorization": f"Bearer {session['session_token']}"},
                json={"name": "TTL Lobby", "map_id": "city_default", "max_players": 2, "car_config": sample_car_config("Cooper")},
            )
            assert create.status_code == 201
            lobby_id = create.json()["lobby_id"]

            ws.send_json({"type": "subscribe_lobby", "lobby_id": lobby_id})
            assert ws.receive_json()["type"] == "lobby_snapshot"

            closed = receive_until(ws, {"lobby_closed"}, max_messages=64)
            assert closed["lobby_id"] == lobby_id
            assert closed["reason"] == "timeout"

        listed = client.get("/api/v1/lobbies").json()
        assert listed["items"] == []


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
            match_created_payload = None
            seen_types: set[str] = set()
            deadline = time.time() + 5
            while time.time() < deadline and match_id is None:
                for ws in (ws1, ws2):
                    message = ws.receive_json()
                    seen_types.add(message["type"])
                    if message["type"] == "match_created":
                        match_id = message["match_id"]
                        match_created_payload = message
                if match_id:
                    break
            assert "lobby_starting" in seen_types
            assert match_id is not None
            assert match_created_payload is not None
            assert len(match_created_payload["players"]) == 2
            assert len({player["spawn_point_id"] for player in match_created_payload["players"]}) == 2
            assert all(player["spawn_position"]["y"] == 0.5 for player in match_created_payload["players"])

            match_info = client.get(f"/api/v1/matches/{match_id}")
            assert match_info.status_code == 200
            assert len(match_info.json()["players"]) == 2
            assert {player["spawn_point_id"] for player in match_info.json()["players"]} == {
                player["spawn_point_id"] for player in match_created_payload["players"]
            }

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
                    "type": "player_state",
                    "match_id": match_id,
                    "seq": 1,
                    "client_time": int(time.time() * 1000),
                    "state": {
                        "position": {"x": 10.0, "y": 0.5, "z": 4.0},
                        "rotation": {"x": 0.0, "y": 18.0, "z": 0.0},
                        "velocity": {"x": 2.5, "y": 0.0, "z": 12.0},
                    },
                }
            )
            ws2.send_json(
                {
                    "type": "player_state",
                    "match_id": match_id,
                    "seq": 1,
                    "client_time": int(time.time() * 1000),
                    "state": {
                        "position": {"x": -12.0, "y": 0.5, "z": 7.0},
                        "rotation": {"x": 0.0, "y": -11.0, "z": 0.0},
                        "velocity": {"x": -3.0, "y": 0.0, "z": 9.0},
                    },
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
                        assert any(player["position"]["x"] == 10.0 for player in message["players"])
                        break
            assert got_match_state


def test_customizations_roundtrip_via_rest_and_realtime() -> None:
    with build_client() as client:
        session_1 = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_3001"}).json()
        session_2 = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_3002"}).json()

        with client.websocket_connect(f"/api/v1/ws?session_token={session_1['session_token']}") as ws1, client.websocket_connect(
            f"/api/v1/ws?session_token={session_2['session_token']}"
        ) as ws2:
            ws1.receive_json()
            ws2.receive_json()

            create = client.post(
                "/api/v1/lobbies",
                headers={"Authorization": f"Bearer {session_1['session_token']}"},
                json={"name": "Customization Test", "map_id": "city_default", "max_players": 2, "car_config": sample_car_config("Cooper")},
            )
            assert create.status_code == 201
            lobby_id = create.json()["lobby_id"]

            ws1.send_json({"type": "subscribe_lobby", "lobby_id": lobby_id})
            first_snapshot = ws1.receive_json()
            assert first_snapshot["type"] == "lobby_snapshot"
            assert first_snapshot["lobby"]["players"][0]["car_config"]["customizations"] == sample_car_config("Cooper")["customizations"]

            join = client.post(
                f"/api/v1/lobbies/{lobby_id}/join",
                headers={"Authorization": f"Bearer {session_2['session_token']}"},
                json={"car_config": sample_car_config("Mustang")},
            )
            assert join.status_code == 200

            detail = client.get(f"/api/v1/lobbies/{lobby_id}")
            assert detail.status_code == 200
            players = detail.json()["players"]
            assert players[0]["car_config"]["customizations"] == sample_car_config("Cooper")["customizations"]
            assert players[1]["car_config"]["customizations"] == sample_car_config("Mustang")["customizations"]

            update = client.put(
                f"/api/v1/lobbies/{lobby_id}/car-config",
                headers={"Authorization": f"Bearer {session_1['session_token']}"},
                json={"car_config": updated_car_config("Cooper")},
            )
            assert update.status_code == 200

            updated_detail = client.get(f"/api/v1/lobbies/{lobby_id}")
            assert updated_detail.status_code == 200
            assert updated_detail.json()["players"][0]["car_config"]["customizations"] == updated_car_config("Cooper")["customizations"]

            deadline = time.time() + 5
            latest_snapshot = None
            while time.time() < deadline:
                message = ws1.receive_json()
                if message["type"] == "lobby_snapshot":
                    latest_snapshot = message
                    if message["lobby"]["players"][0]["car_config"]["customizations"] == updated_car_config("Cooper")["customizations"]:
                        break
            assert latest_snapshot is not None
            assert latest_snapshot["lobby"]["players"][0]["car_config"]["customizations"] == updated_car_config("Cooper")["customizations"]


def test_invalid_customizations_contract_returns_400() -> None:
    with build_client() as client:
        session = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_4001"}).json()
        invalid_config = sample_car_config("Cooper")
        invalid_config["customizations"] = [{"selectorPath": "BumperF", "variantName": "SetA"}]

        response = client.post(
            "/api/v1/lobbies",
            headers={"Authorization": f"Bearer {session['session_token']}"},
            json={"name": "Invalid Customization", "map_id": "city_default", "max_players": 2, "car_config": invalid_config},
        )

        assert response.status_code == 400
        assert response.json()["code"] == "INVALID_REQUEST"


def test_admin_rest_and_panel_html() -> None:
    with build_client_with_admin(admin_token="secret-token") as client:
        panel = client.get("/admin")
        assert panel.status_code == 200
        assert "Observer Console" in panel.text

        unauthorized_response = client.get("/api/v1/admin/lobbies")
        assert unauthorized_response.status_code == 401
        assert unauthorized_response.json()["code"] == "UNAUTHORIZED"

        session = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_5001"}).json()
        create = client.post(
            "/api/v1/lobbies",
            headers={"Authorization": f"Bearer {session['session_token']}"},
            json={"name": "Admin Test Lobby", "map_id": "city_default", "max_players": 2, "car_config": sample_car_config("Cooper")},
        )
        assert create.status_code == 201
        lobby_id = create.json()["lobby_id"]

        admin_lobbies = client.get("/api/v1/admin/lobbies", params={"token": "secret-token"})
        assert admin_lobbies.status_code == 200
        items = admin_lobbies.json()["items"]
        assert len(items) == 1
        assert items[0]["lobby_id"] == lobby_id
        assert items[0]["players"][0]["loadout_display_name"] == "Cooper"
        assert items[0]["players"][0]["customizations"] == sample_car_config("Cooper")["customizations"]

        admin_lobby = client.get(f"/api/v1/admin/lobbies/{lobby_id}", params={"token": "secret-token"})
        assert admin_lobby.status_code == 200
        assert admin_lobby.json()["owner_player_id"] == session["player_id"]

        admin_matches = client.get("/api/v1/admin/matches", params={"token": "secret-token"})
        assert admin_matches.status_code == 200
        assert admin_matches.json()["items"] == []


def test_admin_websocket_receives_realtime_match_updates() -> None:
    with build_client_with_admin(admin_token="secret-token") as client:
        session_1 = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_6001"}).json()
        session_2 = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_6002"}).json()

        with client.websocket_connect("/api/v1/admin/ws?token=secret-token") as admin_ws:
            assert admin_ws.receive_json()["type"] == "admin_connected"
            assert admin_ws.receive_json()["type"] == "admin_lobbies_snapshot"
            assert admin_ws.receive_json()["type"] == "admin_matches_snapshot"

            with client.websocket_connect(f"/api/v1/ws?session_token={session_1['session_token']}") as ws1, client.websocket_connect(
                f"/api/v1/ws?session_token={session_2['session_token']}"
            ) as ws2:
                assert ws1.receive_json()["type"] == "welcome"
                assert ws2.receive_json()["type"] == "welcome"

                create = client.post(
                    "/api/v1/lobbies",
                    headers={"Authorization": f"Bearer {session_1['session_token']}"},
                    json={"name": "Observer Flow", "map_id": "city_default", "max_players": 2, "car_config": sample_car_config("Cooper")},
                )
                assert create.status_code == 201
                lobby_id = create.json()["lobby_id"]

                lobby_update = receive_until(admin_ws, {"admin_lobby_updated"})
                assert lobby_update["lobby"]["lobby_id"] == lobby_id
                assert lobby_update["lobby"]["status"] == "waiting"

                join = client.post(
                    f"/api/v1/lobbies/{lobby_id}/join",
                    headers={"Authorization": f"Bearer {session_2['session_token']}"},
                    json={"car_config": sample_car_config("Mustang")},
                )
                assert join.status_code == 200

                last_lobby_status = None
                match_update = None
                for _ in range(24):
                    message = admin_ws.receive_json()
                    if message["type"] == "admin_lobby_updated":
                        last_lobby_status = message["lobby"]["status"]
                    if message["type"] == "admin_match_updated":
                        match_update = message
                        break

                assert last_lobby_status in {"starting", "in_game"}
                assert match_update is not None
                match_id = match_update["match"]["match_id"]

                admin_match_detail = client.get(f"/api/v1/admin/matches/{match_id}", params={"token": "secret-token"})
                assert admin_match_detail.status_code == 200
                assert admin_match_detail.json()["status"] == "starting"
                assert len(admin_match_detail.json()["players"]) == 2
                assert all(player["spawn_point_id"] for player in admin_match_detail.json()["players"])

                ws1.send_json({"type": "match_loaded", "match_id": match_id})
                ws2.send_json({"type": "match_loaded", "match_id": match_id})

                started = False
                for _ in range(12):
                    message = receive_until(ws1, {"match_started", "match_state"})
                    if message["type"] == "match_started":
                        started = True
                        break
                if not started:
                    for _ in range(12):
                        message = receive_until(ws2, {"match_started", "match_state"})
                        if message["type"] == "match_started":
                            started = True
                            break
                assert started

                ws1.send_json(
                    {
                        "type": "player_state",
                        "match_id": match_id,
                        "seq": 1,
                        "client_time": int(time.time() * 1000),
                        "state": {
                            "position": {"x": 6.0, "y": 0.5, "z": 3.0},
                            "rotation": {"x": 0.0, "y": 24.0, "z": 0.0},
                            "velocity": {"x": 1.0, "y": 0.0, "z": 14.0},
                        },
                    }
                )
                ws2.send_json(
                    {
                        "type": "player_state",
                        "match_id": match_id,
                        "seq": 1,
                        "client_time": int(time.time() * 1000),
                        "state": {
                            "position": {"x": -4.0, "y": 0.5, "z": 8.0},
                            "rotation": {"x": 0.0, "y": -9.0, "z": 0.0},
                            "velocity": {"x": -2.0, "y": 0.0, "z": 10.0},
                        },
                    }
                )

                admin_state = receive_until(admin_ws, {"admin_match_state"}, max_messages=32)
                assert admin_state["match_id"] == match_id
                assert len(admin_state["players"]) == 2
                assert admin_state["server_tick"] >= 1
                assert all(player["connection_state"] == "in_game" for player in admin_state["players"])

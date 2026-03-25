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
        "handling_name": f"{name}_Handling",
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
                    "type": "player_input",
                    "match_id": match_id,
                    "seq": 1,
                    "client_time": int(time.time() * 1000),
                    "input": {
                        "throttle": 1.0,
                        "steer": 0.35,
                        "brake": False,
                        "handbrake": False,
                        "nitro": True,
                    },
                    "state": {
                        "position": {"x": 10.0, "y": 0.5, "z": 4.0},
                        "rotation": {"x": 0.0, "y": 18.0, "z": 0.0},
                        "velocity": {"x": 2.5, "y": 0.0, "z": 12.0},
                        "wheel_states": [
                            {"position": {"x": -0.8, "y": 0.1, "z": 1.2}, "rotation": {"x": 12.0, "y": 18.0, "z": 90.0}},
                            {"position": {"x": 0.8, "y": 0.12, "z": 1.2}, "rotation": {"x": 13.0, "y": 18.0, "z": 90.0}},
                            {"position": {"x": -0.8, "y": 0.08, "z": -1.2}, "rotation": {"x": 20.0, "y": 0.0, "z": 90.0}},
                            {"position": {"x": 0.8, "y": 0.1, "z": -1.2}, "rotation": {"x": 19.0, "y": 0.0, "z": 90.0}},
                        ],
                    },
                }
            )
            ws2.send_json(
                {
                    "type": "player_input",
                    "match_id": match_id,
                    "seq": 1,
                    "client_time": int(time.time() * 1000),
                    "input": {
                        "throttle": 0.8,
                        "steer": -0.2,
                        "brake": False,
                        "handbrake": False,
                        "nitro": False,
                    },
                    "state": {
                        "position": {"x": -12.0, "y": 0.5, "z": 7.0},
                        "rotation": {"x": 0.0, "y": -11.0, "z": 0.0},
                        "velocity": {"x": -3.0, "y": 0.0, "z": 9.0},
                        "wheel_states": [
                            {"position": {"x": -0.9, "y": 0.11, "z": 1.25}, "rotation": {"x": 8.0, "y": -11.0, "z": 90.0}},
                            {"position": {"x": 0.9, "y": 0.1, "z": 1.25}, "rotation": {"x": 9.0, "y": -11.0, "z": 90.0}},
                            {"position": {"x": -0.9, "y": 0.09, "z": -1.25}, "rotation": {"x": 16.0, "y": 0.0, "z": 90.0}},
                            {"position": {"x": 0.9, "y": 0.09, "z": -1.25}, "rotation": {"x": 17.0, "y": 0.0, "z": 90.0}},
                        ],
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
                        assert all("ack_input_seq" in player for player in message["players"])
                        assert all("input" in player for player in message["players"])
                        assert all("car_config" not in player for player in message["players"])
                        assert all(len(player["wheel_states"]) == 4 for player in message["players"])
                        break
            assert got_match_state


def test_start_solo_creates_match_with_idle_server_car() -> None:
    with build_client() as client:
        session = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_Solo"}).json()

        with client.websocket_connect(f"/api/v1/ws?session_token={session['session_token']}") as ws:
            assert ws.receive_json()["type"] == "welcome"

            create = client.post(
                "/api/v1/lobbies",
                headers={"Authorization": f"Bearer {session['session_token']}"},
                json={"name": "Solo Lobby", "map_id": "city_default", "max_players": 2, "car_config": sample_car_config("Cooper")},
            )
            assert create.status_code == 201
            lobby_id = create.json()["lobby_id"]

            ws.send_json({"type": "subscribe_lobby", "lobby_id": lobby_id})
            assert ws.receive_json()["type"] == "lobby_snapshot"

            start_solo = client.post(
                f"/api/v1/lobbies/{lobby_id}/start-solo",
                headers={"Authorization": f"Bearer {session['session_token']}"},
            )
            assert start_solo.status_code == 200
            solo_payload = start_solo.json()
            assert solo_payload["started"] is True
            assert solo_payload["match_id"].startswith("match_")
            assert solo_payload["server_player_id"].startswith("server_bot_")

            match_created = receive_until(ws, {"match_created"}, max_messages=16)
            assert match_created["match_id"] == solo_payload["match_id"]
            assert len(match_created["players"]) == 2
            assert sum(1 for player in match_created["players"] if player["is_server_controlled"]) == 1
            assert any(player["player_id"] == solo_payload["server_player_id"] for player in match_created["players"])

            match_info = client.get(f"/api/v1/matches/{solo_payload['match_id']}")
            assert match_info.status_code == 200
            assert len(match_info.json()["players"]) == 2
            assert sum(1 for player in match_info.json()["players"] if player["is_server_controlled"]) == 1

            ws.send_json({"type": "match_loaded", "match_id": solo_payload["match_id"]})
            started = receive_until(ws, {"match_started"}, max_messages=32)
            assert started["match_id"] == solo_payload["match_id"]


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
        assert "GameSettings" in panel.text

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
                        "type": "player_input",
                        "match_id": match_id,
                        "seq": 1,
                        "client_time": int(time.time() * 1000),
                        "input": {
                            "throttle": 1.0,
                            "steer": 0.5,
                            "brake": False,
                            "handbrake": False,
                            "nitro": False,
                        },
                        "state": {
                            "position": {"x": 6.0, "y": 0.5, "z": 3.0},
                            "rotation": {"x": 0.0, "y": 24.0, "z": 0.0},
                            "velocity": {"x": 1.0, "y": 0.0, "z": 14.0},
                        },
                    }
                )
                ws2.send_json(
                    {
                        "type": "player_input",
                        "match_id": match_id,
                        "seq": 1,
                        "client_time": int(time.time() * 1000),
                        "input": {
                            "throttle": 0.7,
                            "steer": -0.3,
                            "brake": False,
                            "handbrake": False,
                            "nitro": True,
                        },
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
                assert "telemetry" in admin_state
                assert "player_input_in" in admin_state["telemetry"]
                assert "match_state_out" in admin_state["telemetry"]

                damage_payload = {
                    "type": "damage_state",
                    "match_id": match_id,
                    "player_id": session_1["player_id"],
                    "revision": 1,
                    "width": 8,
                    "height": 16,
                    "map_b64": "AAAAAAAAAAAAAAAAAAAAAA==",
                    "world_point": {"x": 1.0, "y": 0.5, "z": 2.0},
                    "world_normal": {"x": 0.0, "y": 1.0, "z": 0.0},
                }
                ws1.send_json(damage_payload)

                damage_event = receive_until(ws2, {"damage_state"}, max_messages=16)
                assert damage_event["player_id"] == session_1["player_id"]
                assert damage_event["revision"] == 1

                admin_match_detail = client.get(f"/api/v1/admin/matches/{match_id}", params={"token": "secret-token"})
                assert admin_match_detail.status_code == 200
                detail_payload = admin_match_detail.json()
                assert detail_payload["telemetry"]["damage_state_in"]["total_messages"] >= 1
                assert any(player["damage_revision"] == 1 for player in detail_payload["players"])


def test_admin_rest_includes_direct_purrnet_observer_match() -> None:
    with build_client_with_admin(admin_token="secret-token", direct_observer_url="http://observer.local") as client:
        runtime = client.app.state.runtime

        async def fake_list_rooms() -> list[dict[str, object]]:
            return [
                {
                    "room_id": "purrnet-live",
                    "match_id": "purrnet-live",
                    "source": "purrnet_direct",
                    "map_id": "city_default",
                    "status": "running",
                    "room_http_url": "http://observer.local/api/v1/rooms/purrnet-live",
                    "room_ws_url": "",
                    "room_token": "observer-token",
                    "scene_name": "Game",
                    "player_count": 2,
                    "created_at": "2026-03-25T10:00:00Z",
                    "tick_rate": 30,
                    "server_tick": 144,
                    "manual_tick": False,
                }
            ]

        async def fake_get_room(match_id: str) -> dict[str, object] | None:
            if match_id != "purrnet-live":
                return None
            return (await fake_list_rooms())[0]

        async def fake_get_snapshot(match_id: str) -> dict[str, object] | None:
            if match_id != "purrnet-live":
                return None
            return {
                "room_id": "purrnet-live",
                "match_id": "purrnet-live",
                "source": "purrnet_direct",
                "map_id": "city_default",
                "status": "running",
                "server_tick": 144,
                "server_time": int(time.time() * 1000),
                "players": [
                    {
                        "player_id": "player_live_1",
                        "player_name": "player_live_1",
                        "connection_state": "in_game",
                        "is_server_controlled": False,
                        "authority_order": 0,
                        "spawn_point_id": "purr_slot_0",
                        "spawn_position": {"x": 0.0, "y": 0.5, "z": 0.0},
                        "spawn_rotation": {"x": 0.0, "y": 0.0, "z": 0.0},
                        "ack_input_seq": 0,
                        "client_time": 0,
                        "server_received_time": 0,
                        "input": {"throttle": 0.92, "steer": 0.15, "brake": False, "handbrake": False, "nitro": True},
                        "position": {"x": 12.2, "y": 0.5, "z": -7.8},
                        "rotation": {"x": 0.0, "y": 32.0, "z": 0.0},
                        "velocity": {"x": 1.0, "y": 0.0, "z": 18.0},
                        "angular_velocity": {"x": 0.0, "y": 0.3, "z": 0.0},
                        "car_config": sample_car_config("Mustang"),
                        "debug": {
                            "resolved_car_config_name": "Mustang_PlayerCar",
                            "grounded_wheels": 4,
                            "wheel_count": 4,
                            "current_gear": 3,
                            "current_rpm": 4200.0,
                            "motor_torque": 680.0,
                            "speed_kph": 65.4,
                            "tracked_queued": False,
                            "tracked_spawned": True,
                        },
                    }
                ],
                "damage_states": [
                    {
                        "player_id": "player_live_1",
                        "revision": 2,
                        "width": 8,
                        "height": 16,
                        "map_b64": "AAAAAAAAAAAAAAAAAAAAAA==",
                    }
                ],
                "collisions": [
                    {
                        "sequence": 3,
                        "server_time": int(time.time() * 1000),
                        "primary_player_id": "player_live_1",
                        "secondary_player_id": "bot_live_1",
                        "world_point": {"x": 1.0, "y": 0.5, "z": 2.0},
                        "world_normal": {"x": 0.0, "y": 1.0, "z": 0.0},
                        "relative_velocity": {"x": 0.0, "y": 0.0, "z": 9.0},
                        "impulse_vector": {"x": 0.0, "y": 0.0, "z": 4.0},
                        "impulse_magnitude": 4.0,
                    }
                ],
                "observer": {
                    "source": "purrnet_direct",
                    "mode": "Server",
                    "scene_name": "Game",
                    "started_at_utc": "2026-03-25T10:00:00Z",
                    "uptime_sec": 12.5,
                    "network": {"server_state": "Connected", "client_state": "Disconnected", "tick_rate": 30, "local_tick": 144},
                    "prediction": {"has_prediction_manager": True, "prediction_spawned": True, "hierarchy_ready": True},
                    "spawner": {"solo_bot_target": 1, "tracked_bot_players": 1, "queued_players": 0, "spawned_players": 2},
                    "counts": {"active_player_cars": 1, "tracked_players": 2},
                    "tracked_players": [
                        {
                            "player_id": "player_live_1",
                            "is_bot": False,
                            "queued": False,
                            "spawned": True,
                            "spawn_slot": 0,
                            "spawn_point_id": "purr_slot_0",
                            "spawn_position": {"x": 0.0, "y": 0.5, "z": 0.0},
                            "spawn_rotation": {"x": 0.0, "y": 0.0, "z": 0.0},
                            "car_config": sample_car_config("Mustang"),
                        },
                        {
                            "player_id": "bot_live_1",
                            "is_bot": True,
                            "queued": True,
                            "spawned": False,
                            "spawn_slot": 1,
                            "spawn_point_id": "purr_slot_1",
                            "spawn_position": {"x": 4.5, "y": 0.5, "z": 0.0},
                            "spawn_rotation": {"x": 0.0, "y": 0.0, "z": 0.0},
                            "last_spawn_failure_reason": "player_not_loaded_in_scene",
                            "car_config": sample_car_config("Cooper"),
                        },
                    ],
                },
            }

        runtime.direct_observer.list_rooms = fake_list_rooms
        runtime.direct_observer.get_room = fake_get_room
        runtime.direct_observer.get_snapshot = fake_get_snapshot

        matches = client.get("/api/v1/admin/matches", params={"token": "secret-token"})
        assert matches.status_code == 200
        payload = matches.json()
        assert len(payload["items"]) == 1
        assert payload["items"][0]["match_id"] == "purrnet-live"
        assert payload["items"][0]["source"] == "purrnet_direct"
        assert payload["items"][0]["debug_summary"]["bot_target"] == 1

        detail = client.get("/api/v1/admin/matches/purrnet-live", params={"token": "secret-token"})
        assert detail.status_code == 200
        detail_payload = detail.json()
        assert detail_payload["source"] == "purrnet_direct"
        assert detail_payload["status"] == "running"
        assert detail_payload["tick_rate"] == 30
        assert len(detail_payload["players"]) == 2
        active_player = next(player for player in detail_payload["players"] if player["player_id"] == "player_live_1")
        queued_bot = next(player for player in detail_payload["players"] if player["player_id"] == "bot_live_1")
        assert active_player["position"]["x"] == 12.2
        assert active_player["car_config"]["loadout_name"] == "Mustang_Loadout"
        assert active_player["damage_revision"] == 2
        assert queued_bot["connection_state"] == "queued"
        assert queued_bot["debug"]["last_spawn_failure_reason"] == "player_not_loaded_in_scene"
        assert detail_payload["telemetry"]["spawner"]["solo_bot_target"] == 1
        assert detail_payload["recent_collisions"][0]["primary_player_id"] == "player_live_1"


def test_admin_global_game_settings_proxy_for_direct_purrnet_observer() -> None:
    with build_client_with_admin(admin_token="secret-token", direct_observer_url="http://observer.local") as client:
        runtime = client.app.state.runtime
        captured_update: dict[str, object] = {}

        damage_fields = {
            "obstacle_tag": "Obstacle",
            "impulse_to_color": 0.17,
            "max_color_step": 0.45,
            "impulse_to_radius": 0.55,
            "impulse_from_speed_factor": 0.12,
            "max_radius_cells": 6,
            "min_speed_for_damage_kmh": 8.0,
            "max_speed_for_damage_kmh": 90.0,
            "min_damage_scale": 0.3,
            "glancing_damage_scale": 0.65,
            "impact_alignment_power": 1.1,
            "speed_radius_boost": 0.4,
            "compute_deform_amplitude": 0.22,
            "compute_deform_direction": 0.38,
            "compute_deform_sin_frequency": 3.2,
            "compute_deform_sin_strength": 0.14,
            "compute_yield_threshold": 0.08,
            "compute_hardening": 0.11,
            "compute_max_deform": 0.44,
            "compute_two_level_damage": True,
            "compute_coarse_radius": 4,
            "compute_coarse_weight": 0.6,
            "compute_coarse_boost": 0.2,
            "compute_coarse_deform_meters": 0.09,
        }

        async def fake_get_global_damage_config() -> dict[str, object] | None:
            return {
                "version": 1,
                "revision": 4,
                "updated_at_unix_ms": 1711459200000,
                "source": "car_config",
                **damage_fields,
            }

        async def fake_update_global_damage_config(payload: dict[str, object]) -> dict[str, object] | None:
            captured_update["payload"] = payload
            return {
                "version": 1,
                "revision": 5,
                "updated_at_unix_ms": 1711459200123,
                "source": "observer_admin",
                **payload,
            }

        runtime.direct_observer.get_global_damage_config = fake_get_global_damage_config
        runtime.direct_observer.update_global_damage_config = fake_update_global_damage_config

        response = client.get("/api/v1/admin/game-settings", params={"token": "secret-token"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["scope"] == "global"
        assert payload["source"] == "purrnet_direct"
        assert payload["note"] is None
        assert len(payload["sections"]) == 1
        assert payload["sections"][0]["section_id"] == "damage"
        assert payload["sections"][0]["meta"]["revision"] == 4
        assert payload["sections"][0]["fields"]["obstacle_tag"] == "Obstacle"

        update = client.put(
            "/api/v1/admin/game-settings",
            params={"token": "secret-token"},
            json={
                "sections": [
                    {
                        "section_id": "damage",
                        "fields": {**damage_fields, "impulse_to_color": 0.25},
                    }
                ]
            },
        )
        assert update.status_code == 200
        update_payload = update.json()
        assert captured_update["payload"]["impulse_to_color"] == 0.25
        assert update_payload["sections"][0]["meta"]["revision"] == 5
        assert update_payload["sections"][0]["fields"]["impulse_to_color"] == 0.25


def test_admin_global_game_settings_falls_back_to_room_damage_config() -> None:
    with build_client_with_admin(admin_token="secret-token", direct_observer_url="http://observer.local") as client:
        runtime = client.app.state.runtime
        captured_update: dict[str, object] = {}

        async def fake_get_global_damage_config() -> dict[str, object] | None:
            raise RuntimeError("global endpoint missing")

        async def fake_update_global_damage_config(payload: dict[str, object]) -> dict[str, object] | None:
            raise RuntimeError("global endpoint missing")

        async def fake_list_rooms() -> list[dict[str, object]]:
            return [{"room_id": "room_live_1", "match_id": "room_live_1"}]

        async def fake_get_damage_config(match_id: str) -> dict[str, object] | None:
            assert match_id == "room_live_1"
            return {
                "version": 1,
                "revision": 8,
                "updated_at_unix_ms": 1711459200999,
                "source": "room_fallback",
                "obstacle_tag": "Obstacle",
                "impulse_to_color": 0.12,
            }

        async def fake_update_damage_config(match_id: str, payload: dict[str, object]) -> dict[str, object] | None:
            assert match_id == "room_live_1"
            captured_update["match_id"] = match_id
            captured_update["payload"] = payload
            return {
                "version": 1,
                "revision": 9,
                "updated_at_unix_ms": 1711459201999,
                "source": "room_fallback",
                **payload,
            }

        runtime.direct_observer.get_global_damage_config = fake_get_global_damage_config
        runtime.direct_observer.update_global_damage_config = fake_update_global_damage_config
        runtime.direct_observer.list_rooms = fake_list_rooms
        runtime.direct_observer.get_damage_config = fake_get_damage_config
        runtime.direct_observer.update_damage_config = fake_update_damage_config

        response = client.get("/api/v1/admin/game-settings", params={"token": "secret-token"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["scope"] == "global"
        assert payload["source"] == "purrnet_direct"
        assert payload["note"] == "Global settings are currently proxied through active room room_live_1."
        assert payload["sections"][0]["fields"]["impulse_to_color"] == 0.12

        update = client.put(
            "/api/v1/admin/game-settings",
            params={"token": "secret-token"},
            json={
                "sections": [
                    {
                        "section_id": "damage",
                        "fields": {
                            "obstacle_tag": "Obstacle",
                            "impulse_to_color": 0.21,
                        },
                    }
                ]
            },
        )
        assert update.status_code == 200
        update_payload = update.json()
        assert captured_update["match_id"] == "room_live_1"
        assert captured_update["payload"]["impulse_to_color"] == 0.21
        assert update_payload["note"] == "Global settings were applied through active room room_live_1."
        assert update_payload["sections"][0]["meta"]["revision"] == 9


def test_authoritative_simulation_snapshot_overrides_client_state() -> None:
    with build_client(simulation_service_url="http://simulation.local") as client:
        runtime = client.app.state.runtime
        captured_batches: list[dict[str, object]] = []

        async def fake_reserve_room(payload: dict[str, object]) -> dict[str, object]:
            return {
                "room_id": payload["match_id"],
                "status": "simulating",
                "room_http_url": f"http://simulation.local/api/v1/rooms/{payload['match_id']}",
                "room_ws_url": "",
                "room_token": "room-token",
            }

        async def fake_apply_inputs(match_id: str, payload: dict[str, object]) -> dict[str, object]:
            captured_batches.append(payload)
            return {
                "match_id": match_id,
                "status": "simulating",
                "accepted_players": len(payload.get("players", [])),
                "server_tick": 1,
            }

        async def fake_get_snapshot(match_id: str) -> dict[str, object]:
            async with runtime.lock:
                match = runtime.matches_by_id[match_id]
                players = list(match.players.values())
            return {
                "room_id": match_id,
                "match_id": match_id,
                "status": "simulating",
                "server_tick": 3,
                "server_time": int(time.time() * 1000),
                "players": [
                    {
                        "player_id": players[0].player_id,
                        "ack_input_seq": players[0].last_input_seq,
                        "client_time": players[0].client_time_ms,
                        "server_received_time": players[0].server_received_time_ms,
                        "input": {
                            "throttle": players[0].throttle,
                            "steer": players[0].steer,
                            "brake": players[0].brake,
                            "handbrake": players[0].handbrake,
                            "nitro": players[0].nitro,
                        },
                        "position": {"x": 55.0, "y": 0.5, "z": 11.0},
                        "rotation": {"x": 0.0, "y": 22.0, "z": 0.0},
                        "velocity": {"x": 0.0, "y": 0.0, "z": 18.0},
                        "angular_velocity": {"x": 0.0, "y": 0.4, "z": 0.0},
                        "wheel_states": [
                            {"position": {"x": -0.8, "y": 0.1, "z": 1.2}, "rotation": {"x": 12.0, "y": 18.0, "z": 90.0}},
                            {"position": {"x": 0.8, "y": 0.12, "z": 1.2}, "rotation": {"x": 13.0, "y": 18.0, "z": 90.0}},
                            {"position": {"x": -0.8, "y": 0.08, "z": -1.2}, "rotation": {"x": 20.0, "y": 0.0, "z": 90.0}},
                            {"position": {"x": 0.8, "y": 0.1, "z": -1.2}, "rotation": {"x": 19.0, "y": 0.0, "z": 90.0}},
                        ],
                    },
                    {
                        "player_id": players[1].player_id,
                        "ack_input_seq": players[1].last_input_seq,
                        "client_time": players[1].client_time_ms,
                        "server_received_time": players[1].server_received_time_ms,
                        "input": {
                            "throttle": players[1].throttle,
                            "steer": players[1].steer,
                            "brake": players[1].brake,
                            "handbrake": players[1].handbrake,
                            "nitro": players[1].nitro,
                        },
                        "position": {"x": -44.0, "y": 0.5, "z": 7.5},
                        "rotation": {"x": 0.0, "y": -13.0, "z": 0.0},
                        "velocity": {"x": -1.5, "y": 0.0, "z": 12.0},
                        "angular_velocity": {"x": 0.0, "y": -0.3, "z": 0.0},
                        "wheel_states": [
                            {"position": {"x": -0.9, "y": 0.11, "z": 1.25}, "rotation": {"x": 8.0, "y": -11.0, "z": 90.0}},
                            {"position": {"x": 0.9, "y": 0.1, "z": 1.25}, "rotation": {"x": 9.0, "y": -11.0, "z": 90.0}},
                            {"position": {"x": -0.9, "y": 0.09, "z": -1.25}, "rotation": {"x": 16.0, "y": 0.0, "z": 90.0}},
                            {"position": {"x": 0.9, "y": 0.09, "z": -1.25}, "rotation": {"x": 17.0, "y": 0.0, "z": 90.0}},
                        ],
                    },
                ],
            }

        async def fake_release_room(match_id: str) -> bool:
            return True

        runtime.simulation_service.reserve_room = fake_reserve_room
        runtime.simulation_service.apply_inputs = fake_apply_inputs
        runtime.simulation_service.get_snapshot = fake_get_snapshot
        runtime.simulation_service.release_room = fake_release_room

        session_1 = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_7001"}).json()
        session_2 = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_7002"}).json()

        with client.websocket_connect(f"/api/v1/ws?session_token={session_1['session_token']}") as ws1, client.websocket_connect(
            f"/api/v1/ws?session_token={session_2['session_token']}"
        ) as ws2:
            ws1.receive_json()
            ws2.receive_json()

            create = client.post(
                "/api/v1/lobbies",
                headers={"Authorization": f"Bearer {session_1['session_token']}"},
                json={"name": "Authoritative Room", "map_id": "city_default", "max_players": 2, "car_config": sample_car_config("Cooper")},
            )
            assert create.status_code == 201
            lobby_id = create.json()["lobby_id"]

            ws1.send_json({"type": "subscribe_lobby", "lobby_id": lobby_id})
            ws2.send_json({"type": "subscribe_lobby", "lobby_id": lobby_id})
            ws1.receive_json()
            ws2.receive_json()

            join = client.post(
                f"/api/v1/lobbies/{lobby_id}/join",
                headers={"Authorization": f"Bearer {session_2['session_token']}"},
                json={"car_config": sample_car_config("Mustang")},
            )
            assert join.status_code == 200

            match_created = receive_until(ws1, {"match_created"}, max_messages=16)
            match_id = match_created["match_id"]
            assert match_created["room_status"] == "simulating"

            receive_until(ws2, {"match_created"}, max_messages=16)

            ws1.send_json({"type": "match_loaded", "match_id": match_id})
            ws2.send_json({"type": "match_loaded", "match_id": match_id})
            receive_until(ws1, {"match_started", "match_state"}, max_messages=16)
            receive_until(ws2, {"match_started", "match_state"}, max_messages=16)

            ws1.send_json(
                {
                    "type": "player_input",
                    "match_id": match_id,
                    "seq": 1,
                    "client_time": int(time.time() * 1000),
                    "input": {"throttle": 1.0, "steer": 0.25, "brake": False, "handbrake": False, "nitro": True},
                    "state": {"position": {"x": 999.0, "y": 0.5, "z": 999.0}},
                }
            )
            ws2.send_json(
                {
                    "type": "player_input",
                    "match_id": match_id,
                    "seq": 1,
                    "client_time": int(time.time() * 1000),
                    "input": {"throttle": 0.7, "steer": -0.3, "brake": False, "handbrake": False, "nitro": False},
                    "state": {"position": {"x": -999.0, "y": 0.5, "z": -999.0}},
                }
            )

            snapshot_message = receive_until(ws1, {"match_state"}, max_messages=32)
            assert captured_batches
            assert snapshot_message["server_tick"] >= 3
            authoritative_positions = {player["player_id"]: player["position"]["x"] for player in snapshot_message["players"]}
            assert authoritative_positions[session_1["player_id"]] == 55.0
            assert authoritative_positions[session_2["player_id"]] == -44.0
            assert authoritative_positions[session_1["player_id"]] != 999.0
            assert authoritative_positions[session_2["player_id"]] != -999.0


def test_abandoned_match_finishes_and_releases_simulation_room() -> None:
    with build_client(
        simulation_service_url="http://simulation.local",
        disconnect_timeout_sec=1,
        match_abandon_timeout_sec=1,
    ) as client:
        runtime = client.app.state.runtime
        released_match_ids: list[str] = []

        async def fake_reserve_room(payload: dict[str, object]) -> dict[str, object]:
            return {
                "room_id": payload["match_id"],
                "status": "simulating",
                "room_http_url": f"http://simulation.local/api/v1/rooms/{payload['match_id']}",
                "room_ws_url": "",
                "room_token": "room-token",
            }

        async def fake_apply_inputs(match_id: str, payload: dict[str, object]) -> dict[str, object]:
            return {
                "match_id": match_id,
                "status": "simulating",
                "accepted_players": len(payload.get("players", [])),
                "server_tick": 1,
            }

        async def fake_get_snapshot(match_id: str) -> dict[str, object]:
            async with runtime.lock:
                match = runtime.matches_by_id[match_id]
                players = list(match.players.values())
            return {
                "room_id": match_id,
                "match_id": match_id,
                "status": "simulating",
                "server_tick": max(1, len(released_match_ids) + 1),
                "server_time": int(time.time() * 1000),
                "players": [
                    {
                        "player_id": player.player_id,
                        "ack_input_seq": player.last_input_seq,
                        "client_time": player.client_time_ms,
                        "server_received_time": player.server_received_time_ms,
                        "input": {
                            "throttle": player.throttle,
                            "steer": player.steer,
                            "brake": player.brake,
                            "handbrake": player.handbrake,
                            "nitro": player.nitro,
                        },
                        "position": player.position.as_dict(),
                        "rotation": player.rotation.as_dict(),
                        "velocity": player.velocity.as_dict(),
                        "angular_velocity": player.angular_velocity.as_dict(),
                        "wheel_states": [],
                    }
                    for player in players
                ],
            }

        async def fake_release_room(match_id: str) -> bool:
            released_match_ids.append(match_id)
            return True

        runtime.simulation_service.reserve_room = fake_reserve_room
        runtime.simulation_service.apply_inputs = fake_apply_inputs
        runtime.simulation_service.get_snapshot = fake_get_snapshot
        runtime.simulation_service.release_room = fake_release_room

        session_1 = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_8001"}).json()
        session_2 = client.post("/api/v1/sessions/guest", json={"player_name": "Guest_8002"}).json()

        with client.websocket_connect(f"/api/v1/ws?session_token={session_1['session_token']}") as ws1, client.websocket_connect(
            f"/api/v1/ws?session_token={session_2['session_token']}"
        ) as ws2:
            ws1.receive_json()
            ws2.receive_json()

            create = client.post(
                "/api/v1/lobbies",
                headers={"Authorization": f"Bearer {session_1['session_token']}"},
                json={"name": "Abandon Match", "map_id": "city_default", "max_players": 2, "car_config": sample_car_config("Cooper")},
            )
            assert create.status_code == 201
            lobby_id = create.json()["lobby_id"]

            ws1.send_json({"type": "subscribe_lobby", "lobby_id": lobby_id})
            ws2.send_json({"type": "subscribe_lobby", "lobby_id": lobby_id})
            ws1.receive_json()
            ws2.receive_json()

            join = client.post(
                f"/api/v1/lobbies/{lobby_id}/join",
                headers={"Authorization": f"Bearer {session_2['session_token']}"},
                json={"car_config": sample_car_config("Mustang")},
            )
            assert join.status_code == 200

            match_created = receive_until(ws1, {"match_created"}, max_messages=16)
            match_id = match_created["match_id"]
            receive_until(ws2, {"match_created"}, max_messages=16)

            ws1.send_json({"type": "match_loaded", "match_id": match_id})
            ws2.send_json({"type": "match_loaded", "match_id": match_id})
            receive_until(ws1, {"match_started", "match_state"}, max_messages=16)
            receive_until(ws2, {"match_started", "match_state"}, max_messages=16)

        deadline = time.time() + 3
        match_response = None
        while time.time() < deadline:
            match_response = client.get(f"/api/v1/matches/{match_id}")
            if match_response.status_code == 404:
                break
            time.sleep(0.1)

        assert match_response is not None
        assert match_response.status_code == 404
        assert match_id in released_match_ids

        lobby_response = client.get(f"/api/v1/lobbies/{lobby_id}")
        assert lobby_response.status_code == 404

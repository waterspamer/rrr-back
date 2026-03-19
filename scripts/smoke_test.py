from __future__ import annotations

import argparse
import asyncio
import json
import time
from typing import Any

import httpx
import websockets


def sample_car_config(name: str) -> dict[str, Any]:
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


async def recv_until(ws: websockets.ClientConnection, expected: set[str], timeout: float = 10) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.monotonic())
        payload = json.loads(raw)
        if payload.get("type") in expected:
            return payload
    raise TimeoutError(f"Timed out waiting for {expected}")


async def recv_matching_snapshot(
    ws: websockets.ClientConnection,
    player_id: str,
    customizations: list[dict[str, Any]],
    timeout: float = 10,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.monotonic())
        payload = json.loads(raw)
        if payload.get("type") != "lobby_snapshot":
            continue
        players = payload["lobby"]["players"]
        for player in players:
            if player["player_id"] == player_id and player["car_config"]["customizations"] == customizations:
                return payload
    raise TimeoutError("Timed out waiting for matching lobby_snapshot")


async def recv_admin_match_state(
    ws: websockets.ClientConnection,
    match_id: str,
    timeout: float = 10,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.monotonic())
        payload = json.loads(raw)
        if payload.get("type") == "admin_match_state" and payload.get("match_id") == match_id:
            return payload
    raise TimeoutError("Timed out waiting for admin_match_state")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True, help="Example: https://rrr-demo.tonforspeed.space")
    parser.add_argument("--admin-token", default="", help="Optional admin token for admin endpoints and websocket")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    ws_url = base_url.replace("https://", "wss://").replace("http://", "ws://") + "/api/v1/ws"
    admin_ws_url = base_url.replace("https://", "wss://").replace("http://", "ws://") + "/api/v1/admin/ws"

    async with httpx.AsyncClient(base_url=base_url, timeout=10) as client:
        session_1 = (
            await client.post("/api/v1/sessions/guest", json={"player_name": "Guest_3001"})
        ).json()
        session_2 = (
            await client.post("/api/v1/sessions/guest", json={"player_name": "Guest_3002"})
        ).json()

        admin_query = f"?token={args.admin_token}" if args.admin_token else ""
        async with websockets.connect(f"{admin_ws_url}{admin_query}") as admin_ws, websockets.connect(
            f"{ws_url}?session_token={session_1['session_token']}"
        ) as ws1, websockets.connect(f"{ws_url}?session_token={session_2['session_token']}") as ws2:
            await recv_until(admin_ws, {"admin_connected"})
            await recv_until(admin_ws, {"admin_lobbies_snapshot"})
            await recv_until(admin_ws, {"admin_matches_snapshot"})
            await recv_until(ws1, {"welcome"})
            await recv_until(ws2, {"welcome"})

            create_resp = await client.post(
                "/api/v1/lobbies",
                headers={"Authorization": f"Bearer {session_1['session_token']}"},
                json={
                    "name": "Smoke Drift",
                    "map_id": "city_default",
                    "max_players": 2,
                    "car_config": sample_car_config("Cooper"),
                },
            )
            create_resp.raise_for_status()
            lobby_id = create_resp.json()["lobby_id"]

            admin_params = {"token": args.admin_token} if args.admin_token else {}
            admin_lobbies_resp = await client.get("/api/v1/admin/lobbies", params=admin_params)
            admin_lobbies_resp.raise_for_status()
            assert admin_lobbies_resp.json()["items"][0]["lobby_id"] == lobby_id

            await ws1.send(json.dumps({"type": "subscribe_lobby", "lobby_id": lobby_id}))
            await ws2.send(json.dumps({"type": "subscribe_lobby", "lobby_id": lobby_id}))
            await recv_until(ws1, {"lobby_snapshot"})
            await recv_until(ws2, {"lobby_snapshot"})

            join_resp = await client.post(
                f"/api/v1/lobbies/{lobby_id}/join",
                headers={"Authorization": f"Bearer {session_2['session_token']}"},
                json={"car_config": sample_car_config("Mustang")},
            )
            join_resp.raise_for_status()

            lobby_detail = await client.get(f"/api/v1/lobbies/{lobby_id}")
            lobby_detail.raise_for_status()
            players = lobby_detail.json()["players"]
            assert players[0]["car_config"]["customizations"] == sample_car_config("Cooper")["customizations"]
            assert players[1]["car_config"]["customizations"] == sample_car_config("Mustang")["customizations"]

            updated_config = sample_car_config("Cooper")
            updated_config["customizations"] = [
                {"selector_path": "Spoiler", "variant_name": "SetX"},
                {"selector_path": "Skirts", "variant_name": "SetY"},
            ]
            update_resp = await client.put(
                f"/api/v1/lobbies/{lobby_id}/car-config",
                headers={"Authorization": f"Bearer {session_1['session_token']}"},
                json={"car_config": updated_config},
            )
            update_resp.raise_for_status()
            await recv_matching_snapshot(ws1, session_1["player_id"], updated_config["customizations"])

            match_created_1 = await recv_until(ws1, {"match_created"})
            match_created_2 = await recv_until(ws2, {"match_created"})
            match_id = match_created_1["match_id"]
            assert match_id == match_created_2["match_id"]
            assert len(match_created_1["players"]) == 2
            assert len({player["spawn_point_id"] for player in match_created_1["players"]}) == 2

            match_info_resp = await client.get(f"/api/v1/matches/{match_id}")
            match_info_resp.raise_for_status()
            match_info = match_info_resp.json()
            assert len(match_info["players"]) == 2
            assert {player["spawn_point_id"] for player in match_info["players"]} == {
                player["spawn_point_id"] for player in match_created_1["players"]
            }

            await ws1.send(json.dumps({"type": "match_loaded", "match_id": match_id}))
            await ws2.send(json.dumps({"type": "match_loaded", "match_id": match_id}))
            await recv_until(ws1, {"match_started"})
            await recv_until(ws2, {"match_started"})

            await ws1.send(
                json.dumps(
                    {
                        "type": "player_input",
                        "match_id": match_id,
                        "seq": 1,
                        "client_time": int(time.time() * 1000),
                        "input": {"throttle": 1.0, "brake": 0.0, "steer": 0.2, "handbrake": False, "nitro": False},
                    }
                )
            )
            await ws2.send(
                json.dumps(
                    {
                        "type": "player_input",
                        "match_id": match_id,
                        "seq": 1,
                        "client_time": int(time.time() * 1000),
                        "input": {"throttle": 0.8, "brake": 0.0, "steer": -0.1, "handbrake": False, "nitro": True},
                    }
                )
            )

            state = await recv_until(ws1, {"match_state"})
            admin_state = await recv_admin_match_state(admin_ws, match_id)
            admin_match_resp = await client.get(f"/api/v1/admin/matches/{match_id}", params=admin_params)
            admin_match_resp.raise_for_status()
            admin_match = admin_match_resp.json()

            print(
                json.dumps(
                    {
                        "status": "ok",
                        "match_id": match_id,
                        "players": len(state["players"]),
                        "spawn_points": [player["spawn_point_id"] for player in match_info["players"]],
                        "admin_players": len(admin_state["players"]),
                        "admin_match_status": admin_match["status"],
                    },
                    indent=2,
                )
            )


if __name__ == "__main__":
    asyncio.run(main())

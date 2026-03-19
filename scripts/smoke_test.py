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


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True, help="Example: https://rrr-demo.tonforspeed.space")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    ws_url = base_url.replace("https://", "wss://").replace("http://", "ws://") + "/api/v1/ws"

    async with httpx.AsyncClient(base_url=base_url, timeout=10) as client:
        session_1 = (
            await client.post("/api/v1/sessions/guest", json={"player_name": "Guest_3001"})
        ).json()
        session_2 = (
            await client.post("/api/v1/sessions/guest", json={"player_name": "Guest_3002"})
        ).json()

        async with websockets.connect(f"{ws_url}?session_token={session_1['session_token']}") as ws1, websockets.connect(
            f"{ws_url}?session_token={session_2['session_token']}"
        ) as ws2:
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
            print(json.dumps({"status": "ok", "match_id": match_id, "players": len(state["players"])}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

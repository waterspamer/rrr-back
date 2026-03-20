(function () {
    const state = {
        token: "",
        lobbies: [],
        matches: [],
        selectedLobbyId: null,
        selectedMatchId: null,
        selectedLobby: null,
        selectedMatch: null,
        socket: null,
        reconnectTimer: null,
        manualClose: false,
    };

    const elements = {
        tokenInput: document.getElementById("tokenInput"),
        applyTokenButton: document.getElementById("applyTokenButton"),
        refreshButton: document.getElementById("refreshButton"),
        wsDot: document.getElementById("wsDot"),
        wsStatus: document.getElementById("wsStatus"),
        statusMessage: document.getElementById("statusMessage"),
        lobbyCount: document.getElementById("lobbyCount"),
        matchCount: document.getElementById("matchCount"),
        lobbyBadge: document.getElementById("lobbyBadge"),
        matchBadge: document.getElementById("matchBadge"),
        lobbiesList: document.getElementById("lobbiesList"),
        matchesList: document.getElementById("matchesList"),
        selectedLobbyLabel: document.getElementById("selectedLobbyLabel"),
        selectedLobbyMeta: document.getElementById("selectedLobbyMeta"),
        selectedMatchLabel: document.getElementById("selectedMatchLabel"),
        selectedMatchMeta: document.getElementById("selectedMatchMeta"),
        serverTickLabel: document.getElementById("serverTickLabel"),
        serverTickMeta: document.getElementById("serverTickMeta"),
        lobbyStatusPill: document.getElementById("lobbyStatusPill"),
        matchStatusPill: document.getElementById("matchStatusPill"),
        lobbyDetail: document.getElementById("lobbyDetail"),
        matchSummary: document.getElementById("matchSummary"),
        playersTable: document.getElementById("playersTable"),
        rawSnapshot: document.getElementById("rawSnapshot"),
        canvas: document.getElementById("matchCanvas"),
    };

    function init() {
        const url = new URL(window.location.href);
        state.token = url.searchParams.get("token") || "";
        elements.tokenInput.value = state.token;

        elements.applyTokenButton.addEventListener("click", applyToken);
        elements.refreshButton.addEventListener("click", refreshAll);
        window.addEventListener("beforeunload", closeSocket);

        refreshAll().finally(connectSocket);
    }

    function applyToken() {
        state.token = elements.tokenInput.value.trim();
        const url = new URL(window.location.href);
        if (state.token) {
            url.searchParams.set("token", state.token);
        } else {
            url.searchParams.delete("token");
        }
        window.history.replaceState({}, "", url);
        closeSocket();
        refreshAll().finally(connectSocket);
    }

    async function refreshAll() {
        setStatus("syncing", "Refreshing admin snapshots");
        try {
            const [lobbies, matches] = await Promise.all([
                apiGet("/api/v1/admin/lobbies"),
                apiGet("/api/v1/admin/matches"),
            ]);
            state.lobbies = Array.isArray(lobbies.items) ? lobbies.items : [];
            state.matches = Array.isArray(matches.items) ? matches.items : [];
            syncSelectedLobbyFromList();
            syncSelectedMatchFromList();

            await Promise.all([loadSelectedLobby(), loadSelectedMatch()]);
            render();
            setStatus(state.socket && state.socket.readyState === WebSocket.OPEN ? "connected" : "disconnected", "Snapshot synced");
        } catch (error) {
            renderError(error);
        }
    }

    async function loadSelectedLobby() {
        if (!state.selectedLobbyId) {
            state.selectedLobby = null;
            return;
        }
        try {
            state.selectedLobby = await apiGet(`/api/v1/admin/lobbies/${state.selectedLobbyId}`);
        } catch (error) {
            state.selectedLobby = null;
            renderError(error);
        }
    }

    async function loadSelectedMatch() {
        if (!state.selectedMatchId) {
            state.selectedMatch = null;
            return;
        }
        try {
            state.selectedMatch = await apiGet(`/api/v1/admin/matches/${state.selectedMatchId}`);
        } catch (error) {
            state.selectedMatch = null;
            renderError(error);
        }
    }

    async function apiGet(path) {
        const url = new URL(path, window.location.origin);
        if (state.token) {
            url.searchParams.set("token", state.token);
        }
        const response = await fetch(url.toString(), { headers: { Accept: "application/json" } });
        if (!response.ok) {
            let message = `${response.status} ${response.statusText}`;
            try {
                const payload = await response.json();
                message = payload.message || payload.detail?.message || message;
            } catch (error) {
                void error;
            }
            throw new Error(message);
        }
        return response.json();
    }

    function connectSocket() {
        closeSocket();
        const url = new URL(window.location.origin.replace("http", "ws") + "/api/v1/admin/ws");
        if (state.token) {
            url.searchParams.set("token", state.token);
        }
        state.manualClose = false;
        state.socket = new WebSocket(url.toString());
        setStatus("disconnected", "Connecting observer websocket");

        state.socket.addEventListener("open", function () {
            setStatus("connected", "Admin websocket connected");
        });

        state.socket.addEventListener("message", function (event) {
            const payload = JSON.parse(event.data);
            handleSocketMessage(payload);
        });

        state.socket.addEventListener("close", function () {
            if (state.manualClose) {
                state.manualClose = false;
                return;
            }
            setStatus("disconnected", "Observer websocket disconnected, retrying");
            state.reconnectTimer = window.setTimeout(connectSocket, 1500);
        });

        state.socket.addEventListener("error", function () {
            setStatus("disconnected", "Observer websocket error");
        });
    }

    function closeSocket() {
        if (state.reconnectTimer) {
            window.clearTimeout(state.reconnectTimer);
            state.reconnectTimer = null;
        }
        if (state.socket) {
            const socket = state.socket;
            state.manualClose = true;
            state.socket = null;
            socket.close();
        }
    }

    function handleSocketMessage(payload) {
        switch (payload.type) {
            case "admin_connected":
                setStatus("connected", "Observer websocket connected");
                break;
            case "admin_lobbies_snapshot":
                state.lobbies = Array.isArray(payload.items) ? payload.items : [];
                syncSelectedLobbyFromList();
                break;
            case "admin_lobby_updated":
                upsertById(state.lobbies, payload.lobby, "lobby_id");
                if (state.selectedLobbyId === payload.lobby.lobby_id) {
                    state.selectedLobby = payload.lobby;
                }
                syncSelectedLobbyFromList();
                break;
            case "admin_matches_snapshot":
                state.matches = Array.isArray(payload.items) ? payload.items : [];
                syncSelectedMatchFromList();
                break;
            case "admin_match_updated":
                upsertById(state.matches, toMatchSummary(payload.match), "match_id");
                if (state.selectedMatchId === payload.match.match_id) {
                    state.selectedMatch = payload.match;
                }
                syncSelectedMatchFromList();
                break;
            case "admin_match_state":
                upsertById(state.matches, {
                    match_id: payload.match_id,
                    server_tick: payload.server_tick,
                }, "match_id");
                if (state.selectedMatchId === payload.match_id) {
                    const previous = state.selectedMatch || {};
                    state.selectedMatch = {
                        ...previous,
                        match_id: payload.match_id,
                        server_tick: payload.server_tick,
                        players: payload.players || [],
                        raw_snapshot: payload,
                    };
                }
                syncSelectedMatchFromList();
                break;
            case "error":
                renderError(new Error(payload.message || "Admin websocket error"));
                break;
            default:
                break;
        }
        render();
    }

    function upsertById(items, nextItem, idField) {
        if (!nextItem || !nextItem[idField]) {
            return;
        }
        const index = items.findIndex(function (item) {
            return item[idField] === nextItem[idField];
        });
        if (index === -1) {
            items.unshift(nextItem);
        } else {
            items[index] = { ...items[index], ...nextItem };
        }
    }

    function toMatchSummary(match) {
        return {
            match_id: match.match_id,
            lobby_id: match.lobby_id,
            status: match.status,
            map_id: match.map_id,
            player_count: Array.isArray(match.players) ? match.players.length : 0,
            server_tick: match.server_tick || 0,
        };
    }

    function syncSelectedLobbyFromList() {
        if (!state.lobbies.length) {
            state.selectedLobbyId = null;
            state.selectedLobby = null;
            return;
        }

        if (!state.selectedLobbyId || !state.lobbies.some(function (lobby) {
            return lobby.lobby_id === state.selectedLobbyId;
        })) {
            state.selectedLobbyId = state.lobbies[0].lobby_id;
        }

        const fromList = state.lobbies.find(function (lobby) {
            return lobby.lobby_id === state.selectedLobbyId;
        });
        state.selectedLobby = fromList ? { ...(state.selectedLobby || {}), ...fromList } : null;
    }

    function syncSelectedMatchFromList() {
        if (!state.matches.length) {
            state.selectedMatchId = null;
            state.selectedMatch = null;
            return;
        }

        if (!state.selectedMatchId || !state.matches.some(function (match) {
            return match.match_id === state.selectedMatchId;
        })) {
            state.selectedMatchId = state.matches[0].match_id;
        }

        const summary = state.matches.find(function (match) {
            return match.match_id === state.selectedMatchId;
        });
        state.selectedMatch = summary ? { ...summary, ...(state.selectedMatch || {}) } : null;
    }

    function selectLobby(lobbyId) {
        state.selectedLobbyId = lobbyId;
        syncSelectedLobbyFromList();
        render();
        loadSelectedLobby().then(render).catch(renderError);
    }

    function selectMatch(matchId) {
        state.selectedMatchId = matchId;
        syncSelectedMatchFromList();
        render();
        loadSelectedMatch().then(render).catch(renderError);
    }

    function render() {
        renderLists();
        renderLobbyDetail();
        renderMatchDetail();
    }

    function renderLists() {
        elements.lobbyCount.textContent = String(state.lobbies.length);
        elements.matchCount.textContent = String(state.matches.length);
        elements.lobbyBadge.textContent = `${state.lobbies.length} active`;
        elements.matchBadge.textContent = `${state.matches.length} active`;

        renderList(elements.lobbiesList, state.lobbies, state.selectedLobbyId, "lobby_id", function (lobby) {
            selectLobby(lobby.lobby_id);
        }, formatLobbyCard);

        renderList(elements.matchesList, state.matches, state.selectedMatchId, "match_id", function (match) {
            selectMatch(match.match_id);
        }, formatMatchCard);
    }

    function renderList(container, items, activeId, idField, onClick, formatter) {
        if (!items.length) {
            container.className = "stack empty-state";
            container.textContent = "No active entities";
            return;
        }
        container.className = "stack";
        container.replaceChildren();
        items.forEach(function (item) {
            const button = document.createElement("button");
            button.type = "button";
            button.className = `item-card${item[idField] === activeId ? " active" : ""}`;
            button.innerHTML = formatter(item);
            button.addEventListener("click", function () {
                onClick(item);
            });
            container.appendChild(button);
        });
    }

    function renderLobbyDetail() {
        const lobby = state.selectedLobby;
        elements.selectedLobbyLabel.textContent = lobby ? lobby.lobby_id : "none";
        elements.selectedLobbyMeta.textContent = lobby ? `${lobby.name} on ${lobby.map_id}` : "pick a lobby from the list";
        renderPill(elements.lobbyStatusPill, lobby ? lobby.status : "idle");

        if (!lobby) {
            elements.lobbyDetail.className = "empty-state";
            elements.lobbyDetail.textContent = "Select a lobby to inspect players and car configs.";
            return;
        }

        const wrapper = document.createElement("div");
        wrapper.className = "lobby-players";

        const summary = document.createElement("div");
        summary.className = "match-summary";
        summary.innerHTML = [
            `<div class="small">owner</div><strong>${escapeHtml(lobby.owner_player_id)}</strong>`,
            `<div class="small">slots</div><strong>${lobby.current_players}/${lobby.max_players}</strong>`,
            `<div class="small">match</div><strong>${escapeHtml(lobby.match_id || "none")}</strong>`,
        ].join(" ");
        wrapper.appendChild(summary);

        (lobby.players || []).forEach(function (player) {
            const card = document.createElement("article");
            card.className = "player-card";
            const customizations = (player.customizations || []).map(function (item) {
                return `${item.selector_path}:${item.variant_name}`;
            }).join(", ") || "none";
            card.innerHTML = `
                <div class="item-title">
                    <span>${escapeHtml(player.player_name)}</span>
                    <span class="badge">${escapeHtml(player.connection_state)}</span>
                </div>
                <p class="card-meta">${escapeHtml(player.player_id)}</p>
                <div class="player-grid">
                    <div>
                        <div class="small">loadout</div>
                        <strong>${escapeHtml(player.loadout_display_name || "n/a")}</strong>
                    </div>
                    <div>
                        <div class="small">paint</div>
                        <strong>${escapeHtml(player.paint_name || "n/a")}</strong>
                    </div>
                    <div>
                        <div class="small">joined_at</div>
                        <strong>${escapeHtml(player.joined_at)}</strong>
                    </div>
                    <div>
                        <div class="small">customizations</div>
                        <code>${escapeHtml(customizations)}</code>
                    </div>
                </div>
            `;
            wrapper.appendChild(card);
        });

        elements.lobbyDetail.className = "";
        elements.lobbyDetail.replaceChildren(wrapper);
    }

    function renderMatchDetail() {
        const match = state.selectedMatch;
        elements.selectedMatchLabel.textContent = match ? match.match_id : "none";
        elements.selectedMatchMeta.textContent = match ? `${match.status || "unknown"} on ${match.map_id || "n/a"}` : "pick a match from the list";
        elements.serverTickLabel.textContent = String(match ? match.server_tick || 0 : 0);
        elements.serverTickMeta.textContent = match ? `lobby ${match.lobby_id || "n/a"}` : "updates on every admin_match_state";
        renderPill(elements.matchStatusPill, match ? match.status : "idle");

        if (!match) {
            elements.matchSummary.className = "match-summary empty-state";
            elements.matchSummary.textContent = "Select a match to inspect.";
            elements.playersTable.replaceChildren();
            elements.rawSnapshot.textContent = "{}";
            drawMap([]);
            return;
        }

        elements.matchSummary.className = "match-summary";
        elements.matchSummary.innerHTML = `
            <div class="small">match_id</div><strong>${escapeHtml(match.match_id)}</strong>
            <div class="small">map / tick_rate</div><strong>${escapeHtml(match.map_id || "n/a")} / ${escapeHtml(String(match.tick_rate || "n/a"))}</strong>
            <div class="small">players</div><strong>${escapeHtml(String((match.players || []).length))}</strong>
        `;

        elements.playersTable.replaceChildren();
        (match.players || []).forEach(function (player) {
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>${escapeHtml(player.player_id)}</td>
                <td>${escapeHtml(player.player_name || "n/a")}</td>
                <td>${formatNumber(player.position && player.position.x)}</td>
                <td>${formatNumber(player.position && player.position.y)}</td>
                <td>${formatNumber(player.position && player.position.z)}</td>
                <td>${formatNumber(player.rotation && player.rotation.y)}</td>
                <td>${formatNumber(player.speed)}</td>
                <td>${escapeHtml(player.connection_state || "unknown")}</td>
            `;
            elements.playersTable.appendChild(tr);
        });

        elements.rawSnapshot.textContent = JSON.stringify(match.raw_snapshot || match, null, 2);
        drawMap(match.players || []);
    }

    function drawMap(players) {
        const canvas = elements.canvas;
        const context = canvas.getContext("2d");
        context.clearRect(0, 0, canvas.width, canvas.height);

        context.fillStyle = "rgba(255, 255, 255, 0.06)";
        context.fillRect(20, 20, canvas.width - 40, canvas.height - 40);

        if (!players.length) {
            context.fillStyle = "rgba(247, 240, 223, 0.75)";
            context.font = "16px Segoe UI";
            context.fillText("Waiting for match_state", 28, 42);
            return;
        }

        const positions = players.map(function (player) {
            return {
                label: player.player_name || player.player_id,
                x: Number(player.position && player.position.x) || 0,
                z: Number(player.position && player.position.z) || 0,
                speed: Number(player.speed) || 0,
            };
        });
        const xs = positions.map(function (point) { return point.x; });
        const zs = positions.map(function (point) { return point.z; });
        const minX = Math.min.apply(null, xs);
        const maxX = Math.max.apply(null, xs);
        const minZ = Math.min.apply(null, zs);
        const maxZ = Math.max.apply(null, zs);
        const padding = 48;
        const width = Math.max(1, maxX - minX);
        const height = Math.max(1, maxZ - minZ);

        context.strokeStyle = "rgba(255, 255, 255, 0.16)";
        context.lineWidth = 1;
        context.beginPath();
        context.moveTo(padding, canvas.height / 2);
        context.lineTo(canvas.width - padding, canvas.height / 2);
        context.moveTo(canvas.width / 2, padding);
        context.lineTo(canvas.width / 2, canvas.height - padding);
        context.stroke();

        positions.forEach(function (point, index) {
            const px = padding + ((point.x - minX) / width) * (canvas.width - padding * 2);
            const py = canvas.height - padding - ((point.z - minZ) / height) * (canvas.height - padding * 2);
            const hue = (index * 67) % 360;

            context.fillStyle = `hsl(${hue} 85% 62%)`;
            context.beginPath();
            context.arc(px, py, 8, 0, Math.PI * 2);
            context.fill();

            context.fillStyle = "rgba(247, 240, 223, 0.94)";
            context.font = "13px IBM Plex Mono, monospace";
            context.fillText(`${point.label} (${point.speed.toFixed(1)})`, px + 12, py - 10);
        });
    }

    function setStatus(kind, message) {
        elements.wsDot.classList.toggle("connected", kind === "connected");
        elements.wsStatus.textContent = kind;
        elements.statusMessage.textContent = message;
    }

    function renderError(error) {
        setStatus("disconnected", error.message || "Observer error");
    }

    function formatLobbyCard(lobby) {
        return `
            <div class="item-title">
                <span>${escapeHtml(lobby.name)}</span>
                <span class="badge">${escapeHtml(lobby.status)}</span>
            </div>
            <p class="card-meta">${escapeHtml(lobby.lobby_id)}</p>
            <p>${escapeHtml(lobby.map_id)} | ${escapeHtml(String(lobby.current_players))}/${escapeHtml(String(lobby.max_players))} players</p>
        `;
    }

    function formatMatchCard(match) {
        return `
            <div class="item-title">
                <span>${escapeHtml(match.match_id)}</span>
                <span class="badge">${escapeHtml(match.status)}</span>
            </div>
            <p class="card-meta">${escapeHtml(match.lobby_id || "no lobby")}</p>
            <p>${escapeHtml(match.map_id || "n/a")} | tick ${escapeHtml(String(match.server_tick || 0))} | players ${escapeHtml(String(match.player_count || 0))}</p>
        `;
    }

    function renderPill(element, status) {
        const normalized = String(status || "idle");
        element.textContent = normalized;
        element.className = "status-pill neutral";
        if (normalized === "running" || normalized === "in_game" || normalized === "connected") {
            element.className = "status-pill good";
        } else if (normalized === "starting" || normalized === "loading" || normalized === "waiting") {
            element.className = "status-pill warn";
        } else if (normalized === "finished" || normalized === "aborted" || normalized === "disconnected") {
            element.className = "status-pill bad";
        }
    }

    function formatNumber(value) {
        const number = Number(value);
        return Number.isFinite(number) ? number.toFixed(2) : "0.00";
    }

    function escapeHtml(value) {
        return String(value)
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll("\"", "&quot;")
            .replaceAll("'", "&#39;");
    }

    init();
})();

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
        lobbyRequestVersion: 0,
        matchRequestVersion: 0,
        gameSettingsRequestVersion: 0,
        matchPollTimer: null,
        matchListPollTimer: null,
        activeTab: "observer",
        selectedGameSettings: null,
        gameSettingsDraftBySection: {},
        dirtyGameSettingsSections: {},
        gameSettingsLoading: false,
        gameSettingsSavingSection: null,
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
        killLobbyButton: document.getElementById("killLobbyButton"),
        matchStatusPill: document.getElementById("matchStatusPill"),
        lobbyDetail: document.getElementById("lobbyDetail"),
        matchSummary: document.getElementById("matchSummary"),
        networkStats: document.getElementById("networkStats"),
        playersTable: document.getElementById("playersTable"),
        matchPlayersDebug: document.getElementById("matchPlayersDebug"),
        damageMaps: document.getElementById("damageMaps"),
        collisionFeed: document.getElementById("collisionFeed"),
        rawSnapshot: document.getElementById("rawSnapshot"),
        canvas: document.getElementById("matchCanvas"),
        observerTabButton: document.getElementById("observerTabButton"),
        gameSettingsTabButton: document.getElementById("gameSettingsTabButton"),
        observerTabContent: document.getElementById("observerTabContent"),
        gameSettingsTabContent: document.getElementById("gameSettingsTabContent"),
        gameSettingsSourceBadge: document.getElementById("gameSettingsSourceBadge"),
        refreshGameSettingsButton: document.getElementById("refreshGameSettingsButton"),
        gameSettingsSummary: document.getElementById("gameSettingsSummary"),
        gameSettingsSections: document.getElementById("gameSettingsSections"),
    };

    function init() {
        const url = new URL(window.location.href);
        state.token = url.searchParams.get("token") || "";
        elements.tokenInput.value = state.token;

        elements.applyTokenButton.addEventListener("click", applyToken);
        elements.refreshButton.addEventListener("click", refreshAll);
        elements.killLobbyButton.addEventListener("click", killSelectedLobby);
        elements.observerTabButton.addEventListener("click", function () {
            switchTab("observer");
        });
        elements.gameSettingsTabButton.addEventListener("click", function () {
            switchTab("gameSettings");
        });
        elements.refreshGameSettingsButton.addEventListener("click", function () {
            loadGameSettings(true).then(render).catch(renderError);
        });
        window.addEventListener("beforeunload", closeSocket);

        refreshAll().finally(function () {
            connectSocket();
            startMatchListPolling();
        });
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
        refreshAll().finally(function () {
            connectSocket();
            startMatchListPolling();
        });
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

            await Promise.all([loadSelectedLobby(), loadSelectedMatch(), loadGameSettings(true)]);
            render();
            setStatus(state.socket && state.socket.readyState === WebSocket.OPEN ? "connected" : "disconnected", "Snapshot synced");
        } catch (error) {
            renderError(error);
        }
    }

    async function loadSelectedLobby() {
        const requestVersion = ++state.lobbyRequestVersion;
        if (!state.selectedLobbyId) {
            state.selectedLobby = null;
            return;
        }
        try {
            const lobbyId = state.selectedLobbyId;
            const payload = await apiGet(`/api/v1/admin/lobbies/${lobbyId}`);
            if (requestVersion !== state.lobbyRequestVersion || lobbyId !== state.selectedLobbyId) {
                return;
            }
            state.selectedLobby = payload;
        } catch (error) {
            state.selectedLobby = null;
            renderError(error);
        }
    }

    async function loadSelectedMatch() {
        const requestVersion = ++state.matchRequestVersion;
        if (!state.selectedMatchId) {
            state.selectedMatch = null;
            syncSelectedMatchPolling();
            return;
        }
        try {
            const matchId = state.selectedMatchId;
            const payload = await apiGet(`/api/v1/admin/matches/${matchId}`);
            if (requestVersion !== state.matchRequestVersion || matchId !== state.selectedMatchId) {
                return;
            }
            state.selectedMatch = payload;
            upsertById(state.matches, toMatchSummary(payload), "match_id");
            syncSelectedMatchPolling();
        } catch (error) {
            state.selectedMatch = null;
            syncSelectedMatchPolling();
            if (state.selectedMatchId) {
                await refreshMatchesListSilently();
            }
            renderError(error);
        }
    }

    function clearMatchPollTimer() {
        if (state.matchPollTimer) {
            window.clearTimeout(state.matchPollTimer);
            state.matchPollTimer = null;
        }
    }

    function startMatchListPolling() {
        clearMatchListPollTimer();
        state.matchListPollTimer = window.setInterval(function () {
            refreshMatchesListSilently().catch(function () {
                return null;
            });
        }, 3000);
    }

    function clearMatchListPollTimer() {
        if (state.matchListPollTimer) {
            window.clearInterval(state.matchListPollTimer);
            state.matchListPollTimer = null;
        }
    }

    async function refreshMatchesListSilently() {
        const matches = await apiGet("/api/v1/admin/matches");
        state.matches = Array.isArray(matches.items) ? matches.items : [];
        syncSelectedMatchFromList();
        render();
    }

    function syncSelectedMatchPolling() {
        clearMatchPollTimer();
        if (!state.selectedMatch || state.selectedMatch.source !== "purrnet_direct") {
            return;
        }
        state.matchPollTimer = window.setTimeout(function () {
            loadSelectedMatch().then(render).catch(renderError);
        }, 1000);
    }

    async function apiRequest(path, options) {
        const url = new URL(path, window.location.origin);
        if (state.token) {
            url.searchParams.set("token", state.token);
        }
        const response = await fetch(url.toString(), {
            method: options && options.method ? options.method : "GET",
            headers: {
                Accept: "application/json",
                ...(options && options.headers ? options.headers : {}),
            },
            body: options && options.body ? options.body : undefined,
        });
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
        if (response.status === 204) {
            return null;
        }
        return response.json();
    }

    async function apiGet(path) {
        return apiRequest(path, { method: "GET" });
    }

    async function apiDelete(path) {
        return apiRequest(path, { method: "DELETE" });
    }

    async function killSelectedLobby() {
        if (!state.selectedLobbyId) {
            return;
        }

        const lobby = state.selectedLobby;
        const label = lobby ? `${lobby.name} (${lobby.lobby_id})` : state.selectedLobbyId;
        if (!window.confirm(`Kill lobby ${label}?`)) {
            return;
        }

        elements.killLobbyButton.disabled = true;
        setStatus("syncing", `Closing lobby ${state.selectedLobbyId}`);
        try {
            await apiDelete(`/api/v1/admin/lobbies/${state.selectedLobbyId}`);
            await refreshAll();
            setStatus("connected", `Lobby ${label} closed`);
        } catch (error) {
            renderError(error);
        } finally {
            render();
        }
    }

    function connectSocket() {
        closeSocket();
        startMatchListPolling();
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
        clearMatchPollTimer();
        clearMatchListPollTimer();
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
                    room_id: payload.room_id,
                    room_status: payload.room_status,
                    source: payload.source || "backend_runtime",
                }, "match_id");
                if (state.selectedMatchId === payload.match_id) {
                    const previous = state.selectedMatch || {};
                    state.selectedMatch = {
                        ...previous,
                        match_id: payload.match_id,
                        server_tick: payload.server_tick,
                        room_id: payload.room_id || previous.room_id || null,
                        room_status: payload.room_status || previous.room_status || null,
                        source: payload.source || previous.source || "backend_runtime",
                        players: payload.players || [],
                        recent_collisions: payload.recent_collisions || previous.recent_collisions || [],
                        telemetry: payload.telemetry || previous.telemetry || null,
                        raw_snapshot: payload.raw_snapshot || payload,
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
        syncSelectedMatchPolling();
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
            room_id: match.room_id || null,
            room_status: match.room_status || null,
            source: match.source || "backend_runtime",
            debug_summary: match.debug_summary || {},
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
            syncSelectedMatchPolling();
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
        syncSelectedMatchPolling();
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

    function switchTab(tabId) {
        state.activeTab = tabId === "gameSettings" ? "gameSettings" : "observer";
        renderTabs();
        renderGameSettings();
    }

    function renderTabs() {
        const observerActive = state.activeTab === "observer";
        elements.observerTabButton.classList.toggle("active", observerActive);
        elements.gameSettingsTabButton.classList.toggle("active", !observerActive);
        elements.observerTabContent.classList.toggle("active", observerActive);
        elements.gameSettingsTabContent.classList.toggle("active", !observerActive);
    }

    function clearSelectedGameSettings() {
        state.selectedGameSettings = null;
        state.gameSettingsDraftBySection = {};
        state.dirtyGameSettingsSections = {};
        state.gameSettingsLoading = false;
        state.gameSettingsSavingSection = null;
    }

    async function loadGameSettings(forceReload) {
        const requestVersion = ++state.gameSettingsRequestVersion;
        if (!forceReload && state.selectedGameSettings) {
            return;
        }

        state.gameSettingsLoading = true;
        try {
            const payload = await apiGet("/api/v1/admin/game-settings");
            if (requestVersion !== state.gameSettingsRequestVersion) {
                return;
            }
            state.selectedGameSettings = payload;
            state.gameSettingsDraftBySection = buildSectionDrafts(payload.sections || []);
            state.dirtyGameSettingsSections = {};
        } catch (error) {
            if (requestVersion === state.gameSettingsRequestVersion) {
                clearSelectedGameSettings();
            }
            renderError(error);
        } finally {
            if (requestVersion === state.gameSettingsRequestVersion) {
                state.gameSettingsLoading = false;
            }
        }
    }

    function render() {
        renderTabs();
        renderLists();
        renderLobbyDetail();
        renderMatchDetail();
        renderTelemetry();
        renderGameSettings();
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
        elements.killLobbyButton.disabled = !lobby;

        if (!lobby) {
            elements.lobbyDetail.className = "empty-state";
            elements.lobbyDetail.textContent = "Select a lobby to inspect players and car configs.";
            return;
        }

        const wrapper = document.createElement("div");
        wrapper.className = "lobby-players";

        const summary = document.createElement("div");
        summary.className = "fact-grid";
        summary.innerHTML = buildFactTiles([
            { label: "owner", value: lobby.owner_player_id },
            { label: "slots", value: `${lobby.current_players}/${lobby.max_players}` },
            { label: "match", value: lobby.match_id || "none" },
            { label: "expires_at", value: lobby.expires_at || "n/a" },
        ]);
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
        elements.selectedMatchMeta.textContent = match
            ? `${match.status || "unknown"} on ${match.map_id || "n/a"} via ${match.source || "backend_runtime"}`
            : "pick a match from the list";
        elements.serverTickLabel.textContent = String(match ? match.server_tick || 0 : 0);
        elements.serverTickMeta.textContent = match
            ? (match.source === "purrnet_direct" ? "polled from direct dedicated observer" : `lobby ${match.lobby_id || "n/a"}`)
            : "updates on every admin_match_state";
        renderPill(elements.matchStatusPill, match ? match.status : "idle");

        if (!match) {
            elements.matchSummary.className = "match-summary empty-state";
            elements.matchSummary.textContent = "Select a match to inspect.";
            elements.playersTable.replaceChildren();
            elements.matchPlayersDebug.className = "stack empty-state";
            elements.matchPlayersDebug.textContent = "Select a match to inspect player debug state.";
            elements.damageMaps.className = "damage-grid empty-state";
            elements.damageMaps.textContent = "Select a match to inspect damage textures.";
            elements.collisionFeed.className = "stack empty-state";
            elements.collisionFeed.textContent = "Select a match to inspect collision events.";
            elements.rawSnapshot.textContent = "{}";
            drawMap([]);
            return;
        }

        elements.matchSummary.className = "match-summary";
        elements.matchSummary.innerHTML = renderMatchSummary(match);

        elements.playersTable.replaceChildren();
        (match.players || []).forEach(function (player) {
            const debug = player.debug || {};
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>${escapeHtml(player.player_id)}</td>
                <td>${escapeHtml(player.player_name || "n/a")}</td>
                <td>${escapeHtml(player.is_server_controlled ? "bot/server" : "player")}</td>
                <td>${formatNumber(player.position && player.position.x)}</td>
                <td>${formatNumber(player.position && player.position.y)}</td>
                <td>${formatNumber(player.position && player.position.z)}</td>
                <td>${formatNumber(player.rotation && player.rotation.x)}</td>
                <td>${formatNumber(player.rotation && player.rotation.y)}</td>
                <td>${formatNumber(player.rotation && player.rotation.z)}</td>
                <td>${formatNumber(player.velocity && player.velocity.x)}</td>
                <td>${formatNumber(player.velocity && player.velocity.y)}</td>
                <td>${formatNumber(player.velocity && player.velocity.z)}</td>
                <td>${formatNumber(player.speed)}</td>
                <td>${escapeHtml(String(player.last_input_seq != null ? player.last_input_seq : "n/a"))}</td>
                <td>${escapeHtml(String(debug.grounded_wheels != null ? debug.grounded_wheels : "n/a"))}</td>
                <td>${escapeHtml(String(player.damage_revision || 0))}</td>
                <td>${escapeHtml(player.connection_state || "unknown")}</td>
            `;
            elements.playersTable.appendChild(tr);
        });

        renderMatchPlayersDebug(match.players || []);
        renderDamageMaps(match.players || []);
        renderCollisionFeed(match.recent_collisions || []);
        elements.rawSnapshot.textContent = JSON.stringify(match.raw_snapshot || match, null, 2);
        drawMap(match.players || []);
    }

    function renderMatchPlayersDebug(players) {
        if (!Array.isArray(players) || !players.length) {
            elements.matchPlayersDebug.className = "stack empty-state";
            elements.matchPlayersDebug.textContent = "Waiting for player debug data.";
            return;
        }

        const cards = players.map(function (player) {
            const debug = player.debug || {};
            const carConfig = player.car_config || {};
            const customizations = Array.isArray(carConfig.customizations)
                ? carConfig.customizations.map(function (item) {
                    return `${item.selector_path}:${item.variant_name}`;
                }).join(", ")
                : "";
            const card = document.createElement("article");
            card.className = "player-card";
            card.innerHTML = `
                <div class="item-title">
                    <span>${escapeHtml(player.player_name || player.player_id)}</span>
                    <span class="badge">${escapeHtml(player.connection_state || "unknown")}</span>
                </div>
                <p class="card-meta">${escapeHtml(player.player_id)}</p>
                <div class="player-grid">
                    <div><div class="small">loadout</div><strong>${escapeHtml(carConfig.loadout_display_name || carConfig.loadout_name || debug.loadout_display_name || debug.loadout_name || "n/a")}</strong></div>
                    <div><div class="small">config</div><strong>${escapeHtml(debug.resolved_car_config_name || "n/a")}</strong></div>
                    <div><div class="small">spawn</div><strong>${formatNumber(player.spawn_position && player.spawn_position.x)}, ${formatNumber(player.spawn_position && player.spawn_position.y)}, ${formatNumber(player.spawn_position && player.spawn_position.z)}</strong></div>
                    <div><div class="small">position</div><strong>${formatNumber(player.position && player.position.x)}, ${formatNumber(player.position && player.position.y)}, ${formatNumber(player.position && player.position.z)}</strong></div>
                    <div><div class="small">velocity</div><strong>${formatNumber(player.velocity && player.velocity.x)}, ${formatNumber(player.velocity && player.velocity.y)}, ${formatNumber(player.velocity && player.velocity.z)}</strong></div>
                    <div><div class="small">rotation</div><strong>${formatNumber(player.rotation && player.rotation.x)}, ${formatNumber(player.rotation && player.rotation.y)}, ${formatNumber(player.rotation && player.rotation.z)}</strong></div>
                    <div><div class="small">gear / rpm</div><strong>${escapeHtml(String(debug.current_gear != null ? debug.current_gear : "n/a"))} / ${formatNumber(debug.current_rpm)}</strong></div>
                    <div><div class="small">torque / speed</div><strong>${formatNumber(debug.motor_torque)} / ${formatNumber(debug.speed_kph)}</strong></div>
                    <div><div class="small">grounded / wheels</div><strong>${escapeHtml(String(debug.grounded_wheels != null ? debug.grounded_wheels : "n/a"))} / ${escapeHtml(String(debug.wheel_count != null ? debug.wheel_count : "n/a"))}</strong></div>
                    <div><div class="small">nitro</div><strong>${formatNumber(debug.nitro_amount)} (${escapeHtml(String(Boolean(debug.nitro_active)))})</strong></div>
                    <div><div class="small">damage</div><strong>rev ${escapeHtml(String(player.damage_revision != null ? player.damage_revision : "n/a"))} / ${escapeHtml(`${player.damage_width || 0}x${player.damage_height || 0}`)}</strong></div>
                    <div><div class="small">spawn flags</div><strong>${escapeHtml(`queued=${Boolean(debug.tracked_queued)} spawned=${Boolean(debug.tracked_spawned)}`)}</strong></div>
                    <div><div class="small">inputs</div><strong>${escapeHtml(`thr=${player.throttle ?? 0} steer=${player.steer ?? 0} brake=${Boolean(player.brake)} hb=${Boolean(player.handbrake)} nitro=${Boolean(player.nitro)}`)}</strong></div>
                    <div><div class="small">failure</div><code>${escapeHtml(debug.last_spawn_failure_reason || "none")}</code></div>
                    <div><div class="small">customizations</div><code>${escapeHtml(customizations || "none")}</code></div>
                </div>
            `;
            return card;
        });

        elements.matchPlayersDebug.className = "stack";
        elements.matchPlayersDebug.replaceChildren(...cards);
    }

    function renderDamageMaps(players) {
        if (!players.length) {
            elements.damageMaps.className = "damage-grid empty-state";
            elements.damageMaps.textContent = "Waiting for match player data.";
            return;
        }

        const cards = players.map(function (player) {
            const card = document.createElement("article");
            card.className = "damage-card";

            const title = document.createElement("div");
            title.className = "item-title";
            title.innerHTML = `
                <span>${escapeHtml(player.player_name || player.player_id)}</span>
                <span class="badge">rev ${escapeHtml(String(player.damage_revision || 0))}</span>
            `;
            card.appendChild(title);

            const meta = document.createElement("div");
            meta.className = "damage-meta";
            meta.innerHTML = `
                <div><div class="small">bytes</div><strong>${escapeHtml(String(player.damage_map_bytes || 0))}</strong></div>
                <div><div class="small">size</div><strong>${escapeHtml(`${player.damage_width || 0}x${player.damage_height || 0}`)}</strong></div>
                <div><div class="small">last damage</div><strong>${escapeHtml(player.last_damage_at || "n/a")}</strong></div>
                <div><div class="small">connection</div><strong>${escapeHtml(player.connection_state || "unknown")}</strong></div>
            `;
            card.appendChild(meta);

            const preview = document.createElement("canvas");
            preview.className = "damage-preview";
            preview.width = Math.max(1, Number(player.damage_width) || 8);
            preview.height = Math.max(1, Number(player.damage_height) || 16);
            card.appendChild(preview);
            drawDamagePreview(preview, player);

            return card;
        });

        elements.damageMaps.className = "damage-grid";
        elements.damageMaps.replaceChildren(...cards);
    }

    function renderCollisionFeed(collisions) {
        if (!Array.isArray(collisions) || !collisions.length) {
            elements.collisionFeed.className = "stack empty-state";
            elements.collisionFeed.textContent = "No collision events yet.";
            return;
        }

        const cards = collisions.slice().reverse().slice(0, 12).map(function (collision) {
            const card = document.createElement("article");
            card.className = "telemetry-card";
            const point = collision.world_point || {};
            card.innerHTML = `
                <div class="item-title">
                    <span>${escapeHtml(collision.primary_player_id || "unknown")} -> ${escapeHtml(collision.secondary_player_id || "unknown")}</span>
                    <span class="badge">${escapeHtml(String(collision.sequence != null ? collision.sequence : "evt"))}</span>
                </div>
                <div class="config-list">
                    <div><div class="small">impulse</div><strong>${formatNumber(collision.impulse_magnitude)}</strong></div>
                    <div><div class="small">point</div><strong>${formatNumber(point.x)}, ${formatNumber(point.y)}, ${formatNumber(point.z)}</strong></div>
                    <div><div class="small">server_time</div><strong>${escapeHtml(collision.server_time ? new Date(Number(collision.server_time)).toISOString() : "n/a")}</strong></div>
                </div>
            `;
            return card;
        });

        elements.collisionFeed.className = "stack";
        elements.collisionFeed.replaceChildren(...cards);
    }

    function drawDamagePreview(canvas, player) {
        const context = canvas.getContext("2d");
        context.clearRect(0, 0, canvas.width, canvas.height);

        if (!player || !player.damage_map_b64 || !player.damage_width || !player.damage_height) {
            context.fillStyle = "rgba(31, 27, 22, 0.45)";
            context.font = "10px IBM Plex Mono, monospace";
            context.fillText("no data", 4, 12);
            return;
        }

        try {
            const raw = atob(player.damage_map_b64);
            const expectedLength = player.damage_width * player.damage_height * 4;
            if (raw.length < expectedLength) {
                context.fillStyle = "rgba(162, 44, 41, 0.75)";
                context.font = "10px IBM Plex Mono, monospace";
                context.fillText("truncated", 4, 12);
                return;
            }

            const imageData = context.createImageData(player.damage_width, player.damage_height);
            for (let index = 0; index < expectedLength; index += 1) {
                imageData.data[index] = raw.charCodeAt(index);
            }
            context.putImageData(imageData, 0, 0);
        } catch (error) {
            context.fillStyle = "rgba(162, 44, 41, 0.75)";
            context.font = "10px IBM Plex Mono, monospace";
            context.fillText("decode error", 4, 12);
        }
    }

    function renderTelemetry() {
        const match = state.selectedMatch;
        if (!match || !match.telemetry) {
            elements.networkStats.className = "telemetry-grid empty-state";
            elements.networkStats.textContent = "Select a match to inspect relay load.";
            return;
        }

        const telemetry = match.telemetry;
        if (match.source === "purrnet_direct") {
            const groups = [
                { title: "Observer", payload: telemetry.observer },
                { title: "Network", payload: telemetry.network },
                { title: "Prediction", payload: telemetry.prediction },
                { title: "Spawner", payload: telemetry.spawner },
                { title: "Counts", payload: telemetry.counts },
            ].filter(function (group) {
                return group.payload && typeof group.payload === "object";
            });

            const cards = groups.map(function (group) {
                const lines = Object.keys(group.payload).map(function (key) {
                    return metricLine(key.replace(/_/g, " "), group.payload[key]);
                });
                return createTelemetryCard(group.title, null, lines);
            });

            elements.networkStats.className = "telemetry-grid";
            elements.networkStats.replaceChildren(...cards);
            return;
        }

        const cards = [
            createTelemetryCard("Player Input In", telemetry.player_input_in, [
                metricLine("msg/s", telemetry.player_input_in && telemetry.player_input_in.messages_per_sec),
                metricLine("KB/s", kbFromBytes(telemetry.player_input_in && telemetry.player_input_in.bytes_per_sec)),
                metricLine("total msg", telemetry.player_input_in && telemetry.player_input_in.total_messages),
            ]),
            createTelemetryCard("Player State In", telemetry.player_state_in, [
                metricLine("msg/s", telemetry.player_state_in && telemetry.player_state_in.messages_per_sec),
                metricLine("KB/s", kbFromBytes(telemetry.player_state_in && telemetry.player_state_in.bytes_per_sec)),
                metricLine("total msg", telemetry.player_state_in && telemetry.player_state_in.total_messages),
            ]),
            createTelemetryCard("Simulation I/O", telemetry.simulation_snapshot_in, [
                metricLine("snapshot KB/s", kbFromBytes(telemetry.simulation_snapshot_in && telemetry.simulation_snapshot_in.bytes_per_sec)),
                metricLine("input out/s", telemetry.simulation_input_out && telemetry.simulation_input_out.messages_per_sec),
                metricLine("last snapshot", `${telemetry.last_simulation_snapshot_bytes || 0} B`),
            ]),
            createTelemetryCard("Match State Out", telemetry.match_state_out, [
                metricLine("msg/s", telemetry.match_state_out && telemetry.match_state_out.messages_per_sec),
                metricLine("KB/s", kbFromBytes(telemetry.match_state_out && telemetry.match_state_out.bytes_per_sec)),
                metricLine("last snapshot", `${telemetry.last_match_snapshot_bytes || 0} B`),
            ]),
            createTelemetryCard("Damage Relay", telemetry.damage_state_in, [
                metricLine("damage in/s", telemetry.damage_state_in && telemetry.damage_state_in.messages_per_sec),
                metricLine("damage out/s", telemetry.damage_state_out && telemetry.damage_state_out.messages_per_sec),
                metricLine("last damage", `${telemetry.last_damage_payload_bytes || 0} B`),
            ]),
            createTelemetryCard("Collision Relay", telemetry.collision_event_in, [
                metricLine("collision in/s", telemetry.collision_event_in && telemetry.collision_event_in.messages_per_sec),
                metricLine("collision out/s", telemetry.collision_event_out && telemetry.collision_event_out.messages_per_sec),
                metricLine("last collision", `${telemetry.last_collision_payload_bytes || 0} B`),
            ]),
            createTelemetryCard("Admin Stream", telemetry.admin_state_out, [
                metricLine("msg/s", telemetry.admin_state_out && telemetry.admin_state_out.messages_per_sec),
                metricLine("KB/s", kbFromBytes(telemetry.admin_state_out && telemetry.admin_state_out.bytes_per_sec)),
                metricLine("tick source", `server tick ${match.server_tick || 0}`),
            ]),
        ];

        elements.networkStats.className = "telemetry-grid";
        elements.networkStats.replaceChildren(...cards);
    }

    function renderGameSettings() {
        const gameSettings = state.selectedGameSettings;
        const source = gameSettings ? (gameSettings.source || "backend_runtime") : "backend_runtime";
        elements.gameSettingsSourceBadge.textContent = source;
        elements.refreshGameSettingsButton.disabled = state.gameSettingsLoading || state.gameSettingsSavingSection !== null;

        if (state.gameSettingsLoading && !gameSettings) {
            elements.gameSettingsSummary.className = "empty-state";
            elements.gameSettingsSummary.textContent = "Loading global runtime settings.";
            elements.gameSettingsSections.className = "stack empty-state";
            elements.gameSettingsSections.textContent = "Loading runtime settings.";
            return;
        }

        if (!gameSettings) {
            elements.gameSettingsSummary.className = "empty-state";
            elements.gameSettingsSummary.textContent = "Global GameSettings are unavailable.";
            elements.gameSettingsSections.className = "stack empty-state";
            elements.gameSettingsSections.textContent = "No settings loaded.";
            return;
        }

        const summaryCards = [
            { label: "scope", value: gameSettings.scope || "global" },
            { label: "source", value: source },
            { label: "sections", value: String((gameSettings.sections || []).length) },
            { label: "note", value: gameSettings.note || "none" },
        ];
        elements.gameSettingsSummary.className = "fact-grid";
        elements.gameSettingsSummary.innerHTML = buildFactTiles(summaryCards);

        if (!gameSettings || !Array.isArray(gameSettings.sections) || !gameSettings.sections.length) {
            elements.gameSettingsSections.className = "stack empty-state";
            elements.gameSettingsSections.textContent = source === "purrnet_direct"
                ? "Dedicated observer does not expose global settings yet."
                : "Runtime GameSettings are currently available only for direct PurrNet observer-backed servers.";
            return;
        }

        const sections = gameSettings.sections.map(function (section) {
            return createGameSettingsSection(section);
        });
        elements.gameSettingsSections.className = "stack";
        elements.gameSettingsSections.replaceChildren(...sections);
    }

    function createGameSettingsSection(section) {
        const card = document.createElement("article");
        card.className = "settings-card";

        const head = document.createElement("div");
        head.className = "panel-head nested-headless";
        head.innerHTML = `
            <div>
                <h2>${escapeHtml(section.title || section.section_id)}</h2>
                <p class="settings-description">${escapeHtml(section.description || "Runtime settings section.")}</p>
            </div>
            <div class="panel-actions">
                <span class="badge">${escapeHtml(section.section_id)}</span>
                <button
                    type="button"
                    class="secondary"
                    ${section.editable ? "" : "disabled"}
                >${state.gameSettingsSavingSection === section.section_id ? "Saving..." : "Save section"}</button>
            </div>
        `;
        const saveButton = head.querySelector("button");
        if (saveButton) {
            saveButton.disabled = !section.editable || state.gameSettingsSavingSection === section.section_id;
            saveButton.addEventListener("click", function () {
                saveGameSettingsSection(section.section_id);
            });
        }
        card.appendChild(head);

        const metaEntries = Object.entries(section.meta || {});
        if (metaEntries.length) {
            const meta = document.createElement("div");
            meta.className = "fact-grid";
            meta.innerHTML = buildFactTiles(metaEntries.map(function (entry) {
                return {
                    label: entry[0],
                    value: entry[0] === "updated_at_unix_ms" ? formatTimestamp(entry[1]) : String(entry[1]),
                };
            }));
            card.appendChild(meta);
        }

        const fields = document.createElement("div");
        fields.className = "settings-grid";
        Object.entries(section.fields || {}).forEach(function (entry) {
            const key = entry[0];
            const originalValue = entry[1];
            const draftValue = getDraftFieldValue(section.section_id, key, originalValue);
            fields.appendChild(createGameSettingsField(section.section_id, key, draftValue, originalValue, Boolean(section.editable)));
        });
        card.appendChild(fields);

        const footer = document.createElement("div");
        footer.className = "settings-footer";
        footer.textContent = state.dirtyGameSettingsSections[section.section_id]
            ? "Unsaved changes"
            : "Saved";
        card.appendChild(footer);

        return card;
    }

    function createGameSettingsField(sectionId, key, value, sampleValue, editable) {
        const wrapper = document.createElement("label");
        wrapper.className = "settings-field";

        const caption = document.createElement("span");
        caption.className = "small";
        caption.textContent = formatSettingLabel(key);
        wrapper.appendChild(caption);

        let input;
        if (typeof sampleValue === "boolean") {
            input = document.createElement("input");
            input.type = "checkbox";
            input.checked = Boolean(value);
            input.disabled = !editable || state.gameSettingsSavingSection === sectionId;
            input.addEventListener("change", function () {
                updateGameSettingsDraft(sectionId, key, input.checked);
            });
            wrapper.classList.add("checkbox-field");
        } else {
            input = document.createElement("input");
            input.type = typeof sampleValue === "number" ? "number" : "text";
            input.step = typeof sampleValue === "number" && Number.isInteger(sampleValue) ? "1" : "any";
            input.value = value == null ? "" : String(value);
            input.disabled = !editable || state.gameSettingsSavingSection === sectionId;
            input.addEventListener("input", function () {
                updateGameSettingsDraft(sectionId, key, coerceSettingValue(input.value, sampleValue));
            });
        }

        wrapper.appendChild(input);
        return wrapper;
    }

    async function saveGameSettingsSection(sectionId) {
        const fields = state.gameSettingsDraftBySection[sectionId];
        if (!fields) {
            return;
        }

        state.gameSettingsSavingSection = sectionId;
        renderGameSettings();
        setStatus("syncing", `Saving ${sectionId} settings`);
        try {
            const payload = await apiRequest("/api/v1/admin/game-settings", {
                method: "PUT",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({
                    sections: [
                        {
                            section_id: sectionId,
                            fields: fields,
                        },
                    ],
                }),
            });
            state.selectedGameSettings = payload;
            state.gameSettingsDraftBySection = buildSectionDrafts(payload.sections || []);
            state.dirtyGameSettingsSections = {};
            setStatus("connected", `${sectionId} settings saved`);
        } catch (error) {
            renderError(error);
        } finally {
            state.gameSettingsSavingSection = null;
            renderGameSettings();
        }
    }

    function drawMap(players) {
        const canvas = elements.canvas;
        const context = canvas.getContext("2d");
        context.clearRect(0, 0, canvas.width, canvas.height);

        const padding = 36;
        const mapSizeMeters = 300;
        const halfRange = mapSizeMeters * 0.5;
        const drawableWidth = canvas.width - padding * 2;
        const drawableHeight = canvas.height - padding * 2;

        context.fillStyle = "rgba(255, 255, 255, 0.06)";
        context.fillRect(padding, padding, drawableWidth, drawableHeight);

        context.strokeStyle = "rgba(255, 255, 255, 0.08)";
        context.lineWidth = 1;
        for (let i = 0; i <= 6; i += 1) {
            const x = padding + (drawableWidth / 6) * i;
            const y = padding + (drawableHeight / 6) * i;
            context.beginPath();
            context.moveTo(x, padding);
            context.lineTo(x, canvas.height - padding);
            context.stroke();
            context.beginPath();
            context.moveTo(padding, y);
            context.lineTo(canvas.width - padding, y);
            context.stroke();
        }

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

        context.strokeStyle = "rgba(255, 255, 255, 0.16)";
        context.lineWidth = 1;
        context.beginPath();
        context.moveTo(padding, canvas.height / 2);
        context.lineTo(canvas.width - padding, canvas.height / 2);
        context.moveTo(canvas.width / 2, padding);
        context.lineTo(canvas.width / 2, canvas.height - padding);
        context.stroke();

        positions.forEach(function (point, index) {
            const clampedX = Math.max(-halfRange, Math.min(halfRange, point.x));
            const clampedZ = Math.max(-halfRange, Math.min(halfRange, point.z));
            const px = padding + ((clampedX + halfRange) / mapSizeMeters) * drawableWidth;
            const py = canvas.height - padding - ((clampedZ + halfRange) / mapSizeMeters) * drawableHeight;
            const hue = (index * 67) % 360;

            context.fillStyle = `hsl(${hue} 85% 62%)`;
            context.beginPath();
            context.arc(px, py, 8, 0, Math.PI * 2);
            context.fill();

            context.fillStyle = "rgba(247, 240, 223, 0.94)";
            context.font = "13px IBM Plex Mono, monospace";
            context.fillText(`${point.label} (${point.speed.toFixed(1)})`, px + 12, py - 10);
        });

        context.fillStyle = "rgba(247, 240, 223, 0.65)";
        context.font = "12px IBM Plex Mono, monospace";
        context.fillText("-150m", padding, canvas.height - 12);
        context.fillText("+150m", canvas.width - padding - 38, canvas.height - 12);
        context.fillText("300m x 300m", canvas.width - 116, 20);
    }

    function setStatus(kind, message) {
        elements.wsDot.classList.toggle("connected", kind === "connected");
        elements.wsStatus.textContent = kind;
        elements.statusMessage.textContent = message;
    }

    function renderError(error) {
        setStatus("disconnected", error.message || "Observer error");
    }

    function createTelemetryCard(title, metric, lines) {
        const card = document.createElement("article");
        card.className = "telemetry-card";
        const windowLabel = metric && metric.window_sec ? `${metric.window_sec}s window` : "rolling window";
        card.innerHTML = `
            <div class="small">${escapeHtml(windowLabel)}</div>
            <strong>${escapeHtml(title)}</strong>
            <div class="config-list">${lines.map(function (line) {
                return `<div><div class="small">${escapeHtml(line.label)}</div><strong>${escapeHtml(line.value)}</strong></div>`;
            }).join("")}</div>
        `;
        return card;
    }

    function metricLine(label, value) {
        return { label: label, value: value == null ? "n/a" : String(value) };
    }

    function kbFromBytes(bytesPerSec) {
        const number = Number(bytesPerSec);
        return Number.isFinite(number) ? (number / 1024).toFixed(2) : "0.00";
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
        const debugSummary = match.debug_summary || {};
        const cleanupLine = debugSummary.transient_cleanup_enabled
            ? `<p class="card-accent">solo ${escapeHtml(debugSummary.solo_session_status || (debugSummary.solo_session_active ? "active" : "idle"))} | idle ${escapeHtml(formatDuration(debugSummary.seconds_until_idle_close))}</p>`
            : "";
        return `
            <div class="item-title">
                <span>${escapeHtml(match.match_id)}</span>
                <span class="badge">${escapeHtml(match.status)}</span>
            </div>
            <p class="card-meta">${escapeHtml(match.lobby_id || "no lobby")} | ${escapeHtml(match.source || "backend_runtime")}</p>
            <p>${escapeHtml(match.map_id || "n/a")} | tick ${escapeHtml(String(match.server_tick || 0))} | players ${escapeHtml(String(match.player_count || debugSummary.player_count || 0))}</p>
            ${cleanupLine}
        `;
    }

    function renderMatchSummary(match) {
        const debugSummary = match.debug_summary || {};
        const cards = [
            { label: "match_id", value: match.match_id },
            { label: "map / tick_rate", value: `${match.map_id || "n/a"} / ${match.tick_rate || "n/a"}` },
            { label: "players", value: String((match.players || []).length) },
            { label: "source", value: match.source || "backend_runtime" },
            { label: "room", value: match.room_status || "n/a" },
            { label: "room_id", value: match.room_id || "n/a" },
        ];

        if (debugSummary.transient_cleanup_enabled) {
            cards.push(
                { label: "solo session", value: debugSummary.solo_session_status || (debugSummary.solo_session_active ? "active" : "idle") },
                { label: "human player", value: debugSummary.solo_session_human_player_id || "n/a" },
                { label: "idle timeout", value: formatDuration(debugSummary.solo_idle_timeout_sec) },
                { label: "time to close", value: formatDuration(debugSummary.seconds_until_idle_close) },
                { label: "last input", value: debugSummary.seconds_since_last_input >= 0 ? `${formatDuration(debugSummary.seconds_since_last_input)} ago` : "n/a" },
                {
                    label: "last close",
                    value: debugSummary.last_close_reason
                        ? (debugSummary.seconds_since_last_close >= 0
                            ? `${debugSummary.last_close_reason} | ${formatDuration(debugSummary.seconds_since_last_close)} ago`
                            : debugSummary.last_close_reason)
                        : "n/a"
                },
            );
        }

        return `<div class="fact-grid">${buildFactTiles(cards)}</div>`;
    }

    function buildFactTiles(items) {
        return items.map(function (item) {
            return `
                <article class="fact-tile">
                    <div class="small">${escapeHtml(item.label)}</div>
                    <strong>${escapeHtml(item.value == null ? "n/a" : String(item.value))}</strong>
                </article>
            `;
        }).join("");
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

    function formatDuration(value) {
        const number = Number(value);
        if (!Number.isFinite(number) || number < 0) {
            return "n/a";
        }
        if (number >= 60) {
            return `${(number / 60).toFixed(1)}m`;
        }
        return `${number.toFixed(1)}s`;
    }

    function buildSectionDrafts(sections) {
        const drafts = {};
        sections.forEach(function (section) {
            drafts[section.section_id] = cloneJson(section.fields || {});
        });
        return drafts;
    }

    function getDraftFieldValue(sectionId, key, fallbackValue) {
        const draft = state.gameSettingsDraftBySection[sectionId];
        if (draft && Object.prototype.hasOwnProperty.call(draft, key)) {
            return draft[key];
        }
        return fallbackValue;
    }

    function updateGameSettingsDraft(sectionId, key, value) {
        if (!state.gameSettingsDraftBySection[sectionId]) {
            state.gameSettingsDraftBySection[sectionId] = {};
        }
        state.gameSettingsDraftBySection[sectionId][key] = value;
        state.dirtyGameSettingsSections[sectionId] = true;
    }

    function coerceSettingValue(rawValue, sampleValue) {
        if (typeof sampleValue === "number") {
            const parsed = Number(rawValue);
            return Number.isFinite(parsed) ? parsed : sampleValue;
        }
        if (typeof sampleValue === "boolean") {
            return Boolean(rawValue);
        }
        return String(rawValue);
    }

    function formatSettingLabel(value) {
        return String(value || "")
            .replaceAll("_", " ")
            .replace(/\b\w/g, function (char) {
                return char.toUpperCase();
            });
    }

    function formatTimestamp(value) {
        const number = Number(value);
        if (!Number.isFinite(number) || number <= 0) {
            return "n/a";
        }
        return new Date(number).toISOString();
    }

    function cloneJson(value) {
        return JSON.parse(JSON.stringify(value));
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

/**
 * WC2026 Live Score Backend - Fixed Compatibility Edition
 * ─────────────────────────────────────────────────────────
 * Data: wcup2026.org — free, no API key, live scores + results
 * Polls every 14 min passive, every 5 min when live match detected
 * Broadcasts via WebSocket to all clients in the exact format expected by App.jsx
 */

require("dotenv").config();
const express   = require("express");
const cors      = require("cors");
const http      = require("http");
const WebSocket = require("ws");
const cron      = require("node-cron");

const PORT       = process.env.PORT || 3001;
const BASE_URL   = "https://wcup2026.org/api/data.php";
const PASSIVE_MS = 14 * 60 * 1000; // 14 min when no live game
const LIVE_MS    =  5 * 60 * 1000; // 5 min when live game active

// ─── TEAM NAME MAP ─────────────────────────────────────────────────────────
const TEAM_MAP = {
  "Canada":"Canada","Bosnia-Herzegovina":"Bosnia & Herz.",
  "Bosnia and Herzegovina":"Bosnia & Herz.","Bosnia & Herzegovina":"Bosnia & Herz.",
  "Qatar":"Qatar","Switzerland":"Switzerland","Brazil":"Brazil","Morocco":"Morocco",
  "Haiti":"Haiti","Scotland":"Scotland","Germany":"Germany","Curaçao":"Curaçao",
  "Curacao":"Curaçao","Ivory Coast":"Ivory Coast","Côte d'Ivoire":"Ivory Coast",
  "Ecuador":"Ecuador","Netherlands":"Netherlands","Japan":"Japan","Sweden":"Sweden",
  "Tunisia":"Tunisia","Belgium":"Belgium","Egypt":"Egypt","Iran":"Iran",
  "New Zealand":"New Zealand","Spain":"Spain","Cape Verde":"Cape Verde",
  "Saudi Arabia":"Saudi Arabia","Uruguay":"Uruguay","France":"France",
  "Senegal":"Senegal","Norway":"Norway","Iraq":"Iraq","Argentina":"Argentina",
  "Algeria":"Algeria","Austria":"Austria","Jordan":"Jordan","Portugal":"Portugal",
  "Uzbekistan":"Uzbekistan","Colombia":"Colombia","Congo DR":"Congo DR",
  "DR Congo":"Congo DR","England":"England","Croatia":"Croatia","Ghana":"Ghana",
  "Panama":"Panama","Mexico":"Mexico","South Africa":"South Africa",
  "South Korea":"South Korea","Korea Republic":"South Korea","Czechia":"Czechia",
  "Czech Republic":"Czechia","United States":"United States","USA":"United States",
  "Paraguay":"Paraguay","Australia":"Australia","Turkey":"Türkiye","Türkiye":"Türkiye",
};

// ─── MATCH ID MAP ───────────────────────────────────────────────────────────
const MATCH_ID_MAP = {
  "Canada|Bosnia & Herz.":"B1","Qatar|Switzerland":"B2",
  "Switzerland|Bosnia & Herz.":"B3","Canada|Qatar":"B4",
  "Switzerland|Canada":"B5","Bosnia & Herz.|Qatar":"B6",
  "Brazil|Morocco":"C1","Haiti|Scotland":"C2","Scotland|Morocco":"C3",
  "Brazil|Haiti":"C4","Scotland|Brazil":"C5","Morocco|Haiti":"C6",
  "Germany|Curaçao":"E1","Ivory Coast|Ecuador":"E2","Germany|Ivory Coast":"E3",
  "Ecuador|Curaçao":"E4","Ecuador|Germany":"E5","Curaçao|Ivory Coast":"E6",
  "Netherlands|Japan":"F1","Sweden|Tunisia":"F2","Netherlands|Sweden":"F3",
  "Tunisia|Japan":"F4","Japan|Sweden":"F5","Tunisia|Netherlands":"F6",
  "Belgium|Egypt":"G1","Iran|New Zealand":"G2","Belgium|Iran":"G3",
  "New Zealand|Egypt":"G4","Egypt|Iran":"G5","New Zealand|Belgium":"G6",
  "Spain|Cape Verde":"H1","Saudi Arabia|Uruguay":"H2","Spain|Saudi Arabia":"H3",
  "Uruguay|Cape Verde":"H4","Cape Verde|Saudi Arabia":"H5","Uruguay|Spain":"H6",
  "France|Senegal":"I1","Iraq|Norway":"I2","France|Iraq":"I3",
  "Norway|Senegal":"I4","Norway|France":"I5","Senegal|Iraq":"I6",
  "Argentina|Algeria":"J1","Austria|Jordan":"J2","Argentina|Austria":"J3",
  "Jordan|Algeria":"J4","Algeria|Austria":"J5","Jordan|Argentina":"J6",
  "Portugal|Congo DR":"K1","Uzbekistan|Colombia":"K2","Portugal|Uzbekistan":"K3",
  "Colombia|Congo DR":"K4","Colombia|Portugal":"K5","Congo DR|Uzbekistan":"K6",
  "England|Croatia":"L1","Ghana|Panama":"L2","England|Ghana":"L3",
  "Panama|Croatia":"L4","Croatia|England":"L5","Ghana|Panama":"L6",
  "Mexico|South Africa":"A1","South Korea|Czechia":"A2","Czechia|South Africa":"A3",
  "Mexico|South Korea":"A4","Czechia|Mexico":"A5","South Africa|South Korea":"A6",
  "United States|Paraguay":"D1","Australia|Türkiye":"D2","United States|Australia":"D3",
  "Türkiye|Paraguay":"D4","Türkiye|United States":"D5","Paraguay|Australia":"D6",
};

function norm(name){ return TEAM_MAP[name] || name; }

// ─── CACHE MANAGEMENT ────────────────────────────────────────────────────────
let cache = {
  all_matches: {}, // Store matches keyed by Group ID (B1, I1, J1, etc.) to match frontend's expected whitelist structure
  liveNow:     false,
  lastFetch:   null,
  fetchCount:  0,
};

// ─── EXTRACT AND TRANSLATE TO OPENFOOTBALL SIGNATURE ──────────────────────────
function parseMatches(raw) {
  if (!raw || !Array.isArray(raw.matches)) return { matchesMap: {}, liveNow: false };
  const matchesMap = {};
  let liveNow = false;

  raw.matches.forEach(m => {
    const home = norm(m.team1 || "");
    const away = norm(m.team2 || "");
    const key  = `${home}|${away}`;
    const id   = MATCH_ID_MAP[key];
    if (!id) return;

    const status = m.status || "upcoming";
    if (status === "live") liveNow = true;

    // Convert the open API data fields into string values matching the App.jsx parser whitelist
    const hGoals = (m.homeGoals !== undefined && m.homeGoals !== null) ? String(m.homeGoals) : "";
    const aGoals = (m.awayGoals !== undefined && m.awayGoals !== null) ? String(m.awayGoals) : "";

    // Build the format expected by App.jsx line 457 (data.all_matches)
    matchesMap[id] = {
      homeGoals: hGoals,
      awayGoals: aGoals,
      source: "openfootball",
      goals: Array.isArray(m.goals) ? m.goals : []
    };
  });

  return { matchesMap, liveNow };
}

// ─── UTILITY API CONSUMER ───────────────────────────────────────────────────
async function fetchData(action = "all") {
  const url = `${BASE_URL}?action=${action}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 12000);
  try {
    const res = await fetch(url, {
      method: "GET",
      headers: { "User-Agent": "WC2026-Tracker/1.0" },
      signal: controller.signal,
    });
    clearTimeout(timeout);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (e) {
    clearTimeout(timeout);
    throw e;
  }
}

// ─── EXPRESS SERVER CONFIGURATION ───────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
app.use(cors({ origin: "*" }));

app.get("/health", (req, res) => res.json({
  status: "ok", 
  lastFetch: cache.lastFetch,
  fetchCount: cache.fetchCount, 
  matchesTracked: Object.keys(cache.all_matches).length,
  liveNow: cache.liveNow,
  uptime: Math.floor(process.uptime()) + "s"
}));

// Serves the data payload during REST polling fallbacks (App.jsx line 506)
app.get("/api/fixtures", (req, res) => res.json({
  all_matches: cache.all_matches, 
  lastFetch: cache.lastFetch, 
  liveNow: cache.liveNow
}));

// ─── WEBSOCKET ROUTING ──────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server });

wss.on("connection", ws => {
  console.log(`📱 Client connection initialized [${wss.clients.size} connected]`);
  // Push the current match standings mapping immediately upon connection
  ws.send(JSON.stringify({
    type: "snapshot", 
    all_matches: cache.all_matches,
    lastFetch: cache.lastFetch, 
    liveNow: cache.liveNow
  }));
  ws.on("close", () => console.log(`📵 Client left [${wss.clients.size} remaining]`));
  ws.on("error", e => console.error("WS client stream error:", e.message));
});

function broadcast(data) {
  const msg = JSON.stringify(data);
  let activeClients = 0;
  wss.clients.forEach(c => { 
    if (c.readyState === WebSocket.OPEN) { 
      c.send(msg); 
      activeClients++; 
    }
  });
  if (activeClients > 0) console.log(`📡 Broadcasted update payload to ${activeClients} client(s).`);
}

// ─── EXTRACTION ENGINE ──────────────────────────────────────────────────────
let liveInterval = null;

async function fetchAndBroadcast(reason = "cron") {
  const timeLabel = new Date().toLocaleString("en-US", { timeZone: "America/New_York" });
  console.log(`\n⚽ Pulling data pipeline [Trigger: ${reason}] at ${timeLabel} EST`);
  try {
    // Pull full database results to guarantee historic rows like Group I/J are populated
    const raw = await fetchData("all");
    const { matchesMap, liveNow } = parseMatches(raw);

    // Deep merge into local memory cache to keep tracking stable
    cache.all_matches = { ...cache.all_matches, ...matchesMap };
    cache.liveNow     = liveNow;
    cache.lastFetch   = new Date().toISOString();
    cache.fetchCount++;

    console.log(`✅ Cache updated. Map contains ${Object.keys(cache.all_matches).length} matches. Live state: ${liveNow}`);

    // Broadcast the updated structure to the clients
    broadcast({
      type: "update", 
      all_matches: cache.all_matches,
      lastFetch: cache.lastFetch, 
      liveNow
    });

    // Handle dynamic polling speed based on whether matches are live
    if (liveNow && !liveInterval) {
      console.log("🔴 Live matches active — setting interval to 5 minutes.");
      liveInterval = setInterval(() => fetchAndBroadcast("live-poll"), LIVE_MS);
    } else if (!liveNow && liveInterval) {
      console.log("⏸ No active live matches — dropping down to passive cron cadence.");
      clearInterval(liveInterval);
      liveInterval = null;
    }

  } catch (e) {
    console.error("❌ API Fetch pipeline exception handled safely:", e.message);
  }
}

// Passive Poll Task Sequence (Runs every 14 minutes as scheduled in your frontend configurations)
cron.schedule("*/14 * * * *", () => {
  if (!liveInterval) fetchAndBroadcast("14-min-cron");
});

// Server Process Worker Keep-Alive Health Probe Loop
cron.schedule("*/10 * * * *", async () => {
  try {
    await fetch(`http://localhost:${PORT}/health`);
  } catch (e) {}
});

// App Initiation
server.listen(PORT, async () => {
  console.log(`\n🚀 WC2026 Service successfully bound to local port ${PORT}`);
  await fetchAndBroadcast("initial-startup-sync");
});

process.on("SIGTERM", () => {
  if (liveInterval) clearInterval(liveInterval);
  server.close(() => process.exit(0));
});

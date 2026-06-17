/**
 * WC2026 Live Score Backend — Auto-Updating Edition
 * Data Source: openfootball/worldcup.json (GitHub) — free, no API key, daily updates
 * Polls every 5 minutes during tournament hours, every 30 min otherwise
 * Broadcasts via WebSocket + serves REST fallback
 */

require("dotenv").config();
const express   = require("express");
const cors      = require("cors");
const http      = require("http");
const WebSocket = require("ws");
const cron      = require("node-cron");

const PORT       = process.env.PORT || 3001;
const DATA_URL   = "https://raw.githubusercontent.com/openfootball/worldcup.json/master/2026/worldcup.json";
const PASSIVE_MS = 30 * 60 * 1000; // 30 min when no active games
const ACTIVE_MS  =  5 * 60 * 1000; // 5 min during tournament hours (11am-11pm ET)

// ─── TEAM NAME NORMALIZATION ─────────────────────────────────────────────
const TEAM_MAP = {
  "Bosnia and Herzegovina": "Bosnia & Herz.",
  "Bosnia & Herzegovina": "Bosnia & Herz.",
  "Bosnia-Herzegovina": "Bosnia & Herz.",
  "Curaçao": "Curaçao",
  "Curacao": "Curaçao",
  "Côte d'Ivoire": "Ivory Coast",
  "DR Congo": "Congo DR",
  "Korea Republic": "South Korea",
  "Czech Republic": "Czechia",
  "USA": "United States",
  "Turkey": "Türkiye",
  "Türkiye": "Türkiye",
  "IR Iran": "Iran",
};

function norm(name) { return TEAM_MAP[name] || name; }

// ─── MATCH ID MAP (your existing groups B-L) ─────────────────────────────
const MATCH_ID_MAP = {
  "Canada|Bosnia & Herz.": "B1", "Qatar|Switzerland": "B2",
  "Switzerland|Bosnia & Herz.": "B3", "Canada|Qatar": "B4",
  "Switzerland|Canada": "B5", "Bosnia & Herz.|Qatar": "B6",
  "Brazil|Morocco": "C1", "Haiti|Scotland": "C2",
  "Scotland|Morocco": "C3", "Brazil|Haiti": "C4",
  "Scotland|Brazil": "C5", "Morocco|Haiti": "C6",
  "Germany|Curaçao": "E1", "Ivory Coast|Ecuador": "E2",
  "Germany|Ivory Coast": "E3", "Ecuador|Curaçao": "E4",
  "Ecuador|Germany": "E5", "Curaçao|Ivory Coast": "E6",
  "Netherlands|Japan": "F1", "Sweden|Tunisia": "F2",
  "Netherlands|Sweden": "F3", "Tunisia|Japan": "F4",
  "Japan|Sweden": "F5", "Tunisia|Netherlands": "F6",
  "Belgium|Egypt": "G1", "Iran|New Zealand": "G2",
  "Belgium|Iran": "G3", "New Zealand|Egypt": "G4",
  "Egypt|Iran": "G5", "New Zealand|Belgium": "G6",
  "Spain|Cape Verde": "H1", "Saudi Arabia|Uruguay": "H2",
  "Spain|Saudi Arabia": "H3", "Uruguay|Cape Verde": "H4",
  "Cape Verde|Saudi Arabia": "H5", "Uruguay|Spain": "H6",
  "France|Senegal": "I1", "Iraq|Norway": "I2",
  "France|Iraq": "I3", "Norway|Senegal": "I4",
  "Norway|France": "I5", "Senegal|Iraq": "I6",
  "Argentina|Algeria": "J1", "Austria|Jordan": "J2",
  "Argentina|Austria": "J3", "Jordan|Algeria": "J4",
  "Algeria|Austria": "J5", "Jordan|Argentina": "J6",
  "Portugal|Congo DR": "K1", "Uzbekistan|Colombia": "K2",
  "Portugal|Uzbekistan": "K3", "Colombia|Congo DR": "K4",
  "Colombia|Portugal": "K5", "Congo DR|Uzbekistan": "K6",
  "England|Croatia": "L1", "Ghana|Panama": "L2",
  "England|Ghana": "L3", "Panama|Croatia": "L4",
  "Croatia|England": "L5", "Ghana|Panama": "L6",
  // Group A (added for completeness)
  "Mexico|South Africa": "A1", "South Korea|Czechia": "A2",
  "Czechia|South Africa": "A3", "Mexico|South Korea": "A4",
  "Czechia|Mexico": "A5", "South Africa|South Korea": "A6",
  // Group D
  "United States|Paraguay": "D1", "Australia|Türkiye": "D2",
  "United States|Australia": "D3", "Türkiye|Paraguay": "D4",
  "Türkiye|United States": "D5", "Paraguay|Australia": "D6",
};

// ─── CACHE ───────────────────────────────────────────────────────────────
let cache = {
  all_matches: {},
  liveNow: false,
  lastFetch: null,
  fetchCount: 0,
  rawData: null,
};

// ─── PARSE OPENFOOTBALL DATA ────────────────────────────────────────────
function parseOpenfootball(raw) {
  if (!raw || !Array.isArray(raw.matches)) return { matchesMap: {}, liveNow: false };

  const matchesMap = {};
  let liveNow = false;

  raw.matches.forEach(m => {
    const home = norm(m.team1 || "");
    const away = norm(m.team2 || "");
    const key = `${home}|${away}`;
    const id = MATCH_ID_MAP[key];

    if (!id) return; // Skip matches not in our tracked groups

    // Determine match status
    let status = "upcoming";
    let homeGoals = "";
    let awayGoals = "";
    let goals = [];
    let elapsed = null;

    if (m.score) {
      if (m.score.ft) {
        status = "FT";
        homeGoals = String(m.score.ft[0]);
        awayGoals = String(m.score.ft[1]);
      } else if (m.score.ht) {
        status = "HT";
        homeGoals = String(m.score.ht[0]);
        awayGoals = String(m.score.ht[1]);
        elapsed = 45;
        liveNow = true;
      }

      // Parse goal scorers
      if (m.goals1 && Array.isArray(m.goals1)) {
        m.goals1.forEach(g => {
          goals.push({
            elapsed: parseInt(g.minute, 10) || 0,
            player: g.name,
            team: home,
            detail: "Goal"
          });
        });
      }
      if (m.goals2 && Array.isArray(m.goals2)) {
        m.goals2.forEach(g => {
          goals.push({
            elapsed: parseInt(g.minute, 10) || 0,
            player: g.name,
            team: away,
            detail: "Goal"
          });
        });
      }
      goals.sort((a, b) => a.elapsed - b.elapsed);
    }

    // Check if match is today and might be live (within game hours)
    const matchDate = new Date(m.date + "T12:00:00Z");
    const now = new Date();
    const isToday = matchDate.toDateString() === now.toDateString();
    const hour = now.getHours();
    const isGameHour = hour >= 11 && hour <= 23; // ET game hours roughly

    if (isToday && isGameHour && status === "upcoming") {
      // Match might be live but data not updated yet — mark as potential live
      liveNow = true;
    }

    matchesMap[id] = {
      homeGoals,
      awayGoals,
      source: "openfootball",
      status,
      elapsed,
      goals
    };
  });

  return { matchesMap, liveNow };
}

// ─── FETCH DATA ──────────────────────────────────────────────────────────
async function fetchData() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const res = await fetch(DATA_URL, {
      method: "GET",
      headers: { 
        "User-Agent": "WC2026-Tracker/1.0",
        "Accept": "application/json"
      },
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

// ─── EXPRESS ─────────────────────────────────────────────────────────────
const app = express();
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

app.get("/api/fixtures", (req, res) => res.json({
  all_matches: cache.all_matches,
  lastFetch: cache.lastFetch,
  liveNow: cache.liveNow,
  raw: cache.rawData
}));

// ─── WEBSOCKET ───────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server });

wss.on("connection", ws => {
  console.log(`📱 Client connected [${wss.clients.size} total]`);
  ws.send(JSON.stringify({
    type: "snapshot",
    all_matches: cache.all_matches,
    lastFetch: cache.lastFetch,
    liveNow: cache.liveNow
  }));
  ws.on("close", () => console.log(`📵 Client disconnected [${wss.clients.size} remaining]`));
  ws.on("error", e => console.error("WS error:", e.message));
});

function broadcast(data) {
  const msg = JSON.stringify(data);
  let active = 0;
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) {
      c.send(msg);
      active++;
    }
  });
  if (active > 0) console.log(`📡 Broadcasted to ${active} client(s)`);
}

// ─── FETCH & BROADCAST ───────────────────────────────────────────────────
let activeInterval = null;

async function fetchAndBroadcast(reason = "cron") {
  const timeLabel = new Date().toLocaleString("en-US", { timeZone: "America/New_York" });
  console.log(`\n⚽ Fetching data [${reason}] at ${timeLabel} ET`);

  try {
    const raw = await fetchData();
    cache.rawData = raw;
    const { matchesMap, liveNow } = parseOpenfootball(raw);

    // Deep merge — keep existing data, overwrite with new
    cache.all_matches = { ...cache.all_matches, ...matchesMap };
    cache.liveNow = liveNow;
    cache.lastFetch = new Date().toISOString();
    cache.fetchCount++;

    console.log(`✅ Cache updated: ${Object.keys(cache.all_matches).length} matches, live=${liveNow}`);

    broadcast({
      type: "update",
      all_matches: cache.all_matches,
      lastFetch: cache.lastFetch,
      liveNow
    });

    // Dynamic polling speed
    const now = new Date();
    const hour = now.getHours();
    const isTournamentHours = hour >= 10 && hour <= 24; // 10am-12am ET

    if (liveNow && !activeInterval) {
      console.log("🔴 Live matches detected — switching to 5-min polling");
      activeInterval = setInterval(() => fetchAndBroadcast("live-poll"), ACTIVE_MS);
    } else if (!liveNow && activeInterval) {
      console.log("⏸ No live matches — reverting to passive polling");
      clearInterval(activeInterval);
      activeInterval = null;
    }

  } catch (e) {
    console.error("❌ Fetch error:", e.message);
    // Don't clear cache on error — serve stale data
  }
}

// ─── CRON SCHEDULES ──────────────────────────────────────────────────────
// Tournament hours: every 5 min (Jun 11 - Jul 19, 2026)
// Off-hours: every 30 min
const isTournamentActive = () => {
  const now = new Date();
  const start = new Date("2026-06-11T00:00:00-04:00");
  const end = new Date("2026-07-20T00:00:00-04:00");
  return now >= start && now <= end;
};

// Primary cron: every 5 minutes during tournament, 30 min otherwise
cron.schedule("*/5 * * * *", () => {
  if (isTournamentActive()) fetchAndBroadcast("5-min-tournament");
});

cron.schedule("*/30 * * * *", () => {
  if (!isTournamentActive()) fetchAndBroadcast("30-min-offseason");
});

// Health check every 10 min
cron.schedule("*/10 * * * *", async () => {
  try {
    await fetch(`http://localhost:${PORT}/health`);
  } catch (e) {}
});

// ─── START ───────────────────────────────────────────────────────────────
server.listen(PORT, async () => {
  console.log(`\n🚀 WC2026 Backend running on port ${PORT}`);
  console.log(`📊 Data source: openfootball/worldcup.json`);
  console.log(`⏰ Tournament active: ${isTournamentActive()}`);
  await fetchAndBroadcast("initial-startup");
});

process.on("SIGTERM", () => {
  if (activeInterval) clearInterval(activeInterval);
  server.close(() => process.exit(0));
});

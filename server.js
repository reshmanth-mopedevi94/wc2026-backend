/**
 * WC2026 Live Score Backend
 * ─────────────────────────
 * Data source: openfootball/worldcup.json — free, no API key needed
 * URL: https://raw.githubusercontent.com/openfootball/worldcup.json/master/2026/worldcup.json
 * Polls every 14 minutes — broadcasts via WebSocket to all clients
 * No API key needed — completely free forever
 */

require("dotenv").config();
const express   = require("express");
const cors      = require("cors");
const http      = require("http");
const WebSocket = require("ws");
const cron      = require("node-cron");

// ─── CONFIG ────────────────────────────────────────────────────────────────
const PORT         = process.env.PORT || 3001;
const DATA_URL     = "https://raw.githubusercontent.com/openfootball/worldcup.json/master/2026/worldcup.json";
const BACKUP_URL   = "https://raw.githubusercontent.com/openfootball/worldcup.json/master/2026/worldcup.json";

// ─── IN-MEMORY CACHE ───────────────────────────────────────────────────────
let cache = {
  matches:      [],   // parsed match results
  raw:          null, // raw JSON from source
  lastFetch:    null,
  fetchCount:   0,
  lastError:    null,
};

// ─── TEAM NAME NORMALIZATION ───────────────────────────────────────────────
// Maps openfootball team names to our app's team names
const TEAM_NAME_MAP = {
  "Canada":          "Canada",
  "Bosnia-Herzegovina": "Bosnia & Herz.",
  "Bosnia and Herzegovina": "Bosnia & Herz.",
  "Qatar":           "Qatar",
  "Switzerland":     "Switzerland",
  "Brazil":          "Brazil",
  "Morocco":         "Morocco",
  "Haiti":           "Haiti",
  "Scotland":        "Scotland",
  "Germany":         "Germany",
  "Curacao":         "Curaçao",
  "Curaçao":         "Curaçao",
  "Ivory Coast":     "Ivory Coast",
  "Côte d'Ivoire":   "Ivory Coast",
  "Ecuador":         "Ecuador",
  "Netherlands":     "Netherlands",
  "Japan":           "Japan",
  "Tunisia":         "Tunisia",
  "Sweden":          "Sweden",
  "Belgium":         "Belgium",
  "Egypt":           "Egypt",
  "Iran":            "Iran",
  "New Zealand":     "New Zealand",
  "Spain":           "Spain",
  "Cape Verde":      "Cape Verde",
  "Saudi Arabia":    "Saudi Arabia",
  "Uruguay":         "Uruguay",
  "France":          "France",
  "Senegal":         "Senegal",
  "Norway":          "Norway",
  "Iraq":            "Iraq",
  "Argentina":       "Argentina",
  "Algeria":         "Algeria",
  "Austria":         "Austria",
  "Jordan":          "Jordan",
  "Portugal":        "Portugal",
  "Uzbekistan":      "Uzbekistan",
  "Colombia":        "Colombia",
  "Congo DR":        "Congo DR",
  "DR Congo":        "Congo DR",
  "England":         "England",
  "Croatia":         "Croatia",
  "Ghana":           "Ghana",
  "Panama":          "Panama",
  "Mexico":          "Mexico",
  "South Africa":    "South Africa",
  "Korea Republic":  "South Korea",
  "South Korea":     "South Korea",
  "Czechia":         "Czechia",
  "Czech Republic":  "Czechia",
  "United States":   "United States",
  "USA":             "United States",
  "Paraguay":        "Paraguay",
  "Australia":       "Australia",
  "Turkey":          "Türkiye",
  "Türkiye":         "Türkiye",
};

function normalizeName(name) {
  return TEAM_NAME_MAP[name] || name;
}

// ─── MATCH ID MAPPING ──────────────────────────────────────────────────────
// Map home+away team to our internal match IDs
const MATCH_ID_MAP = {
  "Canada|Bosnia & Herz.":    "B1",
  "Qatar|Switzerland":        "B2",
  "Switzerland|Bosnia & Herz.": "B3",
  "Canada|Qatar":             "B4",
  "Switzerland|Canada":       "B5",
  "Bosnia & Herz.|Qatar":     "B6",
  "Brazil|Morocco":           "C1",
  "Haiti|Scotland":           "C2",
  "Scotland|Morocco":         "C3",
  "Brazil|Haiti":             "C4",
  "Scotland|Brazil":          "C5",
  "Morocco|Haiti":            "C6",
  "Germany|Curaçao":          "E1",
  "Ivory Coast|Ecuador":      "E2",
  "Germany|Ivory Coast":      "E3",
  "Ecuador|Curaçao":          "E4",
  "Ecuador|Germany":          "E5",
  "Curaçao|Ivory Coast":      "E6",
  "Netherlands|Japan":        "F1",
  "Sweden|Tunisia":           "F2",
  "Netherlands|Sweden":       "F3",
  "Tunisia|Japan":            "F4",
  "Japan|Sweden":             "F5",
  "Tunisia|Netherlands":      "F6",
  "Belgium|Egypt":            "G1",
  "Iran|New Zealand":         "G2",
  "Belgium|Iran":             "G3",
  "New Zealand|Egypt":        "G4",
  "Egypt|Iran":               "G5",
  "New Zealand|Belgium":      "G6",
  "Spain|Cape Verde":         "H1",
  "Saudi Arabia|Uruguay":     "H2",
  "Spain|Saudi Arabia":       "H3",
  "Uruguay|Cape Verde":       "H4",
  "Cape Verde|Saudi Arabia":  "H5",
  "Uruguay|Spain":            "H6",
  "France|Senegal":           "I1",
  "Iraq|Norway":              "I2",
  "France|Iraq":              "I3",
  "Norway|Senegal":           "I4",
  "Norway|France":            "I5",
  "Senegal|Iraq":             "I6",
  "Argentina|Algeria":        "J1",
  "Austria|Jordan":           "J2",
  "Argentina|Austria":        "J3",
  "Jordan|Algeria":           "J4",
  "Algeria|Austria":          "J5",
  "Jordan|Argentina":         "J6",
  "Portugal|Congo DR":        "K1",
  "Uzbekistan|Colombia":      "K2",
  "Portugal|Uzbekistan":      "K3",
  "Colombia|Congo DR":        "K4",
  "Colombia|Portugal":        "K5",
  "Congo DR|Uzbekistan":      "K6",
  "England|Croatia":          "L1",
  "Ghana|Panama":             "L2",
  "England|Ghana":            "L3",
  "Panama|Croatia":           "L4",
  "Croatia|England":          "L5",
  "Ghana|Panama":             "L6",
  "Mexico|South Africa":      "A1",
  "South Korea|Czechia":      "A2",
  "Czechia|South Africa":     "A3",
  "Mexico|South Korea":       "A4",
  "Czechia|Mexico":           "A5",
  "South Africa|South Korea": "A6",
};

// ─── PARSE openfootball JSON → our match format ────────────────────────────
function parseOpenfootball(data) {
  const matches = [];
  if (!data || !Array.isArray(data.rounds)) return matches;

  data.rounds.forEach(round => {
    if (!Array.isArray(round.matches)) return;
    round.matches.forEach(m => {
      const home = normalizeName(m.team1?.name || m.team1 || "");
      const away = normalizeName(m.team2?.name || m.team2 || "");
      const key  = `${home}|${away}`;
      const id   = MATCH_ID_MAP[key];
      if (!id) return; // not a tracked match

      // Determine if played
      const played = m.score1 !== null && m.score1 !== undefined &&
                     m.score2 !== null && m.score2 !== undefined;

      // Parse goal scorers
      const goals = [];
      if (Array.isArray(m.goals1)) {
        m.goals1.forEach(g => {
          goals.push({
            elapsed: g.minute || 0,
            extra:   g.offset || null,
            player:  g.name || "Unknown",
            team:    home,
            detail:  g.owngoal ? "Own Goal" : g.penalty ? "Penalty" : "Normal Goal"
          });
        });
      }
      if (Array.isArray(m.goals2)) {
        m.goals2.forEach(g => {
          goals.push({
            elapsed: g.minute || 0,
            extra:   g.offset || null,
            player:  g.name || "Unknown",
            team:    away,
            detail:  g.owngoal ? "Own Goal" : g.penalty ? "Penalty" : "Normal Goal"
          });
        });
      }
      goals.sort((a, b) => a.elapsed - b.elapsed);

      matches.push({
        id,
        home,
        away,
        date:      m.date,
        played,
        homeGoals: played ? String(m.score1) : null,
        awayGoals: played ? String(m.score2) : null,
        source:    "openfootball",
        goals,
      });
    });
  });

  return matches;
}

// ─── EXPRESS + HTTP SERVER ─────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
app.use(cors({ origin: "*" }));
app.use(express.json());

// Health check
app.get("/health", (req, res) => {
  res.json({
    status:     "ok",
    lastFetch:  cache.lastFetch,
    fetchCount: cache.fetchCount,
    matches:    cache.matches.length,
    lastError:  cache.lastError,
    uptime:     Math.floor(process.uptime()) + "s",
    dataSource: "openfootball/worldcup.json (free, no key)",
  });
});

// REST endpoint — frontend polls this if WebSocket disconnects
app.get("/api/fixtures", (req, res) => {
  res.json({
    matches:   cache.matches,
    lastFetch: cache.lastFetch,
    source:    "openfootball",
  });
});

// ─── WEBSOCKET SERVER ──────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server });

wss.on("connection", (ws) => {
  console.log(`📱 Client connected [${wss.clients.size} total]`);

  // Send cached data immediately on connect
  if (cache.matches.length > 0) {
    ws.send(JSON.stringify({
      type:      "snapshot",
      matches:   cache.matches,
      lastFetch: cache.lastFetch,
    }));
  }

  ws.on("close", () => {
    console.log(`📵 Client disconnected [${wss.clients.size} remaining]`);
  });

  ws.on("error", (err) => {
    console.error("WS client error:", err.message);
  });
});

function broadcast(data) {
  const msg   = JSON.stringify(data);
  let   count = 0;
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
      count++;
    }
  });
  if (count > 0) console.log(`📡 Broadcast to ${count} client(s)`);
}

// ─── FETCH FROM openfootball ───────────────────────────────────────────────
async function fetchAndBroadcast(reason = "cron") {
  const now = new Date().toLocaleString("en-US", { timeZone: "America/New_York" });
  console.log(`\n⚽ Fetching [${reason}] at ${now} ET`);

  const controller = new AbortController();
  const timeout    = setTimeout(() => controller.abort(), 15000);

  try {
    const res = await fetch(DATA_URL, {
      method:  "GET",
      headers: { "User-Agent": "WC2026-Tracker/1.0" },
      signal:  controller.signal,
    });
    clearTimeout(timeout);

    if (!res.ok) {
      throw new Error(`HTTP ${res.status} from openfootball`);
    }

    const raw     = await res.json();
    const matches = parseOpenfootball(raw);
    const played  = matches.filter(m => m.played).length;

    cache.matches   = matches;
    cache.raw       = raw;
    cache.lastFetch = new Date().toISOString();
    cache.fetchCount++;
    cache.lastError = null;

    console.log(`✅ ${matches.length} matches parsed | ${played} played | fetch #${cache.fetchCount}`);

    broadcast({
      type:      "update",
      matches:   cache.matches,
      lastFetch: cache.lastFetch,
    });

  } catch (e) {
    clearTimeout(timeout);
    cache.lastError = e.message;
    console.error(`❌ Fetch failed: ${e.message}`);
  }
}

// ─── CRON: every 14 minutes ────────────────────────────────────────────────
cron.schedule("*/14 * * * *", () => {
  fetchAndBroadcast("14-min-cron");
});

// ─── KEEP-ALIVE: ping self every 10 min ────────────────────────────────────
cron.schedule("*/10 * * * *", async () => {
  try {
    await fetch(`http://localhost:${PORT}/health`);
    console.log(`💓 Keep-alive | uptime: ${Math.floor(process.uptime())}s | fetches: ${cache.fetchCount}`);
  } catch (e) { /* not ready yet */ }
});

// ─── START ─────────────────────────────────────────────────────────────────
server.listen(PORT, async () => {
  console.log(`\n🚀 WC2026 Backend on port ${PORT}`);
  console.log(`📦 Data: openfootball/worldcup.json (free, no API key)`);
  console.log(`⏱  Polling every 14 minutes\n`);
  await fetchAndBroadcast("startup");
});

process.on("SIGTERM", () => {
  console.log("Shutting down...");
  server.close(() => process.exit(0));
});

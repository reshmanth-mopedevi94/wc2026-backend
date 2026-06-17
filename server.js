/**
 * WC2026 Live Score Backend - Fixed Compatibility Edition
 * ─────────────────────────────────────────────────────────
 * Data: wcup2026.org — free, no API key, live scores + results
 * Endpoints:
 * today:  https://wcup2026.org/api/data.php?action=today
 * all:    https://wcup2026.org/api/data.php?action=all
 * Polls every 14 min passive, every 5 min when live match detected
 * Broadcasts via WebSocket to all clients in the format expected by App.jsx
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

// ─── CACHE MAP FOR ALL_MATCHES COMPATIBILITY ───────────────────────────────
let cache = {
  all_matches: {}, // Stored keyed by match ID to easily map into the UI structure
  liveNow:     false,
  lastFetch:   null,
  fetchCount:  0,
};

// ─── PARSE wcup2026.org response directly into frontend model ──────────────
function parseMatches(raw){
  if(!raw || !Array.isArray(raw.matches)) return { matchesMap: {}, liveNow: false };
  const matchesMap = {};
  let liveNow = false;

  raw.matches.forEach(m => {
    const home = norm(m.team1 || "");
    const away = norm(m.team2 || "");
    const key  = `${home}|${away}`;
    const id   = MATCH_ID_MAP[key];
    if(!id) return;

    const status     = m.status || "upcoming";
    const isLive     = status === "live";
    const score      = Array.isArray(m.score) && m.score.length === 2 ? m.score : null;

    if(isLive) liveNow = true;

    // Convert API objects explicitly to match the frontend key mapping expectations
    matchesMap[id] = {
      homeGoals: score ? String(score) : "",
      awayGoals: score ? String(score) : "",
      source: "openfootball",
      goals: Array.isArray(m.goals) ? m.goals : []
    };
  });

  return { matchesMap, liveNow };
}

// ─── FETCH FROM APIS ────────────────────────────────────────────────────────
async function fetchData(action = "today"){
  const url = `${BASE_URL}?action=${action}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 12000);
  try{
    const res = await fetch(url, {
      method: "GET",
      headers: { "User-Agent": "WC2026-Tracker/1.0 (github.com/reshmanth-mopedevi94)" },
      signal: controller.signal,
    });
    clearTimeout(timeout);
    if(!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  }catch(e){
    clearTimeout(timeout);
    throw e;
  }
}

// ─── EXPRESS CONFIGURATION ──────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
app.use(cors({ origin:"*" }));

app.get("/health", (req,res) => res.json({
  status: "ok", 
  lastFetch: cache.lastFetch,
  fetchCount: cache.fetchCount, 
  matchesTracked: Object.keys(cache.all_matches).length,
  liveNow: cache.liveNow, 
  uptime: Math.floor(process.uptime())+"s",
}));

// Route matches structure for REST fallbacks
app.get("/api/fixtures", (req,res) => res.json({
  all_matches: cache.all_matches, 
  lastFetch: cache.lastFetch, 
  liveNow: cache.liveNow,
}));

// ─── WEBSOCKET FUNCTIONALITY ───────────────────────────────────────────────
const wss = new WebSocket.Server({ server });

wss.on("connection", ws => {
  console.log(`📱 Client connected [${wss.clients.size} total]`);
  if(Object.keys(cache.all_matches).length > 0){
    ws.send(JSON.stringify({
      type: "snapshot", 
      all_matches: cache.all_matches,
      lastFetch: cache.lastFetch, 
      liveNow: cache.liveNow,
    }));
  }
  ws.on("close", () => console.log(`📵 Client left [${wss.clients.size} remaining]`));
  ws.on("error", e => console.error("WS error:", e.message));
});

function broadcast(data){
  const msg = JSON.stringify(data);
  let n = 0;
  wss.clients.forEach(c => { if(c.readyState===WebSocket.OPEN){ c.send(msg); n++; }});
  if(n>0) console.log(`📡 Broadcast to ${n} client(s)`);
}

// ─── MAIN FETCH AND SYNC CORE ───────────────────────────────────────────────
let liveInterval = null;

async function fetchAndBroadcast(reason = "cron"){
  const now = new Date().toLocaleString("en-US",{timeZone:"America/New_York"});
  console.log(`\n⚽ Fetch [${reason}] at ${now} ET`);
  try{
    // Pull active data from your free, key-less endpoint
    const raw = await fetchData("today");
    const { matchesMap, liveNow } = parseMatches(raw);

    // Deep merge updates into the state registry cache
    cache.all_matches = { ...cache.all_matches, ...matchesMap };
    cache.liveNow     = liveNow;
    cache.lastFetch   = new Date().toISOString();
    cache.fetchCount++;

    console.log(`✅ Synced matches map size: ${Object.keys(cache.all_matches).length} | live state: ${liveNow} | fetch #${cache.fetchCount}`);

    // Pushing structural update mimicking openfootball framework signature cleanly matching frontend whitelist structures
    broadcast({
      type: "update", 
      all_matches: cache.all_matches,
      lastFetch: cache.lastFetch, 
      liveNow,
    });

    // Toggle Polling cadences responsively based on dynamic context 
    if(liveNow && !liveInterval){
      console.log("🔴 Live match found — speeding up polling to 5-min intervals");
      liveInterval = setInterval(()=>fetchAndBroadcast("live-poll"), LIVE_MS);
    } else if(!liveNow && liveInterval){
      console.log("⏸ No live matches running — stepping back to 14-min passive cron cycles");
      clearInterval(liveInterval);
      liveInterval = null;
    }

  }catch(e){
    console.error("❌ Fetch error handled:", e.message);
  }
}

// Passive Routine Cycle
cron.schedule("*/14 * * * *", () => {
  if(!liveInterval) fetchAndBroadcast("14-min-cron");
});

// Production Worker Health Keep Alive Routine
cron.schedule("*/10 * * * *", async () => {
  try{
    await fetch(`http://localhost:${PORT}/health`);
    console.log(`💓 Keep-alive update | uptime: ${Math.floor(process.uptime())}s | total fetches: ${cache.fetchCount} | live match right now: ${cache.liveNow}`);
  }catch(e){}
});

// App Initiation Engine
server.listen(PORT, async () => {
  console.log(`\n🚀 WC2026 Backend operating on port ${PORT}`);
  console.log(`📦 Data payload sync framework configured for client apps.`);
  await fetchAndBroadcast("startup");
});

process.on("SIGTERM", () => {
  if(liveInterval) clearInterval(liveInterval);
  server.close(() => process.exit(0));
});

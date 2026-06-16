/**
 * WC2026 Live Score Backend
 * ─────────────────────────
 * Data: wcup2026.org — free, no API key, live scores + results
 * Endpoints:
 *   today:  https://wcup2026.org/api/data.php?action=today
 *   all:    https://wcup2026.org/api/data.php?action=all
 * Polls every 14 min passive, every 60s when live match detected
 * Broadcasts via WebSocket to all clients instantly
 */

require("dotenv").config();
const express   = require("express");
const cors      = require("cors");
const http      = require("http");
const WebSocket = require("ws");
const cron      = require("node-cron");

const PORT          = process.env.PORT || 3001;
const BASE_URL      = "https://wcup2026.org/api/data.php";
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

// ─── CACHE ──────────────────────────────────────────────────────────────────
let cache = {
  matches:    [],
  liveNow:    false,
  lastFetch:  null,
  fetchCount: 0,
};

// ─── PARSE wcup2026.org response ────────────────────────────────────────────
function parseMatches(raw){
  if(!raw||!Array.isArray(raw.matches)) return {matches:[],liveNow:false};
  const matches = [];
  let liveNow = false;

  raw.matches.forEach(m => {
    const home = norm(m.team1||"");
    const away = norm(m.team2||"");
    const key  = `${home}|${away}`;
    const id   = MATCH_ID_MAP[key];
    if(!id) return;

    const status    = m.status || "upcoming";
    const isLive    = status === "live";
    const isFinished= status === "finished";
    const score     = Array.isArray(m.score) && m.score.length === 2 ? m.score : null;

    if(isLive) liveNow = true;

    matches.push({
      id,
      home,
      away,
      date:      m.date,
      status,
      homeGoals: score ? String(score[0]) : null,
      awayGoals: score ? String(score[1]) : null,
      elapsed:   isLive ? (m.live_minute || 0) : null,
      played:    isFinished || (isLive && score !== null),
      source:    "wcup2026.org",
    });
  });

  return { matches, liveNow };
}

// ─── FETCH ──────────────────────────────────────────────────────────────────
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

// ─── EXPRESS ────────────────────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
app.use(cors({ origin:"*" }));

app.get("/health", (req,res) => res.json({
  status:"ok", lastFetch:cache.lastFetch,
  fetchCount:cache.fetchCount, matches:cache.matches.length,
  liveNow:cache.liveNow, uptime:Math.floor(process.uptime())+"s",
}));

app.get("/api/fixtures", (req,res) => res.json({
  matches:cache.matches, lastFetch:cache.lastFetch, liveNow:cache.liveNow,
}));

// ─── WEBSOCKET ──────────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server });

wss.on("connection", ws => {
  console.log(`📱 Client connected [${wss.clients.size} total]`);
  if(cache.matches.length > 0){
    ws.send(JSON.stringify({
      type:"snapshot", matches:cache.matches,
      lastFetch:cache.lastFetch, liveNow:cache.liveNow,
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

// ─── MAIN FETCH + BROADCAST ─────────────────────────────────────────────────
let liveInterval = null;

async function fetchAndBroadcast(reason = "cron"){
  const now = new Date().toLocaleString("en-US",{timeZone:"America/New_York"});
  console.log(`\n⚽ Fetch [${reason}] at ${now} ET`);
  try{
    // Fetch today's matches (includes live + finished today)
    const raw = await fetchData("today");
    const { matches, liveNow } = parseMatches(raw);
    const played = matches.filter(m=>m.played).length;

    // Merge with existing cache — keep past results
    const existing = {};
    cache.matches.forEach(m => { existing[m.id] = m; });
    matches.forEach(m => { existing[m.id] = m; });
    cache.matches   = Object.values(existing);
    cache.liveNow   = liveNow;
    cache.lastFetch = new Date().toISOString();
    cache.fetchCount++;

    console.log(`✅ ${matches.length} today | ${played} played | live: ${liveNow} | fetch #${cache.fetchCount}`);

    broadcast({
      type:"update", matches:cache.matches,
      lastFetch:cache.lastFetch, liveNow,
    });

    // Switch polling speed based on live status
    if(liveNow && !liveInterval){
      console.log("🔴 Live match — switching to 5-min polling");
      liveInterval = setInterval(()=>fetchAndBroadcast("live-poll"), LIVE_MS);
    } else if(!liveNow && liveInterval){
      console.log("⏸  No live matches — back to 14-min passive");
      clearInterval(liveInterval);
      liveInterval = null;
    }

  }catch(e){
    console.error("❌ Fetch error:", e.message);
  }
}

// ─── CRON: every 14 min ─────────────────────────────────────────────────────
cron.schedule("*/14 * * * *", () => {
  if(!liveInterval) fetchAndBroadcast("14-min-cron");
});

// ─── KEEP-ALIVE ──────────────────────────────────────────────────────────────
cron.schedule("*/10 * * * *", async () => {
  try{
    await fetch(`http://localhost:${PORT}/health`);
    console.log(`💓 Keep-alive | uptime:${Math.floor(process.uptime())}s | fetches:${cache.fetchCount} | live:${cache.liveNow}`);
  }catch(e){}
});

// ─── START ───────────────────────────────────────────────────────────────────
server.listen(PORT, async () => {
  console.log(`\n🚀 WC2026 Backend on port ${PORT}`);
  console.log(`📦 Source: wcup2026.org (free, no key, live scores)`);
  console.log(`⏱  Passive: every 14 min | Live: every 5 min\n`);
  await fetchAndBroadcast("startup");
});

process.on("SIGTERM", () => {
  if(liveInterval) clearInterval(liveInterval);
  server.close(() => process.exit(0));
});

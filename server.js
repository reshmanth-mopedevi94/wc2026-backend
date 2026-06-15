/**
 * WC2026 Live Score Backend
 * ─────────────────────────
 * - Polls API-Football every 14 minutes for ALL fixtures in one call
 * - Budget: 100 req/day ÷ 14min = ~102 req/day — stays within free limit
 * - Broadcasts updates to all clients via WebSocket instantly
 * - API key stored only on server — never sent to client
 * - Falls back gracefully if API is unavailable
 */

require("dotenv").config();
const express   = require("express");
const cors      = require("cors");
const http      = require("http");
const WebSocket = require("ws");
const cron      = require("node-cron");

// ─── CONFIG ────────────────────────────────────────────────────────────────
const PORT     = process.env.PORT || 3001;
const API_KEY  = process.env.WC_API_KEY;
const API_HOST = "v3.football.api-sports.io";
const WC_LEAGUE = 1;
const WC_SEASON = 2026;

if (!API_KEY) {
  console.error("❌  WC_API_KEY not set. Add it to .env or Render environment.");
  process.exit(1);
}

// ─── IN-MEMORY CACHE ───────────────────────────────────────────────────────
let cache = {
  fixtures:     [],
  lastFetch:    null,
  requestsUsed: 0,
};

// ─── EXPRESS + HTTP SERVER ─────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
app.use(cors({ origin: "*" }));
app.use(express.json());

// Health check — Render uses this to keep server alive
app.get("/health", (req, res) => {
  res.json({
    status:       "ok",
    lastFetch:    cache.lastFetch,
    requestsUsed: cache.requestsUsed,
    fixtures:     cache.fixtures.length,
    uptime:       Math.floor(process.uptime()) + "s",
  });
});

// REST endpoint — frontend polls this if WebSocket disconnects
app.get("/api/fixtures", (req, res) => {
  res.json({
    fixtures:  cache.fixtures,
    lastFetch: cache.lastFetch,
  });
});

// ─── WEBSOCKET SERVER ──────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server });

wss.on("connection", (ws) => {
  console.log(`📱 Client connected [${wss.clients.size} total]`);

  // Send cached data immediately on connect
  if (cache.fixtures.length > 0) {
    ws.send(JSON.stringify({
      type:      "snapshot",
      fixtures:  cache.fixtures,
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
  const msg = JSON.stringify(data);
  let count = 0;
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
      count++;
    }
  });
  if (count > 0) console.log(`📡 Broadcast to ${count} client(s)`);
}

// ─── API FETCH ─────────────────────────────────────────────────────────────
async function apiFetch(path) {
  const url = `https://${API_HOST}${path}`;
  console.log(`\n🌐 API call #${cache.requestsUsed + 1}: ${url}`);
  console.log(`🔑 Key length: ${API_KEY ? API_KEY.length : "MISSING"} chars`);

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 12000);

  try {
    const res = await fetch(url, {
      method: "GET",
      headers: {
        "x-apisports-key": API_KEY
      },
      signal: controller.signal,
    });
    clearTimeout(timeout);

    console.log(`📡 HTTP Status: ${res.status} ${res.statusText}`);

    // Log all response headers for debugging
    const headersObj = {};
    res.headers.forEach((v, k) => { headersObj[k] = v; });
    console.log(`📋 Response headers:`, JSON.stringify(headersObj));

    const rawText = await res.text();
    console.log(`📄 Raw response (first 500 chars): ${rawText.slice(0, 500)}`);

    if (res.status === 429) {
      console.warn("⚠️  Rate limited (429)");
      return null;
    }
    if (!res.ok) {
      console.error(`❌  API HTTP ${res.status}`);
      return null;
    }

    let data;
    try {
      data = JSON.parse(rawText);
    } catch(e) {
      console.error("❌  JSON parse failed:", e.message);
      return null;
    }

    // Log errors array from API response
    if (data.errors && Object.keys(data.errors).length > 0) {
      console.error("❌  API errors:", JSON.stringify(data.errors));
    }

    cache.requestsUsed++;
    console.log(`✅  Results: ${data.results} | Response length: ${data.response?.length || 0} | Total requests: ${cache.requestsUsed}`);
    return data.response || [];

  } catch (e) {
    clearTimeout(timeout);
    if (e.name === "AbortError") console.error("❌  Request timed out");
    else console.error("❌  Fetch error:", e.message);
    return null;
  }
}

// ─── CORE FETCH — ONE CALL GETS EVERYTHING ─────────────────────────────────
// Single API call fetches all WC 2026 fixtures — live, finished, upcoming.
// Runs every 14 minutes. Uses 1 request per cycle = ~102 req/day max.
async function fetchAndBroadcast(reason = "cron") {
  console.log(`\n⚽ Fetching [${reason}] at ${new Date().toLocaleString("en-US", { timeZone: "America/New_York" })} ET`);

  // ONE call — gets all fixtures including live scores, FT results, upcoming
  const fixtures = await apiFetch(
    `/fixtures?league=${WC_LEAGUE}&season=${WC_SEASON}`
  );

  if (fixtures === null) {
    console.warn("⏭  Skipping broadcast — API call failed or rate limited");
    return;
  }

  const liveCount = fixtures.filter(f => {
    const s = f.fixture?.status?.short;
    return ["1H","HT","2H","ET","BT","P","INT"].includes(s);
  }).length;

  cache.fixtures  = fixtures;
  cache.lastFetch = new Date().toISOString();

  console.log(`📊 ${fixtures.length} total fixtures | 🔴 ${liveCount} live now`);

  broadcast({
    type:      "update",
    fixtures:  cache.fixtures,
    lastFetch: cache.lastFetch,
  });
}

// ─── CRON: every 14 minutes ────────────────────────────────────────────────
// "*/14 * * * *" = minute 0, 14, 28, 42, 56 of every hour
cron.schedule("*/14 * * * *", () => {
  fetchAndBroadcast("14-min-cron");
});

// ─── KEEP-ALIVE: ping self every 10 min (Render free tier sleeps at 15 min)
cron.schedule("*/10 * * * *", async () => {
  try {
    const res = await fetch(`http://localhost:${PORT}/health`);
    const data = await res.json();
    console.log(`💓 Keep-alive | uptime: ${data.uptime} | requests: ${data.requestsUsed}`);
  } catch (e) {
    // Server not ready yet — ignore
  }
});

// ─── START ─────────────────────────────────────────────────────────────────
server.listen(PORT, async () => {
  console.log(`\n🚀 WC2026 Backend on port ${PORT}`);
  console.log(`🔑 API key: ${API_KEY ? "✅ set" : "❌ MISSING"}`);
  console.log(`📅 League ${WC_LEAGUE} | Season ${WC_SEASON}`);
  console.log(`⏱  Polling every 14 minutes (~102 req/day)\n`);
  // Fetch immediately on startup
  await fetchAndBroadcast("startup");
});

process.on("SIGTERM", () => {
  console.log("Shutting down...");
  server.close(() => process.exit(0));
});

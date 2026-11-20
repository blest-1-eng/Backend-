const path = require("path");
require("dotenv").config({ path: path.join(__dirname, ".env") });
const crypto = require("crypto");
const express = require("express");
const cors = require("cors");
const rateLimit = require("express-rate-limit");
const admin = require("firebase-admin");

if (!admin.apps.length) {
  admin.initializeApp({
    projectId: process.env.FIREBASE_PROJECT_ID || undefined,
  });
}

const app = express();
const PORT = Number(process.env.PORT || 8080);
const GROQ_BASE_URL = (process.env.GROQ_BASE_URL || "https://api.groq.com/openai/v1").replace(/\/$/, "");
const GROQ_MODEL = process.env.GROQ_MODEL || "openai/gpt-oss-120b";
const GROQ_API_KEY = (process.env.GROQ_API_KEY || "").trim();

const REQUIRE_APP_CHECK = String(process.env.REQUIRE_APP_CHECK || "false").toLowerCase() === "true";
const REQUIRE_FIREBASE_AUTH = String(process.env.REQUIRE_FIREBASE_AUTH || "false").toLowerCase() === "true";

// Groq Free-Tier Guardrails (measured: ~1000 RPD, ~8000 TPM on openai/gpt-oss-120b)
const ORG_DAILY_REQ_LIMIT = Number(process.env.NYRA_ORG_DAILY_REQ_LIMIT || 900);
const ORG_DAILY_TOKEN_LIMIT = Number(process.env.NYRA_ORG_DAILY_TOKEN_LIMIT || 180000);
const USER_DAILY_REQ_LIMIT = Number(process.env.NYRA_USER_DAILY_REQ_LIMIT || 25);
const USER_RPM_LIMIT = Number(process.env.NYRA_USER_RPM_LIMIT || 6);
const MAX_CONCURRENT_UPSTREAM = Number(process.env.NYRA_MAX_CONCURRENT_UPSTREAM || 4);
const CACHE_TTL_MS = Number(process.env.NYRA_CACHE_TTL_MS || 6 * 60 * 60 * 1000);
const MAX_CACHE_ENTRIES = 500;
const UPSTREAM_TIMEOUT_MS = 28000;

const QUOTA_BREAK_MESSAGE = "NYRA is taking a little break. Please try again later!";

app.use(cors({ origin: process.env.ALLOWED_ORIGINS || "*" }));
app.use(express.json({ limit: "128kb" }));

const ipLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 40,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (_req, res) => {
    res.status(429).json({
      code: "RATE_LIMITED",
      userMessage: QUOTA_BREAK_MESSAGE,
      error: QUOTA_BREAK_MESSAGE,
      retryAfterSeconds: 60,
    });
  },
});
app.use("/v1/nyra", ipLimiter);

// ---- Quota, Cooldown, Caching, In-Flight Coalescing & Concurrency State ----
let currentDayKey = new Date().toISOString().slice(0, 10);
let orgRequestsToday = 0;
let orgTokensToday = 0;
let userDailyUsage = new Map(); // uid -> { count, tokens }
let userMinuteBuckets = new Map(); // uid -> [timestamps]
let upstreamCooldownUntilMs = 0;
let lastGroqHeaders = {
  limitRequests: null,
  remainingRequests: null,
  limitTokens: null,
  remainingTokens: null,
  resetRequests: null,
  resetTokens: null,
};

const responseCache = new Map(); // cacheKey -> { expiresAt, payload, tokensUsed }
const inFlightRequests = new Map(); // cacheKey -> Promise<object>
let activeUpstreamCalls = 0;
const waitQueue = [];

function resetDailyCountersIfNeeded() {
  const today = new Date().toISOString().slice(0, 10);
  if (today !== currentDayKey) {
    currentDayKey = today;
    orgRequestsToday = 0;
    orgTokensToday = 0;
    userDailyUsage.clear();
  }
}

function getCached(key) {
  const hit = responseCache.get(key);
  if (!hit) return null;
  if (Date.now() > hit.expiresAt) {
    responseCache.delete(key);
    return null;
  }
  // LRU refresh
  responseCache.delete(key);
  responseCache.set(key, hit);
  return hit.payload;
}

function setCached(key, payload) {
  if (responseCache.size >= MAX_CACHE_ENTRIES) {
    const oldestKey = responseCache.keys().next().value;
    if (oldestKey) responseCache.delete(oldestKey);
  }
  responseCache.set(key, { expiresAt: Date.now() + CACHE_TTL_MS, payload });
}

function hashKey(raw) {
  return crypto.createHash("sha256").update(raw).digest("hex").slice(0, 32);
}

function acquireUpstreamSlot() {
  if (activeUpstreamCalls < MAX_CONCURRENT_UPSTREAM) {
    activeUpstreamCalls += 1;
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      const idx = waitQueue.indexOf(grant);
      if (idx >= 0) waitQueue.splice(idx, 1);
      reject(Object.assign(new Error(QUOTA_BREAK_MESSAGE), { status: 429 }));
    }, 8000);
    const grant = () => {
      clearTimeout(timer);
      activeUpstreamCalls += 1;
      resolve();
    };
    waitQueue.push(grant);
  });
}

function releaseUpstreamSlot() {
  activeUpstreamCalls = Math.max(0, activeUpstreamCalls - 1);
  const next = waitQueue.shift();
  if (next) next();
}

function parseGroqDurationMs(raw) {
  if (!raw || typeof raw !== "string") return 60000;
  let totalMs = 0;
  const mins = raw.match(/(\d+(?:\.\d+)?)m/);
  const secs = raw.match(/(\d+(?:\.\d+)?)s/);
  const ms = raw.match(/(\d+(?:\.\d+)?)ms/);
  if (mins) totalMs += parseFloat(mins[1]) * 60000;
  if (secs && !raw.endsWith("ms")) totalMs += parseFloat(secs[1]) * 1000;
  if (ms) totalMs += parseFloat(ms[1]);
  return Math.max(5000, Math.min(totalMs || 60000, 15 * 60 * 1000));
}

function updateRateLimitSnapshot(headers) {
  if (!headers) return;
  const get = (h) => headers.get(h);
  lastGroqHeaders = {
    limitRequests: get("x-ratelimit-limit-requests"),
    remainingRequests: get("x-ratelimit-remaining-requests"),
    limitTokens: get("x-ratelimit-limit-tokens"),
    remainingTokens: get("x-ratelimit-remaining-tokens"),
    resetRequests: get("x-ratelimit-reset-requests"),
    resetTokens: get("x-ratelimit-reset-tokens"),
  };
  const remReq = Number(lastGroqHeaders.remainingRequests);
  if (!Number.isNaN(remReq) && remReq <= 0) {
    const waitMs = parseGroqDurationMs(lastGroqHeaders.resetRequests);
    upstreamCooldownUntilMs = Math.max(upstreamCooldownUntilMs, Date.now() + waitMs);
  }
}

async function verifyAppCheckIfConfigured(req, res, next) {
  const appCheckToken = req.header("X-Firebase-AppCheck");
  if (!appCheckToken) {
    if (REQUIRE_APP_CHECK) {
      return res.status(401).json({
        code: "APP_CHECK_MISSING",
        userMessage: "App verification failed. Please update or restart Fun & Learn.",
        error: "Missing X-Firebase-AppCheck token",
      });
    }
    req.appCheckVerified = false;
    return next();
  }
  try {
    await admin.appCheck().verifyToken(appCheckToken);
    req.appCheckVerified = true;
    return next();
  } catch (err) {
    if (REQUIRE_APP_CHECK) {
      return res.status(401).json({
        code: "APP_CHECK_INVALID",
        userMessage: "App verification failed. Please update or restart Fun & Learn.",
        error: "Invalid Firebase App Check token",
      });
    }
    req.appCheckVerified = false;
    return next();
  }
}

async function resolveCallerIdentity(req, res, next) {
  const authHeader = req.headers.authorization || "";
  if (authHeader.startsWith("Bearer ")) {
    const token = authHeader.slice(7).trim();
    try {
      const decoded = await admin.auth().verifyIdToken(token);
      req.callerId = `fb:${decoded.uid}`;
      req.firebaseAuthVerified = true;
      return next();
    } catch (_err) {
      if (REQUIRE_FIREBASE_AUTH) {
        return res.status(401).json({
          code: "AUTH_INVALID",
          userMessage: "Session refresh needed. Please reopen Fun & Learn.",
          error: "Invalid Firebase ID token",
        });
      }
    }
  }

  if (REQUIRE_FIREBASE_AUTH) {
    return res.status(401).json({
      code: "AUTH_REQUIRED",
      userMessage: "Session refresh needed. Please reopen Fun & Learn.",
      error: "Missing Firebase ID Bearer token",
    });
  }

  const clientId = String(req.header("X-FunLearn-Client-Id") || "").replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 64);
  if (clientId.length >= 8) {
    req.callerId = `inst:${clientId}`;
    req.firebaseAuthVerified = false;
    return next();
  }
  req.callerId = `ip:${hashKey(req.ip || "unknown")}`;
  req.firebaseAuthVerified = false;
  return next();
}

function checkAndReserveQuota(req, res, next) {
  resetDailyCountersIfNeeded();
  const now = Date.now();

  if (now < upstreamCooldownUntilMs) {
    const retrySec = Math.ceil((upstreamCooldownUntilMs - now) / 1000);
    return res.status(429).json({
      code: "QUOTA_EXHAUSTED",
      userMessage: QUOTA_BREAK_MESSAGE,
      error: QUOTA_BREAK_MESSAGE,
      retryAfterSeconds: retrySec,
    });
  }

  if (orgRequestsToday >= ORG_DAILY_REQ_LIMIT || orgTokensToday >= ORG_DAILY_TOKEN_LIMIT) {
    return res.status(429).json({
      code: "ORG_QUOTA_REACHED",
      userMessage: QUOTA_BREAK_MESSAGE,
      error: QUOTA_BREAK_MESSAGE,
      retryAfterSeconds: 3600,
    });
  }

  const uid = req.callerId;
  const recent = (userMinuteBuckets.get(uid) || []).filter((ts) => now - ts < 60000);
  if (recent.length >= USER_RPM_LIMIT) {
    return res.status(429).json({
      code: "USER_RPM_LIMIT",
      userMessage: QUOTA_BREAK_MESSAGE,
      error: QUOTA_BREAK_MESSAGE,
      retryAfterSeconds: 30,
    });
  }

  const daily = userDailyUsage.get(uid) || { count: 0, tokens: 0 };
  if (daily.count >= USER_DAILY_REQ_LIMIT) {
    return res.status(429).json({
      code: "USER_DAILY_LIMIT",
      userMessage: QUOTA_BREAK_MESSAGE,
      error: QUOTA_BREAK_MESSAGE,
      retryAfterSeconds: 3600,
    });
  }

  req.quotaContext = { uid, recent, daily };
  return next();
}

function recordQuotaUsage(uid, tokensUsed) {
  resetDailyCountersIfNeeded();
  const now = Date.now();
  const recent = (userMinuteBuckets.get(uid) || []).filter((ts) => now - ts < 60000);
  recent.push(now);
  userMinuteBuckets.set(uid, recent);

  const daily = userDailyUsage.get(uid) || { count: 0, tokens: 0 };
  daily.count += 1;
  daily.tokens += Number(tokensUsed || 0);
  userDailyUsage.set(uid, daily);

  orgRequestsToday += 1;
  orgTokensToday += Number(tokensUsed || 0);
}

function isGroqConfigured() {
  return Boolean(GROQ_API_KEY && !GROQ_API_KEY.includes("placeholder"));
}

async function callGroqUpstream(bodyObj) {
  await acquireUpstreamSlot();
  try {
    let lastError = null;
    for (let attempt = 0; attempt < 3; attempt++) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
      try {
        const res = await fetch(`${GROQ_BASE_URL}/chat/completions`, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${GROQ_API_KEY}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify(bodyObj),
          signal: controller.signal,
        });
        clearTimeout(timeout);
        updateRateLimitSnapshot(res.headers);

        if (res.status === 429) {
          const retryAfter = Number(res.headers.get("retry-after") || "0");
          const waitMs = retryAfter > 0 ? retryAfter * 1000 : parseGroqDurationMs(lastGroqHeaders.resetTokens || lastGroqHeaders.resetRequests);
          upstreamCooldownUntilMs = Math.max(upstreamCooldownUntilMs, Date.now() + waitMs);
          // Do NOT retry 429 — break cleanly immediately!
          const err = new Error(QUOTA_BREAK_MESSAGE);
          err.status = 429;
          err.retryAfterSeconds = Math.ceil(waitMs / 1000);
          throw err;
        }

        if (res.status >= 500 && attempt < 2) {
          const backoffMs = 400 * Math.pow(2, attempt);
          await new Promise((r) => setTimeout(r, backoffMs));
          continue;
        }

        if (!res.ok) {
          const text = await res.text();
          const err = new Error(`Upstream AI error (${res.status}): ${text.slice(0, 180)}`);
          err.status = res.status;
          throw err;
        }

        return await res.json();
      } catch (err) {
        clearTimeout(timeout);
        lastError = err;
        if (err.status === 429 || (err.status && err.status < 500)) {
          throw err;
        }
        if (attempt < 2) {
          await new Promise((r) => setTimeout(r, 400 * Math.pow(2, attempt)));
          continue;
        }
      }
    }
    throw lastError || new Error("Unable to contact AI service.");
  } finally {
    releaseUpstreamSlot();
  }
}

app.get("/health", (_req, res) => {
  resetDailyCountersIfNeeded();
  res.json({
    status: "ok",
    company: "NEXORALABS",
    service: "NYRA Groq Production Proxy",
    model: GROQ_MODEL,
    groqConfigured: isGroqConfigured(),
    requireAppCheck: REQUIRE_APP_CHECK,
    requireFirebaseAuth: REQUIRE_FIREBASE_AUTH,
    quota: {
      day: currentDayKey,
      orgRequestsToday,
      orgDailyReqLimit: ORG_DAILY_REQ_LIMIT,
      orgTokensToday,
      orgDailyTokenLimit: ORG_DAILY_TOKEN_LIMIT,
      userDailyReqLimit: USER_DAILY_REQ_LIMIT,
      userRpmLimit: USER_RPM_LIMIT,
      cooldownActive: Date.now() < upstreamCooldownUntilMs,
      cacheEntries: responseCache.size,
      groqLimits: lastGroqHeaders,
    },
  });
});

app.get("/v1/nyra/quota", verifyAppCheckIfConfigured, resolveCallerIdentity, (req, res) => {
  resetDailyCountersIfNeeded();
  const daily = userDailyUsage.get(req.callerId) || { count: 0, tokens: 0 };
  res.json({
    day: currentDayKey,
    userUsedToday: daily.count,
    userDailyLimit: USER_DAILY_REQ_LIMIT,
    userRemainingToday: Math.max(0, USER_DAILY_REQ_LIMIT - daily.count),
    cooldownActive: Date.now() < upstreamCooldownUntilMs,
    message: Date.now() < upstreamCooldownUntilMs ? QUOTA_BREAK_MESSAGE : "NYRA is ready!",
  });
});

app.post("/v1/nyra/chat", verifyAppCheckIfConfigured, resolveCallerIdentity, checkAndReserveQuota, async (req, res) => {
  if (!isGroqConfigured()) {
    return res.status(503).json({
      code: "BACKEND_NOT_CONFIGURED",
      userMessage: "NYRA server key is not configured yet.",
      error: "GROQ_API_KEY is missing on backend server.",
    });
  }

  const rawMessages = Array.isArray(req.body?.messages) ? req.body.messages : [];
  if (rawMessages.length === 0) {
    return res.status(400).json({ error: "messages array is required" });
  }

  const classLevel = String(req.body?.classLevel || "Primary").slice(0, 40);
  const languageMode = String(req.body?.languageMode || "ENGLISH").slice(0, 20);
  const wantStream = Boolean(req.body?.stream);
  const trimmedMessages = rawMessages.slice(-8).map((m) => ({
    role: m.role === "system" ? "system" : m.role === "assistant" ? "assistant" : "user",
    content: String(m.content || "").slice(0, 1800),
  }));

  const lastUserMsg = [...trimmedMessages].reverse().find((m) => m.role === "user")?.content || "";
  const isCacheableSingleTurn = trimmedMessages.filter((m) => m.role === "user").length <= 1;
  const cacheKey = isCacheableSingleTurn
    ? `chat:${hashKey(`${classLevel}|${languageMode}|${lastUserMsg.trim().toLowerCase()}`)}`
    : null;

  const cached = cacheKey ? getCached(cacheKey) : null;
  if (cached) {
    if (wantStream) {
      res.setHeader("Content-Type", "text/event-stream");
      res.setHeader("Cache-Control", "no-cache");
      res.write(`data: ${JSON.stringify({ delta: cached.reply, cached: true })}\n\n`);
      res.write("data: [DONE]\n\n");
      return res.end();
    }
    return res.json({ ...cached, cached: true });
  }

  const executeChat = async () => {
    const payload = {
      model: GROQ_MODEL,
      reasoning_effort: "low",
      messages: trimmedMessages,
      temperature: 0.45,
      max_tokens: 480,
      stream: false,
    };
    const data = await callGroqUpstream(payload);
    const reply = data?.choices?.[0]?.message?.content?.trim() || "";
    if (!reply) {
      throw Object.assign(new Error("Empty reply from NYRA AI"), { status: 502 });
    }
    const totalTokens = data?.usage?.total_tokens || 200;
    recordQuotaUsage(req.callerId, totalTokens);
    const result = {
      reply,
      model: GROQ_MODEL,
      usage: data?.usage || null,
    };
    if (cacheKey) setCached(cacheKey, result);
    return result;
  };

  try {
    let result;
    if (cacheKey && inFlightRequests.has(cacheKey)) {
      result = await inFlightRequests.get(cacheKey);
    } else {
      const promise = executeChat();
      if (cacheKey) inFlightRequests.set(cacheKey, promise);
      try {
        result = await promise;
      } finally {
        if (cacheKey) inFlightRequests.delete(cacheKey);
      }
    }

    if (wantStream) {
      res.setHeader("Content-Type", "text/event-stream");
      res.setHeader("Cache-Control", "no-cache");
      const words = result.reply.split(/(\s+)/);
      let chunk = "";
      for (let i = 0; i < words.length; i++) {
        chunk += words[i];
        if (chunk.length >= 24 || i === words.length - 1) {
          res.write(`data: ${JSON.stringify({ delta: chunk })}\n\n`);
          chunk = "";
        }
      }
      res.write("data: [DONE]\n\n");
      return res.end();
    }
    return res.json(result);
  } catch (err) {
    const status = err.status === 429 ? 429 : err.status || 502;
    return res.status(status).json({
      code: status === 429 ? "QUOTA_EXHAUSTED" : "UPSTREAM_ERROR",
      userMessage: status === 429 ? QUOTA_BREAK_MESSAGE : "NYRA had a tiny hiccup. Please try again in a moment!",
      error: status === 429 ? QUOTA_BREAK_MESSAGE : err.message,
      retryAfterSeconds: err.retryAfterSeconds || 60,
    });
  }
});

app.post("/v1/nyra/generate", verifyAppCheckIfConfigured, resolveCallerIdentity, checkAndReserveQuota, async (req, res) => {
  if (!isGroqConfigured()) {
    return res.status(503).json({
      code: "BACKEND_NOT_CONFIGURED",
      userMessage: "NYRA server key is not configured yet.",
      error: "GROQ_API_KEY is missing on backend server.",
    });
  }

  const prompt = String(req.body?.prompt || "").slice(0, 2600);
  if (!prompt.trim()) {
    return res.status(400).json({ error: "prompt is required" });
  }

  const cacheKey = `gen:${hashKey(prompt.trim())}`;
  const cached = getCached(cacheKey);
  if (cached) {
    return res.json(cached);
  }

  const executeGen = async () => {
    const data = await callGroqUpstream({
      model: GROQ_MODEL,
      reasoning_effort: "low",
      response_format: { type: "json_object" },
      messages: [
        {
          role: "system",
          content:
            "You are NYRA by NEXORALABS for Indian primary students (Nursery to Class 5, NEP 2020). " +
            "Return strictly valid JSON only matching the requested schema.",
        },
        { role: "user", content: prompt },
      ],
      temperature: 0.35,
      max_tokens: 950,
    });

    const content = data?.choices?.[0]?.message?.content || "{}";
    let parsed;
    try {
      parsed = JSON.parse(content);
    } catch {
      const cleaned = content.replace(/^```json\s*/i, "").replace(/```$/i, "").trim();
      parsed = JSON.parse(cleaned);
    }
    const totalTokens = data?.usage?.total_tokens || 350;
    recordQuotaUsage(req.callerId, totalTokens);
    setCached(cacheKey, parsed);
    return parsed;
  };

  try {
    let parsed;
    if (inFlightRequests.has(cacheKey)) {
      parsed = await inFlightRequests.get(cacheKey);
    } else {
      const promise = executeGen();
      inFlightRequests.set(cacheKey, promise);
      try {
        parsed = await promise;
      } finally {
        inFlightRequests.delete(cacheKey);
      }
    }
    return res.json(parsed);
  } catch (err) {
    const status = err.status === 429 ? 429 : err.status || 502;
    return res.status(status).json({
      code: status === 429 ? "QUOTA_EXHAUSTED" : "UPSTREAM_ERROR",
      userMessage: status === 429 ? QUOTA_BREAK_MESSAGE : "NYRA could not generate questions right now.",
      error: status === 429 ? QUOTA_BREAK_MESSAGE : err.message,
      retryAfterSeconds: err.retryAfterSeconds || 60,
    });
  }
});

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`NEXORALABS NYRA proxy listening on :${PORT} (model=${GROQ_MODEL}, appCheck=${REQUIRE_APP_CHECK})`);
  });
}

module.exports = { app };

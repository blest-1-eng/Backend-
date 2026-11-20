const test = require("node:test");
const assert = require("node:assert/strict");
const { app } = require("./index");

let server;
let baseUrl;

test.before(async () => {
  await new Promise((resolve) => {
    server = app.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      baseUrl = `http://127.0.0.1:${addr.port}`;
      resolve();
    });
  });
});

test.after(async () => {
  if (server) {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("GET /health reports Groq configured without leaking key", async () => {
  const res = await fetch(`${baseUrl}/health`);
  assert.equal(res.status, 200);
  const text = await res.text();
  assert.ok(!text.includes("gsk_"), "Health response must never leak GROQ_API_KEY");
  const json = JSON.parse(text);
  assert.equal(json.status, "ok");
  assert.equal(json.model, "openai/gpt-oss-120b");
  assert.equal(json.groqConfigured, true);
  assert.ok(json.quota.orgDailyReqLimit <= 1000);
});

test("POST /v1/nyra/chat streams/replies from Groq and caches repeated educational prompt", async () => {
  const headers = {
    "Content-Type": "application/json",
    "X-FunLearn-Client-Id": "test-device-profile-01",
  };
  const payload = {
    classLevel: "Class 3",
    languageMode: "ENGLISH",
    stream: false,
    messages: [
      { role: "system", content: "Be brief and child friendly." },
      { role: "user", content: "What is 6 multiplied by 7?" },
    ],
  };

  const r1 = await fetch(`${baseUrl}/v1/nyra/chat`, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });
  assert.equal(r1.status, 200);
  const j1 = await r1.json();
  assert.ok(j1.reply && j1.reply.includes("42"), `Expected 42 in reply, got: ${j1.reply}`);

  // Second identical educational request must hit cache cleanly
  const r2 = await fetch(`${baseUrl}/v1/nyra/chat`, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });
  assert.equal(r2.status, 200);
  const j2 = await r2.json();
  assert.equal(j2.cached, true);
  assert.equal(j2.reply, j1.reply);
});

test("POST /v1/nyra/generate returns structured JSON from Groq", async () => {
  const res = await fetch(`${baseUrl}/v1/nyra/generate`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-FunLearn-Client-Id": "test-device-profile-02",
    },
    body: JSON.stringify({
      prompt: 'Return JSON {"questions":[{"prompt":"What is 2+2?","options":["3","4","5"],"correctAnswer":"4","explanation":"2 plus 2 is 4"}]} for Class 1.',
    }),
  });
  assert.equal(res.status, 200);
  const json = await res.json();
  assert.ok(Array.isArray(json.questions) && json.questions.length >= 1);
  assert.equal(json.questions[0].correctAnswer, "4");
});

test("POST /v1/nyra/chat enforces per-user RPM limit with friendly HTTP 429 break message", async () => {
  const clientId = "test-rpm-burst-client-99";
  let got429 = false;
  let lastPayload = null;
  for (let i = 0; i < 8; i++) {
    const r = await fetch(`${baseUrl}/v1/nyra/chat`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-FunLearn-Client-Id": clientId,
      },
      body: JSON.stringify({
        classLevel: "Class 1",
        languageMode: "ENGLISH",
        stream: false,
        messages: [
          { role: "user", content: `Count apples question number ${i + 100}: what is 1 + ${i}?` },
        ],
      }),
    });
    if (r.status === 429) {
      got429 = true;
      lastPayload = await r.json();
      break;
    }
  }
  assert.equal(got429, true, "Expected per-user RPM limit to return HTTP 429");
  assert.equal(lastPayload.userMessage, "NYRA is taking a little break. Please try again later!");
});

test("GET /v1/nyra/quota tracks per-user remaining requests", async () => {
  const res = await fetch(`${baseUrl}/v1/nyra/quota`, {
    headers: { "X-FunLearn-Client-Id": "test-device-profile-01" },
  });
  assert.equal(res.status, 200);
  const json = await res.json();
  assert.equal(json.userUsedToday, 1);
  assert.ok(json.userRemainingToday >= 1);
});

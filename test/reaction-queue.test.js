const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

class FakeRedis {
  constructor() { this.values = new Map(); this.lists = new Map(); this.sets = new Map(); }
  on() {}
  async set(key, value, options = {}) {
    if (options.NX && this.values.has(key)) return null;
    this.values.set(key, String(value)); return "OK";
  }
  async get(key) { return this.values.get(key) || null; }
  async del(key) { return this.values.delete(key) ? 1 : 0; }
  async exists(key) { return this.values.has(key) ? 1 : 0; }
  async expire() { return 1; }
  async rPush(key, value) { const list = this.lists.get(key) || []; list.push(String(value)); this.lists.set(key, list); return list.length; }
  async lPush(key, value) { const list = this.lists.get(key) || []; list.unshift(String(value)); this.lists.set(key, list); return list.length; }
  async lTrim(key, start, end) { const list = this.lists.get(key) || []; this.lists.set(key, list.slice(start, end + 1)); }
  async blPop(key) { const list = this.lists.get(key) || []; if (list.length) return { key, element: list.shift() }; return new Promise(() => {}); }
  async sAdd(key, value) { const set = this.sets.get(key) || new Set(); set.add(String(value)); this.sets.set(key, set); return 1; }
  async sRem(key, value) { return this.sets.get(key)?.delete(String(value)) ? 1 : 0; }
  async sMembers(key) { return [...(this.sets.get(key) || [])]; }
}

function loadWorker(redis, reaction) {
  let source = fs.readFileSync(require.resolve("../index.js"), "utf8");
  source = source.replace(
    'const redis = createClient({\n  url: REDIS_URL\n});',
    'const redis = global.__testRedis;'
  );
  source = source.replace('const INITIAL_REACTION_RETRY_DELAY_MS = 750;', 'const INITIAL_REACTION_RETRY_DELAY_MS = 1;');
  source = source.replace(
    /start\(\)\.catch\(\(err\) => \{\n  console\.error\("START ERROR:", err\);\n  process\.exit\(1\);\n\}\);/,
    'global.__hooks = { enqueueInitialReaction_, initialReactionWorker, recoverPendingInitialReactions_, deletedOrderKey, reactionKeyCacheName };'
  );
  const context = {
    require, console, process: { env: { EVOLUTION_API_URL: "http://evolution.test", EVOLUTION_API_KEY: "test-key" }, exit() {} }, Buffer, setTimeout, setInterval,
    clearTimeout, AbortController, Intl, Date, JSON, Math, String, Number, Boolean,
    global: { __testRedis: redis }, fetch: reaction
  };
  vm.runInNewContext(source, context, { filename: "index.js" });
  return context.global.__hooks;
}

const bookSquad = "120363214326633370@g.us";
const baseJob = { groupId: bookSquad, messageId: "m-1", participant: "628123@s.whatsapp.net", instance: "Jastip-bot" };
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function testSuccessfulBookSquadReaction() {
  const redis = new FakeRedis();
  const calls = [];
  const hooks = loadWorker(redis, async (_url, init) => { calls.push(JSON.parse(init.body)); return { ok: true, text: async () => "{}" }; });
  assert.equal(await hooks.enqueueInitialReaction_(baseJob), "queued");
  hooks.initialReactionWorker(); await pause(15);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].reaction, "✅");
}

async function testBbwInitialReactionStaysCart() {
  const redis = new FakeRedis();
  const calls = [];
  const hooks = loadWorker(redis, async (_url, init) => { calls.push(JSON.parse(init.body)); return { ok: true, text: async () => "{}" }; });
  const bbwJob = { ...baseJob, groupId: "120363414084709085@g.us", messageId: "bbw-1" };
  assert.equal(await hooks.enqueueInitialReaction_(bbwJob), "queued");
  hooks.initialReactionWorker(); await pause(15);
  assert.equal(calls[0].reaction, "🛒");
}

async function testRetriesDoNotCancelOrder() {
  const redis = new FakeRedis();
  let calls = 0;
  const hooks = loadWorker(redis, async () => { calls += 1; return { ok: false, status: 404, text: async () => "message not found" }; });
  assert.equal(await hooks.enqueueInitialReaction_(baseJob), "queued");
  hooks.initialReactionWorker(); await pause(30);
  assert.equal(calls, 3, "initial plus at most two retries");
  assert.equal(await redis.exists(hooks.deletedOrderKey(bookSquad, "m-1")), 0, "reaction error is not a deletion tombstone");
}

async function testValidDeleteSuppressesReaction() {
  const redis = new FakeRedis();
  let calls = 0;
  const hooks = loadWorker(redis, async () => { calls += 1; return { ok: true, text: async () => "{}" }; });
  await redis.set(hooks.deletedOrderKey(bookSquad, "m-1"), "1");
  assert.equal(await hooks.enqueueInitialReaction_(baseJob), "deleted");
  await pause(5);
  assert.equal(calls, 0);
}

async function testRecoveryRequeuesPendingReaction() {
  const redis = new FakeRedis();
  const hooks = loadWorker(redis, async () => ({ ok: true, text: async () => "{}" }));
  const pendingName = hooks.reactionKeyCacheName(bookSquad, "m-restart") + ":pending-initial-reaction";
  await redis.set(pendingName, JSON.stringify({ ...baseJob, messageId: "m-restart", emoji: "✅" }));
  await redis.sAdd("jastip:initial-reaction:pending-index", pendingName);
  await hooks.recoverPendingInitialReactions_();
  assert.deepEqual(redis.lists.get("jastip:initial-reaction:queue"), [pendingName]);
}

(async () => {
  await testSuccessfulBookSquadReaction();
  await testBbwInitialReactionStaysCart();
  await testRetriesDoNotCancelOrder();
  await testValidDeleteSuppressesReaction();
  await testRecoveryRequeuesPendingReaction();
  console.log("reaction-queue tests passed");
})().catch((err) => { console.error(err); process.exitCode = 1; });

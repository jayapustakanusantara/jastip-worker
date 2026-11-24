// JASTIP WORKER V3.8 - V3.7 + EVOLUTION EMPTY-BUBBLE BUG GUARD
const express = require("express");
const { createClient } = require("redis");

const app = express();
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 8080;
const REDIS_URL = process.env.REDIS_URL;
const APPS_SCRIPT_URL = process.env.APPS_SCRIPT_URL;
const WORKER_SECRET = process.env.WORKER_SECRET;

// Tambahan untuk reaction ❌ langsung dari Railway worker ke Evolution API
const EVOLUTION_API_URL = process.env.EVOLUTION_API_URL;const EVOLUTION_API_KEY = process.env.EVOLUTION_API_KEY;

// TRACE / Reply Private is intentionally isolated from the order queue.
// This secret is kept only in Railway and Google Apps Script Script Properties;
// it is never returned to the Admin Portal browser.
// Gunakan secret Trace khusus bila tersedia. Fallback ke secret reaction yang
// sudah dipasang Portal/Railway agar tidak perlu mengekspos atau membuat key baru.
const TRACE_PORTAL_SECRET =
  process.env.TRACE_PORTAL_SECRET ||
  process.env.REACTION_SYNC_SECRET ||
  WORKER_SECRET;
const TRACE_SOURCE_GROUPS_JSON = process.env.TRACE_SOURCE_GROUPS_JSON || "";
const TRACE_REPLY_MAX_AGE_MS = 14 * 24 * 60 * 60 * 1000;
const TRACE_REPLY_INSTANCE = process.env.TRACE_REPLY_INSTANCE || "Jastip-bot";
const TRACE_MESSAGE_CACHE_SECONDS = Math.floor(TRACE_REPLY_MAX_AGE_MS / 1000);
const TRACE_MESSAGE_CACHE_PREFIX = "jastip:trace-message:";

// PRIVATE REKAP REQUEST
// Jalur ini hanya untuk chat pribadi customer yang menulis command rekap.
// Tidak memakai queue FIX/MAU dan tidak pernah membaca / mengubah order langsung.
// Link tetap dibuat oleh Web App Rekapan yang sudah dipakai Portal Admin.
const RECAP_WEB_APP_URL = String(process.env.RECAP_WEB_APP_URL || "").trim();
const RECAP_API_KEY = String(process.env.RECAP_API_KEY || "").trim();
const RECAP_INSTANCE = String(process.env.RECAP_INSTANCE || TRACE_REPLY_INSTANCE || "Jastip-bot").trim();
const PRIVATE_RECAP_QUEUE = "jastip:private-recap:queue";
const PRIVATE_RECAP_SEEN_PREFIX = "jastip:private-recap:seen:";
const PRIVATE_RECAP_SEEN_SECONDS = 60 * 60 * 24 * 7;


// =====================================================
// EVOLUTION / RAILWAY SPIKE MONITOR
// =====================================================
const RAILWAY_API_TOKEN = process.env.RAILWAY_API_TOKEN;
const RAILWAY_PROJECT_TOKEN = process.env.RAILWAY_TOKEN;
const RAILWAY_PROJECT_ID = process.env.RAILWAY_PROJECT_ID || "17796c36-2457-49b2-91c9-06feb2895f3b";
const RAILWAY_ENVIRONMENT_ID = process.env.RAILWAY_ENVIRONMENT_ID || "7bdc80f9-323a-4746-aaac-1670754f9bca";
const RAILWAY_EVOLUTION_SERVICE_ID = process.env.RAILWAY_EVOLUTION_SERVICE_ID || "4400361b-e856-4210-96ff-f475b12c76a6";
const RAILWAY_GRAPHQL_URL = process.env.RAILWAY_GRAPHQL_URL || "https://backboard.railway.com/graphql/v2";
const ADMIN_WA_NUMBER = process.env.ADMIN_WA_NUMBER || "6283193967234";
const SPIKE_INSTANCE = process.env.SPIKE_INSTANCE || "Jastip-bot";

// Guard bug Evolution/Baileys yang dapat mengirim bubble kosong ke grup.
// Default hanya monitor; auto logout harus diaktifkan eksplisit di Railway.
const BUBBLE_GUARD_ENABLED =
  String(process.env.BUBBLE_GUARD_ENABLED || "true").toLowerCase() !== "false";
const BUBBLE_GUARD_AUTO_DISCONNECT =
  String(process.env.BUBBLE_GUARD_AUTO_DISCONNECT || "false").toLowerCase() === "true";
const BUBBLE_GUARD_THRESHOLD = Math.max(2, Number(process.env.BUBBLE_GUARD_THRESHOLD || 3));
const BUBBLE_GUARD_WINDOW_SECONDS = Math.max(30, Number(process.env.BUBBLE_GUARD_WINDOW_SECONDS || 90));
const BUBBLE_GUARD_HISTORY_KEY = "jastip:bubble-guard:history";
const BUBBLE_GUARD_LAST_KEY = "jastip:bubble-guard:last";
const BUBBLE_GUARD_ACTION_LOCK_KEY = "jastip:bubble-guard:action-lock";

const SPIKE_CHECK_INTERVAL_MS = 60 * 1000; // 1 menit
const SPIKE_MEMORY_GB = 1.0;
const SPIKE_CPU_VCPU = 1.5;
const SPIKE_WARNING_AFTER_MS = 5 * 60 * 1000;
const SPIKE_CRITICAL_AFTER_MS = 10 * 60 * 1000;
const SPIKE_RECOVERY_AFTER_MS = 5 * 60 * 1000;
const SPIKE_STATE_KEY = "jastip:spike:active";
const SPIKE_HISTORY_KEY = "jastip:spike:history";
const SPIKE_HISTORY_LIMIT = 20;

const QUEUE_NAME = "jastip:queue";
const PROCESSING_QUEUE = "jastip:processing";
const DEAD_QUEUE = "jastip:dead";
// Persistent delayed retries: a failed Apps Script request must not sleep in the order loop.
const SHEET_RETRY_DELAYED_QUEUE = "jastip:sheet-retry:delayed";

// Reaction hasil pembagian diproses di belakang layar agar Portal cepat.
const ALLOCATION_REACTION_QUEUE = "jastip:allocation-reaction:queue";
const ALLOCATION_REACTION_PENDING_SECONDS = 60 * 10;

// Reaction awal dipisahkan dari antrean order agar Evolution yang lambat/offline
// tidak pernah menunda pencatatan FIX/MAU ke Google Sheets.
const INITIAL_REACTION_QUEUE = "jastip:initial-reaction:queue";
const INITIAL_REACTION_PENDING_SECONDS = 60 * 15;
const INITIAL_REACTION_MAX_RETRIES = 5;
const INITIAL_REACTION_RETRY_DELAYS_MS = [2000, 5000, 15000, 30000, 60000];
const INITIAL_REACTION_FAILED_QUEUE = "jastip:initial-reaction:failed";
const INITIAL_REACTION_FAILED_TTL_SECONDS = 60 * 60 * 24 * 7;
// Index ini memastikan job reaction yang sudah di-pop tetapi worker restart
// tetap bisa dimasukkan kembali saat startup. Data order tidak ikut disentuh.
const INITIAL_REACTION_PENDING_INDEX = "jastip:initial-reaction:pending-index";
const INITIAL_REACTION_RETRY_SCHEDULE = "jastip:initial-reaction:retry-schedule";

// Key WhatsApp asli untuk update reaction setelah admin menyimpan pembagian.
// TTL terbatas agar Redis tidak menjadi arsip chat permanen.
const REACTION_KEY_CACHE_SECONDS = 60 * 60 * 24 * 30;
const REACTION_KEY_CACHE_PREFIX = "jastip:reaction-key:";
const BBW_GROUP_ID = "120363414084709085@g.us";
const BOOK_SQUAD_GROUP_ID = "120363214326633370@g.us"; // ORDER
const DELETED_ORDER_TTL_SECONDS = 60 * 60 * 24 * 7;
function deletedOrderKey(groupId, messageId) {
  return `jastip:deleted-order:${groupId}:${messageId}`;
}
async function isDeletedOrder(job) {
  return job?.action !== "CANCEL_DELETED_MESSAGE" &&
    job?.groupId === BOOK_SQUAD_GROUP_ID &&
    Boolean(await redis.exists(deletedOrderKey(job.groupId, job.messageId)));
}


const SUPPORTED_GROUPS = new Set([
  "120363214326633370@g.us", // ORDER
  "120363427983824748@g.us", // ORDER LIVE
  "120363414084709085@g.us"  // ORDER BBW
]);

// Grup ORDER menerima konfirmasi langsung setelah FIX/MAU berhasil dicatat.
// Grup BBW dan LIVE tetap memakai 🛒 sampai ada pembagian buku.
// Dipisahkan dari aturan validasi/pembagian supaya hanya reaction awal yang berubah.
const DIRECT_CONFIRMED_REACTION_GROUPS = new Set([
  "120363214326633370@g.us"
]);

const redis = createClient({
  url: REDIS_URL
});

/*
  A blocking Redis command (BLPOP / BRPOPLPUSH) holds its TCP connection
  until an item arrives or the timeout expires. The order, allocation
  reaction, initial reaction and private-recap loops must therefore not share
  the ordinary Redis connection used by webhook handlers and queue updates.
  These duplicates point to the same Redis database; they only isolate the
  sockets used for blocking reads.
*/
const orderBlockingRedis = redis.duplicate();
const allocationReactionBlockingRedis = redis.duplicate();
const initialReactionBlockingRedis = redis.duplicate();
// Separate blocking connection: two initial reactions can be delivered concurrently.
const initialReactionBlockingRedis2 = redis.duplicate();
const privateRecapBlockingRedis = redis.duplicate();

const redisClients = [
  ["main", redis],
  ["order-blocking", orderBlockingRedis],
  ["allocation-reaction-blocking", allocationReactionBlockingRedis],
  ["initial-reaction-blocking", initialReactionBlockingRedis],
  ["initial-reaction-blocking-2", initialReactionBlockingRedis2],
  ["private-recap-blocking", privateRecapBlockingRedis]
];

for (const [name, client] of redisClients) {
  client.on("error", (err) => {
    console.error(`REDIS ${name} ERROR:`, err);
  });

  client.on("ready", () => {
    console.log(`Redis ${name} READY`);
  });
}

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "jastip-worker",
    redis: redis.isReady,
    appsScriptConfigured: Boolean(APPS_SCRIPT_URL),
    workerSecretConfigured: Boolean(WORKER_SECRET),
    evolutionConfigured: Boolean(EVOLUTION_API_URL && EVOLUTION_API_KEY),
    reactionSyncConfigured: Boolean(process.env.REACTION_SYNC_SECRET),
    privateRecapConfigured: Boolean(RECAP_WEB_APP_URL && RECAP_API_KEY)
  });
});

/*
  Internal-only allocation reaction endpoint.
  Endpoint hanya menaruh status terbaru ke Redis; Evolution diproses oleh
  worker khusus agar SIMPAN PEMBAGIAN tidak ikut menunggu WhatsApp.
*/
app.post("/internal/allocation-reaction", async (req, res) => {
  const crypto = require("node:crypto");
  const secret = String(process.env.REACTION_SYNC_SECRET || "");
  const supplied = String(req.get("x-reaction-sync-secret") || "");
  if (!secret || !supplied || Buffer.byteLength(secret) !== Buffer.byteLength(supplied) || !crypto.timingSafeEqual(Buffer.from(secret), Buffer.from(supplied))) {
    return res.status(401).json({ ok: false, error: "unauthorized" });
  }
  const groupId = String(req.body?.groupId || "").trim();
  const messageId = String(req.body?.messageId || "").trim();
  const emoji = String(req.body?.reaction || "").trim();
  const sourceSheet = String(req.body?.sourceSheet || "").trim();
  const allowedSheets = new Set(["ORDER BBW", "ORDER BBW JOGJA", "ORDER BBW GI", "ORDER LIVE"]);
  if (!SUPPORTED_GROUPS.has(groupId) || !/^[A-Za-z0-9_:\-]{4,160}$/.test(messageId) || !["🛒", "🟡", "✅"].includes(emoji) || !allowedSheets.has(sourceSheet)) {
    return res.status(400).json({ ok: false, error: "invalid_reference" });
  }
  if (!redis.isReady) return res.status(503).json({ ok: false, error: "redis_unavailable" });
  if (groupId === BOOK_SQUAD_GROUP_ID && await redis.exists(deletedOrderKey(groupId, messageId))) {
    return res.status(410).json({ ok: false, error: "original_message_deleted" });
  }
  const referenceName = reactionKeyCacheName(groupId, messageId);
  const pendingName = referenceName + ":pending-allocation-reaction";
  try {
    const raw = await redis.get(referenceName);
    if (!raw) return res.status(404).json({ ok: false, error: "original_key_not_cached" });
    const key = JSON.parse(raw);
    if (key.groupId !== groupId || key.messageId !== messageId || !key.participant || !key.instance) return res.status(422).json({ ok: false, error: "invalid_cached_key" });
    // Last-write-wins: dua admin dapat menyimpan berdekatan tanpa reaction
    // dobel atau status WhatsApp yang tertinggal.
    await redis.set(
      pendingName,
      JSON.stringify({ groupId, messageId, emoji, queuedAt: new Date().toISOString() }),
      { EX: ALLOCATION_REACTION_PENDING_SECONDS }
    );
    // Job kecil boleh lebih dari satu untuk Message ID yang sama. Worker
    // membaca pending key yang sama dan mengirim status terakhir sekali saja.
    await redis.rPush(ALLOCATION_REACTION_QUEUE, referenceName);
    return res.status(202).json({ ok: true, queued: true });
  } catch (err) {
    console.error("ALLOCATION REACTION QUEUE ERROR:", err.message);
    return res.status(502).json({ ok: false, error: "reaction_queue_failed" });
  }
});

/*
  Internal acknowledgement from Apps Script after all order rows have been
  written and verified. This only queues the existing initial-reaction worker;
  it never writes an order and cannot block the FIX queue.
*/
app.post("/internal/order-recorded-reaction", async (req, res) => {
  const crypto = require("node:crypto");
  const supplied = String(req.get("x-worker-secret") || "");
  const expected = String(WORKER_SECRET || "");
  if (!expected || !supplied || Buffer.byteLength(expected) !== Buffer.byteLength(supplied) ||
      !crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(supplied))) {
    return res.status(401).json({ ok: false, error: "unauthorized" });
  }

  const groupId = String(req.body?.groupId || "").trim();
  const messageId = String(req.body?.messageId || "").trim();
  if (!SUPPORTED_GROUPS.has(groupId) || !/^[A-Za-z0-9_:\-]{4,160}$/.test(messageId)) {
    return res.status(400).json({ ok: false, error: "invalid_reference" });
  }
  if (!redis.isReady) return res.status(503).json({ ok: false, error: "redis_unavailable" });
  if (await isDeletedOrder({ groupId, messageId })) {
    return res.status(410).json({ ok: false, error: "original_message_deleted" });
  }

  try {
    const rawKey = await redis.get(reactionKeyCacheName(groupId, messageId));
    if (!rawKey) return res.status(404).json({ ok: false, error: "original_key_not_cached" });
    const originalKey = JSON.parse(rawKey);
    if (originalKey.groupId !== groupId || originalKey.messageId !== messageId ||
        !originalKey.participant || !originalKey.instance) {
      return res.status(422).json({ ok: false, error: "invalid_cached_key" });
    }

    const result = await enqueueInitialReaction_({
      groupId,
      messageId,
      participant: originalKey.participant,
      instance: originalKey.instance
    });
    return res.status(202).json({ ok: true, queued: result });
  } catch (err) {
    console.error("ORDER-RECORDED REACTION QUEUE ERROR:", err.message);
    return res.status(502).json({ ok: false, error: "reaction_queue_failed" });
  }
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    redis: redis.isReady
  });
});

// Status bubble guard, dapat dibuka dari HP.
app.get("/bubble-guard", async (req, res) => {
  try {
    const lastRaw = await redis.get(BUBBLE_GUARD_LAST_KEY);
    const historyRaw = await redis.lRange(BUBBLE_GUARD_HISTORY_KEY, 0, 19);
    return res.json({
      ok: true,
      enabled: BUBBLE_GUARD_ENABLED,
      autoDisconnect: BUBBLE_GUARD_AUTO_DISCONNECT,
      threshold: BUBBLE_GUARD_THRESHOLD,
      windowSeconds: BUBBLE_GUARD_WINDOW_SECONDS,
      last: lastRaw ? JSON.parse(lastRaw) : null,
      history: historyRaw.map((item) => {
        try { return JSON.parse(item); } catch { return null; }
      }).filter(Boolean)
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
  }
});

/*
  ENDPOINT WEBHOOK EVOLUTION

  Evolution akan POST payload messages.upsert ke "/"
  Root "/" bisa menerima GET health check dan POST webhook WhatsApp.
*/
app.post("/", async (req, res) => {
  try {
    const body = req.body || {};

    const event =
      body.event ||
      body.type ||
      body.eventType ||
      "";

    const normalizedEvent =
      String(event)
        .toLowerCase()
        .trim()
        .replace(/[-_]/g, ".");

    const isOrderEvent =
      normalizedEvent === "messages.upsert";

    const isDeleteEvent =
      normalizedEvent === "messages.delete";

    const isUpdateEvent =
      normalizedEvent === "messages.update";

    if (!isOrderEvent && !isDeleteEvent && !isUpdateEvent) {
      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "not messages.upsert/delete/update"
      });
    }

    const instance =
      body.instance ||
      body.instanceName ||
      body.sender ||
      "Jastip-bot";

    let data = body.data || body;

    if (Array.isArray(data)) {
      data = data[0];
    }

    data = data || {};

    /*
      Evolution/Baileys dapat mengirim key pesan yang dihapus dalam
      beberapa bentuk:

      1. data.key
      2. data.keys[0]
      3. data.message.protocolMessage.key
      4. data.update.message.protocolMessage.key

      Untuk REVOKE, protocolMessage.key adalah referensi ke pesan FIX/MAU
      yang asli. Karena itu key tersebut harus diprioritaskan.
    */
    const outerKey =
      data.key ||
      data.message?.key ||
      {};

    const keysArrayKey =
      Array.isArray(data.keys) && data.keys.length
        ? (data.keys[0] || {})
        : {};

    const protocolMessage =
      data.message?.protocolMessage ||
      data.update?.message?.protocolMessage ||
      data.update?.protocolMessage ||
      data.protocolMessage ||
      {};

    const protocolKey =
      protocolMessage.key ||
      {};

    const updateStatus =
      String(
        data.status ||
        data.update?.status ||
        data.messageStatus ||
        ""
      ).toUpperCase();

    const protocolType =
      String(
        protocolMessage.type ??
        ""
      ).toUpperCase();

    const isDeletedUpdate =
      isUpdateEvent &&
      (
        updateStatus.includes("DELETE") ||
        updateStatus.includes("REVOKE") ||
        protocolType === "REVOKE" ||
        protocolType === "MESSAGE_REVOKE" ||
        protocolType === "0"
      );

    // Read/delivered/played update bukan pembatalan dan harus diabaikan.
    if (isUpdateEvent && !isDeletedUpdate) {
      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "messages.update is not delete/revoke"
      });
    }

    const isCancellationEvent =
      isDeleteEvent || isDeletedUpdate;

    const key =
      isCancellationEvent
        ? (
            Object.keys(protocolKey).length
              ? protocolKey
              : Object.keys(keysArrayKey).length
                ? keysArrayKey
                : outerKey
          )
        : outerKey;

    const remoteJid =
      key.remoteJid ||
      protocolKey.remoteJid ||
      keysArrayKey.remoteJid ||
      outerKey.remoteJid ||
      data.remoteJid ||
      "";

    const messageId =
      key.id ||
      protocolKey.id ||
      keysArrayKey.id ||
      outerKey.id ||
      data.id ||
      "";

    /*
      Jangan pernah memproses pesan/update buatan bot sendiri sebagai
      pembatalan. Ini mencegah reaction, notifikasi, atau status outgoing
      masuk kembali dan membentuk loop.
    */
    const fromMe =
      key.fromMe === true ||
      protocolKey.fromMe === true ||
      keysArrayKey.fromMe === true ||
      outerKey.fromMe === true ||
      data.fromMe === true;

    const message =
      data.message ||
      {};

    // Hanya outgoing blank upsert di grup. Reaction dan protocol message
    // dikecualikan oleh isSuspiciousOutgoingBlankMessage().
    if (
      BUBBLE_GUARD_ENABLED &&
      isOrderEvent &&
      fromMe &&
      SUPPORTED_GROUPS.has(remoteJid) &&
      isSuspiciousOutgoingBlankMessage(message)
    ) {
      const guardResult = await recordSuspiciousBlankBubble({
        instance,
        groupId: remoteJid,
        messageId
      });

      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "suspected Evolution empty-bubble bug",
        bubbleGuard: guardResult
      });
    }

    if (isCancellationEvent && fromMe) {
      console.log(
        "DELETE IGNORED: fromMe=true |",
        messageId || "MISSING"
      );

      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "delete/update generated by bot",
        messageId
      });
    }

    if (isCancellationEvent) {
      console.log(
        "DELETE EVENT RECEIVED:",
        normalizedEvent,
        "| protocol:",
        protocolType || "-",
        "| target:",
        messageId || "MISSING",
        "| group:",
        remoteJid || "MISSING"
      );
    }

    /*
      CHAT PRIBADI: customer boleh meminta link rekap ulang dengan command
      persis "rekap bbw", "rekap bbw jogja", atau "rekap bbw gi".
      Ini diproses SEBELUM guard grup karena direct chat tidak mempunyai
      Group ID @g.us. Semua chat lain tetap diabaikan seperti sebelumnya.
    */
    if (isOrderEvent && !fromMe && !String(remoteJid || "").endsWith("@g.us")) {
      const privateText = extractMessageText(message);
      const recapRequest = parsePrivateRecapCommand_(privateText);

      if (recapRequest) {
        const customerPhone = privateChatPhone_(
          remoteJid,
          data.participantAlt || data.participant || outerKey.participantAlt || outerKey.participant || ""
        );
        if (!messageId || !customerPhone) {
          console.warn("PRIVATE RECAP IGNORED: missing message ID or customer phone");
          return res.status(200).json({ ok: true, ignored: true, reason: "private recap sender unavailable" });
        }

        const firstRequest = await redis.set(
          PRIVATE_RECAP_SEEN_PREFIX + messageId,
          "1",
          { NX: true, EX: PRIVATE_RECAP_SEEN_SECONDS }
        );

        if (!firstRequest) {
          return res.status(200).json({ ok: true, duplicate: true, action: "PRIVATE_RECAP" });
        }

        await redis.rPush(PRIVATE_RECAP_QUEUE, JSON.stringify({
          messageId,
          instance: String(instance || RECAP_INSTANCE),
          customerPhone,
          recapType: recapRequest.recapType,
          sourceSheet: recapRequest.sourceSheet,
          eventLabel: recapRequest.eventLabel,
          receivedAt: new Date().toISOString()
        }));

        console.log("PRIVATE RECAP QUEUED:", recapRequest.recapType, "|", customerPhone);
        return res.status(200).json({ ok: true, queued: true, action: "PRIVATE_RECAP" });
      }
    }

    if (!SUPPORTED_GROUPS.has(remoteJid)) {
      if (isCancellationEvent) {
        console.warn(
          "DELETE IGNORED: unsupported group |",
          remoteJid || "MISSING",
          "| target:",
          messageId || "MISSING"
        );
      }

      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "unsupported group",
        remoteJid
      });
    }

    if (!messageId) {
      if (isCancellationEvent) {
        console.warn(
          "DELETE IGNORED: missing target messageId | group:",
          remoteJid
        );
      }

      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "missing messageId"
      });
    }

    const participant =
      key.participant ||
      protocolKey.participant ||
      keysArrayKey.participant ||
      outerKey.participant ||
      data.participant ||
      "";

    const participantAlt =
      key.participantAlt ||
      protocolKey.participantAlt ||
      keysArrayKey.participantAlt ||
      outerKey.participantAlt ||
      data.participantAlt ||
      "";

    const pushName =
      data.pushName ||
      data.notifyName ||
      "";

    /*
      Customer menghapus pesan FIX/MAU.
      Tidak semua pesan yang dihapus adalah order, jadi Apps Script nanti
      hanya menghapus bila Message ID memang ditemukan di tab ORDER.
    */
    if (isCancellationEvent) {
      // Fitur batal karena Delete for Everyone hanya berlaku di grup BBW.
      if (remoteJid !== BBW_GROUP_ID && remoteJid !== BOOK_SQUAD_GROUP_ID) {
        return res.status(200).json({
          ok: true,
          ignored: true,
          reason: "message delete cancellation is BBW only",
          remoteJid,
          messageId
        });
      }

      // Mark deletion before queueing cancellation: the FIX may still be waiting.
      // The marker also suppresses reactions to a message WhatsApp no longer has.
      if (remoteJid === BOOK_SQUAD_GROUP_ID) {
        await redis.set(deletedOrderKey(remoteJid, messageId), "1", { EX: DELETED_ORDER_TTL_SECONDS });
        await redis.del(reactionKeyCacheName(remoteJid, messageId) + ":pending-allocation-reaction");
      }

      const deleteSeenKey =
        `jastip:delete-seen:${remoteJid}:${messageId}`;

      const firstDelete = await redis.set(
        deleteSeenKey,
        "1",
        {
          NX: true,
          EX: 60 * 60 * 24 * 7
        }
      );

      if (!firstDelete) {
        return res.status(200).json({
          ok: true,
          duplicate: true,
          action: "CANCEL_DELETED_MESSAGE",
          messageId
        });
      }

      const cancelJob = {
        action: "CANCEL_DELETED_MESSAGE",
        receivedAt: new Date().toISOString(),
        instance,
        messageId,
        groupId: remoteJid,
        participant,
        participantAlt,
        senderPhone: cleanPhone(
          participantAlt || participant
        ),
        pushName,
        rawKey: key
      };

      await redis.lPush(
        QUEUE_NAME,
        JSON.stringify(cancelJob)
      );

      console.log(
        "DELETE TARGET RESOLVED:",
        messageId,
        "|",
        remoteJid,
        "| sender:",
        cleanPhone(participantAlt || participant) || "unknown"
      );

      console.log(
        "DELETE QUEUED:",
        messageId,
        "|",
        remoteJid
      );

      return res.status(200).json({
        ok: true,
        queued: true,
        action: "CANCEL_DELETED_MESSAGE",
        messageId
      });
    }

    const text = extractMessageText(message);

    if (!text) {
      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "no text"
      });
    }

    // Hanya FIX / MAU
    if (!/^\s*(FIX|MAU)\b/i.test(text)) {
      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "not FIX/MAU"
      });
    }

    const quotedProduct = extractQuotedProduct(message, data.contextInfo);
    const mentionJid = extractMentionJid(message, data.contextInfo);

    const senderPhone = cleanPhone(
      participantAlt || participant
    );

    const job = {
      receivedAt: new Date().toISOString(),

      instance,
      messageId,

      groupId: remoteJid,

      participant,
      participantAlt,
      senderPhone,

      pushName,

      text,
      quotedProduct,
      mentionJid,

      rawKey: key,
      // Salinan payload asli untuk native quote. Disimpan terbatas 14 hari
      // hanya setelah FIX/MAU lolos deduplication.
      rawMessage: message
    };

    /*
      Anti duplicate message ID.
      Evolution bisa retry webhook yang sama.
    */
    // Message ID WhatsApp hanya unik dalam konteks chat/group. Gunakan keduanya
    // agar webhook dari grup berbeda tidak saling dianggap duplikat.
    const seenKey = `jastip:seen:${remoteJid}:${messageId}`;

    const firstTime = await redis.set(
      seenKey,
      "1",
      {
        NX: true,
        EX: 60 * 60 * 24 * 7
      }
    );

    if (!firstTime) {
      console.log("DUPLICATE WEBHOOK:", messageId);

      return res.status(200).json({
        ok: true,
        duplicate: true,
        messageId
      });
    }

    // Cache only after webhook deduplication; cache failures never block FIX/MAU.
    try {
      await cacheReactionKey(job);
    } catch (cacheError) {
      console.error("REACTION KEY CACHE ERROR:", messageId, "|", cacheError.message);
    }
    try {
      await cacheTraceOriginalMessage(job);
    } catch (cacheError) {
      // Cache TRACE bersifat tambahan; FIX/MAU tidak boleh gagal bila Redis cache bermasalah.
      console.error("TRACE MESSAGE CACHE ERROR:", messageId, "|", cacheError.message);
    }

    await redis.lPush(
      QUEUE_NAME,
      JSON.stringify(job)
    );

    console.log(
      "QUEUED:",
      messageId,
      "|",
      remoteJid,
      "|",
      text
    );

    /*

      RETURN CEPAT KE EVOLUTION.
      Jangan proses Google Sheet / reaction di webhook.
    */
    return res.status(200).json({
      ok: true,
      queued: true,
      messageId
    });

  } catch (err) {
    console.error("WEBHOOK ERROR:", err);

    return res.status(500).json({
      ok: false,
      error: err.message
    });
  }
});


/*
  Endpoint manual untuk test queue.
*/
app.post("/queue", async (req, res) => {
  try {
    const payload = req.body || {};

    if (!payload.messageId) {
      return res.status(400).json({
        ok: false,
        error: "messageId required"
      });
    }

    const queueGroupId = String(payload.groupId || "manual").trim();
    const seenKey = `jastip:seen:${queueGroupId}:${payload.messageId}`;

    const firstTime = await redis.set(
      seenKey,
      "1",
      {
        NX: true,
        EX: 60 * 60 * 24 * 7
      }
    );

    if (!firstTime) {
      return res.json({
        ok: true,
        duplicate: true,
        messageId: payload.messageId
      });
    }

    const job = {
      ...payload,
      queuedAt: new Date().toISOString()
    };

    await redis.lPush(
      QUEUE_NAME,
      JSON.stringify(job)
    );

    return res.json({
      ok: true,
      queued: true,
      messageId: payload.messageId
    });

  } catch (err) {
    console.error("QUEUE ERROR:", err);

    return res.status(500).json({
      ok: false,
      error: err.message
    });
  }
});


app.get("/queue/status", async (req, res) => {
  try {
    const waiting = await redis.lLen(QUEUE_NAME);
    const processing = await redis.lLen(PROCESSING_QUEUE);
    const dead = await redis.lLen(DEAD_QUEUE);

    return res.json({
      ok: true,
      waiting,
      processing,
      dead
    });

  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err.message
    });
  }
});


// Status ringkas untuk dicek dari HP.
app.get("/status", async (req, res) => {
  try {
    const waiting = await redis.lLen(QUEUE_NAME);
    const processing = await redis.lLen(PROCESSING_QUEUE);
    const dead = await redis.lLen(DEAD_QUEUE);
    const pending = waiting + processing;

    return res.json({
      ok: true,
      worker: redis.isReady ? "healthy" : "redis-not-ready",
      redis: redis.isReady ? "connected" : "disconnected",
      queuePending: pending,
      waiting,
      processing,
      dead,
      state: pending === 0 ? "IDLE - queue empty" : "BUSY"
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
  }
});


// =====================================================
// SPIKE LOG - enak dibuka dari HP
// =====================================================
app.get("/spike-log", async (req, res) => {
  try {
    const activeRaw = await redis.get(SPIKE_STATE_KEY);
    const historyRaw = await redis.lRange(SPIKE_HISTORY_KEY, 0, SPIKE_HISTORY_LIMIT - 1);
    const active = activeRaw ? JSON.parse(activeRaw) : null;
    const history = historyRaw.map((x) => {
      try { return JSON.parse(x); } catch { return null; }
    }).filter(Boolean);

    const fmt = (iso) => iso ? formatJakarta(iso) : "-";
    const line = (label, value) => `<div><b>${escapeHtml(label)}</b>: ${escapeHtml(String(value ?? "-"))}</div>`;

    let html = `<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Evolution Spike Log</title><style>body{font-family:Arial,sans-serif;max-width:760px;margin:24px auto;padding:0 16px;line-height:1.55}.card{border:1px solid #ddd;border-radius:12px;padding:16px;margin:14px 0}.muted{color:#666}.ok{font-weight:700}.bad{font-weight:700}</style></head><body><h2>Evolution API Spike Monitor</h2>`;

    if (active) {
      html += `<div class="card"><div class="bad">🔴 SPIKE ACTIVE</div>`;
      html += line("Started", fmt(active.startedAt));
      html += line("Current RAM", `${Number(active.currentMemoryGB || 0).toFixed(2)} GB`);
      html += line("Current CPU", `${Number(active.currentCpu || 0).toFixed(2)} vCPU`);
      html += line("Peak RAM", `${Number(active.peakMemoryGB || 0).toFixed(2)} GB`);
      html += line("Peak CPU", `${Number(active.peakCpu || 0).toFixed(2)} vCPU`);
      html += line("WA Warning", active.warningSent ? "SENT" : "NOT SENT");
      html += line("WA Critical", active.criticalSent ? "SENT" : "NOT SENT");
      html += `</div>`;
    } else {
      html += `<div class="card"><div class="ok">🟢 CURRENT STATUS: NORMAL</div></div>`;
    }

    html += `<h3>20 kejadian terakhir</h3>`;
    if (!history.length) html += `<div class="muted">Belum ada spike yang selesai tercatat.</div>`;
    for (const item of history) {
      html += `<div class="card">`;
      html += line("Start", fmt(item.startedAt));
      html += line("End", fmt(item.endedAt));
      html += line("Duration", `${Math.max(1, Math.round(Number(item.durationMs || 0) / 60000))} menit`);
      html += line("Peak RAM", `${Number(item.peakMemoryGB || 0).toFixed(2)} GB`);
      html += line("Peak CPU", `${Number(item.peakCpu || 0).toFixed(2)} vCPU`);
      html += line("WA Warning", item.warningSent ? "SENT" : "NOT SENT");
      html += line("WA Critical", item.criticalSent ? "SENT" : "NOT SENT");
      html += `</div>`;
    }
    html += `</body></html>`;
    res.type("html").send(html);
  } catch (err) {
    console.error("SPIKE LOG ERROR:", err);
    res.status(500).json({ ok: false, error: err.message });
  }
});


/*
  WORKER

  Redis queue
  -> ambil job
  -> VALIDASI DULU DI RAILWAY
  -> invalid = reaction ❌, selesai, TIDAK ke Apps Script, TIDAK retry
  -> valid = baru POST ke Apps Script

  Error teknis Apps Script masih boleh retry.
*/
// Atomic transition prevents a retry from disappearing between processing and delayed storage.
async function scheduleSheetRetry_(rawJob, retryJob, delayMs) {
  return redis.eval(`
    local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
    if removed == 1 then
      redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
    end
    return removed
  `, {
    keys: [PROCESSING_QUEUE, SHEET_RETRY_DELAYED_QUEUE],
    arguments: [rawJob, JSON.stringify(retryJob), String(Date.now() + delayMs)]
  });
}

// Move only due retries back to the regular order queue, atomically.
// A failure/restart leaves other scheduled retries safely in Redis.
async function sheetRetryDispatcher_() {
  console.log("Sheet retry dispatcher started...");
  while (true) {
    try {
      const moved = await redis.eval(`
        local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 25)
        for _, job in ipairs(jobs) do
          if redis.call('ZREM', KEYS[1], job) == 1 then
            redis.call('RPUSH', KEYS[2], job)
          end
        end
        return #jobs
      `, {
        keys: [SHEET_RETRY_DELAYED_QUEUE, QUEUE_NAME],
        arguments: [String(Date.now())]
      });
      if (Number(moved) > 0) console.log("SHEET RETRIES RELEASED:", moved);
    } catch (err) {
      console.error("SHEET RETRY DISPATCHER ERROR:", err.message);
    }
    await sleep(500);
  }
}

async function worker(blockingRedis = orderBlockingRedis) {
  console.log("Worker started...");

  while (true) {
    try {
      const rawJob = await blockingRedis.brPopLPush(
        QUEUE_NAME,
        PROCESSING_QUEUE,
        5
      );

      if (!rawJob) {
        continue;
      }

      let job;

      try {
        job = JSON.parse(rawJob);
      } catch (err) {
        console.error("INVALID JOB:", rawJob);

        await redis.lRem(
          PROCESSING_QUEUE,
          1,
          rawJob
        );

        await redis.lPush(
          DEAD_QUEUE,
          rawJob
        );

        continue;
      }

      const queueWaitingNow = await redis.lLen(QUEUE_NAME);
      const queueProcessingNow = await redis.lLen(PROCESSING_QUEUE);
      console.log(`QUEUE: ${queueWaitingNow + queueProcessingNow} (waiting ${queueWaitingNow}, processing ${queueProcessingNow})`);

      console.log("");
      console.log("================================");
      console.log("PROCESS JOB");
      console.log("Message ID:", job.messageId);
      console.log("Group:", job.groupId);
      console.log("Sender:", job.senderPhone);
      console.log("Name:", job.pushName);
      console.log("Text:", job.text);
      console.log("Quoted:", job.quotedProduct);
      console.log("================================");
      console.log("");

      /*
        VALIDASI CEPAT DI RAILWAY.

        Aturan:
        1. Tidak reply / quoted kosong -> ❌
        2. Reply gambar tapi caption/judul kosong -> ❌
        3. Tidak ditemukan baris judul -> ❌
        4. Judul < 5 huruf/angka -> ❌

        Semua invalid langsung selesai.
        Tidak dikirim ke Apps Script.
        Tidak di-requeue.
      */
      const isCancellation =
        job.action === "CANCEL_DELETED_MESSAGE";

      // A deleted BOOK SQUAD FIX must never be written after its delete event.
      // Cancellation job stays queued separately to clean up any saved rows.
      if (await isDeletedOrder(job)) {
        await redis.lRem(PROCESSING_QUEUE, 1, rawJob);
        console.log("DELETED ORDER SKIPPED:", job.messageId);
        continue;
      }

      const validation =
        isCancellation
          ? { ok: true }
          : validateOrderJob(job);

      if (!validation.ok) {
        console.log(
          "INVALID ORDER:",
          job.messageId,
          "|",
          validation.reason
        );

        try {
          await sendReaction(job, "❌");

          console.log(
            "INVALID REACTION SENT:",
            job.messageId,
            "| ❌"
          );
        } catch (reactionError) {
          /*
            Reaction invalid gagal TIDAK BOLEH membuat order invalid
            berputar-putar lagi di Redis.
          */
          console.error(
            "INVALID REACTION ERROR:",
            job.messageId,
            "|",
            reactionError.message
          );
        }

        await redis.lRem(
          PROCESSING_QUEUE,
          1,
          rawJob
        );

        console.log(
          "INVALID DONE - NO RETRY:",
          job.messageId
        );

        continue;
      }

      try {
        const result = await sendJobToAppsScript(job);

        await redis.lRem(
          PROCESSING_QUEUE,
          1,
          rawJob
        );

        console.log(
          "SHEET ACCEPTED:",
          job.messageId,
          "|",
          result
        );

        // A delete may arrive while Apps Script is saving; cancellation is queued
        // separately and the original message must not receive a reaction.
        if (await isDeletedOrder(job)) {
          console.log("DELETED AFTER SHEET SAVE - CANCELLATION PENDING:", job.messageId);
          continue;
        }

        // Only new accepted orders get an initial reaction; retries remain silent.
        // ORDER langsung ✅, sementara BBW/LIVE tetap 🛒 sampai pembagian dilakukan.
        if (!isCancellation && result === "QUEUED") {
          try {
            const queueResult = await enqueueInitialReaction_(job);
            console.log("INITIAL REACTION QUEUED:", job.messageId, "|", queueResult);
          } catch (reactionError) {
            // Order sudah tersimpan. Gagal membuat job reaction tidak boleh
            // memasukkan ulang order ke Apps Script atau menghambat FIX lain.
            console.error("INITIAL REACTION QUEUE ERROR:", job.messageId, "|", reactionError.message);
          }
        }

      } catch (bridgeError) {
        console.error(
          "SHEET BRIDGE ERROR:",
          job.messageId,
          "|",
          bridgeError.message
        );

        /*
          Pengaman tambahan:
          kalau Apps Script tetap mengembalikan error PRODUCT/TITLE,
          anggap invalid, beri ❌, lalu selesai.
          JANGAN requeue.
        */
        if (await isDeletedOrder(job)) {
          await redis.lRem(PROCESSING_QUEUE, 1, rawJob);
          console.log("DELETED ORDER BRIDGE RETRY SKIPPED:", job.messageId);
          continue;
        }
        if (isInvalidProductBridgeError(bridgeError)) {
          try {
            await sendReaction(job, "❌");

            console.log(
              "BRIDGE INVALID REACTION SENT:",
              job.messageId,
              "| ❌"
            );
          } catch (reactionError) {
            console.error(
              "BRIDGE INVALID REACTION ERROR:",
              job.messageId,
              "|",
              reactionError.message
            );
          }

          await redis.lRem(
            PROCESSING_QUEUE,
            1,
            rawJob
          );

          console.log(
            "BRIDGE INVALID DONE - NO RETRY:",
            job.messageId
          );

          continue;
        }

        /*
          Hanya error teknis sungguhan yang retry.
        */
        const retryCount =
          Number(job.bridgeRetry || 0) + 1;

        const retryJob = {
          ...job,
          bridgeRetry: retryCount,
          lastBridgeError: bridgeError.message,
          lastBridgeRetryAt: new Date().toISOString()
        };

        const maxBridgeRetry =
          isCancellation ? 10 : 20;

        if (retryCount <= maxBridgeRetry) {
          const retryDelayMs =
            isCancellation
              ? Math.min(60000, 10000 * retryCount)
              : Math.min(30000, 1500 * retryCount);
          const scheduled = await scheduleSheetRetry_(rawJob, retryJob, retryDelayMs);
          console.log("SHEET RETRY SCHEDULED:", job.messageId,
            "| retry", retryCount, "| delayMs", retryDelayMs, "| scheduled", scheduled);

        } else {
          await redis.lRem(PROCESSING_QUEUE, 1, rawJob);
          await redis.lPush(
            DEAD_QUEUE,
            JSON.stringify(retryJob)
          );

          console.error(
            "MOVED TO DEAD QUEUE:",
            job.messageId
          );
        }
      }

    } catch (err) {
      console.error("WORKER ERROR:", err);
      await sleep(2000);
    }
  }
}

/*
  Worker terpisah untuk reaction hasil SIMPAN PEMBAGIAN.
  Tidak memakai antrean order utama agar retry Evolution tidak menghambat
  pencatatan FIX/MAU ke Google Sheets.
*/
async function allocationReactionWorker() {
  console.log("Allocation reaction worker started...");

  while (true) {
    let referenceName = "";
    try {
      const popped = await allocationReactionBlockingRedis.blPop(ALLOCATION_REACTION_QUEUE, 5);
      referenceName = String(popped?.element || "");
      if (!referenceName) continue;

      const pendingName = referenceName + ":pending-allocation-reaction";
      const rawPending = await redis.get(pendingName);
      if (!rawPending) continue; // Job lama, sudah digantikan/selesai.

      let pending;
      try {
        pending = JSON.parse(rawPending);
      } catch (err) {
        console.error("ALLOCATION REACTION INVALID JOB:", referenceName);
        await redis.del(pendingName);
        continue;
      }

      const result = await processAllocationReaction_(pending);
      const latest = await redis.get(pendingName);

      if (latest === rawPending) {
        await redis.del(pendingName);
        console.log(
          "ALLOCATION REACTION DONE:",
          pending.messageId,
          "|",
          pending.emoji,
          "|",
          result
        );
      } else if (latest) {
        // Ada save yang lebih baru saat Evolution sedang diproses.
        await redis.rPush(ALLOCATION_REACTION_QUEUE, referenceName);
      }
    } catch (err) {
      const nonRetryable = new Set([
        "original_key_not_cached",
        "invalid_cached_key"
      ]);
      console.error(
        "ALLOCATION REACTION WORKER ERROR:",
        referenceName || "NO_JOB",
        "|",
        err.message
      );
      // Pending status tetap disimpan; retry tidak pernah membatalkan
      // pembagian buku yang sudah tersimpan di Google Sheets.
      if (referenceName && nonRetryable.has(String(err.message || ""))) {
        await redis.del(referenceName + ":pending-allocation-reaction");
      } else if (referenceName) {
        await redis.rPush(ALLOCATION_REACTION_QUEUE, referenceName);
      }
      await sleep(1000);
    }
  }
}

/*
  Reaction awal setelah order diterima Apps Script.
  Dipisahkan dari allocationReactionWorker karena BOOK SQUAD hanya memakai ✅
  sebagai konfirmasi pencatatan, sedangkan BBW memakai status pembagian sendiri.
*/
// Event-driven recovery: no periodic pending-index scan.
const initialReactionActive = new Set();
let initialReactionBusy = false;
let initialReactionCheckRunning = false;
async function checkInitialReactionBacklog_() {
  if (initialReactionCheckRunning) return;
  initialReactionCheckRunning = true;
  try {
    const depth = await redis.lLen(INITIAL_REACTION_QUEUE);
    if (depth >= 30) {
      if (!initialReactionBusy) console.log("INITIAL REACTION BUSY:", depth);
      initialReactionBusy = true;
    }
    if (!initialReactionBusy || depth > 5) return;
    const queued = new Set(await redis.lRange(INITIAL_REACTION_QUEUE, 0, -1));
    const names = await redis.sMembers(INITIAL_REACTION_PENDING_INDEX);
    let recovered = 0;
    for (const name of names) {
      if (queued.has(name) || initialReactionActive.has(name)) continue;
      if (await redis.zScore(INITIAL_REACTION_RETRY_SCHEDULE, name) !== null) continue;
      const raw = await redis.get(name);
      if (!raw) continue;
      const pending = JSON.parse(raw);
      if (Date.now() - Date.parse(pending.retriedAt || pending.queuedAt) < 60000) continue;
      if (await isDeletedOrder(pending)) continue;
      if (await redis.get(reactionKeyCacheName(pending.groupId, pending.messageId) + ":last-reaction")) continue;
      if (initialReactionActive.has(name)) continue;
      if (await redis.zScore(INITIAL_REACTION_RETRY_SCHEDULE, name) !== null) continue;
      await redis.rPush(INITIAL_REACTION_QUEUE, name);
      recovered++;
    }
    initialReactionBusy = false;
    console.log("INITIAL REACTION ADAPTIVE RECOVERY:", recovered, "| depth", depth);
  } catch (err) {
    console.error("INITIAL REACTION ADAPTIVE ERROR:", err.message);
  } finally {
    initialReactionCheckRunning = false;
  }
}
async function initialReactionWorker(blockingClient, workerNumber) {
  console.log(`Initial reaction worker ${workerNumber} started...`);

  while (true) {
    let pendingName = "";
    try {
      const popped = await blockingClient.blPop(INITIAL_REACTION_QUEUE, 5);
      pendingName = String(popped?.element || "");
      if (!pendingName) continue;
      if (initialReactionActive.has(pendingName)) { pendingName = ""; continue; }
      initialReactionActive.add(pendingName);

      const rawPending = await redis.get(pendingName);
      if (!rawPending) continue; // Selesai/superseded/expired.

      let pending;
      try {
        pending = JSON.parse(rawPending);
      } catch (err) {
        console.error("INITIAL REACTION INVALID JOB:", pendingName);
        await clearInitialReactionPending_(pendingName);
        continue;
      }

      if (await isDeletedOrder(pending)) {
        await clearInitialReactionPending_(pendingName);
        console.log("INITIAL REACTION SKIPPED: deleted BOOK SQUAD message |", pending.messageId);
        continue;
      }

      const lastName = reactionKeyCacheName(pending.groupId, pending.messageId) + ":last-reaction";
      const lastReaction = await redis.get(lastName);
      // Jangan pernah menimpa status pembagian BBW yang lebih baru.
      if (lastReaction) {
        await clearInitialReactionPending_(pendingName);
        console.log("INITIAL REACTION SKIPPED: already updated |", pending.messageId, "|", lastReaction);
        continue;
      }

      try {
        await sendReaction(pending, pending.emoji);
        await rememberReaction_(pending, pending.emoji);
        await clearInitialReactionPending_(pendingName);
        console.log("INITIAL REACTION SENT:", pending.messageId, "|", pending.emoji);
      } catch (reactionError) {
        const attempts = Number(pending.attempts || 0);

        // Error dari Evolution (termasuk reference/message not found) bukan
        // bukti pesan customer dihapus. Hanya tombstone webhook revoke yang
        // boleh menghentikan job tanpa retry atau memicu pembatalan order.
        if (await isDeletedOrder(pending)) {
          await clearInitialReactionPending_(pendingName);
          console.warn("INITIAL REACTION SKIPPED: deleted BOOK SQUAD message |", pending.messageId);
          continue;
        }

        if (attempts >= INITIAL_REACTION_MAX_RETRIES) {
          await clearInitialReactionPending_(pendingName);
          await recordInitialReactionFailure_(pending, reactionError);
          console.error("INITIAL REACTION FAILED AFTER RETRIES:", pending.messageId, "|", reactionError.message);
          continue;
        }

        const retryPending = {
          ...pending,
          attempts: attempts + 1,
          retriedAt: new Date().toISOString()
        };
        await redis.set(
          pendingName,
          JSON.stringify(retryPending),
          { EX: INITIAL_REACTION_PENDING_SECONDS }
        );
        // Schedule durable retry in Redis; do not block this reaction worker.
        await redis.zAdd(INITIAL_REACTION_RETRY_SCHEDULE, { score: Date.now() + (INITIAL_REACTION_RETRY_DELAYS_MS[attempts] || 60000), value: pendingName });
        console.warn(
          "INITIAL REACTION RETRY:",
          pending.messageId,
          "| attempt",
          retryPending.attempts,
          "of",
          INITIAL_REACTION_MAX_RETRIES,
          "|",
          reactionError.message
        );
      }
    } catch (err) {
      console.error("INITIAL REACTION WORKER ERROR:", pendingName || "NO_JOB", "|", err.message);
      // Error Redis/internal worker tidak boleh menghentikan order worker.
      await sleep(1000);
    } finally {
      if (pendingName) { initialReactionActive.delete(pendingName); await checkInitialReactionBacklog_(); }
    }
  }
}

async function enqueueInitialReaction_(job) {
  const groupId = String(job?.groupId || "").trim();
  const messageId = String(job?.messageId || "").trim();
  const participant = String(job?.participant || job?.rawKey?.participant || "").trim();
  const instance = String(job?.instance || "Jastip-bot").trim();
  if (!groupId || !messageId || !participant || !instance) {
    throw new Error("initial_reaction_reference_incomplete");
  }

  if (await isDeletedOrder({ groupId, messageId })) return "deleted";

  const emoji = DIRECT_CONFIRMED_REACTION_GROUPS.has(groupId) ? "✅" : "🛒";
  const referenceName = reactionKeyCacheName(groupId, messageId);
  const lastReaction = await redis.get(referenceName + ":last-reaction");
  if (lastReaction) return "unchanged";

  const pendingName = referenceName + ":pending-initial-reaction";
  const pending = {
    version: 1,
    groupId,
    messageId,
    participant,
    instance,
    emoji,
    attempts: 0,
    queuedAt: new Date().toISOString()
  };
  const queued = await redis.set(
    pendingName,
    JSON.stringify(pending),
    { NX: true, EX: INITIAL_REACTION_PENDING_SECONDS }
  );
  if (queued !== "OK") return "already_queued";

  await redis.sAdd(INITIAL_REACTION_PENDING_INDEX, pendingName);
  await redis.expire(INITIAL_REACTION_PENDING_INDEX, INITIAL_REACTION_PENDING_SECONDS);
  await redis.rPush(INITIAL_REACTION_QUEUE, pendingName);
  await checkInitialReactionBacklog_();
  return "queued";
}

async function clearInitialReactionPending_(pendingName) {
  await redis.zRem(INITIAL_REACTION_RETRY_SCHEDULE, pendingName);
  await redis.del(pendingName);
  await redis.sRem(INITIAL_REACTION_PENDING_INDEX, pendingName);
}

async function recoverPendingInitialReactions_() {
  const pendingNames = await redis.sMembers(INITIAL_REACTION_PENDING_INDEX);
  if (!pendingNames.length) {
    console.log("INITIAL REACTION RECOVERY: no pending jobs");
    return;
  }

  let recovered = 0;
  for (const pendingName of pendingNames) {
    if (await redis.exists(pendingName)) {
      await redis.zRem(INITIAL_REACTION_RETRY_SCHEDULE, pendingName);
      await redis.rPush(INITIAL_REACTION_QUEUE, pendingName);
      recovered += 1;
    } else {
      await redis.sRem(INITIAL_REACTION_PENDING_INDEX, pendingName);
    }
  }
  await redis.expire(INITIAL_REACTION_PENDING_INDEX, INITIAL_REACTION_PENDING_SECONDS);
  console.log(`INITIAL REACTION RECOVERY: requeued ${recovered} pending job(s)`);
}

// Periodically dispatch scheduled retries without blocking WhatsApp reaction workers.
async function initialReactionRetryDispatcher_() {
  while (true) {
    try {
      const due = await redis.zRangeByScore(INITIAL_REACTION_RETRY_SCHEDULE, 0, Date.now(), { LIMIT: { offset: 0, count: 100 } });
      for (const name of due) {
        if (await redis.zRem(INITIAL_REACTION_RETRY_SCHEDULE, name)) {
          if (await redis.exists(name)) await redis.rPush(INITIAL_REACTION_QUEUE, name);
        }
      }
    } catch (err) {
      console.error("INITIAL REACTION RETRY DISPATCHER ERROR:", err.message);
    }
    await sleep(2000);
  }
}

async function recordInitialReactionFailure_(pending, err) {
  const item = JSON.stringify({
    groupId: String(pending?.groupId || ""),
    messageId: String(pending?.messageId || ""),
    emoji: String(pending?.emoji || ""),
    attempts: Number(pending?.attempts || 0),
    failedAt: new Date().toISOString(),
    error: String(err?.message || err || "unknown error").slice(0, 300)
  });
  await redis.lPush(INITIAL_REACTION_FAILED_QUEUE, item);
  await redis.lTrim(INITIAL_REACTION_FAILED_QUEUE, 0, 99);
  await redis.expire(INITIAL_REACTION_FAILED_QUEUE, INITIAL_REACTION_FAILED_TTL_SECONDS);
}


/*
  VALIDASI AWAL ORDER
*/
function validateOrderJob(job) {
  const quoted = String(
    job.quotedProduct ||
    job.quotedText ||
    ""
  ).trim();

  if (!quoted) {
    return {
      ok: false,
      reason: "NO QUOTED PRODUCT"
    };
  }

  const title = extractLikelyTitle(quoted);

  if (!title) {
    return {
      ok: false,
      reason: "NO TITLE"
    };
  }

  /*
    Hitung huruf/angka saja.
    Spasi, emoji dan simbol tidak ikut dihitung.
    Contoh:
      "ABC" = 3 -> ❌
      "Book" = 4 -> ❌
      "Dog Man" = 6 -> valid
  */
  const comparable = title
    .replace(/[^\p{L}\p{N}]/gu, "");

  if (comparable.length < 5) {
    return {
      ok: false,
      reason: `TITLE TOO SHORT: "${title}"`
    };
  }

  // Harga wajib ada di quoted product. FREE dianggap valid untuk giveaway.
  // ISBN 10/13 digit tidak boleh salah terbaca sebagai harga.
  const price = extractLikelyPrice(quoted);

  if (!price) {
    return {
      ok: false,
      reason: "NO PRICE"
    };
  }

  return {
    ok: true,
    title,
    price
  };
}


/*
  Cari harga di caption/text produk. Format valid antara lain:
  - 185.000 / 180000 / Rp 185.000
  - 120 rb / 120rb / 185k
  - FREE (giveaway)

  Angka ISBN 10/13 digit sengaja ditolak sebagai harga.
*/
function extractLikelyPrice(text) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  for (const line of lines) {
    if (/\bFREE\b/i.test(line)) {
      return "FREE";
    }

    // Format dengan Rp / rb / ribu / k
    const tagged = line.match(/(?:\bRp\.?\s*)?(\d{1,3}(?:[.,]\d{3})+|\d{2,6})\s*(rb|ribu|k)?\b/i);
    if (tagged) {
      const digits = String(tagged[1] || "").replace(/\D/g, "");
      const suffix = String(tagged[2] || "").toLowerCase();
      const hasRp = /\bRp\.?/i.test(line);

      // Jangan pernah menganggap ISBN sebagai harga.
      if (digits.length === 10 || digits.length === 13) {
        continue;
      }

      // Tanpa Rp/suffix, terima angka nominal wajar 4-7 digit atau angka bertitik/koma ribuan.
      if (hasRp || suffix || /[.,]/.test(tagged[1]) || (digits.length >= 4 && digits.length <= 7)) {
        return tagged[0].trim();
      }
    }
  }

  return "";
}


/*
  Ambil kandidat judul dari quoted caption/text.

  Lewati:
  - FIX / MAU
  - ISBN 10 atau 13 digit
  - baris harga murni

  Ambil baris teks pertama yang terlihat seperti judul.
*/
function extractLikelyTitle(text) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  for (const line of lines) {
    if (/^\s*(FIX|MAU)\b/i.test(line)) {
      continue;
    }

    const compactDigits = line.replace(/\D/g, "");

    if (
      compactDigits.length === 10 ||
      compactDigits.length === 13
    ) {
      continue;
    }

    if (
      /^(rp\.?\s*)?[\d.,]+\s*(k|rb|ribu)?$/i.test(line)
    ) {
      continue;
    }

    const meaningful = line.replace(/[^\p{L}\p{N}]/gu, "");

    if (meaningful.length > 0) {
      return line;
    }
  }

  return "";
}


/*
  Kalau Apps Script menjawab PRODUCT/TITLE invalid,
  jangan anggap ini error teknis.
*/
function isInvalidProductBridgeError(err) {
  const msg = String(
    err?.message ||
    err ||
    ""
  ).toUpperCase();

  return (
    msg.includes("NO PRODUCT") ||
    msg.includes("NO TITLE") ||
    msg.includes("NO PRICE") ||
    msg.includes("TITLE TOO SHORT") ||
    msg.includes("PRODUCT")
  );
}


/*
  Kirim job valid ke Apps Script
*/
async function sendJobToAppsScript(job) {
  if (!APPS_SCRIPT_URL) {
    throw new Error("APPS_SCRIPT_URL is missing");
  }

  if (!WORKER_SECRET) {
    throw new Error("WORKER_SECRET is missing");
  }

  const isCancellation =

    job.action === "CANCEL_DELETED_MESSAGE";

  const payload = {
    source: isCancellation
      ? "railway-worker-delete"
      : "railway-worker",
    workerSecret: WORKER_SECRET,
    job: {
      action: String(job.action || ""),
      receivedAt:
        job.receivedAt ||
        job.queuedAt ||
        new Date().toISOString(),

      messageId: String(job.messageId || ""),
      instance: String(job.instance || "Jastip-bot"),
      groupId: String(job.groupId || ""),

      senderLid: String(
        job.participant ||
        job.senderLid ||
        ""
      ),

      senderPhoneRaw: String(
        job.participantAlt ||
        job.senderPhoneRaw ||
        job.senderPhone ||
        ""
      ),

      pushName: String(job.pushName || ""),
      text: String(job.text || ""),

      quotedText: String(
        job.quotedProduct ||
        job.quotedText ||
        ""
      ),

      mentionJid: String(
        job.mentionJid ||
        ""
      )
    }
  };

  const controller = new AbortController();

  // Cancellation dapat menunggu Apps Script lock sampai 30 detik.
  // Timeout lebih panjang mencegah retry palsu saat proses masih berjalan.
  const timeoutMs =
    isCancellation
      ? 65000
      : 20000;

  const timeout = setTimeout(
    () => controller.abort(),
    timeoutMs
  );

  try {
    const response = await fetch(
      APPS_SCRIPT_URL,
      {
        method: "POST",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify(payload),
        redirect: "follow",
        signal: controller.signal
      }
    );

    const bodyText =
      (await response.text()).trim();

    if (!response.ok) {
      throw new Error(
        `Apps Script HTTP ${response.status}: ${bodyText}`
      );
    }

    const accepted = new Set([
      "ACCEPTED",
      "QUEUED",
      "ALREADY QUEUED",
      "CANCELLED",
      "ALREADY CANCELLED",
      "PROTECTED"
    ]);

    if (!accepted.has(bodyText)) {
      throw new Error(
        `Apps Script rejected job: ${bodyText || "(empty response)"}`
      );
    }

    return bodyText;

  } catch (err) {
    if (err && err.name === "AbortError") {
      throw new Error(
        `Apps Script timeout after ${Math.round(timeoutMs / 1000)}s`
      );
    }

    throw err;

  } finally {
    clearTimeout(timeout);
  }
}


/*
  Kirim reaction langsung dari Railway worker ke Evolution API.
*/
function reactionKeyCacheName(groupId, messageId) {
  const reference = String(groupId || "") + "\u0000" + String(messageId || "");
  return REACTION_KEY_CACHE_PREFIX + Buffer.from(reference).toString("base64url");
}

function traceMessageCacheName(groupId, messageId) {
  const reference = String(groupId || "") + "\u0000" + String(messageId || "");
  return TRACE_MESSAGE_CACHE_PREFIX + Buffer.from(reference).toString("base64url");
}

async function cacheTraceOriginalMessage(job) {
  const key = job?.rawKey;
  const message = job?.rawMessage;
  const groupId = String(job?.groupId || key?.remoteJid || "").trim();
  const messageId = String(job?.messageId || key?.id || "").trim();
  if (!groupId || !messageId || !key || typeof key !== "object" || !message || typeof message !== "object") {
    console.warn("TRACE MESSAGE CACHE SKIPPED: incomplete original |", messageId || "MISSING");
    return false;
  }
  await redis.set(
    traceMessageCacheName(groupId, messageId),
    JSON.stringify({ version: 1, storedAt: new Date().toISOString(), key, message }),
    { EX: TRACE_MESSAGE_CACHE_SECONDS }
  );
  console.log("TRACE MESSAGE CACHED:", messageId, "| ttlDays:", TRACE_MESSAGE_CACHE_SECONDS / 86400);
  return true;
}

async function cacheReactionKey(job) {
  const groupId = String(job?.groupId || "").trim();
  const messageId = String(job?.messageId || "").trim();
  const participant = String(job?.participant || job?.rawKey?.participant || "").trim();
  if (!groupId || !messageId || !participant) {
    console.warn("REACTION KEY CACHE SKIPPED: incomplete reference |", messageId || "MISSING");
    return false;
  }
  const cachedReference = {
    version: 1, instance: String(job.instance || "Jastip-bot"), groupId, messageId, participant,
    participantAlt: String(job.participantAlt || ""), senderPhone: String(job.senderPhone || ""), storedAt: new Date().toISOString()
  };
  await redis.set(reactionKeyCacheName(groupId, messageId), JSON.stringify(cachedReference), { EX: REACTION_KEY_CACHE_SECONDS });
  console.log("REACTION KEY CACHED:", messageId, "| ttlDays:", REACTION_KEY_CACHE_SECONDS / 86400);
  return true;
}

async function rememberReaction_(job, emoji) {
  const groupId = String(job?.groupId || "").trim();
  const messageId = String(job?.messageId || "").trim();
  if (!groupId || !messageId) return false;
  await redis.set(reactionKeyCacheName(groupId, messageId) + ":last-reaction", emoji, { EX: REACTION_KEY_CACHE_SECONDS });
  return true;
}

async function processAllocationReaction_(pending) {
  const groupId = String(pending?.groupId || "").trim();
  const messageId = String(pending?.messageId || "").trim();
  const emoji = String(pending?.emoji || "").trim();
  if (groupId === BOOK_SQUAD_GROUP_ID && await redis.exists(deletedOrderKey(groupId, messageId))) return "deleted";
  const referenceName = reactionKeyCacheName(groupId, messageId);
  const lastName = referenceName + ":last-reaction";
  const lockName = referenceName + ":update-lock";
  const lockValue = require("node:crypto").randomUUID();

  const lock = await redis.set(lockName, lockValue, { NX: true, EX: 20 });
  if (lock !== "OK") {
    throw new Error("reaction_update_busy");
  }

  try {
    const raw = await redis.get(referenceName);
    if (!raw) throw new Error("original_key_not_cached");

    const key = JSON.parse(raw);
    if (key.groupId !== groupId || key.messageId !== messageId || !key.participant || !key.instance) {
      throw new Error("invalid_cached_key");
    }

    if (await redis.get(lastName) === emoji) return "unchanged";

    await sendReaction({
      instance: key.instance,
      groupId: key.groupId,
      messageId: key.messageId,
      participant: key.participant
    }, emoji);
    await redis.set(lastName, emoji, { EX: REACTION_KEY_CACHE_SECONDS });
    return "updated";
  } finally {
    try {
      if (await redis.get(lockName) === lockValue) await redis.del(lockName);
    } catch (err) {
      console.error("ALLOCATION REACTION LOCK ERROR:", err.message);
    }
  }
}

async function sendReaction(job, emoji) {
  if (!EVOLUTION_API_URL) {
    throw new Error("EVOLUTION_API_URL is missing");
  }

  if (!EVOLUTION_API_KEY) {
    throw new Error("EVOLUTION_API_KEY is missing");
  }

  const instance = String(
    job.instance ||
    "Jastip-bot"
  );

  const baseUrl = String(EVOLUTION_API_URL)
    .replace(/\/+$/, "");

  const endpoint =
    `${baseUrl}/message/sendReaction/${encodeURIComponent(instance)}`;

  const reactionKey = {
    remoteJid: String(job.groupId || ""),
    fromMe: false,
    id: String(job.messageId || "")
  };

  const participant = String(
    job.participant ||
    job.rawKey?.participant ||
    ""
  );

  if (participant) {
    reactionKey.participant = participant;
  }

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    10000
  );

  try {
    const response = await fetch(
      endpoint,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          apikey: EVOLUTION_API_KEY
        },
        body: JSON.stringify({
          key: reactionKey,
          reaction: emoji
        }),
        signal: controller.signal
      }
    );

    const responseText =
      (await response.text()).trim();

    if (!response.ok) {
      throw new Error(
        `Evolution reaction HTTP ${response.status}: ${responseText}`
      );
    }

    return responseText;

  } catch (err) {
    if (err && err.name === "AbortError") {
      throw new Error(
        "Evolution reaction timeout after 10s"
      );
    }

    throw err;

  } finally {
    clearTimeout(timeout);
  }
}


function isSuspiciousOutgoingBlankMessage(message) {
  if (!message || typeof message !== "object") return false;

  // Tipe internal/non-visible yang normal dan tidak boleh dihitung.
  if (
    message.reactionMessage ||
    message.protocolMessage ||
    message.senderKeyDistributionMessage ||
    message.pollUpdateMessage ||
    message.editedMessage ||
    message.keepInChatMessage
  ) {
    return false;
  }

  if (extractMessageText(message)) return false;

  const contentKeys = Object.keys(message).filter(
    (key) => key !== "messageContextInfo" && key !== "deviceListMetadata"
  );

  // Bubble bug biasanya berupa conversation/text kosong atau envelope kosong.
  return (
    contentKeys.length === 0 ||
    Object.prototype.hasOwnProperty.call(message, "conversation") ||
    Object.prototype.hasOwnProperty.call(message, "extendedTextMessage")
  );
}

async function disconnectEvolutionInstance(instance) {
  if (!EVOLUTION_API_URL || !EVOLUTION_API_KEY) {
    throw new Error("Evolution API config missing for bubble guard");
  }

  const baseUrl = String(EVOLUTION_API_URL).replace(/\/+$/, "");
  const endpoint = `${baseUrl}/instance/logout/${encodeURIComponent(instance)}`;
  const response = await fetch(endpoint, {
    method: "DELETE",
    headers: { apikey: EVOLUTION_API_KEY }
  });
  const body = (await response.text()).trim();
  if (!response.ok) {
    throw new Error(`Evolution logout HTTP ${response.status}: ${body}`);
  }
  return body;
}

async function recordSuspiciousBlankBubble(info) {
  const groupId = String(info.groupId || "");
  const instance = String(info.instance || SPIKE_INSTANCE);
  const counterKey = `jastip:bubble-guard:count:${groupId}`;
  const count = await redis.incr(counterKey);
  if (count === 1) {
    await redis.expire(counterKey, BUBBLE_GUARD_WINDOW_SECONDS);
  }

  const event = {
    detectedAt: new Date().toISOString(),
    instance,
    groupId,
    messageId: String(info.messageId || ""),
    count,
    threshold: BUBBLE_GUARD_THRESHOLD,
    autoDisconnect: BUBBLE_GUARD_AUTO_DISCONNECT,
    action: "MONITOR"
  };

  console.error(
    `BUBBLE GUARD DETECTED | group=${groupId} | count=${count}/${BUBBLE_GUARD_THRESHOLD} | message=${event.messageId || "MISSING"}`
  );

  if (count >= BUBBLE_GUARD_THRESHOLD) {
    const claimed = await redis.set(
      BUBBLE_GUARD_ACTION_LOCK_KEY,
      "1",
      { NX: true, EX: 300 }
    );

    if (claimed) {
      try {
        await sendAdminText(
          `🚨 BUBBLE KOSONG TERDETEKSI\n${count} pesan kosong dari Evolution dalam ${BUBBLE_GUARD_WINDOW_SECONDS} detik.\nGroup: ${groupId}\nMode: ${BUBBLE_GUARD_AUTO_DISCONNECT ? "AUTO DISCONNECT" : "MONITOR SAJA"}`
        );
      } catch (alertError) {
        console.error("BUBBLE GUARD ALERT ERROR:", alertError.message);
      }

      if (BUBBLE_GUARD_AUTO_DISCONNECT) {
        try {
          await disconnectEvolutionInstance(instance);
          event.action = "DISCONNECTED";
          console.error("BUBBLE GUARD: EVOLUTION INSTANCE DISCONNECTED", instance);
        } catch (disconnectError) {
          event.action = "DISCONNECT_FAILED";
          event.error = disconnectError.message;
          console.error("BUBBLE GUARD DISCONNECT ERROR:", disconnectError.message);
        }
      } else {
        event.action = "ALERTED";
      }
    }
  }

  await redis.set(BUBBLE_GUARD_LAST_KEY, JSON.stringify(event));
  await redis.lPush(BUBBLE_GUARD_HISTORY_KEY, JSON.stringify(event));
  await redis.lTrim(BUBBLE_GUARD_HISTORY_KEY, 0, 19);

  return { count, action: event.action };
}

function extractMessageText(message) {
  if (!message || typeof message !== "object") {
    return "";
  }

  if (message.conversation) {
    return String(message.conversation).trim();
  }

  if (message.extendedTextMessage?.text) {
    return String(
      message.extendedTextMessage.text
    ).trim();
  }

  if (message.imageMessage?.caption) {
    return String(
      message.imageMessage.caption
    ).trim();
  }

  if (message.videoMessage?.caption) {
    return String(
      message.videoMessage.caption
    ).trim();
  }

  if (message.documentMessage?.caption) {
    return String(
      message.documentMessage.caption
    ).trim();
  }

  return "";
}


// =====================================================
// PRIVATE REKAP COMMAND — "rekap bbw", "rekap bbw jogja", "rekap bbw gi"
// =====================================================
function parsePrivateRecapCommand_(text) {
  const normalized = String(text || "")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ");

  const commands = {
    "rekap bbw": {
      recapType: "BBW",
      sourceSheet: "ORDER BBW",
      eventLabel: "BBW"
    },
    "rekap bbw sekarang": {
      recapType: "BBW",
      sourceSheet: "ORDER BBW",
      eventLabel: "BBW"
    },
    "rekap bbw jogja": {
      recapType: "BBW_JOGJA",
      sourceSheet: "ORDER BBW JOGJA",
      eventLabel: "BBW Jogja"
    },
    "rekap bbw gi": {
      recapType: "BBW_GI",
      sourceSheet: "ORDER BBW GI",
      eventLabel: "BBW GI"
    }
  };

  return commands[normalized] || null;
}

function privateChatPhone_(remoteJid, fallback) {
  const source = String(remoteJid || fallback || "").trim();
  const bare = source.replace(/@.+$/, "").replace(/\D/g, "");
  const phone = bare.replace(/^0/, "62");
  return /^62\d{8,15}$/.test(phone) ? phone : "";
}

function privateRecapReplyText_(job, data) {
  const eventLabel = String(job.eventLabel || "BBW");
  const name = String(data?.customerName || "").trim();
  const link = String(data?.link || "").trim();
  const greeting = name ? `Hai kak ${name},` : "Hai kak,";

  return [
    `📚 REKAP ORDER ${eventLabel.toUpperCase()}`,
    "",
    greeting,
    `Ini link rekapan order ${eventLabel} kak:`,
    link,
    "",
    "Link berlaku 7 hari ya kak 😊"
  ].join("\n");
}

function privateRecapNotFoundText_(job) {
  return [
    `Maaf kak, data order ${String(job.eventLabel || "BBW")} untuk nomor WhatsApp ini belum ditemukan.`,
    "Pastikan nomor yang dipakai sama dengan nomor saat FIX ya 😊"
  ].join("\n");
}

async function requestPrivateRecapLink_(job) {
  if (!RECAP_WEB_APP_URL || !RECAP_API_KEY) {
    throw new Error("private_recap_not_configured");
  }

  const endpoint = RECAP_WEB_APP_URL.replace(/\/+$/, "");
  const body = JSON.stringify({
    action: "generate_link_only",
    apiKey: RECAP_API_KEY,
    phone: job.customerPhone,
    recapType: job.recapType,
    sourceSheet: job.sourceSheet
  });

  let lastError = null;
  for (let attempt = 1; attempt <= 3; attempt++) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 20000);
    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body,
        redirect: "follow",
        signal: controller.signal
      });
      const text = await response.text();

      // Google Apps Script kadang memberi 404 sesaat tepat setelah redirect.
      if (response.status === 404 && attempt < 3) {
        await sleep(attempt * 700);
        continue;
      }

      if (!response.ok) {
        throw new Error(`recap_http_${response.status}`);
      }

      let data;
      try {
        data = JSON.parse(text);
      } catch (_) {
        throw new Error("recap_invalid_response");
      }
      if (!data || data.success !== true) {
        throw new Error(data?.message || "recap_rejected");
      }
      return data;
    } catch (err) {
      lastError = err;
      if (attempt < 3 && (err?.name === "AbortError" || String(err.message || "").startsWith("recap_http_5"))) {
        await sleep(attempt * 700);
        continue;
      }
      break;
    } finally {
      clearTimeout(timeout);
    }
  }
  throw lastError || new Error("recap_request_failed");
}

async function sendPrivateRecapText_(job, text) {
  if (!EVOLUTION_API_URL || !EVOLUTION_API_KEY) {
    throw new Error("evolution_not_configured");
  }

  const endpoint = String(EVOLUTION_API_URL).replace(/\/+$/, "") +
    "/message/sendText/" + encodeURIComponent(String(job.instance || RECAP_INSTANCE));
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json", apikey: EVOLUTION_API_KEY },
      body: JSON.stringify({ number: job.customerPhone, text }),
      signal: controller.signal
    });
    if (!response.ok) throw new Error(`evolution_send_http_${response.status}`);
  } finally {
    clearTimeout(timeout);
  }
}

async function privateRecapWorker() {
  console.log("Private recap worker started...");

  while (true) {
    try {
      const popped = await privateRecapBlockingRedis.blPop(PRIVATE_RECAP_QUEUE, 5);
      const raw = String(popped?.element || "");
      if (!raw) continue;

      let job;
      try {
        job = JSON.parse(raw);
      } catch (_) {
        console.error("PRIVATE RECAP INVALID JOB");
        continue;
      }

      const data = await requestPrivateRecapLink_(job);
      const reply = data.found === false
        ? privateRecapNotFoundText_(job)
        : privateRecapReplyText_(job, data);
      await sendPrivateRecapText_(job, reply);
      console.log("PRIVATE RECAP SENT:", job.recapType, "|", job.customerPhone);
    } catch (err) {
      // Tidak pernah memasukkan ulang ke queue utama / queue FIX. Customer
      // bisa mengirim command lagi bila layanan rekap sedang tidak tersedia.
      console.error("PRIVATE RECAP WORKER ERROR:", err.message);
      await sleep(1000);
    }
  }
}


function extractQuotedProduct(message, dataContextInfo) {
  try {
    const nestedCtx =
      message?.extendedTextMessage?.contextInfo ||
      message?.imageMessage?.contextInfo ||
      message?.videoMessage?.contextInfo ||
      message?.documentMessage?.contextInfo ||
      {};

    const ctx =
      (dataContextInfo &&
       typeof dataContextInfo === "object" &&
       dataContextInfo.quotedMessage)
        ? dataContextInfo
        : nestedCtx;

    const quoted =
      ctx?.quotedMessage ||
      {};

    if (quoted.conversation) {
      return String(
        quoted.conversation
      ).trim();
    }

    if (quoted.extendedTextMessage?.text) {
      return String(
        quoted.extendedTextMessage.text
      ).trim();
    }

    if (quoted.imageMessage?.caption) {
      return String(
        quoted.imageMessage.caption
      ).trim();
    }

    if (quoted.videoMessage?.caption) {
      return String(
        quoted.videoMessage.caption
      ).trim();
    }

    if (quoted.documentMessage?.caption) {
      return String(
        quoted.documentMessage.caption
      ).trim();
    }

    return "";

  } catch (err) {
    console.error("QUOTED PARSE ERROR:", err);
    return "";
  }
}


function extractMentionJid(message, dataContextInfo) {
  try {
    const topCtx =
      dataContextInfo &&
      typeof dataContextInfo === "object"
        ? dataContextInfo
        : {};

    const nestedCtx =
      message?.extendedTextMessage?.contextInfo ||
      message?.imageMessage?.contextInfo ||
      message?.videoMessage?.contextInfo ||
      message?.documentMessage?.contextInfo ||

      {};

    const mentioned =
      topCtx.mentionedJid ||
      nestedCtx.mentionedJid ||
      [];

    if (!Array.isArray(mentioned) || !mentioned.length) {
      return "";
    }

    return String(mentioned[0] || "").trim();

  } catch (err) {
    return "";
  }
}


function cleanPhone(jid) {
  if (!jid) {
    return "";
  }

  return String(jid)
    .replace(/@s\.whatsapp\.net$/i, "")
    .replace(/@lid$/i, "")
    .replace(/\D/g, "");
}


function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}


/*
  RECOVERY SETELAH WORKER RESTART

  BRPOPLPUSH memindahkan job ke jastip:processing sebelum diproses.
  Kalau Railway restart di tengah proses, job bisa tertinggal di sana.
  Saat startup, kembalikan semuanya ke queue agar tidak macet selamanya.

  Apps Script tetap menjadi pengaman duplikat: bila job sebelumnya sudah
  diterima, jawabannya ALREADY QUEUED dan worker akan menyelesaikannya.
*/
async function recoverStuckProcessingJobs() {
  const stuckCount = await redis.lLen(PROCESSING_QUEUE);

  if (stuckCount === 0) {
    console.log("RECOVERY: no stuck processing jobs");
    return;
  }

  console.warn(
    `RECOVERY: moving ${stuckCount} stuck processing job(s) back to queue`
  );

  let recovered = 0;

  while (true) {
    const movedJob = await redis.rPopLPush(
      PROCESSING_QUEUE,
      QUEUE_NAME
    );

    if (!movedJob) {
      break;
    }

    recovered += 1;
  }

  console.log(
    `RECOVERY DONE: ${recovered} job(s) returned to queue`
  );
}


// =====================================================
// SPIKE MONITOR HELPERS
// =====================================================
function formatJakarta(value) {
  try {
    return new Intl.DateTimeFormat("id-ID", {
      timeZone: "Asia/Jakarta",
      day: "2-digit", month: "short", year: "numeric",
      hour: "2-digit", minute: "2-digit", second: "2-digit",
      hour12: false
    }).format(new Date(value));
  } catch {
    return String(value || "-");
  }
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

async function sendAdminText(text) {
  if (!EVOLUTION_API_URL || !EVOLUTION_API_KEY) {
    throw new Error("Evolution API config missing for spike alert");
  }

  const baseUrl = String(EVOLUTION_API_URL).replace(/\/+$/, "");
  const endpoint = `${baseUrl}/message/sendText/${encodeURIComponent(SPIKE_INSTANCE)}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10000);

  try {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        apikey: EVOLUTION_API_KEY
      },
      body: JSON.stringify({
        number: ADMIN_WA_NUMBER,
        text
      }),
      signal: controller.signal
    });
    const body = (await response.text()).trim();
    if (!response.ok) {
      throw new Error(`Evolution sendText HTTP ${response.status}: ${body}`);
    }
    return body;
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchEvolutionRailwayMetrics() {
  if (!RAILWAY_API_TOKEN && !RAILWAY_PROJECT_TOKEN) {
    throw new Error("RAILWAY_API_TOKEN atau RAILWAY_TOKEN is missing");
  }

  const railwayHeaders = {
    "content-type": "application/json"
  };

  if (RAILWAY_PROJECT_TOKEN) {
    railwayHeaders["Project-Access-Token"] = RAILWAY_PROJECT_TOKEN;
  } else {
    railwayHeaders.authorization = `Bearer ${RAILWAY_API_TOKEN}`;
  }

  const end = new Date();
  const start = new Date(end.getTime() - 3 * 60 * 1000);
  const query = `
    query EvolutionSpikeMetrics(
      $projectId: String!,
      $environmentId: String!,
      $serviceId: String!,
      $startDate: DateTime!,
      $endDate: DateTime!,
      $sampleRateSeconds: Int!,
      $measurements: [MetricMeasurement!]!
    ) {
      metrics(
        projectId: $projectId,
        environmentId: $environmentId,
        serviceId: $serviceId,
        startDate: $startDate,
        endDate: $endDate,
        sampleRateSeconds: $sampleRateSeconds,
        measurements: $measurements
      ) {
        measurement
        values { ts value }
      }
    }
  `;

  const response = await fetch(RAILWAY_GRAPHQL_URL, {
    method: "POST",
    headers: railwayHeaders,
    body: JSON.stringify({
      query,
      variables: {
        projectId: RAILWAY_PROJECT_ID,
        environmentId: RAILWAY_ENVIRONMENT_ID,
        serviceId: RAILWAY_EVOLUTION_SERVICE_ID,
        startDate: start.toISOString(),
        endDate: end.toISOString(),
        sampleRateSeconds: 60,
        measurements: ["CPU_USAGE", "MEMORY_USAGE_GB"]
      }
    })
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload.errors) {
    const msg = payload?.errors?.map((e) => e.message).join(" | ") || `HTTP ${response.status}`;
    throw new Error(`Railway metrics API: ${msg}`);
  }

  const rows = Array.isArray(payload?.data?.metrics) ? payload.data.metrics : [];
  const latest = (measurement) => {
    const row = rows.find((r) => String(r.measurement).toUpperCase() === measurement);
    const values = Array.isArray(row?.values) ? row.values : [];
    if (!values.length) return 0;
    const last = values[values.length - 1];
    return Number(last?.value || 0);
  };

  return {
    cpu: latest("CPU_USAGE"),
    memoryGB: latest("MEMORY_USAGE_GB"),
    checkedAt: end.toISOString()
  };
}

async function saveSpikeState(state) {
  await redis.set(SPIKE_STATE_KEY, JSON.stringify(state));
}

async function finishSpike(state, endedAt) {
  const finished = {
    ...state,
    active: false,
    endedAt,
    durationMs: new Date(endedAt).getTime() - new Date(state.startedAt).getTime()
  };
  delete finished.normalSince;
  await redis.lPush(SPIKE_HISTORY_KEY, JSON.stringify(finished));
  await redis.lTrim(SPIKE_HISTORY_KEY, 0, SPIKE_HISTORY_LIMIT - 1);
  await redis.del(SPIKE_STATE_KEY);
  console.log(`SPIKE END | peak RAM ${finished.peakMemoryGB.toFixed(2)} GB | peak CPU ${finished.peakCpu.toFixed(2)} vCPU | duration ${Math.round(finished.durationMs / 60000)} min`);
}

async function runSpikeMonitorOnce() {
  const metrics = await fetchEvolutionRailwayMetrics();
  const nowMs = new Date(metrics.checkedAt).getTime();
  const isHigh = metrics.memoryGB >= SPIKE_MEMORY_GB || metrics.cpu >= SPIKE_CPU_VCPU;

  console.log(`SPIKE MONITOR | CPU ${metrics.cpu.toFixed(2)} vCPU | RAM ${metrics.memoryGB.toFixed(2)} GB | ${isHigh ? "HIGH" : "NORMAL"}`);

  const raw = await redis.get(SPIKE_STATE_KEY);
  let state = raw ? JSON.parse(raw) : null;

  if (!state && !isHigh) return;

  if (!state && isHigh) {
    state = {
      active: true,
      startedAt: metrics.checkedAt,
      lastCheckedAt: metrics.checkedAt,
      currentCpu: metrics.cpu,
      currentMemoryGB: metrics.memoryGB,
      peakCpu: metrics.cpu,
      peakMemoryGB: metrics.memoryGB,
      warningSent: false,
      criticalSent: false,
      normalSince: null
    };
    await saveSpikeState(state);
    console.warn(`SPIKE START | CPU ${metrics.cpu.toFixed(2)} | RAM ${metrics.memoryGB.toFixed(2)} GB`);
    return;
  }

  state.lastCheckedAt = metrics.checkedAt;
  state.currentCpu = metrics.cpu;
  state.currentMemoryGB = metrics.memoryGB;
  state.peakCpu = Math.max(Number(state.peakCpu || 0), metrics.cpu);
  state.peakMemoryGB = Math.max(Number(state.peakMemoryGB || 0), metrics.memoryGB);

  if (isHigh) {
    state.normalSince = null;
    const durationMs = nowMs - new Date(state.startedAt).getTime();

    if (!state.warningSent && durationMs >= SPIKE_WARNING_AFTER_MS) {
      const msg = `⚠️ EVOLUTION API WARNING\nResource tinggi sudah ±${Math.round(durationMs / 60000)} menit.\nRAM: ${metrics.memoryGB.toFixed(2)} GB (peak ${state.peakMemoryGB.toFixed(2)} GB)\nCPU: ${metrics.cpu.toFixed(2)} vCPU (peak ${state.peakCpu.toFixed(2)} vCPU)\n\nCek: https://jastip-worker-production.up.railway.app/spike-log`;
      try {
        await sendAdminText(msg);
        state.warningSent = true;
        state.warningSentAt = metrics.checkedAt;
        console.warn("SPIKE WARNING WA SENT");
      } catch (err) {
        console.error("SPIKE WARNING WA ERROR:", err.message);
      }
    }

    if (!state.criticalSent && durationMs >= SPIKE_CRITICAL_AFTER_MS) {
      const msg = `🚨 EVOLUTION API CRITICAL\nResource masih tinggi ±${Math.round(durationMs / 60000)} menit.\nRAM: ${metrics.memoryGB.toFixed(2)} GB (peak ${state.peakMemoryGB.toFixed(2)} GB)\nCPU: ${metrics.cpu.toFixed(2)} vCPU (peak ${state.peakCpu.toFixed(2)} vCPU)\n\nTidak ada auto-restart.\nCek: https://jastip-worker-production.up.railway.app/spike-log`;
      try {
        await sendAdminText(msg);
        state.criticalSent = true;
        state.criticalSentAt = metrics.checkedAt;
        console.error("SPIKE CRITICAL WA SENT");
      } catch (err) {
        console.error("SPIKE CRITICAL WA ERROR:", err.message);
      }
    }

    await saveSpikeState(state);
    return;
  }

  if (!state.normalSince) {
    state.normalSince = metrics.checkedAt;
    await saveSpikeState(state);
    return;
  }

  const normalForMs = nowMs - new Date(state.normalSince).getTime();
  if (normalForMs >= SPIKE_RECOVERY_AFTER_MS) {
    await finishSpike(state, metrics.checkedAt);
  } else {
    await saveSpikeState(state);
  }
}

function startSpikeMonitor() {
  if (!RAILWAY_API_TOKEN && !RAILWAY_PROJECT_TOKEN) {
    console.warn("SPIKE MONITOR DISABLED: Railway token missing");
    return;
  }

  console.log("SPIKE MONITOR STARTED: Evolution API checked every 60 seconds");

  const tick = async () => {
    try {
      await runSpikeMonitorOnce();
    } catch (err) {
      // Monitor tidak boleh menjatuhkan Jastip Worker.
      console.error("SPIKE MONITOR ERROR:", err.message);
    }
  };

  setTimeout(tick, 5000);
  setInterval(tick, SPIKE_CHECK_INTERVAL_MS);
}


async function start() {
  if (!REDIS_URL) {
    console.error("REDIS_URL IS MISSING");
    process.exit(1);
  }

  console.log("Connecting Redis...");

  await Promise.all(redisClients.map(([, client]) => client.connect()));

  console.log("Redis connected.");

  await recoverStuckProcessingJobs();
  await recoverPendingInitialReactions_();

  app.listen(
    PORT,
    "0.0.0.0",
    () => {
      console.log(
        `Jastip Worker running on port ${PORT}`
      );
    }
  );

  // Spike monitor berjalan terpisah dan tidak mengubah alur FIX/MAU.
  startSpikeMonitor();

  sheetRetryDispatcher_().catch((err) => {
    console.error("FATAL SHEET RETRY DISPATCHER ERROR:", err);
    process.exit(1);
  });

  worker().catch((err) => {
    console.error(
      "FATAL WORKER ERROR:",
      err
    );

    process.exit(1);
  });

  // Dipisahkan dari worker FIX/MAU supaya masalah Evolution tidak membuat
  // order baru berhenti dicatat ke Google Sheets.
  allocationReactionWorker().catch((err) => {
    console.error("FATAL ALLOCATION REACTION WORKER ERROR:", err);
    process.exit(1);
  });

  initialReactionRetryDispatcher_().catch((err) => console.error("FATAL INITIAL REACTION RETRY DISPATCHER:", err));

  // Evolution reaction awal diproses terpisah; order baru tetap menuju
  // Google Sheets walau Evolution sedang lambat atau sementara offline.
  for (const [workerNumber, client] of [[1, initialReactionBlockingRedis], [2, initialReactionBlockingRedis2]]) {
    initialReactionWorker(client, workerNumber).catch((err) => {
      console.error("FATAL INITIAL REACTION WORKER ERROR:", workerNumber, err);
      process.exit(1);
    });
  }

  // Dipisahkan dari order queue: request rekap private tidak boleh menunda
  // pencatatan FIX/MAU bila Web App Rekapan sedang lambat.
  privateRecapWorker().catch((err) => {
    console.error("FATAL PRIVATE RECAP WORKER ERROR:", err);
    process.exit(1);
  });
}


start().catch((err) => {
  console.error("START ERROR:", err);
  process.exit(1);
});

// TRACE REPLY PRIVATE ENDPOINT

// TRACE Reply Private: isolated from order queue, Sheets, and normal webhook flow.
app.post("/trace/reply-private", async (req, res) => {
  try {
    if (!hasValidTraceSecret_(req)) {
      return res.status(401).json({ ok: false, error: "Unauthorized trace request." });
    }
    const checked = validateTraceReplyRequest_(req.body || {});
    if (!checked.ok) return res.status(400).json({ ok: false, error: checked.error });

    const original = await findOriginalEvolutionMessage_(checked.value);
    if (!original) {
      return res.status(404).json({ ok: false, error: "Original WhatsApp message not available for native quote." });
    }
    await sendNativePrivateQuote_(checked.value, original);
    return res.json({ ok: true, nativeQuote: true });
  } catch (err) {
    console.error("TRACE REPLY PRIVATE ERROR:", err.message);
    const unavailable = String(err.message || "").includes("Original WhatsApp message not available");
    return res.status(unavailable ? 404 : 502).json({
      ok: false,
      error: unavailable ? "Original WhatsApp message not available for native quote." : "Reply Private could not be sent."
    });
  }
});

function hasValidTraceSecret_(req) {
  const supplied = String(
    req.get("x-bma-trace-secret") ||
    req.get("x-reaction-sync-secret") ||
    ""
  );
  const expected = String(TRACE_PORTAL_SECRET || "");
  if (!supplied || !expected) return false;
  const left = Buffer.from(supplied);
  const right = Buffer.from(expected);
  return left.length === right.length && require("crypto").timingSafeEqual(left, right);
}

function normalizeTracePhone_(value) {
  return String(value || "").replace(/\D/g, "").replace(/^0/, "62");
}

function traceSourceGroups_() {
  const builtIn = {
    "ORDER": "120363214326633370@g.us",
    "ORDER LIVE": "120363427983824748@g.us",
    "ORDER BBW": "120363414084709085@g.us"
  };
  if (!TRACE_SOURCE_GROUPS_JSON) return builtIn;
  try {
    const groups = JSON.parse(TRACE_SOURCE_GROUPS_JSON);
    if (!groups || typeof groups !== "object" || Array.isArray(groups)) throw new Error("must be an object");
    return { ...builtIn, ...groups };
  } catch (err) {
    throw new Error("Invalid TRACE_SOURCE_GROUPS_JSON: " + err.message);
  }
}

function validateTraceReplyRequest_(input) {
  const source = String(input.source || input.sourceSheet || "").trim();
  const groupId = String(input.groupId || "").trim();
  const messageId = String(input.messageId || "").trim();
  const customerPhone = normalizeTracePhone_(input.customerPhone);
  const qty = Number(input.qty);
  const orderAt = new Date(input.orderAt);
  const replyText = String(input.replyText || "").trim();
  if (!source) return { ok: false, error: "source required" };
  if (!groupId || !groupId.endsWith("@g.us")) return { ok: false, error: "valid groupId required" };
  if (!messageId) return { ok: false, error: "messageId required" };
  if (!/^62\d{8,15}$/.test(customerPhone)) return { ok: false, error: "valid customerPhone required" };
  if (!Number.isFinite(qty) || qty <= 0) return { ok: false, error: "Qty must be greater than 0" };
  if (Number.isNaN(orderAt.getTime()) || Date.now() - orderAt.getTime() > TRACE_REPLY_MAX_AGE_MS || orderAt.getTime() > Date.now() + 300000) {
    return { ok: false, error: "Order must be within the last 14 days" };
  }
  if (!replyText || replyText.length > 4096) return { ok: false, error: "replyText required (max 4096 characters)" };
  if (String(traceSourceGroups_()[source] || "") !== groupId || !SUPPORTED_GROUPS.has(groupId)) {
    return { ok: false, error: "Group ID is not allowed for this source" };
  }
  return { ok: true, value: { source, groupId, messageId, customerPhone, qty, orderAt, replyText, instance: String(input.instance || TRACE_REPLY_INSTANCE) } };
}

function traceCandidate_(payload) {
  const data = payload && (payload.response || payload.data || payload);
  const list = Array.isArray(data) ? data : (data && (data.messages || data.records || data.items));
  return Array.isArray(list) ? (list[0] || null) : null;
}

function traceParticipantIds_(candidate, key) {
  const nested = candidate && candidate.message && candidate.message.key;
  return [key && key.participant, key && key.participantAlt, candidate && candidate.participant, candidate && candidate.participantAlt, nested && nested.participant, nested && nested.participantAlt].filter(Boolean).map(String);
}

// Google Sheet menambahkan "-1", "-2", dan seterusnya pada Message ID supaya
// satu chat yang memesan beberapa judul mempunyai row ID yang unik. Itu bukan
// bagian dari key WhatsApp asli. Lookup tetap mencoba nilai yang tersimpan dulu,
// lalu fallback ke raw key tanpa suffix item. Dengan begitu message key asli
// tetap wajib cocok dan quote tidak pernah dibuat dari teks palsu.
function traceLookupMessageIds_(messageId) {
  const stored = String(messageId || "").trim();
  const raw = stored.replace(/-\d+$/, "");
  return raw && raw !== stored ? [stored, raw] : [stored];
}

function isValidTraceOriginal_(candidate, request, lookupMessageId) {
  const key = candidate && candidate.key;
  const message = candidate && candidate.message;
  if (!key || !message || key.id !== lookupMessageId || key.remoteJid !== request.groupId || key.fromMe === true) return false;
  const phones = traceParticipantIds_(candidate, key)
    .filter((id) => /^(?:0|62)\d{8,15}(?:@|$)/.test(id))
    .map(normalizeTracePhone_);
  return !phones.length || phones.includes(request.customerPhone);
}

async function findCachedTraceOriginalMessage_(request) {
  for (const lookupMessageId of traceLookupMessageIds_(request.messageId)) {
    const raw = await redis.get(traceMessageCacheName(request.groupId, lookupMessageId));
    if (!raw) continue;
    let candidate;
    try { candidate = JSON.parse(raw); } catch { continue; }
    if (isValidTraceOriginal_(candidate, request, lookupMessageId)) return candidate;
  }
  return null;
}

async function findOriginalEvolutionMessage_(request) {
  if (!EVOLUTION_API_URL || !EVOLUTION_API_KEY) throw new Error("Original WhatsApp message not available for native quote.");
  // Evolution message database tidak selalu menyimpan incoming payload. Prioritaskan
  // cache payload asli yang Worker terima, lalu gunakan lookup Evolution sebagai fallback.
  const cached = await findCachedTraceOriginalMessage_(request);
  if (cached) return cached;
  const endpoint = String(EVOLUTION_API_URL).replace(/\/+$/, "") + "/chat/findMessages/" + encodeURIComponent(request.instance);
  for (const lookupMessageId of traceLookupMessageIds_(request.messageId)) {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json", apikey: EVOLUTION_API_KEY },
      body: JSON.stringify({ where: { key: { remoteJid: request.groupId, fromMe: false, id: lookupMessageId } }, limit: 1 })
    });
    if (!response.ok) {
      if (response.status === 404) continue;
      throw new Error("Evolution message lookup HTTP " + response.status);
    }
    const candidate = traceCandidate_(await response.json());
    const key = candidate && (candidate.key || (candidate.message && candidate.message.key));
    const message = candidate && (candidate.message || candidate);
    if (!isValidTraceOriginal_({ key, message, participant: candidate?.participant, participantAlt: candidate?.participantAlt }, request, lookupMessageId)) continue;
    return { key, message };
  }
  throw new Error("Original WhatsApp message not available for native quote.");
}

async function sendNativePrivateQuote_(request, original) {
  const endpoint = String(EVOLUTION_API_URL).replace(/\/+$/, "") + "/message/sendText/" + encodeURIComponent(request.instance);
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "content-type": "application/json", apikey: EVOLUTION_API_KEY },
    body: JSON.stringify({ number: request.customerPhone, text: request.replyText, quoted: original })
  });
  if (!response.ok) throw new Error("Evolution quoted send HTTP " + response.status);
}

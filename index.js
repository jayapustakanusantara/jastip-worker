// JASTIP WORKER V3.8 - V3.7 + EVOLUTION EMPTY-BUBBLE BUG GUARD
const express = require("express");
const { createClient } = require("redis");

// Routine logs are disabled by default. Errors retain their existing paths.
function debugLog() { /* deliberately omit routine/customer payloads */ }

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
const TRACE_PORTAL_SECRET = process.env.TRACE_PORTAL_SECRET || WORKER_SECRET;
const TRACE_SOURCE_GROUPS_JSON = process.env.TRACE_SOURCE_GROUPS_JSON || "";
const TRACE_REPLY_MAX_AGE_MS = 14 * 24 * 60 * 60 * 1000;
const TRACE_REPLY_INSTANCE = process.env.TRACE_REPLY_INSTANCE || "Jastip-bot";


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
const BBW_GROUP_ID = "120363414084709085@g.us";

const SUPPORTED_GROUPS = new Set([
  "120363214326633370@g.us", // ORDER
  "120363427983824748@g.us", // ORDER LIVE
  "120363414084709085@g.us"  // ORDER BBW
]);

const redis = createClient({
  url: REDIS_URL
});

redis.on("error", (err) => {
  console.error("REDIS ERROR:", err);
});

redis.on("ready", () => {
  debugLog("Redis READY");
});

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "jastip-worker",
    redis: redis.isReady,
    appsScriptConfigured: Boolean(APPS_SCRIPT_URL),
    workerSecretConfigured: Boolean(WORKER_SECRET),
    evolutionConfigured: Boolean(EVOLUTION_API_URL && EVOLUTION_API_KEY)
  });
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
      debugLog(
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
      debugLog(
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
      if (remoteJid !== BBW_GROUP_ID) {
        return res.status(200).json({
          ok: true,
          ignored: true,
          reason: "message delete cancellation is BBW only",
          remoteJid,
          messageId
        });
      }

      const deleteSeenKey =
        `jastip:delete-seen:${messageId}`;

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

      debugLog(
        "DELETE TARGET RESOLVED:",
        messageId,
        "|",
        remoteJid,
        "| sender:",
        cleanPhone(participantAlt || participant) || "unknown"
      );

      debugLog(
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

    // Outgoing bot messages never become customer orders. Keep delete and
    // bubble detection above this guard so their existing paths are preserved.
    if (fromMe) return res.status(200).json({ ok: true, ignored: true, reason: "bot message" });

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

      rawKey: key
    };

    /*
      Anti duplicate message ID.
      Evolution bisa retry webhook yang sama.
    */
    const seenKey = `jastip:seen:${messageId}`;

    const firstTime = await redis.set(
      seenKey,
      "1",
      {
        NX: true,
        EX: 60 * 60 * 24 * 7
      }
    );

    if (!firstTime) {
      debugLog("DUPLICATE WEBHOOK:", messageId);

      return res.status(200).json({
        ok: true,
        duplicate: true,
        messageId
      });
    }

    await redis.lPush(
      QUEUE_NAME,
      JSON.stringify(job)
    );

    debugLog(
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

    const seenKey = `jastip:seen:${payload.messageId}`;

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
async function worker() {
  debugLog("Worker started...");

  while (true) {
    try {
      const rawJob = await redis.brPopLPush(
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
        console.error("INVALID_JOB: malformed queue payload");

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
      debugLog(`QUEUE: ${queueWaitingNow + queueProcessingNow} (waiting ${queueWaitingNow}, processing ${queueProcessingNow})`);

      // Customer content is deliberately omitted from routine logs.


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

      const validation =
        isCancellation
          ? { ok: true }
          : validateOrderJob(job);

      if (!validation.ok) {
        debugLog(
          "INVALID ORDER:",
          job.messageId,
          "|",
          validation.reason
        );

        try {
          await sendReaction(job, "❌");

          debugLog(
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

        debugLog(
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

        if (!isCancellation && (result === "QUEUED" || result === "ALREADY QUEUED")) {
          console.log("ORDER_ACCEPTED"); // Apps Script acceptance, not confirmed Sheets write.
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
        if (isInvalidProductBridgeError(bridgeError)) {
          try {
            await sendReaction(job, "❌");

            debugLog(
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

          debugLog(
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

        await redis.lRem(
          PROCESSING_QUEUE,
          1,
          rawJob
        );

        const maxBridgeRetry =
          isCancellation ? 10 : 20;

        if (retryCount <= maxBridgeRetry) {
          await redis.rPush(
            QUEUE_NAME,
            JSON.stringify(retryJob)
          );

          debugLog(
            "REQUEUED:",
            job.messageId,
            "| retry",
            retryCount
          );

          const retryDelayMs =
            isCancellation
              ? Math.min(60000, 10000 * retryCount)
              : Math.min(30000, 1500 * retryCount);

          await sleep(retryDelayMs);

        } else {
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
    debugLog("RECOVERY: no stuck processing jobs");
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

  debugLog(
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
  const { readMetrics, latestReading } = require("./lib/railway-metrics.cjs");
  const end = Date.now();
  const env = { ...process.env, RAILWAY_PROJECT_ID, RAILWAY_ENVIRONMENT_ID,
    RAILWAY_EVOLUTION_SERVICE_ID, RAILWAY_GRAPHQL_URL };
  const series = await readMetrics({ env, start: end - 3 * 60000, end });
  return latestReading(series, end);
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

  // Log state transitions below, not every successful metrics poll.

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

  // A gap in observations cannot establish five continuous normal minutes.
  if (nowMs <= Date.parse(state.lastCheckedAt)) return;
  if (nowMs - Date.parse(state.lastCheckedAt) > 2 * SPIKE_CHECK_INTERVAL_MS) {
    state.normalSince = null;
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
    console.log(`SPIKE_NORMAL_DETECTED RAM=${metrics.memoryGB.toFixed(2)}GB CPU=${metrics.cpu.toFixed(2)}`);
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

  debugLog("SPIKE MONITOR STARTED: Evolution API checked every 60 seconds");

  let running = false;
  let lastError = "";
  let lastErrorAt = 0;
  const tick = async () => {
    if (running) return;
    running = true;
    try {
      await runSpikeMonitorOnce();
      if (lastError) console.log("METRICS_RECOVERED");
      lastError = "";
    } catch (err) {
      // Monitor failure never stops order processing; repeated errors are bounded.
      if (err.message !== lastError || Date.now() - lastErrorAt >= 15 * 60000) {
        console.error("SPIKE MONITOR ERROR:", err.message);
        lastErrorAt = Date.now();
      }
      lastError = err.message;
    } finally { running = false; }
  };

  setTimeout(tick, 5000);
  setInterval(tick, SPIKE_CHECK_INTERVAL_MS);
}


async function start() {
  if (!REDIS_URL) {
    console.error("REDIS_URL IS MISSING");
    process.exit(1);
  }

  debugLog("Connecting Redis...");

  await redis.connect();

  debugLog("Redis connected.");

  await recoverStuckProcessingJobs();

  app.listen(
    PORT,
    "0.0.0.0",
    () => {
      debugLog(
        `Jastip Worker running on port ${PORT}`
      );
    }
  );

  // Spike monitor berjalan terpisah dan tidak mengubah alur FIX/MAU.
  startSpikeMonitor();

  worker().catch((err) => {
    console.error(
      "FATAL WORKER ERROR:",
      err
    );

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
  const supplied = String(req.get("x-bma-trace-secret") || "");
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
  if (!TRACE_SOURCE_GROUPS_JSON) throw new Error("TRACE_SOURCE_GROUPS_JSON is missing");
  try {
    const groups = JSON.parse(TRACE_SOURCE_GROUPS_JSON);
    if (!groups || typeof groups !== "object" || Array.isArray(groups)) throw new Error("must be an object");
    return groups;
  } catch (err) {
    throw new Error("Invalid TRACE_SOURCE_GROUPS_JSON: " + err.message);
  }
}

function validateTraceReplyRequest_(input) {
  const source = String(input.source || "").trim();
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

async function findOriginalEvolutionMessage_(request) {
  if (!EVOLUTION_API_URL || !EVOLUTION_API_KEY) throw new Error("Original WhatsApp message not available for native quote.");
  const endpoint = String(EVOLUTION_API_URL).replace(/\/+$/, "") + "/chat/findMessages/" + encodeURIComponent(request.instance);
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "content-type": "application/json", apikey: EVOLUTION_API_KEY },
    body: JSON.stringify({ where: { key: { remoteJid: request.groupId, fromMe: false, id: request.messageId } }, limit: 1 })
  });
  if (!response.ok) {
    if (response.status === 404) throw new Error("Original WhatsApp message not available for native quote.");
    throw new Error("Evolution message lookup HTTP " + response.status);
  }
  const candidate = traceCandidate_(await response.json());
  const key = candidate && (candidate.key || (candidate.message && candidate.message.key));
  const message = candidate && (candidate.message || candidate);
  if (!key || !message || key.id !== request.messageId || key.remoteJid !== request.groupId || key.fromMe === true) {
    throw new Error("Original WhatsApp message not available for native quote.");
  }
  const phones = traceParticipantIds_(candidate, key).filter((id) => /^(?:0|62)\d{8,15}(?:@|$)/.test(id)).map(normalizeTracePhone_);
  if (phones.length && !phones.includes(request.customerPhone)) throw new Error("Original WhatsApp message not available for native quote.");
  return { key, message };
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

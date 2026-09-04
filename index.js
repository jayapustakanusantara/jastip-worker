// JASTIP WORKER V3.3 - ROBUST DELETE/UPDATE REVOKE + QUEUE RECOVERY
const express = require("express");
const { createClient } = require("redis");

const app = express();
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 8080;
const REDIS_URL = process.env.REDIS_URL;
const APPS_SCRIPT_URL = process.env.APPS_SCRIPT_URL;
const WORKER_SECRET = process.env.WORKER_SECRET;

// Tambahan untuk reaction ❌ langsung dari Railway worker ke Evolution API
const EVOLUTION_API_URL = process.env.EVOLUTION_API_URL;
const EVOLUTION_API_KEY = process.env.EVOLUTION_API_KEY;

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
  console.log("Redis READY");
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

    const updateExplicitlyRemovedMessage =
      Boolean(
        data.update &&
        Object.prototype.hasOwnProperty.call(
          data.update,
          "message"
        ) &&
        data.update.message === null
      );

    const isDeletedUpdate =
      isUpdateEvent &&
      (
        updateStatus.includes("DELETE") ||
        updateStatus.includes("REVOKE") ||
        protocolType === "REVOKE" ||
        protocolType === "MESSAGE_REVOKE" ||
        protocolType === "0" ||
        updateExplicitlyRemovedMessage
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

    const message =
      data.message ||
      {};

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
      console.log("DUPLICATE WEBHOOK:", messageId);

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
  console.log("Worker started...");

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

        await redis.lRem(
          PROCESSING_QUEUE,
          1,
          rawJob
        );

        if (retryCount <= 20) {
          await redis.rPush(
            QUEUE_NAME,
            JSON.stringify(retryJob)
          );

          console.log(
            "REQUEUED:",
            job.messageId,
            "| retry",
            retryCount
          );

          await sleep(
            Math.min(30000, 1500 * retryCount)
          );

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
  const timeout = setTimeout(
    () => controller.abort(),
    20000
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
        "Apps Script timeout after 20s"
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


async function start() {
  if (!REDIS_URL) {
    console.error("REDIS_URL IS MISSING");
    process.exit(1);
  }

  console.log("Connecting Redis...");

  await redis.connect();

  console.log("Redis connected.");

  await recoverStuckProcessingJobs();

  app.listen(
    PORT,
    "0.0.0.0",
    () => {
      console.log(
        `Jastip Worker running on port ${PORT}`
      );
    }
  );

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

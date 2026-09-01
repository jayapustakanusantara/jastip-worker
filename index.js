const express = require("express");
const { createClient } = require("redis");

const app = express();
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 8080;
const REDIS_URL = process.env.REDIS_URL;

const QUEUE_NAME = "jastip:queue";
const PROCESSING_QUEUE = "jastip:processing";

const SUPPORTED_GROUPS = new Set([
  "120363214326633370@g.us", // ORDER
  "120363427983824748@g.us", // ORDER LIVE
  "120363045287815681@g.us"  // ORDER BBW
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
    redis: redis.isReady
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
  Jadi sekarang root "/" bisa menerima GET health check
  dan POST webhook WhatsApp.
*/
app.post("/", async (req, res) => {
  try {
    const body = req.body || {};

    // Evolution biasanya kirim event seperti messages.upsert
    const event =
      body.event ||
      body.type ||
      body.eventType ||
      "";

    if (
      String(event).toLowerCase() !== "messages.upsert" &&
      String(event).toLowerCase() !== "messages_upsert"
    ) {
      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "not messages.upsert"
      });
    }

    const instance =
      body.instance ||
      body.instanceName ||
      body.sender ||
      "Jastip-bot";

    let data = body.data || body;

    // Beberapa format Evolution bisa membungkus record dalam array
    if (Array.isArray(data)) {
      data = data[0];
    }

    const key =
      data.key ||
      data.message?.key ||
      {};

    const remoteJid =
      key.remoteJid ||
      data.remoteJid ||
      "";

    if (!SUPPORTED_GROUPS.has(remoteJid)) {
      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "unsupported group",
        remoteJid
      });
    }

    const messageId =
      key.id ||
      data.id ||
      "";

    if (!messageId) {
      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "missing messageId"
      });
    }

    const participant =
      key.participant ||
      data.participant ||
      "";

    const participantAlt =
      key.participantAlt ||
      data.participantAlt ||
      "";

    const pushName =
      data.pushName ||
      data.notifyName ||
      "";

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

    /*
      Hanya terima FIX / MAU.

      Contoh valid:
      FIX
      FIX 2
      FIX +1
      FIX +2
      FIX NAMI
      FIX NAMI +2
      MAU
      MAU +3
    */
    if (!/^\s*(FIX|MAU)\b/i.test(text)) {
      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "not FIX/MAU"
      });
    }

    const quotedProduct = extractQuotedProduct(message);

    /*
      Kita TIDAK tolak jika quotedProduct kosong di webhook.
      Job tetap masuk Redis.

      Nanti worker order yang menentukan valid / invalid.
      Ini penting supaya webhook tetap super ringan
      dan pesan tidak hilang hanya karena format payload berubah.
    */

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
      Jangan proses Google Sheet / reaction di sini.
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
  Bisa kita pakai nanti dari Postman/browser tool.
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

    return res.json({
      ok: true,
      waiting,
      processing
    });

  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err.message
    });
  }
});

/*
  WORKER TEST MODE

  Untuk saat ini worker hanya:
  Redis queue
  -> ambil job
  -> print ke log
  -> tandai selesai

  BELUM:
  - masuk Google Sheet
  - kirim reaction ✅

  Kita pastikan webhook Redis stabil dulu.
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

        continue;
      }

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
        TEST MODE DONE.
        Nanti bagian ini kita ganti dengan:
        parse FIX
        -> ORDER
        -> Google Sheets
        -> reaction ✅
      */

      await redis.lRem(
        PROCESSING_QUEUE,
        1,
        rawJob
      );

      console.log("DONE TEST:", job.messageId);

    } catch (err) {
      console.error("WORKER ERROR:", err);
      await sleep(2000);
    }
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

function extractQuotedProduct(message) {
  try {
    const ctx =
      message.extendedTextMessage?.contextInfo ||
      message.imageMessage?.contextInfo ||
      message.videoMessage?.contextInfo ||
      message.documentMessage?.contextInfo ||
      {};

    const quoted =
      ctx.quotedMessage ||
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

async function start() {
  if (!REDIS_URL) {
    console.error("REDIS_URL IS MISSING");
    process.exit(1);
  }

  console.log("Connecting Redis...");

  await redis.connect();

  console.log("Redis connected.");

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

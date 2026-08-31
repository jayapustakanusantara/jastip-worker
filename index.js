const express = require("express");
const { createClient } = require("redis");

const app = express();
app.use(express.json({ limit: "5mb" }));

// ===============================
// CONFIG
// ===============================
const PORT = process.env.PORT || 3000;
const REDIS_URL = process.env.REDIS_URL;

const QUEUE_NAME = "jastip:queue";
const PROCESSING_QUEUE = "jastip:processing";

// ===============================
// REDIS
// ===============================
const redis = createClient({
  url: REDIS_URL
});

redis.on("error", (err) => {
  console.error("REDIS ERROR:", err);
});

redis.on("ready", () => {
  console.log("Redis READY");
});

// ===============================
// HEALTH CHECK
// ===============================
app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "Jastip Worker",
    redis: redis.isReady
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    redis: redis.isReady
  });
});

// ===============================
// ADD MESSAGE TO QUEUE
// ===============================
//
// Nanti Apps Script / webhook kita
// kirim FIX ke endpoint ini.
//
// POST /queue
//
app.post("/queue", async (req, res) => {
  try {
    const payload = req.body || {};

    if (!payload.messageId) {
      return res.status(400).json({
        ok: false,
        error: "messageId required"
      });
    }

    // Anti duplicate
    const duplicateKey = `jastip:seen:${payload.messageId}`;

    const firstMessage = await redis.set(
      duplicateKey,
      "1",
      {
        NX: true,
        EX: 60 * 60 * 24 * 7
      }
    );

    if (!firstMessage) {
      console.log("DUPLICATE:", payload.messageId);

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

    const queueLength = await redis.lLen(QUEUE_NAME);

    console.log(
      "QUEUED:",
      payload.messageId,
      "QUEUE:",
      queueLength
    );

    return res.json({
      ok: true,
      queued: true,
      messageId: payload.messageId,
      queueLength
    });

  } catch (err) {

    console.error("QUEUE ERROR:", err);

    return res.status(500).json({
      ok: false,
      error: err.message
    });
  }
});

// ===============================
// QUEUE STATUS
// ===============================
app.get("/queue/status", async (req, res) => {
  try {

    const waiting = await redis.lLen(QUEUE_NAME);
    const processing = await redis.lLen(PROCESSING_QUEUE);

    res.json({
      ok: true,
      waiting,
      processing
    });

  } catch (err) {

    res.status(500).json({
      ok: false,
      error: err.message
    });

  }
});

// ===============================
// WORKER
// ===============================
//
// Untuk sekarang worker hanya mengambil
// pesan dari Redis dan memperlihatkannya
// di Railway log.
//
// BELUM menulis ORDER.
// Kita test Redis dulu.
//
async function worker() {

  console.log("Worker started...");

  while (true) {

    try {

      /*
       * BRPOPLPUSH:
       *
       * Ambil job dari QUEUE,
       * lalu pindahkan dulu ke PROCESSING.
       *
       * Jadi job tidak langsung hilang.
       */

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

      console.log("==============================");
      console.log("PROCESS JOB");
      console.log("Message ID:", job.messageId);
      console.log("Group:", job.groupId);
      console.log("Sender:", job.senderPhone);
      console.log("Text:", job.text);
      console.log("Quoted:", job.quotedProduct);
      console.log("==============================");

      /*
       * TEST MODE
       *
       * Nanti bagian ini kita ganti menjadi:
       *
       * Redis
       *   ↓
       * Google Apps Script
       *   ↓
       * ORDER / ORDER LIVE / ORDER BBW
       *   ↓
       * Reaction ✅
       */

      await redis.lRem(
        PROCESSING_QUEUE,
        1,
        rawJob
      );

      console.log(
        "DONE TEST:",
        job.messageId
      );

    } catch (err) {

      console.error("WORKER ERROR:", err);

      // Jangan bikin worker mati karena 1 error
      await sleep(2000);

    }

  }
}

// ===============================
// HELPER
// ===============================
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ===============================
// START SERVER
// ===============================
async function start() {

  if (!REDIS_URL) {
    console.error("REDIS_URL IS MISSING");
    process.exit(1);
  }

  console.log("Connecting Redis...");

  await redis.connect();

  console.log("Redis connected.");

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Jastip Worker running on port ${PORT}`);
  });

  worker().catch(err => {
    console.error("FATAL WORKER ERROR:", err);
    process.exit(1);
  });
}

start().catch(err => {
  console.error("START ERROR:", err);
  process.exit(1);
});

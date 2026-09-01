// ======================================================
// CONFIG - FINAL V7.2 - REDIS WORKER BRIDGE + CHECKPOINT SCAN
// ======================================================

const ORDER_SHEET_NAME = "ORDER";
const ORDER_LIVE_SHEET_NAME = "ORDER LIVE";
const ORDER_BBW_SHEET_NAME = "ORDER BBW";

// PEMISAHAN ORDER BERDASARKAN GROUP WHATSAPP
const GROUP_TO_ORDER_SHEET = {
  "120363427983824748@g.us": ORDER_LIVE_SHEET_NAME,
  "120363214326633370@g.us": ORDER_SHEET_NAME,
  "120363414084709085@g.us": ORDER_BBW_SHEET_NAME
};
const QUEUE_SHEET_NAME = "QUEUE";
const CONTACT_MAP_SHEET_NAME = "CONTACT_MAP";
const DEBUG_SHEET_NAME = "DEBUG";

const EVOLUTION_BASE_URL =
  "https://evolution-api-production-30a1.up.railway.app";

const DEFAULT_INSTANCE = "Jastip-bot";

const MAX_RETRY = 3;
const MAX_PROCESS_PER_RUN = 50;

// V7.1 SIMPLE QUEUE + CHECKPOINT MANUAL SCAN
const SCAN_FIRST_LOOKBACK_MINUTES = 30;
const SCAN_OVERLAP_MINUTES = 2;
const SCAN_MAX_PAGES = 20;
const SCAN_PAGE_SIZE = 100;

// REACTION RECOVERY
const REACTION_RETRY_NORMAL_BATCH = 15;
const REACTION_RETRY_BURST_BATCH = 50;
const REPAIR_RECENT_HOURS = 48;
const REPAIR_RECENT_MAX = 30;

// BURST DETECTOR
// Jika >= 10 FIX/MAU dalam 2 menit, aktifkan burst mode 5 menit.
const BURST_THRESHOLD = 10;
const BURST_WINDOW_MINUTES = 2;
const BURST_ACTIVE_MINUTES = 5;


// ======================================================
// RESPONSE
// ======================================================

function textResponse(text) {
  return ContentService.createTextOutput(text);
}


// ======================================================
// WEBHOOK UTAMA + FULL DEBUG
// ======================================================

function doPost(e) {
  // V7: WEBHOOK HARUS SERINGAN MUNGKIN.
  // Hanya baca event -> validasi -> append QUEUE -> return.
  // Tidak process order, tidak reaction order, tidak scan backlog, tidak tulis DEBUG.
  try {
    if (!e || !e.postData || !e.postData.contents) {
      return textResponse("NO POST DATA");
    }

    const payload = JSON.parse(e.postData.contents);

    // ==================================================
    // RAILWAY REDIS WORKER -> GOOGLE SHEET QUEUE
    // ==================================================
    // Evolution sekarang masuk ke Railway terlebih dahulu.
    // Worker mengirim job ringkas ke endpoint Apps Script ini.
    if (payload.source === "railway-worker") {
      return receiveRailwayWorkerJob(payload);
    }
    if (payload.event !== "messages.upsert") {
      return textResponse("IGNORED EVENT");
    }

    const data = payload.data || {};
    const key = data.key || {};
    const message = data.message || {};
    const context = data.contextInfo || {};

    if (key.fromMe === true) {
      return textResponse("IGNORED OWN MESSAGE");
    }

    const groupId = String(key.remoteJid || "");
    if (!GROUP_TO_ORDER_SHEET[groupId]) {
      return textResponse("IGNORED GROUP");
    }

    const text = String(
      message.conversation ||
      message.extendedTextMessage?.text ||
      ""
    ).trim();

    if (!text) {
      return textResponse("NO TEXT");
    }

    const messageId = String(key.id || "");
    if (!messageId) {
      return textResponse("NO MESSAGE ID");
    }

    // --------------------------------------------------
    // ADMIN COMMAND: SCAN
    // SCAN tidak menjalankan history scan di webhook.
    // Hanya pasang flag; worker yang mengerjakan kemudian.
    // --------------------------------------------------
    if (/^SCAN$/i.test(text)) {
      const senderPhone = cleanWhatsAppId(key.participantAlt || "");
      const adminPhone = cleanWhatsAppId(
        PropertiesService.getScriptProperties().getProperty("SCAN_ADMIN_PHONE") || ""
      );

      if (!adminPhone || !senderPhone || senderPhone !== adminPhone) {
        return textResponse("SCAN NOT AUTHORIZED");
      }

      const props = PropertiesService.getScriptProperties();
      props.setProperty("SCAN_REQUESTED", "YES");
      props.setProperty("SCAN_GROUP_ID", groupId);
      props.setProperty("SCAN_REQUESTED_AT", String(Date.now()));
      props.setProperty("SCAN_COMMAND_MESSAGE_ID", messageId);
      props.setProperty("SCAN_COMMAND_PARTICIPANT", String(key.participant || ""));
      props.setProperty("SCAN_INSTANCE", String(payload.instance || DEFAULT_INSTANCE));
      return textResponse("SCAN QUEUED");
    }

    const orderCommand = parseOrderCommand(text);
    if (!orderCommand) {
      return textResponse("IGNORED NON ORDER");
    }

    const quotedMessage =
      context.quotedMessage ||
      message.extendedTextMessage?.contextInfo?.quotedMessage ||
      {};

    const quotedText = String(
      quotedMessage.conversation ||
      quotedMessage.extendedTextMessage?.text ||
      quotedMessage.imageMessage?.caption ||
      quotedMessage.videoMessage?.caption ||
      ""
    ).trim();

    // Tetap simpan order valid saja. FIX tanpa reply tidak masuk QUEUE.
    if (!quotedText) {
      return textResponse("ORDER WITHOUT REFERENCE");
    }

    const mentionedJid =
      message.extendedTextMessage?.contextInfo?.mentionedJid ||
      context.mentionedJid ||
      [];
    const mention = mentionedJid.length ? String(mentionedJid[0]) : "";

    // Cache-only dedup: jangan scan 2000 row QUEUE di webhook.
    const cache = CacheService.getScriptCache();
    const cacheKey = "QUEUE_MSG_" + messageId;
    if (cache.get(cacheKey)) {
      return textResponse("ALREADY QUEUED");
    }

    // Lock hanya melindungi append. Tidak ada kerja berat di dalam lock.
    const enqueueLock = LockService.getUserLock();
    if (!enqueueLock.tryLock(3000)) {
      // Evolution akan retry webhook jika response bukan sukses normal.
      throw new Error("QUEUE BUSY");
    }

    try {
      // Double-check cache setelah mendapat lock.
      if (cache.get(cacheKey)) {
        return textResponse("ALREADY QUEUED");
      }

      const queue = getOrCreateQueueSheet();
      queue.appendRow([
        new Date(),
        messageId,
        payload.instance || DEFAULT_INSTANCE,
        groupId,
        key.participant || "",
        key.participantAlt || "",
        data.pushName || "",
        text,
        quotedText,
        mention,
        "PENDING",
        0,
        "",
        "",
        new Date()
      ]);

      cache.put(cacheKey, "1", 21600);
      PropertiesService.getScriptProperties().setProperty("LAST_QUEUE_RECEIVED_AT", String(Date.now()));
    } finally {
      enqueueLock.releaseLock();
    }

    return textResponse("QUEUED");

  } catch (err) {
    console.log("WEBHOOK ERROR: " + err.message);
    // Jangan tulis DEBUG Sheet dari webhook.
    return textResponse("ERROR: " + err.message);
  }
}

// ======================================================
// RAILWAY WORKER BRIDGE
// ======================================================

function receiveRailwayWorkerJob(payload) {
  const props = PropertiesService.getScriptProperties();

  const expectedSecret = String(
    props.getProperty("WORKER_SECRET") || ""
  );

  const receivedSecret = String(
    payload.workerSecret || ""
  );

  if (!expectedSecret) {
    return textResponse("WORKER SECRET NOT SET");
  }

  if (!receivedSecret || receivedSecret !== expectedSecret) {
    return textResponse("WORKER UNAUTHORIZED");
  }

  const job = payload.job || {};

  const messageId = String(job.messageId || "").trim();
  const instance = String(job.instance || DEFAULT_INSTANCE).trim();
  const groupId = String(job.groupId || "").trim();
  const senderLid = String(job.senderLid || "").trim();
  const senderPhoneRaw = String(job.senderPhoneRaw || "").trim();
  const pushName = String(job.pushName || "").trim();
  const text = String(job.text || "").trim();
  const quotedText = String(job.quotedText || "").trim();
  const mentionJid = String(job.mentionJid || "").trim();

  if (!messageId) {
    return textResponse("WORKER NO MESSAGE ID");
  }

  if (!GROUP_TO_ORDER_SHEET[groupId]) {
    return textResponse("WORKER INVALID GROUP");
  }

  if (!parseOrderCommand(text)) {
    return textResponse("WORKER INVALID ORDER");
  }

  if (!quotedText) {
    return textResponse("WORKER NO QUOTED PRODUCT");
  }

  const cache = CacheService.getScriptCache();
  const cacheKey = "QUEUE_MSG_" + messageId;

  if (cache.get(cacheKey)) {
    return textResponse("ALREADY QUEUED");
  }

  const enqueueLock = LockService.getUserLock();

  if (!enqueueLock.tryLock(3000)) {
    throw new Error("QUEUE BUSY");
  }

  try {
    if (cache.get(cacheKey)) {
      return textResponse("ALREADY QUEUED");
    }

    const queue = getOrCreateQueueSheet();

    // Redis/Evolution retry protection:
    // cek juga row terbaru agar restart cache tidak membuat order dobel.
    if (queueHasMessage(queue, messageId)) {
      cache.put(cacheKey, "1", 21600);
      return textResponse("ALREADY QUEUED");
    }

    let receivedAt = new Date();

    if (job.receivedAt) {
      const parsedDate = new Date(job.receivedAt);
      if (!isNaN(parsedDate.getTime())) {
        receivedAt = parsedDate;
      }
    }

    queue.appendRow([
      receivedAt,
      messageId,
      instance,
      groupId,
      senderLid,
      senderPhoneRaw,
      pushName,
      text,
      quotedText,
      mentionJid,
      "PENDING",
      0,
      "FROM REDIS WORKER",
      "",
      new Date()
    ]);

    cache.put(cacheKey, "1", 21600);
    props.setProperty(
      "LAST_QUEUE_RECEIVED_AT",
      String(Date.now())
    );

    return textResponse("QUEUED");

  } finally {
    enqueueLock.releaseLock();
  }
}


// Jalankan SEKALI dari Apps Script untuk memasang secret Railway.
// Setelah dijalankan, masukkan secret yang sama ke Railway variable WORKER_SECRET.
function setWorkerSecret() {
  const ui = SpreadsheetApp.getUi();

  const result = ui.prompt(
    "Worker Secret",
    "Masukkan secret untuk Railway worker. Simpan secret ini karena nanti harus sama persis di Railway.",
    ui.ButtonSet.OK_CANCEL
  );

  if (result.getSelectedButton() !== ui.Button.OK) {
    return;
  }

  const secret = String(
    result.getResponseText() || ""
  ).trim();

  if (secret.length < 16) {
    ui.alert(
      "Secret terlalu pendek. Gunakan minimal 16 karakter."
    );
    return;
  }

  PropertiesService
    .getScriptProperties()
    .setProperty(
      "WORKER_SECRET",
      secret
    );

  ui.alert(
    "WORKER_SECRET berhasil disimpan."
  );
}


// ======================================================
// PROSES LANGSUNG SATU MESSAGE
// ======================================================

function processMessageNow(messageId) {

  const lock =
    LockService.getScriptLock();


  if (
    !lock.tryLock(1500)
  ) {

    console.log(
      "PROCESS LOCK BUSY"
    );

    return;
  }


  try {

    const queue =
      getOrCreateQueueSheet();


    const rowNumber =
      findQueueRowByMessageId(
        queue,
        messageId
      );


    if (!rowNumber) {
      return;
    }


    const row =
      queue
        .getRange(
          rowNumber,
          1,
          1,
          15
        )
        .getValues()[0];


    const status =
      String(
        row[10] || ""
      );


    if (
      status !== "PENDING" &&
      status !== "RETRY"
    ) {
      return;
    }


    processSingleQueueItem(
      queue,
      rowNumber,
      row
    );


    processPendingWhileLocked(
      queue,
      messageId
    );


  } finally {

    lock.releaseLock();
  }
}


// ======================================================
// PROSES PENDING BERIKUTNYA
// ======================================================

function processPendingWhileLocked(
  queue,
  skipMessageId
) {

  let processed = 0;


  while (
    processed <
    MAX_PROCESS_PER_RUN
  ) {

    const lastRow =
      queue.getLastRow();


    if (lastRow < 2) {
      return;
    }


    const values =
      queue
        .getRange(
          2,
          1,
          lastRow - 1,
          15
        )
        .getValues();


    let found = false;


    for (
      let i = 0;
      i < values.length;
      i++
    ) {

      const row =
        values[i];

      const rowMessageId =
        String(
          row[1] || ""
        );

      const status =
        String(
          row[10] || ""
        );


      if (
        rowMessageId ===
        String(skipMessageId)
      ) {
        continue;
      }


      if (
        status !== "PENDING" &&
        status !== "RETRY"
      ) {
        continue;
      }


      processSingleQueueItem(
        queue,
        i + 2,
        row
      );


      processed++;
      found = true;

      break;
    }


    if (!found) {
      return;
    }
  }
}


// ======================================================
// BACKUP WORKER
// ======================================================

function processQueue() {
  const workerLock = LockService.getScriptLock();
  let gotLock = false;

  try {
    gotLock = workerLock.tryLock(5000);
    if (gotLock) {
      const queue = getOrCreateQueueSheet();
      recoverStuckRows(queue);

      const lastRow = queue.getLastRow();
      if (lastRow >= 2) {
        const values = queue.getRange(2, 1, lastRow - 1, 15).getValues();
        let processed = 0;

        for (let i = 0; i < values.length; i++) {
          if (processed >= MAX_PROCESS_PER_RUN) break;
          const status = String(values[i][10] || "");
          if (status !== "PENDING" && status !== "RETRY") continue;
          processSingleQueueItem(queue, i + 2, values[i]);
          processed++;
        }
      }
    }
  } finally {
    if (gotLock) workerLock.releaseLock();
  }

  // Reaction-only recovery tetap boleh saat burst flag lama/aktif.
  if (isBurstMode()) {
    try {
      retryMissingReactions(REACTION_RETRY_BURST_BATCH);
    } catch (err) {
      console.log("BURST REACTION RECOVERY ERROR: " + err.message);
    }
  }

  // Manual SCAN dijalankan setelah queue worker selesai dan lock sudah lepas.
  try {
    runRequestedScan();
  } catch (scanError) {
    console.log("REQUESTED SCAN ERROR: " + scanError.message);
  }
}

// ======================================================
// V7 - MANUAL SCAN DARI WHATSAPP
// ======================================================

function setScanAdminPhone() {
  const ui = SpreadsheetApp.getUi();
  const result = ui.prompt(
    "Nomor Admin SCAN",
    "Masukkan nomor WhatsApp admin, contoh 6287859856888 (tanpa +, spasi, atau strip):",
    ui.ButtonSet.OK_CANCEL
  );

  if (result.getSelectedButton() !== ui.Button.OK) return;

  const phone = String(result.getResponseText() || "").replace(/\D/g, "");
  if (!phone) throw new Error("Nomor admin kosong.");

  PropertiesService.getScriptProperties().setProperty("SCAN_ADMIN_PHONE", phone);
  ui.alert("Admin SCAN tersimpan: " + phone);
}

function runRequestedScan() {
  const props = PropertiesService.getScriptProperties();
  if (props.getProperty("SCAN_REQUESTED") !== "YES") return;

  const groupId = props.getProperty("SCAN_GROUP_ID") || "";
  const instance = props.getProperty("SCAN_INSTANCE") || DEFAULT_INSTANCE;
  const commandMessageId = props.getProperty("SCAN_COMMAND_MESSAGE_ID") || "";
  const commandParticipant = props.getProperty("SCAN_COMMAND_PARTICIPANT") || "";
  const requestedAt = Number(props.getProperty("SCAN_REQUESTED_AT") || Date.now());

  // Ambil request sekali. Kalau gagal, request dipasang lagi.
  props.deleteProperty("SCAN_REQUESTED");

  try {
    // ⏳ = worker sudah mulai mengerjakan SCAN.
    if (commandMessageId) {
      try {
        sendReactionEmoji(instance, groupId, commandMessageId, commandParticipant, "⏳");
      } catch (reactionError) {
        console.log("SCAN START REACTION ERROR: " + reactionError.message);
      }
    }

    const result = scanEvolutionBacklogToQueue(groupId, instance, requestedAt);

    // Checkpoint HANYA maju kalau scan Evolution selesai sukses.
    props.setProperty("SCAN_CHECKPOINT_" + groupId, String(requestedAt));

    logDebug(
      "SCAN DONE",
      commandMessageId,
      "From=" + new Date(result.fromTime).toISOString() +
      " | To=" + new Date(result.toTime).toISOString() +
      " | Checked=" + result.checked +
      " | Recovered=" + result.recovered
    );

    // Ganti ⏳ menjadi ✅ setelah audit selesai.
    if (commandMessageId) {
      try {
        sendReactionEmoji(instance, groupId, commandMessageId, commandParticipant, "✅");
      } catch (reactionError) {
        console.log("SCAN DONE REACTION ERROR: " + reactionError.message);
      }
    }
  } catch (err) {
    props.setProperty("SCAN_REQUESTED", "YES");
    logDebug("SCAN ERROR", commandMessageId, err.message);
    throw err;
  }
}

function scanEvolutionBacklogToQueue(groupId, instance, requestedAt) {
  groupId = String(groupId || "").trim();
  if (!GROUP_TO_ORDER_SHEET[groupId]) {
    throw new Error("GROUP SCAN TIDAK VALID: " + groupId);
  }

  const props = PropertiesService.getScriptProperties();
  const apiKey = props.getProperty("EVOLUTION_API_KEY");
  if (!apiKey) throw new Error("EVOLUTION_API_KEY TIDAK ADA");

  const toTime = Number(requestedAt || Date.now());
  const checkpoint = Number(props.getProperty("SCAN_CHECKPOINT_" + groupId) || 0);

  // SCAN pertama: hanya 30 menit terakhir.
  // SCAN berikutnya: mulai checkpoint terakhir dikurangi overlap 2 menit.
  const fromTime = checkpoint > 0
    ? Math.max(0, checkpoint - SCAN_OVERLAP_MINUTES * 60000)
    : toTime - SCAN_FIRST_LOOKBACK_MINUTES * 60000;

  const existingQueueIds = getRecentQueueMessageIdSet();
  const existingOrderIds = getOrderBaseMessageIdSet(groupId);
  const recoveredIds = new Set();
  const rowsToAppend = [];
  let checked = 0;
  let reachedOld = false;

  for (let page = 1; page <= SCAN_MAX_PAGES && !reachedOld; page++) {
    const url = EVOLUTION_BASE_URL + "/chat/findMessages/" + encodeURIComponent(instance);
    const response = UrlFetchApp.fetch(url, {
      method: "post",
      contentType: "application/json",
      headers: { apikey: apiKey },
      payload: JSON.stringify({
        where: { key: { remoteJid: groupId } },
        page: page,
        offset: SCAN_PAGE_SIZE
      }),
      muteHttpExceptions: true
    });

    const code = response.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new Error("SCAN EVOLUTION HTTP " + code + ": " + response.getContentText());
    }

    const json = JSON.parse(response.getContentText() || "{}");
    const records = extractEvolutionRecords(json);
    if (!records.length) break;

    for (const rec of records) {
      const key = rec.key || {};
      if (key.fromMe === true) continue;
      if (String(key.remoteJid || "") !== groupId) continue;

      const ts = normalizeEvolutionTimestamp(
        rec.messageTimestamp || rec.timestamp || rec.messageTimestampMs
      );

      // Pesan setelah command SCAN tidak termasuk audit ini; masuk sesi berikutnya.
      if (ts && ts > toTime) continue;

      if (ts && ts < fromTime) {
        reachedOld = true;
        continue;
      }

      const messageId = String(key.id || "");
      if (!messageId) continue;

      const msg = rec.message || {};
      const text = String(
        msg.conversation ||
        msg.extendedTextMessage?.text ||
        ""
      ).trim();

      if (!parseOrderCommand(text)) continue;
      checked++;

      if (
        existingQueueIds.has(messageId) ||
        existingOrderIds.has(messageId) ||
        recoveredIds.has(messageId)
      ) continue;

      const ctx = msg.extendedTextMessage?.contextInfo || rec.contextInfo || {};
      const quoted = ctx.quotedMessage || {};
      const quotedText = String(
        quoted.conversation ||
        quoted.extendedTextMessage?.text ||
        quoted.imageMessage?.caption ||
        quoted.videoMessage?.caption ||
        ""
      ).trim();

      // Tanpa reply produk, jangan menebak barang.
      if (!quotedText) continue;

      const mentioned = ctx.mentionedJid || [];
      const receivedAt = ts ? new Date(ts) : new Date();

      rowsToAppend.push([
        receivedAt,
        messageId,
        instance,
        groupId,
        key.participant || rec.participant || "",
        key.participantAlt || rec.participantAlt || "",
        rec.pushName || "",
        text,
        quotedText,
        mentioned.length ? String(mentioned[0]) : "",
        "PENDING",
        0,
        "RECOVERED BY SCAN",
        "",
        new Date()
      ]);
      recoveredIds.add(messageId);
    }

    if (records.length < SCAN_PAGE_SIZE) break;
  }

  let recovered = 0;
  if (rowsToAppend.length) {
    const lock = LockService.getUserLock();
    lock.waitLock(10000);
    try {
      const queue = getOrCreateQueueSheet();
      const freshIds = getRecentQueueMessageIdSet();
      const finalRows = rowsToAppend.filter(r => !freshIds.has(String(r[1])));
      if (finalRows.length) {
        queue.getRange(queue.getLastRow() + 1, 1, finalRows.length, 15).setValues(finalRows);
        const cache = CacheService.getScriptCache();
        finalRows.forEach(r => cache.put("QUEUE_MSG_" + r[1], "1", 21600));
        recovered = finalRows.length;
      }
    } finally {
      lock.releaseLock();
    }
  }

  return {
    checked: checked,
    recovered: recovered,
    fromTime: fromTime,
    toTime: toTime
  };
}

function extractEvolutionRecords(json) {
  if (Array.isArray(json)) return json;
  if (Array.isArray(json.records)) return json.records;
  if (Array.isArray(json.data)) return json.data;
  if (Array.isArray(json.messages)) return json.messages;
  if (json.messages && Array.isArray(json.messages.records)) return json.messages.records;
  if (json.data && Array.isArray(json.data.records)) return json.data.records;
  return [];
}

function normalizeEvolutionTimestamp(value) {
  if (value === null || value === undefined || value === "") return 0;
  if (value instanceof Date) return value.getTime();
  const n = Number(value);
  if (Number.isFinite(n) && n > 0) {
    return n < 100000000000 ? n * 1000 : n;
  }
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : 0;
}

function getRecentQueueMessageIdSet() {
  const queue = getOrCreateQueueSheet();
  if (queue.getLastRow() < 2) return new Set();
  const last = queue.getLastRow();
  const first = Math.max(2, last - 4999);
  return new Set(
    queue.getRange(first, 2, last - first + 1, 1)
      .getDisplayValues().flat().map(String).filter(Boolean)
  );
}

function getOrderBaseMessageIdSet(groupId) {
  const sheet = getOrCreateOrderSheet(groupId);
  if (sheet.getLastRow() < 2) return new Set();
  const values = sheet.getRange(2, 14, sheet.getLastRow() - 1, 1).getDisplayValues().flat();
  const set = new Set();
  values.forEach(v => {
    const s = String(v || "");
    if (!s) return;
    set.add(s.replace(/-\d+$/, ""));
  });
  return set;
}


// ======================================================
// PROSES SATU ITEM QUEUE
// ======================================================

function processSingleQueueItem(
  queue,
  rowNumber,
  row
) {

  const retry =
    Number(
      row[11] || 0
    );


  queue
    .getRange(
      rowNumber,
      11
    )
    .setValue(
      "PROCESSING"
    );


  queue
    .getRange(
      rowNumber,
      15
    )
    .setValue(
      new Date()
    );


  try {

    const result =
      processQueueRow(row) || {};


    const finalStatus =
      result.reactionOk === false
        ? "DONE_NO_REACTION"
        : "DONE";


    queue
      .getRange(
        rowNumber,
        11
      )
      .setValue(
        finalStatus
      );


    queue
      .getRange(
        rowNumber,
        13
      )
      .setValue(
        result.reactionOk === false
          ? String(
              result.reactionError ||
              "ORDER TERSIMPAN, REACTION BELUM BERHASIL"
            )
          : ""
      );


    queue
      .getRange(
        rowNumber,
        14
      )
      .setValue(
        new Date()
      );


    queue
      .getRange(
        rowNumber,
        15
      )
      .setValue(
        new Date()
      );


  } catch (err) {

    const newRetry =
      retry + 1;


    queue
      .getRange(
        rowNumber,
        12
      )
      .setValue(
        newRetry
      );


    queue
      .getRange(
        rowNumber,
        13
      )
      .setValue(
        err.message
      );


    queue
      .getRange(
        rowNumber,
        15
      )
      .setValue(
        new Date()
      );


    if (
      newRetry < MAX_RETRY
    ) {

      queue
        .getRange(
          rowNumber,
          11
        )
        .setValue(
          "RETRY"
        );


      logDebug(
        "QUEUE RETRY " +
          newRetry,
        row[1],
        err.message
      );


    } else {

      queue
        .getRange(
          rowNumber,
          11
        )
        .setValue(
          "ERROR"
        );


      logDebug(
        "QUEUE FINAL ERROR",
        row[1],
        err.message
      );


      try {

        sendReactionEmoji(
          row[2] ||
            DEFAULT_INSTANCE,
          row[3],
          row[1],
          row[4] || "",
          "❌"
        );

      } catch (reactionError) {

        logDebug(
          "FINAL ❌ ERROR",
          row[1],
          reactionError.message
        );
      }
    }
  }
}


// ======================================================
// PROSES ORDER FINAL
// ======================================================

function processQueueRow(row) {

  const receivedAt =
    row[0];

  const messageId =
    String(
      row[1] || ""
    );

  const instance =
    String(
      row[2] ||
      DEFAULT_INSTANCE
    );

  const groupId =
    String(
      row[3] || ""
    );

  const senderLid =
    String(
      row[4] || ""
    );

  const senderPhoneRaw =
    String(
      row[5] || ""
    );

  const pushName =
    String(
      row[6] || ""
    );

  const text =
    String(
      row[7] || ""
    );

  const quotedText =
    String(
      row[8] || ""
    );

  const mentionJid =
    String(
      row[9] || ""
    );


  if (!messageId) {

    throw new Error(
      "MESSAGE ID KOSONG"
    );
  }


  if (!quotedText) {

    throw new Error(
      "REFERENCE PRODUK KOSONG"
    );
  }


  // ==================================================
  // UPDATE CONTACT MAP DI WORKER, BUKAN DI doPost
  // ==================================================

  try {

    saveContactMapping({
      participant: senderLid,
      participantAlt: senderPhoneRaw
    });

  } catch (mapError) {

    console.log(
      "CONTACT MAP WORKER ERROR: " +
      mapError.message
    );
  }


  // ==================================================
  // FIX / MAU
  // ==================================================

  const orderCommand =
    parseOrderCommand(
      text
    );


  if (!orderCommand) {

    throw new Error(
      "FORMAT ORDER TIDAK VALID"
    );
  }


  const keyword =
    orderCommand.keyword;

  const qty =
    orderCommand.qty;


  if (
    !qty ||
    qty < 1 ||
    qty > 100
  ) {

    throw new Error(
      "QTY TIDAK VALID"
    );
  }


  // ==================================================
  // CUSTOMER
  // ==================================================

  let customerNumber =
    cleanWhatsAppId(
      senderPhoneRaw
    );


  if (!customerNumber) {

    customerNumber =
      getPhoneFromLid(
        senderLid
      );


    if (!customerNumber) {

      customerNumber =
        cleanWhatsAppId(
          senderLid
        );
    }
  }


  let customerName =
    pushName;


  if (
    customerName.includes(
      "@lid"
    ) ||
    /^\d+$/.test(
      customerName
    )
  ) {

    customerName = "";
  }


  let orderCustomerNumber =
    customerNumber;

  let orderCustomerName =
    customerName;


  // ==================================================
  // MENTION
  // ==================================================

  if (mentionJid) {

    const mappedPhone =
      getPhoneFromLid(
        mentionJid
      );


    if (mappedPhone) {

      orderCustomerNumber =
        mappedPhone;

    } else {

      orderCustomerNumber =
        cleanWhatsAppId(
          mentionJid
        );
    }


    orderCustomerName = "";
  }


  // ==================================================
  // PARSE PRODUK
  // ==================================================

  const product =
    parseProductCaption(
      quotedText
    );


  const productName =
    product.title;

  const isbn =
    product.isbn;

  const price =
    product.price;


  if (!productName) {

    throw new Error(
      "JUDUL PRODUK TIDAK TERBACA"
    );
  }


  // ==================================================
  // STATUS DATA PRODUK
  // ==================================================

  let stockStatus = "";


  if (
    !isbn &&
    !price
  ) {

    stockStatus =
      "NO ISBN / NO PRICE";

  } else if (!isbn) {

    stockStatus =
      "NO ISBN";

  } else if (!price) {

    stockStatus =
      "NO PRICE";
  }


  // ==================================================
  // ORDER SHEET + ANTI DOUBLE
  // ==================================================

  const orderSheet =
    getOrCreateOrderSheet(
      groupId
    );


  const alreadySaved =
    orderMessageExists(
      orderSheet,
      messageId
    );


  // ==================================================
  // HANYA TULIS JIKA BELUM ADA
  // ==================================================

  if (!alreadySaved) {

    const rows = [];


    for (
      let i = 1;
      i <= qty;
      i++
    ) {

      rows.push([
        receivedAt ||
          new Date(),
        orderCustomerName,
        orderCustomerNumber,
        groupId,
        isbn,
        productName,
        price,
        1,
        price,
        stockStatus,
        keyword,
        text,
        quotedText,
        messageId +
          "-" +
          i,
        i
      ]);
    }


    const startRow =
      orderSheet.getLastRow() + 1;


    orderSheet
      .getRange(
        startRow,
        1,
        rows.length,
        rows[0].length
      )
      .setValues(rows);


    logDebug(
      "ORDER SAVED",
      messageId,
      productName +
        " | ISBN=" +
        isbn +
        " | PRICE=" +
        price +
        " | QTY=" +
        qty
    );

  } else {

    logDebug(
      "ORDER ALREADY EXISTS",
      messageId,
      "Tidak tulis ulang. Coba reaction saja."
    );
  }


  // ==================================================
  // REACTION ✅
  // Order sudah aman sebelum reaction dicoba.
  // Kalau gagal -> DONE_NO_REACTION, bukan retry order.
  // ==================================================

  try {

    const successEmoji =
      String(row[12] || "").indexOf("RECOVERED BY SCAN") >= 0
        ? "🔁"
        : "✅";

    sendReactionEmoji(
      instance,
      groupId,
      messageId,
      senderLid,
      successEmoji
    );


    return {
      orderSaved: true,
      reactionOk: true,
      alreadySaved: alreadySaved
    };


  } catch (reactionError) {

    logDebug(
      "REACTION ✅ ERROR",
      messageId,
      reactionError.message
    );


    return {
      orderSaved: true,
      reactionOk: false,
      reactionError:
        reactionError.message,
      alreadySaved: alreadySaved
    };
  }
}


// ======================================================
// PARSER PRODUK
//
// Judul boleh beberapa baris.
// ISBN optional.
// Harga boleh dengan / tanpa Rp.
// ======================================================

function parseProductCaption(text) {

  const raw =
    String(
      text || ""
    )
      .replace(/\r/g, "")
      .trim();


  if (!raw) {

    return {
      title: "",
      isbn: "",
      price: 0
    };
  }


  const lines =
    raw
      .split("\n")
      .map(
        line =>
          line.trim()
      )
      .filter(
        line =>
          line !== ""
      );


  const titleParts = [];

  let isbn = "";
  let price = 0;
  let titleFinished = false;


  for (
    let i = 0;
    i < lines.length;
    i++
  ) {

    const line =
      lines[i];


    // LINK
    if (
      isUrlLine(line)
    ) {

      titleFinished = true;
      continue;
    }


    // ==================================================
    // ISBN DULU
    // ==================================================

    const possibleIsbn =
      line
        .replace(
          /^ISBN(?:-1[03])?\s*[:\-]?\s*/i,
          ""
        )
        .replace(
          /[\s-]/g,
          ""
        );


    if (
      /^\d{10,13}$/.test(
        possibleIsbn
      )
    ) {

      isbn =
        possibleIsbn;

      titleFinished = true;
      continue;
    }


    // ==================================================
    // HARGA DENGAN Rp
    // ==================================================

    const priceMatch =
      line.match(
        /\bRp\.?\s*([\d.,]+)/i
      );


    if (priceMatch) {

      const priceText =
        priceMatch[1]
          .replace(/\./g, "")
          .replace(/,/g, "");


      price =
        Number(
          priceText
        ) || 0;


      titleFinished = true;
      continue;
    }


    // ==================================================
    // HARGA TANPA Rp
    //
    // 125.000
    // 125000
    // ==================================================

    const numericPrice =
      line.replace(
        /[.,\s]/g,
        ""
      );


    if (
      /^\d{4,9}$/.test(
        numericPrice
      )
    ) {

      price =
        Number(
          numericPrice
        ) || 0;


      titleFinished = true;
      continue;
    }


    // ==================================================
    // JUDUL
    // ==================================================

    if (!titleFinished) {

      titleParts.push(
        line
      );
    }
  }


  // ==================================================
  // FALLBACK JUDUL
  // ==================================================

  if (
    titleParts.length === 0
  ) {

    for (
      let i = 0;
      i < lines.length;
      i++
    ) {

      const line =
        lines[i];


      if (
        isUrlLine(line)
      ) {
        continue;
      }


      if (
        /\bRp\.?\s*[\d.,]+/i
          .test(line)
      ) {
        continue;
      }


      const possibleIsbn =
        line
          .replace(
            /^ISBN(?:-1[03])?\s*[:\-]?\s*/i,
            ""
          )
          .replace(
            /[\s-]/g,
            ""
          );


      if (
        /^\d{10,13}$/.test(
          possibleIsbn
        )
      ) {
        continue;
      }


      const numeric =
        line.replace(
          /[.,\s]/g,
          ""
        );


      if (
        /^\d{4,9}$/.test(
          numeric
        )
      ) {
        continue;
      }


      titleParts.push(
        line
      );

      break;
    }
  }


  const title =
    titleParts
      .join(" ")
      .replace(
        /\s+/g,
        " "
      )
      .trim();


  return {
    title: title,
    isbn: isbn,
    price: price
  };
}


// ======================================================
// URL DETECTOR
// ======================================================

function isUrlLine(line) {

  const value =
    String(
      line || ""
    )
      .trim();


  return (
    /^https?:\/\//i.test(value) ||
    /^www\./i.test(value) ||
    /instagram\.com/i.test(value) ||
    /facebook\.com/i.test(value) ||
    /fb\.com/i.test(value)
  );
}


// ======================================================
// SETUP
// ======================================================

function setupQueueSystem() {

  const ss =
    SpreadsheetApp
      .getActiveSpreadsheet();


  if (!ss) {

    throw new Error(
      "Buka Apps Script dari Google Sheet."
    );
  }


  PropertiesService
    .getScriptProperties()
    .setProperty(
      "SPREADSHEET_ID",
      ss.getId()
    );


  getOrCreateOrderSheet();
  getOrCreateQueueSheet();
  getOrCreateContactMapSheet();
  getOrCreateDebugSheet();


  const managedHandlers =
    new Set([
      "processQueue",
      "retryMissingReactions"
    ]);


  ScriptApp
    .getProjectTriggers()
    .forEach(
      trigger => {

        if (
          managedHandlers.has(
            trigger.getHandlerFunction()
          )
        ) {

          ScriptApp
            .deleteTrigger(
              trigger
            );
        }
      }
    );


  // Worker order setiap 1 menit
  ScriptApp
    .newTrigger(
      "processQueue"
    )
    .timeBased()
    .everyMinutes(1)
    .create();



  logDebug(
    "SYSTEM",
    "",
    "V7 SIMPLE QUEUE + MANUAL SCAN READY"
  );
}


// ======================================================
// SPREADSHEET
// ======================================================

function getSpreadsheet() {

  const id =
    PropertiesService
      .getScriptProperties()
      .getProperty(
        "SPREADSHEET_ID"
      );


  if (id) {

    return SpreadsheetApp
      .openById(id);
  }


  const ss =
    SpreadsheetApp
      .getActiveSpreadsheet();


  if (!ss) {

    throw new Error(
      "SPREADSHEET_ID BELUM ADA"
    );
  }


  return ss;
}


// ======================================================
// ORDER SHEET
// ======================================================

function getOrderSheetNameByGroup(
  groupId
) {

  return (
    GROUP_TO_ORDER_SHEET[
      String(groupId || "").trim()
    ] ||
    ORDER_SHEET_NAME
  );
}


// ======================================================
// PARSE PERINTAH ORDER
//
// Contoh yang diterima:
// FIX
// FIX 2
// FIX NAMI
// FIX NAMI 2
// FIX 2 NAMI
// MAU judul buku
//
// Catatan setelah FIX / MAU disimpan di Pesan Customer.
// Produk tetap diambil dari pesan yang di-reply.
// ======================================================

function parseOrderCommand(text) {

  const match =
    String(text || "")
      .trim()
      .match(
        /^(FIX|MAU)\b(?:\s+([\s\S]*))?$/i
      );


  if (!match) {

    return null;
  }


  const keyword =
    String(match[1] || "")
      .toUpperCase();


  const remainder =
    String(match[2] || "")
      .trim();


  let qty = 1;

  let note = remainder;


  // FIX 2 NAMI / FIX +2 NAMI
  let qtyMatch =
    remainder.match(
      /^(\+?\d+)(?:\s+([\s\S]*))?$/
    );


  if (qtyMatch) {

    qty = Number(
      String(qtyMatch[1]).replace(
        "+",
        ""
      )
    );


    note =
      String(qtyMatch[2] || "")
        .trim();

  } else {

    // FIX NAMI 2 / FIX NAMI +2
    qtyMatch =
      remainder.match(
        /^([\s\S]*?)(?:\s+\+?(\d+))$/
      );


    if (qtyMatch) {

      qty = Number(
        qtyMatch[2]
      );


      note =
        String(qtyMatch[1] || "")
          .trim();
    }
  }


  if (
    !qty ||
    qty < 1
  ) {

    qty = 1;
  }


  return {
    keyword: keyword,
    qty: qty,
    note: note
  };
}


function getOrCreateOrderSheet(
  groupId
) {

  const ss =
    getSpreadsheet();


  const sheetName =
    getOrderSheetNameByGroup(
      groupId
    );


  let sheet =
    ss.getSheetByName(
      sheetName
    );


  if (!sheet) {

    sheet =
      ss.insertSheet(
        sheetName
      );
  }


  if (
    sheet.getLastRow() === 0
  ) {

    sheet.appendRow([
      "Tanggal",
      "Nama Customer",
      "No WhatsApp",
      "Group ID",
      "ISBN",
      "Barang",
      "Harga",
      "Qty",
      "Total",
      "Status Stok",
      "Keyword",
      "Pesan Customer",
      "Pesan Direply",
      "Message ID",
      "Item Ke"
    ]);
  }


  return sheet;
}


// ======================================================
// QUEUE
// ======================================================

function getOrCreateQueueSheet() {

  const ss =
    getSpreadsheet();


  let sheet =
    ss.getSheetByName(
      QUEUE_SHEET_NAME
    );


  if (!sheet) {

    sheet =
      ss.insertSheet(
        QUEUE_SHEET_NAME
      );
  }


  if (
    sheet.getLastRow() === 0
  ) {

    sheet.appendRow([
      "Received At",
      "Message ID",
      "Instance",
      "Group ID",
      "Sender LID",
      "Sender Phone",
      "Push Name",
      "Text",
      "Quoted Product",
      "Mention JID",
      "Status",
      "Retry",
      "Last Error",
      "Processed At",
      "Last Update"
    ]);
  }


  return sheet;
}


// ======================================================
// FIND QUEUE
// ======================================================

function findQueueRowByMessageId(
  queue,
  messageId
) {

  if (
    !queue ||
    queue.getLastRow() < 2
  ) {
    return 0;
  }


  const ids =
    queue
      .getRange(
        2,
        2,
        queue.getLastRow() - 1,
        1
      )
      .getValues()
      .flat();


  for (
    let i = 0;
    i < ids.length;
    i++
  ) {

    if (
      String(ids[i]) ===
      String(messageId)
    ) {

      return i + 2;
    }
  }


  return 0;
}


function queueHasMessage(
  queue,
  messageId
) {

  const id =
    String(
      messageId || ""
    );


  if (!id) {
    return false;
  }


  try {

    const cached =
      CacheService
        .getScriptCache()
        .get(
          "QUEUE_MSG_" + id
        );


    if (cached) {
      return true;
    }

  } catch (cacheError) {}


  if (
    !queue ||
    queue.getLastRow() < 2
  ) {
    return false;
  }


  // Cukup scan 2000 row terakhir.
  // Retry Evolution biasanya datang dekat waktunya.
  const lastRow =
    queue.getLastRow();

  const firstRow =
    Math.max(
      2,
      lastRow - 1999
    );

  const count =
    lastRow -
    firstRow +
    1;


  const ids =
    queue
      .getRange(
        firstRow,
        2,
        count,
        1
      )
      .getDisplayValues()
      .flat();


  const found =
    ids.some(
      value =>
        String(value) === id
    );


  if (found) {

    try {
      CacheService
        .getScriptCache()
        .put(
          "QUEUE_MSG_" + id,
          "1",
          21600
        );
    } catch (cacheError) {}
  }


  return found;
}


// ======================================================
// ORDER ANTI DOUBLE
// ======================================================

function orderMessageExists(
  sheet,
  messageId
) {

  if (
    !sheet ||
    sheet.getLastRow() < 2
  ) {
    return false;
  }


  const ids =
    sheet
      .getRange(
        2,
        14,
        sheet.getLastRow() - 1,
        1
      )
      .getValues()
      .flat();


  return ids.some(
    id =>
      String(id)
        .startsWith(
          messageId + "-"
        )
  );
}


// ======================================================
// CONTACT MAP
// ======================================================

function getOrCreateContactMapSheet() {

  const ss =
    getSpreadsheet();


  let sheet =
    ss.getSheetByName(
      CONTACT_MAP_SHEET_NAME
    );


  if (!sheet) {

    sheet =
      ss.insertSheet(
        CONTACT_MAP_SHEET_NAME
      );
  }


  if (
    sheet.getLastRow() === 0
  ) {

    sheet.appendRow([
      "LID",
      "No WhatsApp",
      "Terakhir Update"
    ]);
  }


  return sheet;
}


function saveContactMapping(key) {

  const rawLid =
    key.participant || "";

  const rawPhone =
    key.participantAlt || "";


  if (
    !rawLid.endsWith("@lid")
  ) {
    return;
  }


  if (
    !rawPhone.endsWith(
      "@s.whatsapp.net"
    )
  ) {
    return;
  }


  const lid =
    cleanWhatsAppId(
      rawLid
    );


  const phone =
    cleanWhatsAppId(
      rawPhone
    );


  if (
    !lid ||
    !phone
  ) {
    return;
  }


  const sheet =
    getOrCreateContactMapSheet();


  if (
    sheet.getLastRow() < 2
  ) {

    sheet.appendRow([
      lid,
      phone,
      new Date()
    ]);

    return;
  }


  const values =
    sheet
      .getRange(
        2,
        1,
        sheet.getLastRow() - 1,
        2
      )
      .getValues();


  for (
    let i = 0;
    i < values.length;
    i++
  ) {

    if (
      String(
        values[i][0]
      ) ===
      String(lid)
    ) {

      sheet
        .getRange(
          i + 2,
          2
        )
        .setValue(
          phone
        );


      sheet
        .getRange(
          i + 2,
          3
        )
        .setValue(
          new Date()
        );


      return;
    }
  }


  sheet.appendRow([
    lid,
    phone,
    new Date()
  ]);
}


function getPhoneFromLid(
  lidJid
) {

  const lid =
    cleanWhatsAppId(
      lidJid
    );


  if (!lid) {
    return "";
  }


  const sheet =
    getOrCreateContactMapSheet();


  if (
    sheet.getLastRow() < 2
  ) {
    return "";
  }


  const values =
    sheet
      .getRange(
        2,
        1,
        sheet.getLastRow() - 1,
        2
      )
      .getValues();


  for (
    let i = 0;
    i < values.length;
    i++
  ) {

    if (
      String(
        values[i][0]
      ) ===
      String(lid)
    ) {

      return String(
        values[i][1] || ""
      );
    }
  }


  return "";
}


// ======================================================
// DEBUG
// ======================================================

function getOrCreateDebugSheet() {

  const ss =
    getSpreadsheet();


  let sheet =
    ss.getSheetByName(
      DEBUG_SHEET_NAME
    );


  if (!sheet) {

    sheet =
      ss.insertSheet(
        DEBUG_SHEET_NAME
      );
  }


  if (
    sheet.getLastRow() === 0
  ) {

    sheet.appendRow([
      "Tanggal",
      "Status",
      "Message ID",
      "Info"
    ]);
  }


  return sheet;
}


function logDebug(
  status,
  messageId,
  info
) {

  try {

    const sheet =
      getOrCreateDebugSheet();


    sheet.appendRow([
      new Date(),
      status,
      messageId,
      String(
        info || ""
      )
    ]);


  } catch (err) {

    console.log(
      "logDebug FAILED: " +
      err.message
    );

    throw err;
  }
}


// ======================================================
// CLEAN WA ID
// ======================================================

function cleanWhatsAppId(value) {

  return String(
    value || ""
  )
    .replace(
      "@s.whatsapp.net",
      ""
    )
    .replace(
      "@lid",
      ""
    );
}


// ======================================================
// REACTION
// ======================================================

function sendReactionEmoji(
  instanceName,
  remoteJid,
  messageId,
  participant,
  emoji
) {

  const apiKey =
    PropertiesService
      .getScriptProperties()
      .getProperty(
        "EVOLUTION_API_KEY"
      );

  if (!apiKey) {
    throw new Error(
      "EVOLUTION_API_KEY TIDAK ADA"
    );
  }

  const url =
    EVOLUTION_BASE_URL +
    "/message/sendReaction/" +
    encodeURIComponent(
      instanceName
    );

  const body = {
    key: {
      remoteJid: remoteJid,
      fromMe: false,
      id: messageId,
      participant: participant
    },
    reaction: emoji
  };

  let lastStatus = 0;
  let lastBody = "";
  let lastError = null;

  // Retry hanya untuk gangguan sementara Evolution/Railway.
  // Order TIDAK ditulis ulang oleh fungsi ini.
  for (let attempt = 1; attempt <= 3; attempt++) {

    try {
      const response =
        UrlFetchApp.fetch(
          url,
          {
            method: "post",
            contentType: "application/json",
            headers: {
              apikey: apiKey
            },
            payload: JSON.stringify(body),
            muteHttpExceptions: true
          }
        );

      lastStatus =
        response.getResponseCode();

      lastBody =
        response.getContentText();

      logDebug(
        "REACTION " +
          emoji +
          " " +
          lastStatus +
          " TRY " +
          attempt,
        messageId,
        lastBody
      );

      if (
        lastStatus >= 200 &&
        lastStatus < 300
      ) {
        return response;
      }

      const retryable =
        lastStatus === 408 ||
        lastStatus === 429 ||
        lastStatus === 500 ||
        lastStatus === 502 ||
        lastStatus === 503 ||
        lastStatus === 504;

      if (!retryable) {
        throw new Error(
          "REACTION HTTP " +
          lastStatus +
          ": " +
          lastBody
        );
      }

    } catch (err) {
      lastError = err;

      // Error HTTP non-retryable yang kita lempar sendiri: stop.
      if (
        String(err.message || "")
          .indexOf("REACTION HTTP ") === 0 &&
        ![408,429,500,502,503,504]
          .some(code =>
            String(err.message || "")
              .indexOf("REACTION HTTP " + code) === 0
          )
      ) {
        throw err;
      }
    }

    if (attempt < 3) {
      Utilities.sleep(
        attempt * 1200
      );
    }
  }

  throw new Error(
    "REACTION GAGAL SETELAH 3X" +
    (lastStatus
      ? " | HTTP " + lastStatus
      : "") +
    (lastBody
      ? " | " + lastBody
      : "") +
    (lastError && !lastStatus
      ? " | " + lastError.message
      : "")
  );
}


// ======================================================
// BURST DETECTOR
// ======================================================

function recordFixBurst() {

  const cache =
    CacheService
      .getScriptCache();


  const now =
    new Date();


  const bucket =
    Math.floor(
      now.getTime() /
      60000
    );


  const key =
    "FIX_BURST_" +
    bucket;


  const current =
    Number(
      cache.get(key) || 0
    ) + 1;


  cache.put(
    key,
    String(current),
    600
  );


  let total = 0;


  for (
    let i = 0;
    i < BURST_WINDOW_MINUTES;
    i++
  ) {

    total +=
      Number(
        cache.get(
          "FIX_BURST_" +
          (bucket - i)
        ) || 0
      );
  }


  if (
    total >= BURST_THRESHOLD
  ) {

    const until =
      Date.now() +
      BURST_ACTIVE_MINUTES *
      60000;


    PropertiesService
      .getScriptProperties()
      .setProperty(
        "BURST_MODE_UNTIL",
        String(until)
      );
  }
}


function isBurstMode() {

  const until =
    Number(
      PropertiesService
        .getScriptProperties()
        .getProperty(
          "BURST_MODE_UNTIL"
        ) || 0
    );


  return (
    until > Date.now()
  );
}


// ======================================================
// AUTO RETRY REACTION YANG MEMANG GAGAL
// ======================================================

function retryMissingReactions(
  maxItems
) {

  const limit =
    Number(maxItems) > 0
      ? Number(maxItems)
      : (
          isBurstMode()
            ? REACTION_RETRY_BURST_BATCH
            : REACTION_RETRY_NORMAL_BATCH
        );


  const workerLock =
    LockService
      .getScriptLock();


  if (
    !workerLock.tryLock(5000)
  ) {
    return;
  }


  try {

    const queue =
      getOrCreateQueueSheet();


    const lastRow =
      queue.getLastRow();


    if (
      lastRow < 2
    ) {
      return;
    }


    const values =
      queue
        .getRange(
          2,
          1,
          lastRow - 1,
          15
        )
        .getValues();


    let repaired = 0;


    for (
      let i = 0;
      i < values.length;
      i++
    ) {

      if (
        repaired >= limit
      ) {
        break;
      }


      const row =
        values[i];


      const status =
        String(
          row[10] || ""
        );


      if (
        status !==
        "DONE_NO_REACTION"
      ) {
        continue;
      }


      try {

        sendReactionEmoji(
          row[2] ||
            DEFAULT_INSTANCE,
          row[3],
          row[1],
          row[4] || "",
          "✅"
        );


        queue
          .getRange(
            i + 2,
            11
          )
          .setValue(
            "DONE"
          );


        queue
          .getRange(
            i + 2,
            13
          )
          .setValue("");


        queue
          .getRange(
            i + 2,
            15
          )
          .setValue(
            new Date()
          );


        repaired++;


      } catch (err) {

        queue
          .getRange(
            i + 2,
            13
          )
          .setValue(
            "REACTION RETRY: " +
            err.message
          );


        queue
          .getRange(
            i + 2,
            15
          )
          .setValue(
            new Date()
          );
      }
    }


    if (
      repaired > 0
    ) {

      logDebug(
        "REACTION RECOVERY",
        "",
        "Berhasil repair " +
        repaired +
        " reaction"
      );
    }


  } finally {

    workerLock.releaseLock();
  }
}


// ======================================================
// MANUAL REPAIR UNTUK DATA LAMA
//
// Dipakai bila row lama sudah status DONE,
// tetapi WhatsApp belum terlihat ✅.
// Akan resend ✅ ke DONE / DONE_NO_REACTION 48 jam terakhir.
// Resend reaction yang sama umumnya aman/idempotent.
// ======================================================

function repairRecentDoneReactions() {

  const queue =
    getOrCreateQueueSheet();


  const lastRow =
    queue.getLastRow();


  if (
    lastRow < 2
  ) {
    return;
  }


  const values =
    queue
      .getRange(
        2,
        1,
        lastRow - 1,
        15
      )
      .getValues();


  const cutoff =
    Date.now() -
    REPAIR_RECENT_HOURS *
    60 *
    60 *
    1000;


  let repaired = 0;


  // Mulai dari paling baru
  for (
    let i = values.length - 1;
    i >= 0;
    i--
  ) {

    if (
      repaired >= REPAIR_RECENT_MAX
    ) {
      break;
    }


    const row =
      values[i];


    const receivedAt =
      row[0];


    if (
      !(receivedAt instanceof Date) ||
      receivedAt.getTime() < cutoff
    ) {
      continue;
    }


    const status =
      String(
        row[10] || ""
      );


    if (
      status !== "DONE" &&
      status !== "DONE_NO_REACTION"
    ) {
      continue;
    }


    try {

      sendReactionEmoji(
        row[2] ||
          DEFAULT_INSTANCE,
        row[3],
        row[1],
        row[4] || "",
        "✅"
      );


      queue
        .getRange(
          i + 2,
          11
        )
        .setValue(
          "DONE"
        );


      queue
        .getRange(
          i + 2,
          13
        )
        .setValue("");


      queue
        .getRange(
          i + 2,
          15
        )
        .setValue(
          new Date()
        );


      repaired++;


    } catch (err) {

      queue
        .getRange(
          i + 2,
          13
        )
        .setValue(
          "MANUAL REACTION REPAIR: " +
          err.message
        );
    }
  }


  logDebug(
    "MANUAL REPAIR DONE",
    "",
    "Resend reaction: " +
    repaired
  );
}


// ======================================================
// RECOVER STUCK
// ======================================================

function recoverStuckRows(queue) {

  if (
    queue.getLastRow() < 2
  ) {
    return;
  }


  const values =
    queue
      .getRange(
        2,
        1,
        queue.getLastRow() - 1,
        15
      )
      .getValues();


  const now =
    new Date()
      .getTime();


  for (
    let i = 0;
    i < values.length;
    i++
  ) {

    const status =
      String(
        values[i][10] || ""
      );


    const lastUpdate =
      values[i][14];


    if (
      status === "PROCESSING" &&
      lastUpdate instanceof Date
    ) {

      const ageMinutes =
        (
          now -
          lastUpdate.getTime()
        ) /
        60000;


      if (
        ageMinutes > 5
      ) {

        queue
          .getRange(
            i + 2,
            11
          )
          .setValue(
            "RETRY"
          );


        queue
          .getRange(
            i + 2,
            15
          )
          .setValue(
            new Date()
          );


        logDebug(
          "RECOVER STUCK",
          values[i][1],
          "PROCESSING -> RETRY"
        );
      }
    }
  }
}


// ======================================================
// AUTHORIZE EXTERNAL REQUEST
// ======================================================

function authorizeExternalRequest() {

  const response =
    UrlFetchApp.fetch(
      EVOLUTION_BASE_URL
    );


  Logger.log(
    response.getResponseCode()
  );
}

function bersihkanNomorTelepon() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = spreadsheet.getSheetByName('Contact Number');

  if (!sheet) {
    throw new Error('Tab "Contact Number" tidak ditemukan.');
  }

  const data = sheet.getDataRange().getValues();
  const headers = data[0];

  const phoneColumn = headers.findIndex(header =>
    String(header).trim().toLowerCase() === 'no tlp'
  );

  if (phoneColumn === -1) {
    throw new Error('Kolom dengan judul "No Tlp" tidak ditemukan.');
  }

  for (let row = 1; row < data.length; row++) {
    const phone = data[row][phoneColumn];

    if (phone !== '' && phone !== null) {
      data[row][phoneColumn] = String(phone).replace(/\D/g, '');
    }
  }

  if (data.length > 1) {
    sheet
      .getRange(2, phoneColumn + 1, data.length - 1, 1)
      .setNumberFormat('@')
      .setValues(data.slice(1).map(row => [row[phoneColumn]]));
  }

  SpreadsheetApp.getUi().alert('Semua nomor telepon sudah dibersihkan.');
}

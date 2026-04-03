const directus = require('./directus');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseEnvInt(name, fallback) {
  const v = Number.parseInt(String(process.env[name] ?? ''), 10);
  return Number.isFinite(v) ? v : fallback;
}

function clamp(n, min, max) {
  return Math.min(max, Math.max(min, n));
}

function normalizePlate(raw) {
  const plate = String(raw ?? '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
  return plate;
}

function isValidPlate(plate) {
  if (!plate) return false;
  if (plate.length < 5 || plate.length > 8) return false;
  if (!/^[A-Z0-9]+$/.test(plate)) return false;
  return true;
}

class CallSpacer {
  constructor({ callsPerMinute }) {
    const safe = Number.isFinite(callsPerMinute) ? callsPerMinute : 25;
    this.spacingMs = Math.ceil(60000 / clamp(safe, 1, 60000));
    this.nextAllowedAt = 0;
  }

  async waitTurn() {
    const now = Date.now();
    const waitMs = Math.max(0, this.nextAllowedAt - now);
    if (waitMs > 0) await sleep(waitMs);
    this.nextAllowedAt = Math.max(this.nextAllowedAt, Date.now()) + this.spacingMs;
  }
}

async function fetchJsonWithTimeout(url, { headers, timeoutMs } = {}) {
  const controller = new AbortController();
  const ms = Number.isFinite(timeoutMs) ? timeoutMs : 20000;
  const id = setTimeout(() => controller.abort(), ms);
  try {
    const res = await fetch(url, { headers: headers || {}, signal: controller.signal });
    const contentType = res.headers.get('content-type') || '';
    let data = null;
    if (contentType.includes('application/json')) {
      try {
        data = await res.json();
      } catch {
        data = null;
      }
    } else {
      try {
        data = await res.text();
      } catch {
        data = null;
      }
    }
    return { res, data };
  } finally {
    clearTimeout(id);
  }
}

function extractRetryAfterSeconds(res) {
  if (!res) return null;
  const raw = res.headers.get('retry-after');
  if (!raw) return null;
  const n = Number.parseInt(String(raw).trim(), 10);
  if (Number.isFinite(n) && n > 0) return n;
  const d = Date.parse(raw);
  if (Number.isFinite(d)) {
    const diff = Math.ceil((d - Date.now()) / 1000);
    return diff > 0 ? diff : null;
  }
  return null;
}

function computeBackoffSeconds({ attempts, baseSeconds, maxSeconds }) {
  const a = Number.isFinite(Number(attempts)) ? Number(attempts) : 0;
  const base = Number.isFinite(Number(baseSeconds)) ? Number(baseSeconds) : 30;
  const max = Number.isFinite(Number(maxSeconds)) ? Number(maxSeconds) : 3600;
  const exp = Math.min(10, a);
  const raw = base * Math.pow(2, exp);
  const jitter = Math.floor(Math.random() * Math.min(10, base));
  return clamp(raw + jitter, base, max);
}

function getGetApiConfig() {
  const apiKey = String(process.env.GETAPI_API_KEY || process.env.GETAPI_KEY || process.env.GETAPI_X_API_KEY || '').trim();
  const baseUrl = String(process.env.GETAPI_BASE_URL || 'https://chile.getapi.cl').trim().replace(/\/+$/, '');
  if (!apiKey) throw new Error('Falta GETAPI_API_KEY');
  return { apiKey, baseUrl };
}

async function getApiGetPlateInfo(plate, { limiter } = {}) {
  const { apiKey, baseUrl } = getGetApiConfig();
  await limiter.waitTurn();
  const url = `${baseUrl}/v1/vehicles/plate/${encodeURIComponent(plate)}`;
  const { res, data } = await fetchJsonWithTimeout(url, {
    headers: {
      Accept: 'application/json',
      'X-API-KEY': apiKey
    },
    timeoutMs: parseEnvInt('GETAPI_TIMEOUT_MS', 20000)
  });
  return { status: res.status, ok: res.ok, data, res };
}

async function getApiGetAppraisal(plate, { limiter } = {}) {
  const { apiKey, baseUrl } = getGetApiConfig();
  await limiter.waitTurn();
  const url = `${baseUrl}/v1/vehicles/appraisal/${encodeURIComponent(plate)}`;
  const { res, data } = await fetchJsonWithTimeout(url, {
    headers: {
      Accept: 'application/json',
      'X-API-KEY': apiKey
    },
    timeoutMs: parseEnvInt('GETAPI_TIMEOUT_MS', 20000)
  });
  return { status: res.status, ok: res.ok, data, res };
}

async function processOneRow(row, { limiter } = {}) {
  const schema = await directus.resolveGetApiSchema();
  const id = row?.[schema.idField];
  const detectionId = schema.detectionIdField ? row?.[schema.detectionIdField] : null;
  const attempts = schema.attemptsField ? Number(row?.[schema.attemptsField] ?? 0) : 0;
  const rawPlate = schema.plateField ? row?.[schema.plateField] : null;
  const plate = normalizePlate(rawPlate);
  const nowIso = new Date().toISOString();

  if (!id) return { ok: false, reason: 'missing_id' };

  if (!isValidPlate(plate)) {
    const patch = {};
    if (schema.statusField) patch[schema.statusField] = 'invalid_plate';
    if (schema.attemptsField) patch[schema.attemptsField] = attempts + 1;
    if (schema.reasonField) patch[schema.reasonField] = 'invalid_plate';
    if (schema.messageField) patch[schema.messageField] = 'Patente inválida';
    if (schema.fetchedAtField) patch[schema.fetchedAtField] = nowIso;
    if (schema.lastErrorAtField) patch[schema.lastErrorAtField] = nowIso;
    if (schema.nextRetryAtField) patch[schema.nextRetryAtField] = null;
    await directus.patchRowById(id, patch);
    return { ok: true, status: 'invalid_plate', detectionId, plate };
  }

  const plateRes = await getApiGetPlateInfo(plate, { limiter });
  if (plateRes.status === 429) {
    const retryAfter = extractRetryAfterSeconds(plateRes.res);
    const backoffSeconds = retryAfter ?? computeBackoffSeconds({ attempts, baseSeconds: 30, maxSeconds: 3600 });
    const nextRetryAt = new Date(Date.now() + backoffSeconds * 1000).toISOString();
    const patch = {};
    if (schema.statusField) patch[schema.statusField] = 'rate_limited';
    if (schema.attemptsField) patch[schema.attemptsField] = attempts + 1;
    if (schema.upstreamStatusField) patch[schema.upstreamStatusField] = 429;
    if (schema.reasonField) patch[schema.reasonField] = 'rate_limited';
    if (schema.messageField) patch[schema.messageField] = 'Rate limited en /v1/vehicles/plate';
    if (schema.fetchedAtField) patch[schema.fetchedAtField] = nowIso;
    if (schema.lastErrorAtField) patch[schema.lastErrorAtField] = nowIso;
    if (schema.nextRetryAtField) patch[schema.nextRetryAtField] = nextRetryAt;
    if (schema.getapiField) {
      patch[schema.getapiField] = {
        plate,
        fetched_at: nowIso,
        vehicle: null,
        appraisal: null,
        upstream: { plate: { status: plateRes.status, data: plateRes.data } }
      };
    }
    await directus.patchRowById(id, patch);
    return { ok: true, status: 'rate_limited', detectionId, plate, nextRetryAt, backoffSeconds };
  }

  if (plateRes.status === 404) {
    const patch = {};
    if (schema.statusField) patch[schema.statusField] = 'not_found';
    if (schema.attemptsField) patch[schema.attemptsField] = attempts + 1;
    if (schema.upstreamStatusField) patch[schema.upstreamStatusField] = 404;
    if (schema.reasonField) patch[schema.reasonField] = 'not_found';
    if (schema.messageField) patch[schema.messageField] = 'No encontrado en /v1/vehicles/plate';
    if (schema.fetchedAtField) patch[schema.fetchedAtField] = nowIso;
    if (schema.lastErrorAtField) patch[schema.lastErrorAtField] = nowIso;
    if (schema.nextRetryAtField) patch[schema.nextRetryAtField] = null;
    if (schema.getapiField) {
      patch[schema.getapiField] = {
        plate,
        fetched_at: nowIso,
        vehicle: null,
        appraisal: null,
        upstream: { plate: { status: plateRes.status, data: plateRes.data } }
      };
    }
    await directus.patchRowById(id, patch);
    return { ok: true, status: 'not_found', detectionId, plate };
  }

  if (!plateRes.ok) {
    const backoffSeconds = computeBackoffSeconds({ attempts, baseSeconds: 60, maxSeconds: 3600 });
    const nextRetryAt = schema.nextRetryAtField ? new Date(Date.now() + backoffSeconds * 1000).toISOString() : null;
    const patch = {};
    if (schema.statusField) patch[schema.statusField] = 'error';
    if (schema.attemptsField) patch[schema.attemptsField] = attempts + 1;
    if (schema.upstreamStatusField) patch[schema.upstreamStatusField] = plateRes.status;
    if (schema.reasonField) patch[schema.reasonField] = 'upstream_error';
    if (schema.messageField) patch[schema.messageField] = `Error upstream en /v1/vehicles/plate: HTTP ${plateRes.status}`;
    if (schema.fetchedAtField) patch[schema.fetchedAtField] = nowIso;
    if (schema.lastErrorAtField) patch[schema.lastErrorAtField] = nowIso;
    if (schema.nextRetryAtField) patch[schema.nextRetryAtField] = nextRetryAt;
    if (schema.getapiField) {
      patch[schema.getapiField] = {
        plate,
        fetched_at: nowIso,
        vehicle: null,
        appraisal: null,
        upstream: { plate: { status: plateRes.status, data: plateRes.data } }
      };
    }
    await directus.patchRowById(id, patch);
    return { ok: true, status: 'error', detectionId, plate, nextRetryAt };
  }

  const appraisalRes = await getApiGetAppraisal(plate, { limiter });
  if (appraisalRes.status === 429) {
    const retryAfter = extractRetryAfterSeconds(appraisalRes.res);
    const backoffSeconds = retryAfter ?? computeBackoffSeconds({ attempts, baseSeconds: 30, maxSeconds: 3600 });
    const nextRetryAt = new Date(Date.now() + backoffSeconds * 1000).toISOString();
    const patch = {};
    if (schema.statusField) patch[schema.statusField] = 'rate_limited';
    if (schema.attemptsField) patch[schema.attemptsField] = attempts + 1;
    if (schema.upstreamStatusField) patch[schema.upstreamStatusField] = 429;
    if (schema.reasonField) patch[schema.reasonField] = 'rate_limited';
    if (schema.messageField) patch[schema.messageField] = 'Rate limited en /v1/vehicles/appraisal';
    if (schema.fetchedAtField) patch[schema.fetchedAtField] = nowIso;
    if (schema.lastErrorAtField) patch[schema.lastErrorAtField] = nowIso;
    if (schema.nextRetryAtField) patch[schema.nextRetryAtField] = nextRetryAt;
    if (schema.getapiField) {
      patch[schema.getapiField] = {
        plate,
        fetched_at: nowIso,
        vehicle: plateRes.data ?? null,
        appraisal: null,
        upstream: {
          plate: { status: plateRes.status, data: plateRes.data },
          appraisal: { status: appraisalRes.status, data: appraisalRes.data }
        }
      };
    }
    await directus.patchRowById(id, patch);
    return { ok: true, status: 'rate_limited', detectionId, plate, nextRetryAt, backoffSeconds };
  }

  if (appraisalRes.status === 404) {
    const patch = {};
    if (schema.statusField) patch[schema.statusField] = 'not_found';
    if (schema.attemptsField) patch[schema.attemptsField] = attempts + 1;
    if (schema.upstreamStatusField) patch[schema.upstreamStatusField] = 404;
    if (schema.reasonField) patch[schema.reasonField] = 'not_found';
    if (schema.messageField) patch[schema.messageField] = 'No encontrado en /v1/vehicles/appraisal';
    if (schema.fetchedAtField) patch[schema.fetchedAtField] = nowIso;
    if (schema.lastErrorAtField) patch[schema.lastErrorAtField] = nowIso;
    if (schema.nextRetryAtField) patch[schema.nextRetryAtField] = null;
    if (schema.getapiField) {
      patch[schema.getapiField] = {
        plate,
        fetched_at: nowIso,
        vehicle: plateRes.data ?? null,
        appraisal: null,
        upstream: {
          plate: { status: plateRes.status, data: plateRes.data },
          appraisal: { status: appraisalRes.status, data: appraisalRes.data }
        }
      };
    }
    await directus.patchRowById(id, patch);
    return { ok: true, status: 'not_found', detectionId, plate };
  }

  if (!appraisalRes.ok) {
    const backoffSeconds = computeBackoffSeconds({ attempts, baseSeconds: 60, maxSeconds: 3600 });
    const nextRetryAt = schema.nextRetryAtField ? new Date(Date.now() + backoffSeconds * 1000).toISOString() : null;
    const patch = {};
    if (schema.statusField) patch[schema.statusField] = 'error';
    if (schema.attemptsField) patch[schema.attemptsField] = attempts + 1;
    if (schema.upstreamStatusField) patch[schema.upstreamStatusField] = appraisalRes.status;
    if (schema.reasonField) patch[schema.reasonField] = 'upstream_error';
    if (schema.messageField) patch[schema.messageField] = `Error upstream en /v1/vehicles/appraisal: HTTP ${appraisalRes.status}`;
    if (schema.fetchedAtField) patch[schema.fetchedAtField] = nowIso;
    if (schema.lastErrorAtField) patch[schema.lastErrorAtField] = nowIso;
    if (schema.nextRetryAtField) patch[schema.nextRetryAtField] = nextRetryAt;
    if (schema.getapiField) {
      patch[schema.getapiField] = {
        plate,
        fetched_at: nowIso,
        vehicle: plateRes.data ?? null,
        appraisal: null,
        upstream: {
          plate: { status: plateRes.status, data: plateRes.data },
          appraisal: { status: appraisalRes.status, data: appraisalRes.data }
        }
      };
    }
    await directus.patchRowById(id, patch);
    return { ok: true, status: 'error', detectionId, plate, nextRetryAt };
  }

  const resultPayload = {
    plate,
    fetched_at: nowIso,
    vehicle: plateRes.data ?? null,
    appraisal: appraisalRes.data ?? null
  };

  const patch = {};
  if (schema.statusField) patch[schema.statusField] = 'ok';
  if (schema.attemptsField) patch[schema.attemptsField] = attempts + 1;
  if (schema.upstreamStatusField) patch[schema.upstreamStatusField] = 200;
  if (schema.reasonField) patch[schema.reasonField] = null;
  if (schema.messageField) patch[schema.messageField] = null;
  if (schema.fetchedAtField) patch[schema.fetchedAtField] = nowIso;
  if (schema.nextRetryAtField) patch[schema.nextRetryAtField] = null;
  if (schema.getapiField) patch[schema.getapiField] = resultPayload;
  await directus.patchRowById(id, patch);

  return { ok: true, status: 'ok', detectionId, plate };
}

async function startProcessor() {
  const { baseUrl, collection } = directus.getDirectusConfig();
  if (!baseUrl) throw new Error('DIRECTUS_URL no está configurado');
  if (!collection) throw new Error('DIRECTUS_GETAPI_COLLECTION no está configurado');

  await directus.resolveGetApiSchema();

  const callsPerMinute = parseEnvInt('GETAPI_MAX_CALLS_PER_MIN', 25);
  const limiter = new CallSpacer({ callsPerMinute });
  const batchSize = parseEnvInt('PROCESSOR_BATCH_SIZE', 10);
  const idleWaitMs = parseEnvInt('PROCESSOR_IDLE_WAIT_MS', 10000);
  const loopWaitMs = parseEnvInt('PROCESSOR_LOOP_WAIT_MS', 50);

  console.log(`Procesador GetAPI iniciado · Colección: ${collection} · Directus: ${baseUrl}`);

  let globalBackoffUntil = 0;

  while (true) {
    const now = Date.now();
    if (globalBackoffUntil > now) {
      await sleep(Math.min(1000, globalBackoffUntil - now));
      continue;
    }

    const nowIso = new Date().toISOString();
    let rows = await directus.listQueueByStatus('pending', { limit: batchSize, nowIso });
    let source = 'pending';

    if (!rows || rows.length === 0) {
      rows = await directus.listQueueByStatus('rate_limited', { limit: batchSize, nowIso });
      source = 'rate_limited';
    }

    if (!rows || rows.length === 0) {
      await sleep(idleWaitMs);
      continue;
    }

    for (const row of rows) {
      const outcome = await processOneRow(row, { limiter });
      if (outcome?.status === 'rate_limited' && outcome?.backoffSeconds) {
        globalBackoffUntil = Date.now() + clamp(outcome.backoffSeconds, 1, 3600) * 1000;
      }
      if (loopWaitMs > 0) await sleep(loopWaitMs);
    }

    if (source === 'rate_limited' && rows.length < batchSize) {
      await sleep(250);
    }
  }
}

module.exports = { startProcessor };


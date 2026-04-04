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

function getLogLevel() {
  const raw = String(process.env.PROCESSOR_LOG_LEVEL ?? 'info').trim().toLowerCase();
  const levels = { error: 0, warn: 1, info: 2, debug: 3 };
  if (raw in levels) return levels[raw];
  const n = Number.parseInt(raw, 10);
  return Number.isFinite(n) ? clamp(n, 0, 3) : levels.info;
}

function logAt(currentLevel, level, message, meta) {
  const levels = { error: 0, warn: 1, info: 2, debug: 3 };
  const rank = level in levels ? levels[level] : levels.info;
  if (rank > currentLevel) return;
  const base = `[${new Date().toISOString()}] ${String(level).toUpperCase()} ${message}`;
  const extra = meta && typeof meta === 'object' ? ` ${JSON.stringify(meta)}` : '';
  const line = `${base}${extra}`;
  if (rank <= 0) console.error(line);
  else if (rank === 1) console.warn(line);
  else console.log(line);
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

function maybePadMotorcyclePlate(plate) {
  const p = String(plate || '').trim().toUpperCase();
  if (!/^[A-Z]{4}\d{2}$/.test(p)) return null;
  return `${p.slice(0, 4)}0${p.slice(4)}`;
}

class CallSpacer {
  constructor({ callsPerMinute }) {
    const safe = Number.isFinite(callsPerMinute) ? callsPerMinute : 25;
    this.spacingMs = Math.ceil(60000 / clamp(safe, 1, 60000));
    this.nextAllowedAt = 0;
    this.chain = Promise.resolve();
  }

  async waitTurn() {
    const run = async () => {
      const now = Date.now();
      const waitMs = Math.max(0, this.nextAllowedAt - now);
      if (waitMs > 0) await sleep(waitMs);
      this.nextAllowedAt = Math.max(this.nextAllowedAt, Date.now()) + this.spacingMs;
    };
    this.chain = this.chain.then(run, run);
    return this.chain;
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

async function getApiGetPlateInfo(plate, { limiter, waitGlobalBackoff } = {}) {
  const { apiKey, baseUrl } = getGetApiConfig();
  if (typeof waitGlobalBackoff === 'function') await waitGlobalBackoff();
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

async function getApiGetAppraisal(plate, { limiter, waitGlobalBackoff } = {}) {
  const { apiKey, baseUrl } = getGetApiConfig();
  if (typeof waitGlobalBackoff === 'function') await waitGlobalBackoff();
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

async function processOneRow(row, { limiter, schema, waitGlobalBackoff } = {}) {
  const resolvedSchema = schema || await directus.resolveGetApiSchema();
  const schemaRef = resolvedSchema;
  const id = row?.[schemaRef.idField];
  const detectionId = schemaRef.detectionIdField ? row?.[schemaRef.detectionIdField] : null;
  const attempts = schemaRef.attemptsField ? Number(row?.[schemaRef.attemptsField] ?? 0) : 0;
  const rawPlate = schemaRef.plateField ? row?.[schemaRef.plateField] : null;
  const originalPlate = normalizePlate(rawPlate);
  let plate = originalPlate;
  const nowIso = new Date().toISOString();

  if (!id) return { ok: false, reason: 'missing_id' };

  if (!isValidPlate(plate)) {
    const patch = {};
    if (schemaRef.statusField) patch[schemaRef.statusField] = 'invalid_plate';
    if (schemaRef.attemptsField) patch[schemaRef.attemptsField] = attempts + 1;
    if (schemaRef.reasonField) patch[schemaRef.reasonField] = 'invalid_plate';
    if (schemaRef.messageField) patch[schemaRef.messageField] = 'Patente inválida';
    if (schemaRef.fetchedAtField) patch[schemaRef.fetchedAtField] = nowIso;
    if (schemaRef.lastErrorAtField) patch[schemaRef.lastErrorAtField] = nowIso;
    if (schemaRef.nextRetryAtField) patch[schemaRef.nextRetryAtField] = null;
    await directus.patchRowById(id, patch);
    return { ok: true, status: 'invalid_plate', detectionId, plate };
  }

  let plateRes = await getApiGetPlateInfo(plate, { limiter, waitGlobalBackoff });
  if (plateRes.status === 404) {
    const padded = maybePadMotorcyclePlate(plate);
    if (padded && padded !== plate) {
      const alt = await getApiGetPlateInfo(padded, { limiter, waitGlobalBackoff });
      if (alt.ok) {
        plate = padded;
        plateRes = alt;
      } else if (alt.status === 429) {
        plate = padded;
        plateRes = alt;
      }
    }
  }
  if (plateRes.status === 429) {
    const retryAfter = extractRetryAfterSeconds(plateRes.res);
    const backoffSeconds = retryAfter ?? computeBackoffSeconds({ attempts, baseSeconds: 30, maxSeconds: 3600 });
    const nextRetryAt = new Date(Date.now() + backoffSeconds * 1000).toISOString();
    const patch = {};
    if (schemaRef.statusField) patch[schemaRef.statusField] = 'rate_limited';
    if (schemaRef.attemptsField) patch[schemaRef.attemptsField] = attempts + 1;
    if (schemaRef.upstreamStatusField) patch[schemaRef.upstreamStatusField] = 429;
    if (schemaRef.reasonField) patch[schemaRef.reasonField] = 'rate_limited';
    if (schemaRef.messageField) patch[schemaRef.messageField] = 'Rate limited en /v1/vehicles/plate';
    if (schemaRef.fetchedAtField) patch[schemaRef.fetchedAtField] = nowIso;
    if (schemaRef.lastErrorAtField) patch[schemaRef.lastErrorAtField] = nowIso;
    if (schemaRef.nextRetryAtField) patch[schemaRef.nextRetryAtField] = nextRetryAt;
    if (schemaRef.getapiField) {
      patch[schemaRef.getapiField] = {
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
    if (schemaRef.statusField) patch[schemaRef.statusField] = 'not_found';
    if (schemaRef.attemptsField) patch[schemaRef.attemptsField] = attempts + 1;
    if (schemaRef.upstreamStatusField) patch[schemaRef.upstreamStatusField] = 404;
    if (schemaRef.reasonField) patch[schemaRef.reasonField] = 'not_found';
    if (schemaRef.messageField) {
      patch[schemaRef.messageField] = plate === originalPlate
        ? 'No encontrado en /v1/vehicles/plate'
        : 'No encontrado en /v1/vehicles/plate (incluyendo variante con 0 para motos)';
    }
    if (schemaRef.fetchedAtField) patch[schemaRef.fetchedAtField] = nowIso;
    if (schemaRef.lastErrorAtField) patch[schemaRef.lastErrorAtField] = nowIso;
    if (schemaRef.nextRetryAtField) patch[schemaRef.nextRetryAtField] = null;
    if (schemaRef.getapiField) {
      patch[schemaRef.getapiField] = {
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
    const nextRetryAt = schemaRef.nextRetryAtField ? new Date(Date.now() + backoffSeconds * 1000).toISOString() : null;
    const patch = {};
    if (schemaRef.statusField) patch[schemaRef.statusField] = 'error';
    if (schemaRef.attemptsField) patch[schemaRef.attemptsField] = attempts + 1;
    if (schemaRef.upstreamStatusField) patch[schemaRef.upstreamStatusField] = plateRes.status;
    if (schemaRef.reasonField) patch[schemaRef.reasonField] = 'upstream_error';
    if (schemaRef.messageField) patch[schemaRef.messageField] = `Error upstream en /v1/vehicles/plate: HTTP ${plateRes.status}`;
    if (schemaRef.fetchedAtField) patch[schemaRef.fetchedAtField] = nowIso;
    if (schemaRef.lastErrorAtField) patch[schemaRef.lastErrorAtField] = nowIso;
    if (schemaRef.nextRetryAtField) patch[schemaRef.nextRetryAtField] = nextRetryAt;
    if (schemaRef.getapiField) {
      patch[schemaRef.getapiField] = {
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

  const appraisalRes = await getApiGetAppraisal(plate, { limiter, waitGlobalBackoff });
  if (appraisalRes.status === 429) {
    const retryAfter = extractRetryAfterSeconds(appraisalRes.res);
    const backoffSeconds = retryAfter ?? computeBackoffSeconds({ attempts, baseSeconds: 30, maxSeconds: 3600 });
    const nextRetryAt = new Date(Date.now() + backoffSeconds * 1000).toISOString();
    const patch = {};
    if (schemaRef.statusField) patch[schemaRef.statusField] = 'rate_limited';
    if (schemaRef.attemptsField) patch[schemaRef.attemptsField] = attempts + 1;
    if (schemaRef.upstreamStatusField) patch[schemaRef.upstreamStatusField] = 429;
    if (schemaRef.reasonField) patch[schemaRef.reasonField] = 'rate_limited';
    if (schemaRef.messageField) patch[schemaRef.messageField] = 'Rate limited en /v1/vehicles/appraisal';
    if (schemaRef.fetchedAtField) patch[schemaRef.fetchedAtField] = nowIso;
    if (schemaRef.lastErrorAtField) patch[schemaRef.lastErrorAtField] = nowIso;
    if (schemaRef.nextRetryAtField) patch[schemaRef.nextRetryAtField] = nextRetryAt;
    if (schemaRef.getapiField) {
      patch[schemaRef.getapiField] = {
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
    if (schemaRef.statusField) patch[schemaRef.statusField] = 'not_found';
    if (schemaRef.attemptsField) patch[schemaRef.attemptsField] = attempts + 1;
    if (schemaRef.upstreamStatusField) patch[schemaRef.upstreamStatusField] = 404;
    if (schemaRef.reasonField) patch[schemaRef.reasonField] = 'not_found';
    if (schemaRef.messageField) patch[schemaRef.messageField] = 'No encontrado en /v1/vehicles/appraisal';
    if (schemaRef.fetchedAtField) patch[schemaRef.fetchedAtField] = nowIso;
    if (schemaRef.lastErrorAtField) patch[schemaRef.lastErrorAtField] = nowIso;
    if (schemaRef.nextRetryAtField) patch[schemaRef.nextRetryAtField] = null;
    if (schemaRef.getapiField) {
      patch[schemaRef.getapiField] = {
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
    const nextRetryAt = schemaRef.nextRetryAtField ? new Date(Date.now() + backoffSeconds * 1000).toISOString() : null;
    const patch = {};
    if (schemaRef.statusField) patch[schemaRef.statusField] = 'error';
    if (schemaRef.attemptsField) patch[schemaRef.attemptsField] = attempts + 1;
    if (schemaRef.upstreamStatusField) patch[schemaRef.upstreamStatusField] = appraisalRes.status;
    if (schemaRef.reasonField) patch[schemaRef.reasonField] = 'upstream_error';
    if (schemaRef.messageField) patch[schemaRef.messageField] = `Error upstream en /v1/vehicles/appraisal: HTTP ${appraisalRes.status}`;
    if (schemaRef.fetchedAtField) patch[schemaRef.fetchedAtField] = nowIso;
    if (schemaRef.lastErrorAtField) patch[schemaRef.lastErrorAtField] = nowIso;
    if (schemaRef.nextRetryAtField) patch[schemaRef.nextRetryAtField] = nextRetryAt;
    if (schemaRef.getapiField) {
      patch[schemaRef.getapiField] = {
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
  if (schemaRef.statusField) patch[schemaRef.statusField] = 'ok';
  if (schemaRef.attemptsField) patch[schemaRef.attemptsField] = attempts + 1;
  if (schemaRef.upstreamStatusField) patch[schemaRef.upstreamStatusField] = 200;
  if (schemaRef.reasonField) patch[schemaRef.reasonField] = null;
  if (schemaRef.messageField) patch[schemaRef.messageField] = null;
  if (schemaRef.fetchedAtField) patch[schemaRef.fetchedAtField] = nowIso;
  if (schemaRef.nextRetryAtField) patch[schemaRef.nextRetryAtField] = null;
  if (schemaRef.getapiField) patch[schemaRef.getapiField] = resultPayload;
  await directus.patchRowById(id, patch);

  return { ok: true, status: 'ok', detectionId, plate };
}

async function startProcessor() {
  const { baseUrl, collection } = directus.getDirectusConfig();
  if (!baseUrl) throw new Error('DIRECTUS_URL no está configurado');
  if (!collection) throw new Error('DIRECTUS_GETAPI_COLLECTION no está configurado');

  const schema = await directus.resolveGetApiSchema();

  const logLevel = getLogLevel();
  const callsPerMinute = parseEnvInt('GETAPI_MAX_CALLS_PER_MIN', 25);
  const limiter = new CallSpacer({ callsPerMinute });
  const batchSize = parseEnvInt('PROCESSOR_BATCH_SIZE', 10);
  const idleWaitMs = parseEnvInt('PROCESSOR_IDLE_WAIT_MS', 10000);
  const loopWaitMs = parseEnvInt('PROCESSOR_LOOP_WAIT_MS', 0);
  const heartbeatMs = parseEnvInt('PROCESSOR_HEARTBEAT_MS', 30000);
  const idleLogMs = parseEnvInt('PROCESSOR_IDLE_LOG_MS', 60000);
  const concurrency = clamp(parseEnvInt('PROCESSOR_CONCURRENCY', 4), 1, 50);
  const lockMs = clamp(parseEnvInt('PROCESSOR_LOCK_MS', 120000), 5000, 3600000);
  const claimEnabledRaw = String(process.env.PROCESSOR_CLAIM_LOCK ?? 'true').trim().toLowerCase();
  const claimEnabled = !(claimEnabledRaw === '0' || claimEnabledRaw === 'false' || claimEnabledRaw === 'no' || claimEnabledRaw === 'off');
  const queueMax = clamp(parseEnvInt('PROCESSOR_QUEUE_MAX', batchSize * concurrency * 4), batchSize, 2000);
  const refillMin = clamp(parseEnvInt('PROCESSOR_QUEUE_REFILL_MIN', batchSize * concurrency), 1, queueMax);
  const rateWindowMs = clamp(parseEnvInt('PROCESSOR_RATE_WINDOW_MS', 60000), 5000, 3600000);

  logAt(logLevel, 'info', 'Procesador GetAPI iniciado', {
    collection,
    directus: baseUrl,
    calls_per_min: callsPerMinute,
    batch_size: batchSize,
    idle_wait_ms: idleWaitMs,
    loop_wait_ms: loopWaitMs,
    concurrency,
    claim_lock: claimEnabled && Boolean(schema.nextRetryAtField),
    lock_ms: lockMs,
    queue_max: queueMax
  });

  let globalBackoffUntil = 0;
  let lastHeartbeatAt = 0;
  let lastIdleLogAt = 0;
  let lastBackoffLoggedUntil = 0;
  const counters = { processed: 0, ok: 0, rate_limited: 0, error: 0, not_found: 0, invalid_plate: 0 };
  let rateLastAt = Date.now();
  let rateLastCounters = { ...counters };

  const queue = [];
  const queuedIds = new Set();
  const inFlightIds = new Set();
  let lastRefillAt = 0;
  let lastSource = null;

  const waitGlobalBackoff = async () => {
    while (globalBackoffUntil > Date.now()) {
      const now = Date.now();
      if (now >= lastBackoffLoggedUntil) {
        lastBackoffLoggedUntil = now + Math.min(30000, globalBackoffUntil - now);
        logAt(logLevel, 'warn', 'Backoff global activo por rate limit', {
          until: new Date(globalBackoffUntil).toISOString(),
          remaining_ms: globalBackoffUntil - now
        });
      }
      await sleep(Math.min(1000, globalBackoffUntil - now));
    }
  };

  const enqueueRows = (rows, source) => {
    if (!Array.isArray(rows) || rows.length === 0) return 0;
    let added = 0;
    for (const row of rows) {
      const id = row?.[schema.idField];
      if (id == null) continue;
      const key = String(id);
      if (queuedIds.has(key) || inFlightIds.has(key)) continue;
      if (queue.length >= queueMax) break;
      queue.push({ row, source });
      queuedIds.add(key);
      added += 1;
      lastSource = source;
    }
    return added;
  };

  const refillQueue = async () => {
    if (queue.length >= queueMax) return;
    const now = Date.now();
    if (now - lastRefillAt < 200) return;
    lastRefillAt = now;
    const need = Math.min(queueMax - queue.length, Math.max(refillMin, batchSize));
    const nowIso = new Date().toISOString();
    try {
      let rows = await directus.listQueueByStatus('pending', { limit: need, nowIso });
      let added = enqueueRows(rows, 'pending');
      if (added === 0) {
        rows = await directus.listQueueByStatus('rate_limited', { limit: need, nowIso });
        added = enqueueRows(rows, 'rate_limited');
      }
        if (added === 0) {
          rows = await directus.listErrorRateLimitQueue({ limit: need, nowIso });
          added = enqueueRows(rows, 'error_rate_limited');
        }
      if (added > 0) logAt(logLevel, 'info', 'Cola recargada', { added, queue: queue.length, source: lastSource });
    } catch (e) {
      logAt(logLevel, 'error', 'Error recargando cola desde Directus', { message: e?.message || String(e) });
      await sleep(5000);
    }
  };

  const takeNext = async () => {
    while (true) {
      await refillQueue();
      const item = queue.shift();
      if (item) {
        const id = item.row?.[schema.idField];
        const key = String(id);
        queuedIds.delete(key);
        if (inFlightIds.has(key)) continue;
        inFlightIds.add(key);
        return item;
      }
      const now = Date.now();
      if (idleLogMs > 0 && now - lastIdleLogAt >= idleLogMs) {
        lastIdleLogAt = now;
        logAt(logLevel, 'info', 'Sin pendientes (idle)', { idle_wait_ms: idleWaitMs });
      }
      await sleep(idleWaitMs);
    }
  };

  const workerLoop = async (workerId) => {
    while (true) {
      const now = Date.now();
      if (heartbeatMs > 0 && now - lastHeartbeatAt >= heartbeatMs) {
        const elapsedMs = Math.max(1, now - rateLastAt);
        const dt = Math.max(1, elapsedMs / 1000);
        const current = { ...counters };
        const delta = {
          processed: current.processed - (rateLastCounters.processed || 0),
          ok: current.ok - (rateLastCounters.ok || 0),
          not_found: current.not_found - (rateLastCounters.not_found || 0),
          error: current.error - (rateLastCounters.error || 0),
          rate_limited: current.rate_limited - (rateLastCounters.rate_limited || 0),
          invalid_plate: current.invalid_plate - (rateLastCounters.invalid_plate || 0)
        };
        const perMin = (n) => Math.round((Number(n || 0) * 60 * 100) / dt) / 100;
        const processedPerMin = perMin(delta.processed);
        const callsPerMinMin = perMin((delta.ok * 2) + Math.max(0, delta.processed - delta.ok));
        const callsPerMinMax = perMin(delta.processed * 2);
        const efficiencyMin = callsPerMinute > 0 ? Math.round((callsPerMinMin / callsPerMinute) * 1000) / 10 : null;
        const efficiencyMax = callsPerMinute > 0 ? Math.round((callsPerMinMax / callsPerMinute) * 1000) / 10 : null;

        lastHeartbeatAt = now;
        logAt(logLevel, 'info', 'Heartbeat', { ...counters, queue: queue.length, inflight: inFlightIds.size });
        logAt(logLevel, 'info', 'Resumen', {
          window_s: Math.round(dt),
          per_min: {
            processed: processedPerMin,
            ok: perMin(delta.ok),
            not_found: perMin(delta.not_found),
            error: perMin(delta.error),
            rate_limited: perMin(delta.rate_limited),
            invalid_plate: perMin(delta.invalid_plate)
          },
          calls_per_min_est: { min: callsPerMinMin, max: callsPerMinMax },
          efficiency_pct_est: { min: efficiencyMin, max: efficiencyMax }
        });

        if (elapsedMs >= rateWindowMs) {
          rateLastAt = now;
          rateLastCounters = current;
        }
      }

      await waitGlobalBackoff();
      const { row, source } = await takeNext();
      const id = row?.[schema.idField];
      const key = String(id);
      const rawPlate = schema.plateField ? row?.[schema.plateField] : null;
      const plate = normalizePlate(rawPlate);
      const attempts = schema.attemptsField ? Number(row?.[schema.attemptsField] ?? 0) : 0;

      if (claimEnabled && schema.nextRetryAtField && (source === 'pending' || source === 'error_rate_limited')) {
        const lockUntilIso = new Date(Date.now() + lockMs).toISOString();
        try {
          await directus.claimLockById(id, lockUntilIso);
        } catch (e) {
          logAt(logLevel, 'warn', 'No se pudo aplicar claim lock', { id, plate, message: e?.message || String(e) });
        }
      }

      logAt(logLevel, 'debug', 'Procesando registro', { worker: workerId, id, plate, attempts, source, queue: queue.length });

      let outcome = null;
      try {
        outcome = await processOneRow(row, { limiter, schema, waitGlobalBackoff });
      } catch (e) {
        const message = e?.message || String(e);
        const backoffSeconds = computeBackoffSeconds({ attempts, baseSeconds: 60, maxSeconds: 3600 });
        const nextRetryAt = new Date(Date.now() + backoffSeconds * 1000).toISOString();
        const nowIso2 = new Date().toISOString();
        const patch = {};
        if (schema.statusField) patch[schema.statusField] = 'error';
        if (schema.attemptsField) patch[schema.attemptsField] = attempts + 1;
        if (schema.reasonField) patch[schema.reasonField] = 'processor_exception';
        if (schema.messageField) patch[schema.messageField] = message;
        if (schema.lastErrorAtField) patch[schema.lastErrorAtField] = nowIso2;
        if (schema.fetchedAtField) patch[schema.fetchedAtField] = nowIso2;
        if (schema.nextRetryAtField) patch[schema.nextRetryAtField] = nextRetryAt;
        if (id) {
          try {
            await directus.patchRowById(id, patch);
          } catch (e2) {
            logAt(logLevel, 'error', 'No se pudo guardar error en Directus', { id, message: e2?.message || String(e2) });
          }
        }
        outcome = { ok: false, status: 'error', plate, id, nextRetryAt, error: message };
        logAt(logLevel, 'error', 'Excepción procesando registro', { worker: workerId, id, plate, message });
      } finally {
        inFlightIds.delete(key);
      }

      counters.processed += 1;
      if (outcome?.status && outcome.status in counters) counters[outcome.status] += 1;

      logAt(logLevel, 'info', 'Resultado', {
        worker: workerId,
        id,
        plate,
        status: outcome?.status || null,
        next_retry_at: outcome?.nextRetryAt || null
      });

      if (outcome?.status === 'rate_limited' && outcome?.backoffSeconds) {
        globalBackoffUntil = Date.now() + clamp(outcome.backoffSeconds, 1, 3600) * 1000;
      }
      if (loopWaitMs > 0) await sleep(loopWaitMs);
    }
  };

  for (let i = 1; i <= concurrency; i += 1) {
    workerLoop(i).catch((e) => {
      logAt(logLevel, 'error', 'Worker terminó por error', { worker: i, message: e?.message || String(e) });
    });
    await sleep(10);
  }

  while (true) await sleep(60000);
}

module.exports = { startProcessor };

  function normalizeBaseUrl(value) {
    if (typeof value !== 'string') return null;
    let url = value.trim();
    if (!url) return null;
    url = url.replace(/\/+$/, '');
    url = url.replace(/\/collections$/, '');
    url = url.replace(/\/items$/, '');
    url = url.replace(/\/+$/, '');
    return url;
  }

  function buildQueryString(params) {
    const search = new URLSearchParams();
    for (const [k, v] of Object.entries(params || {})) {
      if (v === undefined || v === null || v === '') continue;
      search.set(k, String(v));
    }
    const text = search.toString();
    return text ? `?${text}` : '';
  }

  async function readResponseBody(res) {
    const contentType = res.headers.get('content-type') || '';
    if (contentType.includes('application/json')) {
      try {
        return await res.json();
      } catch {
        return null;
      }
    }
    try {
      return await res.text();
    } catch {
      return null;
    }
  }

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async function fetchWithTimeout(url, init, timeoutMs) {
    const ms = Number.isFinite(timeoutMs) ? timeoutMs : 20000;
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), ms);
    try {
      return await fetch(url, { ...(init || {}), signal: controller.signal });
    } finally {
      clearTimeout(id);
    }
  }

  function getDirectusConfig() {
    const baseUrl =
      normalizeBaseUrl(process.env.DIRECTUS_URL) ||
      normalizeBaseUrl(process.env.DIRECTUSURL) ||
      normalizeBaseUrl(process.env.DIRECTUS_BASE_URL) ||
      null;
    const token =
      (typeof process.env.DIRECTUS_TOKEN === 'string' ? process.env.DIRECTUS_TOKEN.trim() : '') ||
      (typeof process.env.TOKENDIRECTUS === 'string' ? process.env.TOKENDIRECTUS.trim() : '') ||
      null;
    const collection =
      (typeof process.env.DIRECTUS_GETAPI_COLLECTION === 'string' ? process.env.DIRECTUS_GETAPI_COLLECTION.trim() : '') ||
      (typeof process.env.DIRECTUS_COLLECTION_GETAPI === 'string' ? process.env.DIRECTUS_COLLECTION_GETAPI.trim() : '') ||
      'vehicle_detection_getapi2';
    return { baseUrl, token, collection };
  }

  function getDetectionsCollection() {
    const v = String(process.env.DIRECTUS_DETECTIONS_COLLECTION || process.env.DIRECTUS_COLLECTION || 'vehicle_detections').trim();
    return v || 'vehicle_detections';
  }

  async function directusRequest(method, path, { query, body, headers } = {}) {
    const { baseUrl, token } = getDirectusConfig();
    if (!baseUrl) throw new Error('Falta DIRECTUS_URL');
    const url = `${baseUrl}${path}${buildQueryString(query)}`;

    const reqHeaders = { Accept: 'application/json', ...(headers || {}) };
    if (token) reqHeaders.Authorization = `Bearer ${token}`;

    const init = { method, headers: reqHeaders };
    if (body !== undefined) init.body = body;

    const timeoutMs = Number.parseInt(process.env.DIRECTUS_TIMEOUT_MS ?? '20000', 10) || 20000;
    const maxRetries = Number.parseInt(process.env.DIRECTUS_MAX_RETRIES ?? '3', 10) || 3;
    const retryableStatus = new Set([429, 502, 503, 504]);

    let lastError = null;
    for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
      try {
        const res = await fetchWithTimeout(url, init, timeoutMs);
        const payload = await readResponseBody(res);

        if (!res.ok) {
          const message =
            (payload && typeof payload === 'object' && Array.isArray(payload.errors) && payload.errors[0]?.message) ||
            (payload && typeof payload === 'object' && payload.error) ||
            (typeof payload === 'string' && payload) ||
            `HTTP ${res.status}`;

          const err = new Error(message);
          err.status = res.status;
          err.url = url;
          err.method = method;
          err.payload = payload;

          if (retryableStatus.has(res.status) && attempt < maxRetries) {
            lastError = err;
            await sleep(250 * (attempt + 1));
            continue;
          }
          throw err;
        }

        if (payload && typeof payload === 'object' && Array.isArray(payload.errors) && payload.errors.length > 0) {
          const err = new Error(payload.errors[0]?.message || 'Error de Directus');
          err.status = res.status;
          err.url = url;
          err.method = method;
          err.payload = payload;
          throw err;
        }

        return payload;
      } catch (e) {
        const retryable = (e && (e.name === 'AbortError' || e.code === 'ECONNRESET' || e.code === 'ETIMEDOUT')) || false;
        if ((retryable || !('status' in (e || {}))) && attempt < maxRetries) {
          lastError = e;
          await sleep(250 * (attempt + 1));
          continue;
        }
        throw e;
      }
    }

    throw lastError || new Error('Error de Directus');
  }

  let schemaCache = null;

  async function resolveGetApiSchema() {
    if (schemaCache) return schemaCache;
    const { collection } = getDirectusConfig();
    if (!collection) {
      schemaCache = { ok: false };
      return schemaCache;
    }

    try {
      const payload = await directusRequest('GET', `/fields/${encodeURIComponent(collection)}`);
      const fields = Array.isArray(payload?.data) ? payload.data : [];
      const names = fields.map((f) => String(f?.field || '').trim()).filter(Boolean);
      const set = new Set(names);
      const has = (name) => set.has(name) ? name : null;

      const schema = {
        ok: true,
        collection,
        idField: 'id',
        detectionIdField: has('detection_id'),
        plateField: has('license_plate') || has('plate'),
        statusField: has('status'),
        attemptsField: has('attempts'),
        nextRetryAtField: has('next_retry_at'),
        fetchedAtField: has('fetched_at'),
        upstreamStatusField: has('upstream_status'),
        reasonField: has('reason'),
        messageField: has('message'),
        getapiField: has('getapi'),
        lastErrorAtField: has('last_error_at'),
        createdAtField: has('created_at') || has('date_created'),
        updatedAtField: has('updated_at') || has('date_updated')
      };
      schemaCache = schema;
      return schema;
    } catch {
      schemaCache = {
        ok: true,
        collection,
        idField: 'id',
        detectionIdField: 'detection_id',
        plateField: 'license_plate',
        statusField: 'status',
        attemptsField: 'attempts',
        nextRetryAtField: 'next_retry_at',
        fetchedAtField: 'fetched_at',
        upstreamStatusField: 'upstream_status',
        reasonField: 'reason',
        messageField: 'message',
        getapiField: 'getapi',
        lastErrorAtField: 'last_error_at',
        createdAtField: 'created_at',
        updatedAtField: 'updated_at'
      };
      return schemaCache;
    }
  }

  async function listQueueByStatus(status, { limit, nowIso } = {}) {
    const schema = await resolveGetApiSchema();
    if (!schema.ok) return [];
    if (!schema.statusField) throw new Error(`La colección ${schema.collection} no tiene campo status`);
    const safeLimit = Math.min(100, Math.max(1, Number.parseInt(limit ?? '25', 10) || 25));

    const sort = schema.createdAtField ? `${schema.createdAtField},id` : 'id';
    const now = typeof nowIso === 'string' && nowIso ? nowIso : new Date().toISOString();
    const query = {
      limit: safeLimit,
      sort,
      fields: [
        schema.idField,
        schema.detectionIdField,
        schema.plateField,
        schema.statusField,
        schema.attemptsField,
        schema.nextRetryAtField
      ].filter(Boolean).join(','),
      [`filter[${schema.statusField}][_eq]`]: status
    };

    if (status === 'pending' && schema.nextRetryAtField) {
      query[`filter[_or][0][${schema.nextRetryAtField}][_null]`] = 'true';
      query[`filter[_or][1][${schema.nextRetryAtField}][_lte]`] = now;
    }

    if (status === 'rate_limited' && schema.nextRetryAtField) {
      query[`filter[${schema.nextRetryAtField}][_lte]`] = now;
    }

    if (status === 'error' && schema.nextRetryAtField) {
      query[`filter[_or][0][${schema.nextRetryAtField}][_null]`] = 'true';
      query[`filter[_or][1][${schema.nextRetryAtField}][_lte]`] = now;
    }

    if (status === 'invalid_plate' && schema.nextRetryAtField) {
      query[`filter[_or][0][${schema.nextRetryAtField}][_null]`] = 'true';
      query[`filter[_or][1][${schema.nextRetryAtField}][_lte]`] = now;
    }

    const payload = await directusRequest('GET', `/items/${encodeURIComponent(schema.collection)}`, { query });
    return Array.isArray(payload?.data) ? payload.data : [];
  }

  async function listErrorRateLimitQueue({ limit, nowIso } = {}) {
    const schema = await resolveGetApiSchema();
    if (!schema.ok) return [];
    if (!schema.statusField) throw new Error(`La colección ${schema.collection} no tiene campo status`);
    if (!schema.messageField) return [];
    const safeLimit = Math.min(100, Math.max(1, Number.parseInt(limit ?? '25', 10) || 25));
    const sort = schema.createdAtField ? `${schema.createdAtField},id` : 'id';
    const now = typeof nowIso === 'string' && nowIso ? nowIso : new Date().toISOString();

    const query = {
      limit: safeLimit,
      sort,
      fields: [
        schema.idField,
        schema.detectionIdField,
        schema.plateField,
        schema.statusField,
        schema.attemptsField,
        schema.nextRetryAtField
      ].filter(Boolean).join(','),
      [`filter[${schema.statusField}][_eq]`]: 'error',
      [`filter[_or][0][${schema.messageField}][_icontains]`]: 'rate limit exceeded',
      [`filter[_or][1][${schema.messageField}][_icontains]`]: 'rate limited',
      [`filter[_or][2][${schema.messageField}][_icontains]`]: 'try again'
    };

    if (schema.nextRetryAtField) {
      query[`filter[_and][0][_or][0][${schema.nextRetryAtField}][_null]`] = 'true';
      query[`filter[_and][0][_or][1][${schema.nextRetryAtField}][_lte]`] = now;
    }

    const payload = await directusRequest('GET', `/items/${encodeURIComponent(schema.collection)}`, { query });
    return Array.isArray(payload?.data) ? payload.data : [];
  }

  async function listErrorAbortedQueue({ limit, nowIso } = {}) {
    const schema = await resolveGetApiSchema();
    if (!schema.ok) return [];
    if (!schema.statusField) throw new Error(`La colección ${schema.collection} no tiene campo status`);
    if (!schema.messageField) return [];
    const safeLimit = Math.min(100, Math.max(1, Number.parseInt(limit ?? '25', 10) || 25));
    const sort = schema.createdAtField ? `${schema.createdAtField},id` : 'id';
    const now = typeof nowIso === 'string' && nowIso ? nowIso : new Date().toISOString();

    const query = {
      limit: safeLimit,
      sort,
      fields: [
        schema.idField,
        schema.detectionIdField,
        schema.plateField,
        schema.statusField,
        schema.attemptsField,
        schema.nextRetryAtField
      ].filter(Boolean).join(','),
      [`filter[${schema.statusField}][_eq]`]: 'error',
      [`filter[${schema.messageField}][_icontains]`]: 'aborted'
    };

    if (schema.nextRetryAtField) {
      query[`filter[_and][0][_or][0][${schema.nextRetryAtField}][_null]`] = 'true';
      query[`filter[_and][0][_or][1][${schema.nextRetryAtField}][_lte]`] = now;
    }

    const payload = await directusRequest('GET', `/items/${encodeURIComponent(schema.collection)}`, { query });
    return Array.isArray(payload?.data) ? payload.data : [];
  }

  async function listErrorInvalidPlateQueue({ limit, nowIso } = {}) {
    const schema = await resolveGetApiSchema();
    if (!schema.ok) return [];
    if (!schema.statusField) throw new Error(`La colección ${schema.collection} no tiene campo status`);
    if (!schema.messageField) return [];
    const safeLimit = Math.min(100, Math.max(1, Number.parseInt(limit ?? '25', 10) || 25));
    const sort = schema.createdAtField ? `${schema.createdAtField},id` : 'id';
    const now = typeof nowIso === 'string' && nowIso ? nowIso : new Date().toISOString();

    const query = {
      limit: safeLimit,
      sort,
      fields: [
        schema.idField,
        schema.detectionIdField,
        schema.plateField,
        schema.statusField,
        schema.attemptsField,
        schema.nextRetryAtField
      ].filter(Boolean).join(','),
      [`filter[${schema.statusField}][_eq]`]: 'error',
      [`filter[_or][0][${schema.messageField}][_icontains]`]: 'http 422',
      [`filter[_or][1][${schema.messageField}][_icontains]`]: 'formato de patente',
      [`filter[_or][2][${schema.messageField}][_icontains]`]: 'patente inválida',
      [`filter[_or][3][${schema.messageField}][_icontains]`]: 'patente valida',
      [`filter[_or][4][${schema.messageField}][_icontains]`]: 'patente válida'
    };

    if (schema.nextRetryAtField) {
      query[`filter[_and][0][_or][0][${schema.nextRetryAtField}][_null]`] = 'true';
      query[`filter[_and][0][_or][1][${schema.nextRetryAtField}][_lte]`] = now;
    }

    const payload = await directusRequest('GET', `/items/${encodeURIComponent(schema.collection)}`, { query });
    return Array.isArray(payload?.data) ? payload.data : [];
  }

  async function listNotFoundQueue({ limit, nowIso } = {}) {
    const schema = await resolveGetApiSchema();
    if (!schema.ok) return [];
    if (!schema.statusField) throw new Error(`La colección ${schema.collection} no tiene campo status`);
    const safeLimit = Math.min(100, Math.max(1, Number.parseInt(limit ?? '25', 10) || 25));
    const sort = schema.createdAtField ? `${schema.createdAtField},id` : 'id';
    const now = typeof nowIso === 'string' && nowIso ? nowIso : new Date().toISOString();
    const query = {
      limit: safeLimit,
      sort,
      fields: [
        schema.idField,
        schema.detectionIdField,
        schema.plateField,
        schema.statusField,
        schema.attemptsField,
        schema.nextRetryAtField
      ].filter(Boolean).join(','),
      [`filter[${schema.statusField}][_eq]`]: 'not_found'
    };
    if (schema.nextRetryAtField) {
      query[`filter[_or][0][${schema.nextRetryAtField}][_null]`] = 'true';
      query[`filter[_or][1][${schema.nextRetryAtField}][_lte]`] = now;
    }
    const payload = await directusRequest('GET', `/items/${encodeURIComponent(schema.collection)}`, { query });
    return Array.isArray(payload?.data) ? payload.data : [];
  }

  async function patchRowById(id, patch) {
    const schema = await resolveGetApiSchema();
    if (!schema.ok) return null;
    const payload = await directusRequest('PATCH', `/items/${encodeURIComponent(schema.collection)}/${encodeURIComponent(id)}`, {
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(patch || {})
    });
    return payload?.data || null;
  }

  async function getItemById(collection, id, { fields } = {}) {
    const safeCollection = String(collection || '').trim();
    const safeId = String(id || '').trim();
    if (!safeCollection || !safeId) return null;
    const query = {};
    if (fields) query.fields = String(fields);
    const payload = await directusRequest('GET', `/items/${encodeURIComponent(safeCollection)}/${encodeURIComponent(safeId)}`, { query });
    return payload?.data || null;
  }

  async function patchItemById(collection, id, patch) {
    const safeCollection = String(collection || '').trim();
    const safeId = String(id || '').trim();
    if (!safeCollection || !safeId) return null;
    const payload = await directusRequest('PATCH', `/items/${encodeURIComponent(safeCollection)}/${encodeURIComponent(safeId)}`, {
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(patch || {})
    });
    return payload?.data || null;
  }

  async function deleteItemById(collection, id) {
    const safeCollection = String(collection || '').trim();
    const safeId = String(id || '').trim();
    if (!safeCollection || !safeId) return null;
    await directusRequest('DELETE', `/items/${encodeURIComponent(safeCollection)}/${encodeURIComponent(safeId)}`);
    return true;
  }

  async function deleteFileById(id) {
    const safeId = String(id || '').trim();
    if (!safeId) return null;
    await directusRequest('DELETE', `/files/${encodeURIComponent(safeId)}`);
    return true;
  }

  async function claimLockById(id, lockUntilIso) {
    const schema = await resolveGetApiSchema();
    if (!schema.ok) return null;
    if (!schema.nextRetryAtField) return null;
    const until = typeof lockUntilIso === 'string' && lockUntilIso ? lockUntilIso : new Date(Date.now() + 120000).toISOString();
    const patch = {};
    patch[schema.nextRetryAtField] = until;
    return patchRowById(id, patch);
  }

  module.exports = {
    getDirectusConfig,
    getDetectionsCollection,
    directusRequest,
    resolveGetApiSchema,
    listQueueByStatus,
    listErrorRateLimitQueue,
    listErrorAbortedQueue,
    listErrorInvalidPlateQueue,
    listNotFoundQueue,
    getItemById,
    patchItemById,
    deleteItemById,
    deleteFileById,
    claimLockById,
    patchRowById
  };

#!/usr/bin/env node

import process from "node:process";

const config = {
  steelBaseUrl: String(process.env.PSDSTEEL_API_BASE_URL || "https://steelapi.mysteelsoftware.com")
    .trim()
    .replace(/\/+$/, ""),
  steelUsername: String(process.env.PSDSTEEL_API_USERNAME || "").trim(),
  steelPassword: String(process.env.PSDSTEEL_API_PASSWORD || ""),
  steelAppName: String(process.env.PSDSTEEL_API_APP_NAME || "API CALL").trim(),
  steelDbId: Number(process.env.PSDSTEEL_API_DBID || 19) || 19,
  steelAuthScheme: String(process.env.PSDSTEEL_API_AUTH_SCHEME || "Bearer").trim(),
  recordSetCode: String(process.env.PSDSTEEL_INVENTORY_RECORDSET || "api_custInv").trim(),
  startDate: String(process.env.PSDSTEEL_INVENTORY_START_DATE || "2025-06-01").trim(),
  pageLimit: Math.min(15000, Math.max(100, Number(process.env.PSDSTEEL_PAGE_LIMIT || 15000) || 15000)),
  maxPages: Math.min(100, Math.max(1, Number(process.env.PSDSTEEL_MAX_PAGES || 50) || 50)),
  batchSize: Math.min(1000, Math.max(50, Number(process.env.INVENTORY_SYNC_BATCH_SIZE || 500) || 500)),
  requestTimeoutMs: Math.max(10000, Number(process.env.PSDSTEEL_REQUEST_TIMEOUT_MS || 120000) || 120000),
  requestAttempts: Math.min(5, Math.max(1, Number(process.env.PSDSTEEL_REQUEST_ATTEMPTS || 3) || 3)),
  minimumActiveTags: Math.max(1, Number(process.env.INVENTORY_SYNC_MIN_ACTIVE_TAGS || 1000) || 1000),
  minimumPreviousRatio: Math.min(
    1,
    Math.max(0, Number(process.env.INVENTORY_SYNC_MIN_PREVIOUS_RATIO || 0.65) || 0.65)
  ),
  supabaseUrl: String(process.env.SUPABASE_URL || "").trim().replace(/\/+$/, ""),
  supabaseServiceKey: String(process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim(),
  dryRun: /^(1|true|yes)$/i.test(String(process.env.INVENTORY_SYNC_DRY_RUN || "false").trim())
};

const required = (name, value) => {
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
};

const sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

const parseJsonValue = (value) => {
  if (typeof value !== "string") return value;
  const trimmed = value.trim();
  if (!trimmed) return value;
  try {
    return JSON.parse(trimmed);
  } catch {
    return value;
  }
};

const findRecordArray = (payload, depth = 0) => {
  if (depth > 8) return null;
  const parsed = parseJsonValue(payload);
  if (Array.isArray(parsed)) return parsed;
  if (!parsed || typeof parsed !== "object") return null;
  for (const key of ["records", "recordset", "recordSet", "rows", "value", "data", "result", "response", "output", "body"]) {
    if (!(key in parsed)) continue;
    const found = findRecordArray(parsed[key], depth + 1);
    if (found) return found;
  }
  return null;
};

const findAuthToken = (payload, depth = 0) => {
  if (depth > 8) return "";
  const parsed = parseJsonValue(payload);
  if (typeof parsed === "string") return parsed.trim();
  if (!parsed || typeof parsed !== "object") return "";
  for (const key of ["access_token", "accessToken", "auth_token", "authToken", "sessionToken", "authorization", "auth", "token", "jwt"]) {
    if (typeof parsed[key] === "string" && parsed[key].trim()) return parsed[key].trim();
  }
  for (const key of ["data", "result", "response", "output", "body"]) {
    if (!(key in parsed)) continue;
    const token = findAuthToken(parsed[key], depth + 1);
    if (token) return token;
  }
  return "";
};

const requestJson = async (url, options = {}, label = "Request") => {
  let lastError;
  for (let attempt = 1; attempt <= config.requestAttempts; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), config.requestTimeoutMs);
    try {
      const response = await fetch(url, { ...options, signal: controller.signal });
      const text = await response.text();
      const body = parseJsonValue(text);
      if (!response.ok) {
        const detail = body && typeof body === "object"
          ? body.message || body.error_description || body.error || ""
          : "";
        const error = new Error(`${label} failed with HTTP ${response.status}${detail ? `: ${detail}` : ""}`);
        error.retryable = response.status === 408 || response.status === 429 || response.status >= 500;
        throw error;
      }
      return { body, headers: response.headers, status: response.status };
    } catch (error) {
      lastError = error;
      const retryable = error?.retryable !== false;
      if (!retryable || attempt === config.requestAttempts) throw error;
      await sleep(attempt * 2000);
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastError;
};

const authenticateSteelApi = async () => {
  const { body } = await requestJson(`${config.steelBaseUrl}/api/authenticate`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({
      username: config.steelUsername,
      password: config.steelPassword,
      type: "db",
      appname: config.steelAppName
    })
  }, "Steel API authentication");
  const token = findAuthToken(body).replace(/^Bearer\s+/i, "");
  if (!token) throw new Error("Steel API authentication returned no token.");
  return token;
};

const firstValue = (row, keys) => {
  for (const key of keys) {
    if (row?.[key] !== undefined && row?.[key] !== null) return row[key];
  }
  return null;
};

const normalizeStatus = (value) => String(value || "A").trim().toUpperCase() || "A";

const normalizeInventoryRow = (row) => {
  const scaleWeightValue = firstValue(row, ["scaleWeight", "scaleweight", "scale_weight_lbs"]);
  const suppliedTonsValue = firstValue(row, ["tons", "TONS"]);
  const scaleWeight = Number(scaleWeightValue);
  const suppliedTons = Number(suppliedTonsValue);
  const hasSuppliedTons = suppliedTonsValue !== null && String(suppliedTonsValue).trim() !== "";
  return {
    tag_number: String(firstValue(row, ["tagNumber", "tagnumber", "tag_number", "TAGNUMBER"]) || "").trim(),
    status: normalizeStatus(firstValue(row, ["status", "STATUS"])),
    vendor_number: String(firstValue(row, ["vendorNumber", "vendornumber", "vendor_number", "VENDORNUMBER"]) || "").trim().toUpperCase(),
    prod_class: String(firstValue(row, ["prodClass", "prodclass", "prod_class", "PRODUCT"]) || "").trim().toUpperCase(),
    tag_type: String(firstValue(row, ["tagType", "tagtype", "tag_type", "TAGTYPE"]) || "").trim().toUpperCase(),
    location: String(firstValue(row, ["location", "LOCATION"]) || "").trim().toUpperCase(),
    has_release: String(firstValue(row, ["hasRelease", "hasrelease", "has_release", "releaseNumber", "RELEASENUMBER"]) || "").trim(),
    length: Number(firstValue(row, ["length", "LENGTH"])) || 0,
    tons: hasSuppliedTons && Number.isFinite(suppliedTons)
      ? suppliedTons
      : (Number.isFinite(scaleWeight) ? scaleWeight : 0) / 2000,
    input_date: firstValue(row, ["inputdate", "inputDate", "input_date", "INPUTDATE"]) || null,
    raw_json: row
  };
};

const fetchCompleteSnapshot = async () => {
  const rowsByTag = new Map();
  let completed = false;

  for (let page = 0; page < config.maxPages; page += 1) {
    const offset = page * config.pageLimit;
    // PSDSteel tokens are short-lived, so obtain a new token for every data request.
    const token = await authenticateSteelApi();
    const { body } = await requestJson(`${config.steelBaseUrl}/api/execute`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        Authorization: `${config.steelAuthScheme} ${token}`.trim()
      },
      body: JSON.stringify({
        dbid: config.steelDbId,
        method: "GetRecordSet",
        params: {
          input: JSON.stringify({
            offset,
            limit: config.pageLimit,
            recordSetCode: config.recordSetCode,
            lastUpdatedDate: config.startDate
          })
        }
      })
    }, `Steel API inventory page ${page + 1}`);

    const pageRows = findRecordArray(body);
    if (!pageRows) throw new Error("Steel API response did not contain an inventory array.");
    for (const rawRow of pageRows) {
      const row = normalizeInventoryRow(rawRow);
      if (row.tag_number) rowsByTag.set(row.tag_number, row);
    }
    console.log(`Downloaded page ${page + 1}: ${pageRows.length.toLocaleString()} rows`);
    if (pageRows.length < config.pageLimit) {
      completed = true;
      break;
    }
  }

  if (!completed) throw new Error(`Inventory exceeded the ${config.maxPages}-page safety limit.`);
  if (!rowsByTag.size) throw new Error("Steel API returned an empty inventory snapshot.");
  return [...rowsByTag.values()];
};

const supabaseHeaders = (additional = {}) => ({
  apikey: config.supabaseServiceKey,
  Authorization: `Bearer ${config.supabaseServiceKey}`,
  ...additional
});

const getCurrentActiveCount = async () => {
  const { headers } = await requestJson(
    `${config.supabaseUrl}/rest/v1/psdata_cust_inv?status=eq.A&select=tag_number`,
    {
      headers: supabaseHeaders({ Prefer: "count=exact", Range: "0-0" })
    },
    "Supabase active-count check"
  );
  const contentRange = headers.get("content-range") || "";
  const match = contentRange.match(/\/(\d+)$/);
  if (!match) throw new Error("Supabase did not return the current active-tag count.");
  return Number(match[1]);
};

const validateSnapshot = (incomingActiveCount, previousActiveCount) => {
  if (incomingActiveCount < config.minimumActiveTags) {
    throw new Error(
      `Safety stop: snapshot contains only ${incomingActiveCount.toLocaleString()} active tags; ` +
      `minimum is ${config.minimumActiveTags.toLocaleString()}.`
    );
  }
  if (previousActiveCount > 0) {
    const ratio = incomingActiveCount / previousActiveCount;
    if (ratio < config.minimumPreviousRatio) {
      throw new Error(
        `Safety stop: snapshot contains ${(ratio * 100).toFixed(1)}% of the previous active count; ` +
        `minimum allowed is ${(config.minimumPreviousRatio * 100).toFixed(0)}%.`
      );
    }
  }
};

const upsertSnapshot = async (rows, snapshotAt) => {
  const prepared = rows.map((row) => ({
    ...row,
    last_seen_at: snapshotAt,
    closed_at: row.status === "A" ? null : snapshotAt,
    updated_at: snapshotAt
  }));
  for (let offset = 0; offset < prepared.length; offset += config.batchSize) {
    const batch = prepared.slice(offset, offset + config.batchSize);
    await requestJson(
      `${config.supabaseUrl}/rest/v1/psdata_cust_inv?on_conflict=tag_number`,
      {
        method: "POST",
        headers: supabaseHeaders({
          "Content-Type": "application/json",
          Prefer: "resolution=merge-duplicates,return=minimal"
        }),
        body: JSON.stringify(batch)
      },
      `Supabase upsert batch ${Math.floor(offset / config.batchSize) + 1}`
    );
    console.log(`Upserted ${Math.min(offset + batch.length, prepared.length).toLocaleString()} of ${prepared.length.toLocaleString()} rows`);
  }
};

const finalizeSnapshot = async (snapshotAt, activeCount) => {
  const { body } = await requestJson(
    `${config.supabaseUrl}/rest/v1/rpc/finalize_psdata_cust_inv_sync`,
    {
      method: "POST",
      headers: supabaseHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify({
        p_snapshot_at: snapshotAt,
        p_source: `GitHub Actions / Steel API ${config.recordSetCode}`,
        p_active_count: activeCount
      })
    },
    "Supabase inventory finalization"
  );
  const result = Array.isArray(body) ? body[0] : body;
  return Number(result?.closed_count ?? result ?? 0) || 0;
};

const main = async () => {
  required("PSDSTEEL_API_USERNAME", config.steelUsername);
  required("PSDSTEEL_API_PASSWORD", config.steelPassword);
  required("SUPABASE_URL", config.supabaseUrl);
  required("SUPABASE_SERVICE_ROLE_KEY", config.supabaseServiceKey);

  console.log(`Starting ${config.dryRun ? "dry-run " : ""}inventory synchronization.`);
  const rows = await fetchCompleteSnapshot();
  const snapshotAt = new Date().toISOString();
  const activeCount = rows.filter((row) => row.status === "A").length;
  const activeTons = rows
    .filter((row) => row.status === "A")
    .reduce((total, row) => total + (Number(row.tons) || 0), 0);
  const statusCounts = rows.reduce((counts, row) => {
    counts[row.status] = (counts[row.status] || 0) + 1;
    return counts;
  }, {});
  const previousActiveCount = await getCurrentActiveCount();

  console.log(`Snapshot received: ${rows.length.toLocaleString()} unique tags`);
  console.log(`Active tags in snapshot: ${activeCount.toLocaleString()}`);
  console.log(`Active tons in snapshot: ${activeTons.toLocaleString(undefined, { maximumFractionDigits: 2 })}`);
  console.log(`Snapshot status counts: ${JSON.stringify(statusCounts)}`);
  console.log(`Active tags currently in Supabase: ${previousActiveCount.toLocaleString()}`);
  validateSnapshot(activeCount, previousActiveCount);

  if (config.dryRun) {
    console.log("Dry run complete. Supabase was not changed.");
    return;
  }

  await upsertSnapshot(rows, snapshotAt);
  const closedCount = await finalizeSnapshot(snapshotAt, activeCount);
  console.log(`Inventory sync complete: ${activeCount.toLocaleString()} active, ${closedCount.toLocaleString()} closed.`);
};

main().catch((error) => {
  console.error(`Inventory sync failed: ${error?.message || error}`);
  process.exitCode = 1;
});

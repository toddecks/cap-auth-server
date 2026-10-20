#!/usr/bin/env node

import process from "node:process";
import {randomUUID} from "node:crypto";

const configuredMinimumPreviousRatio = Number(process.env.INVENTORY_SYNC_MIN_PREVIOUS_RATIO ?? 0.65);

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
  supabaseRequestTimeoutMs: Math.max(10000, Number(process.env.INVENTORY_DATABASE_TIMEOUT_MS || 60000) || 60000),
  supabaseRequestAttempts: Math.min(5, Math.max(1, Number(process.env.INVENTORY_DATABASE_ATTEMPTS || 5) || 5)),
  minimumActiveTags: Math.max(1, Number(process.env.INVENTORY_SYNC_MIN_ACTIVE_TAGS || 1000) || 1000),
  minimumPreviousRatio: Math.min(
    1,
    Math.max(0, Number.isFinite(configuredMinimumPreviousRatio) ? configuredMinimumPreviousRatio : 0.65)
  ),
  supabaseUrl: String(process.env.SUPABASE_URL || "").trim().replace(/\/+$/, ""),
  supabaseServiceKey: String(process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim(),
  snapshotTimeZone: String(process.env.INVENTORY_SNAPSHOT_TIME_ZONE || "America/New_York").trim(),
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
  const databaseRequest = label.startsWith("Supabase ");
  const attempts = databaseRequest ? config.supabaseRequestAttempts : config.requestAttempts;
  const timeoutMs = databaseRequest ? config.supabaseRequestTimeoutMs : config.requestTimeoutMs;
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
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
      // Include the stage without logging request URLs, headers, or credentials.
      const reason = controller.signal.aborted
        ? `timed out after ${timeoutMs / 1000}s`
        : (error?.message || "request failed");
      lastError = new Error(`${label}: ${reason} (attempt ${attempt}/${attempts})`, { cause: error });
      const retryable = error?.retryable !== false;
      if (!retryable || attempt === attempts) throw lastError;
      const delayMs = databaseRequest ? Math.min(120000, 15000 * 2 ** (attempt - 1)) : attempt * 2000;
      clearTimeout(timer);
      console.warn(`${lastError.message}; retrying in ${delayMs / 1000}s.`);
      await sleep(delayMs);
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

const database=async(path,method='GET',body)=> (await requestJson(`${config.supabaseUrl}/rest/v1/${path}`,{method,headers:{apikey:config.supabaseServiceKey,Authorization:`Bearer ${config.supabaseServiceKey}`,'Content-Type':'application/json',Prefer:'resolution=merge-duplicates,return=minimal'},...(body!==undefined?{body:JSON.stringify(body)}:{})},'Supabase setup details')).body;
export function normalizeSetup(setup,rows){
 if(rows.some(row=>String(row.setupNumber)!==String(setup)))throw Error('Response contains a different setup number');
 return {setup_number:setup,customer_numbers:[...new Set(rows.map(r=>String(r.customerNumber||'').trim()).filter(Boolean))],tag_numbers:[...new Set(rows.map(r=>String(r.tagNumber??'').trim()).filter(Boolean))],details:rows,record_count:rows.length,fetched_at:new Date().toISOString()};
}
async function fetchSetup(setup){const all=[];for(let page=0;page<100;page++){
 let body;for(let authAttempt=0;authAttempt<3;authAttempt++){try{
 const token=await authenticateSteelApi();
 const response=await requestJson(`${config.steelBaseUrl}/api/execute`,{method:'POST',headers:{'Content-Type':'application/json',Accept:'application/json',Authorization:`${config.steelAuthScheme} ${token}`},body:JSON.stringify({dbid:config.steelDbId,method:'GetRecordSet',params:{input:JSON.stringify({offset:page*100,limit:100,recordSetCode:'api_setupDetails',setupNumber:setup})}})},'Steel API setup details');
 body=response.body;break;
 }catch(error){if(!error.message.includes('HTTP 401')||authAttempt===2)throw error;await sleep(300);}}
 const rows=findRecordArray(body);if(!rows)throw Error('Setup details response is not an array');all.push(...rows);if(rows.length<100)return all;
 }throw Error('Setup exceeded pagination limit');}
export async function syncSetupDetails(){
 const requested=String(process.env.PSDSTEEL_SETUP_NUMBER||" ").trim();
 if(requested&&!/^\d{1,12}$/.test(requested))throw Error("Setup number must contain only digits");
 for(const[name,value]of Object.entries({PSDSTEEL_API_USERNAME:config.steelUsername,PSDSTEEL_API_PASSWORD:config.steelPassword,SUPABASE_URL:config.supabaseUrl,SUPABASE_SERVICE_ROLE_KEY:config.supabaseServiceKey}))required(name,value);
 if(config.dryRun){console.log('Setup sync skipped during inventory dry run.');return;}
 const run={id:randomUUID(),started_at:new Date().toISOString(),status:'running',setups_found:0,setups_saved:0,setups_failed:0,empty_setups:0};await database('psdata_setup_details_sync_runs','POST',run);
 try{
 const setups=requested?[Number(requested)]:[];if(!requested)for(let offset=0;;offset+=500){const rows=await database(`psdata_active_setups?select=setup_number&order=setup_number&limit=500&offset=${offset}`);if(!Array.isArray(rows))throw Error('Invalid active setup response');setups.push(...rows.map(r=>r.setup_number));if(rows.length<500)break;}
 run.setups_found=setups.length;console.log(`Refreshing ${setups.length} ${requested?"requested":"open"} setups.`);let next=0;
 // Serial API calls avoid invalidating a short-lived token with concurrent authentication.
 for(const setup of setups){try{const rows=await fetchSetup(setup);if(!rows.length){run.empty_setups++;continue;}await database('psdata_setup_details?on_conflict=setup_number','POST',normalizeSetup(setup,rows));run.setups_saved++;}catch(e){run.setups_failed++;console.error(`Setup ${setup} refresh failed; previous data retained: ${e.message}`);}finally{next++;if(next%25===0){console.log(`Setup progress ${next}/${setups.length}; saved ${run.setups_saved}, failed ${run.setups_failed}, empty ${run.empty_setups}`);await database(`psdata_setup_details_sync_runs?id=eq.${run.id}`,'PATCH',run);}}}
 run.status=run.setups_failed?'partial':'complete';
 }catch(e){run.status='failed';throw e;}finally{run.finished_at=new Date().toISOString();await database(`psdata_setup_details_sync_runs?id=eq.${run.id}`,'PATCH',run);console.log(`Setup sync ${run.status}: ${run.setups_saved} saved, ${run.setups_failed} failed, ${run.empty_setups} empty.`);}
 if(run.setups_failed)throw Error('Some setup refreshes failed; see sync run status.');
}

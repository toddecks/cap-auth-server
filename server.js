require("dotenv").config();
const express = require("express");
const jwt = require("jsonwebtoken");
const cors = require("cors");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const OpenAI = require("openai");
function getOpenAIClient() {
  const key = process.env.OPENAI_API_KEY;
  if (!key) return null;
  return new OpenAI({ apiKey: key });
}

const app = express();
app.use(cors({
  origin: "*",
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
}));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// Tableau Connected App Credentials
const CLIENT_ID = "966451f7-3322-4cd6-8e74-7d30e0acda54";
const SECRET_ID = "990a94b9-d8b8-499b-b337-fd56b73aeffa";
const SECRET_VALUE = "OkmYUPAwi/IHZiICQ4thL0IstO58wsUVoQL0jA/kIAw=";

app.get("/", (req, res) => {
  res.send("Token server running");
});

// Endpoint to generate Tableau Embed Token
app.get("/getTableauToken", (req, res) => {
  const now = Math.floor(Date.now() / 1000);
  const tableauUser = req.query.user || "todd@coilsteelprocessing.com";

  const payload = {
    iss: CLIENT_ID,
    exp: now + 300,
    aud: "tableau",
    jti: crypto.randomUUID(),
    sub: tableauUser,
    scp: ["tableau:views:embed"]
  };

  const header = {
    kid: SECRET_ID,
    alg: "HS256",
    iss: CLIENT_ID
  };

  const token = jwt.sign(payload, SECRET_VALUE, { algorithm: "HS256", header });

  res.json({ token });
});

// --- CREATE USER ENDPOINT ---
// Allows admin users to create new users and assign roles via Supabase
const { createClient } = require("@supabase/supabase-js");

const AUTH_SUPABASE_URL = "https://pxydsxadvmuffniluokk.supabase.co";
const AUTH_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB4eWRzeGFkdm11ZmZuaWx1b2trIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NTM0Nzg3MiwiZXhwIjoyMDgwOTIzODcyfQ.jTtNfZUlS6Ue0W7SWpmQLDLRerNGP7tlPxzZlJfuxPc";
const CHART_SUPABASE_URL = "https://wtjrucerrbzwxnwhqgma.supabase.co";
const CHART_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind0anJ1Y2VycmJ6d3hud2hxZ21hIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NzcwMTY4MCwiZXhwIjoyMDgzMjc3NjgwfQ.GtYd5FL1KVhTyhEUshfiCCsytwLbEf1YSf4T1XObFUo";

const supabase = createClient(AUTH_SUPABASE_URL, AUTH_SERVICE_ROLE_KEY);
const chartSupabase = createClient(CHART_SUPABASE_URL, CHART_SERVICE_ROLE_KEY);
const ROLE_DEFINITIONS = [
  { id: 1, name: "admin" },
  { id: 2, name: "shipping" },
  { id: 3, name: "receiving" },
  { id: 4, name: "production" },
  { id: 5, name: "sales" },
  { id: 6, name: "finance" },
  { id: 8, name: "maintenance" },
  { id: 9, name: "bonus_report" },
  { id: 10, name: "ai_assistant" },
  { id: 11, name: "shipping_overview" },
  { id: 12, name: "shipping_performance" },
  { id: 13, name: "customer_summary" },
  { id: 14, name: "inventory" },
  { id: 15, name: "iso" },
  { id: 16, name: "alarm_logs" }
];
const ROLE_BY_ID = new Map(ROLE_DEFINITIONS.map((role) => [role.id, role]));
const ROLE_BY_NAME = new Map(ROLE_DEFINITIONS.map((role) => [role.name, role]));

const normalizeRoleName = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");

const ensureKnownRoles = async () => {
  const { error } = await supabase
    .from("roles")
    .upsert(ROLE_DEFINITIONS, { onConflict: "id" });

  if (error) {
    throw new Error(`Failed to ensure roles: ${error.message}`);
  }
};

const resolveRoleIds = async (requestedRoles) => {
  await ensureKnownRoles();

  const ids = new Set();
  (Array.isArray(requestedRoles) ? requestedRoles : []).forEach((role) => {
    if (typeof role === "number" && ROLE_BY_ID.has(role)) {
      ids.add(role);
      return;
    }

    const parsedId = Number(role);
    if (Number.isInteger(parsedId) && ROLE_BY_ID.has(parsedId)) {
      ids.add(parsedId);
      return;
    }

    const normalizedName = normalizeRoleName(role);
    const roleDef = ROLE_BY_NAME.get(normalizedName);
    if (roleDef) ids.add(roleDef.id);
  });

  return Array.from(ids);
};
const PRO_FORMS_RECIPIENTS = String(process.env.PRO_FORMS_RECIPIENTS || "")
  .split(",")
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const RESEND_API_KEY = String(process.env.RESEND_API_KEY || "").trim();
const PRO_FORMS_FROM_EMAIL = String(process.env.PRO_FORMS_FROM_EMAIL || "").trim();
const PRO_MAINTENANCE_TEAMS_WEBHOOK_URL = String(process.env.PRO_MAINTENANCE_TEAMS_WEBHOOK_URL || "").trim();
const PRO_MAINTENANCE_ACK_BASE_URL = String(process.env.PRO_MAINTENANCE_ACK_BASE_URL || "").trim();

const sanitizePlainObject = (value) => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return Object.fromEntries(
    Object.entries(value).filter(([key]) => typeof key === "string" && key.trim())
  );
};

const coerceText = (value, maxLen = 500) => {
  if (value === null || value === undefined) return "";
  return String(value).trim().slice(0, maxLen);
};

const coerceNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const coerceInteger = (value) => {
  const parsed = coerceNumber(value);
  return parsed === null ? null : Math.trunc(parsed);
};

const coerceDateText = (value) => coerceText(value, 20) || null;

const getArray = (value) => (Array.isArray(value) ? value : []);

const buildFormSpecificSubmission = ({ submissionId, formKey, submittedAt, submittedBy, dimensions, metrics, payload, notes }) => {
  const base = {
    submission_id: submissionId,
    submitted_at: submittedAt,
    submitted_by_email: submittedBy,
    payload
  };

  if (formKey === "shift_report") {
    return {
      table: "pro_shift_report_submissions",
      row: {
        ...base,
        report_date: coerceDateText(dimensions.report_date || dimensions.submission_date || payload.reportDate),
        operator: coerceText(dimensions.operator || payload.operator, 160) || null,
        shift: coerceText(dimensions.shift || payload.shift, 80) || null,
        hours_worked: coerceNumber(metrics.hours_worked ?? payload.hoursWorked) || 0,
        tons: coerceNumber(metrics.tons ?? payload.tons) || 0,
        linear_feet: coerceNumber(metrics.linear_feet ?? payload.linearFeet) || 0,
        stroke_count: coerceInteger(metrics.stroke_count ?? payload.strokeCount) || 0,
        total_coils_ran: coerceInteger(metrics.total_coils_ran ?? payload.totalCoilsRan) || 0,
        had_downtime: coerceText(dimensions.had_downtime || payload.hadDowntime, 40) || null,
        planned_downtime_minutes: coerceInteger(metrics.planned_downtime_minutes ?? payload.plannedDowntimeMinutes) || 0,
        planned_downtime_details: coerceText(payload.plannedDowntimeDetails, 1000) || null,
        unplanned_downtime_minutes: coerceInteger(metrics.unplanned_downtime_minutes ?? payload.unplannedDowntimeMinutes) || 0,
        unplanned_downtime_details: coerceText(payload.unplannedDowntimeDetails, 1000) || null,
        skipped_orders: getArray(payload.skippedOrders),
        maintenance_times: getArray(payload.maintenanceTimes),
        maintenance_reason: coerceText(payload.maintenanceReason, 1000) || null,
        maintenance_tech: coerceText(dimensions.maintenance_tech || payload.maintenanceTech, 160) || null,
        additional_comments: notes || coerceText(payload.additionalComments, 5000) || null
      }
    };
  }

  if (formKey === "forklift_inspection") {
    return {
      table: "pro_forklift_inspection_submissions",
      row: {
        ...base,
        inspection_date: coerceDateText(dimensions.inspection_date || dimensions.submission_date || payload.inspectionDate),
        first_name: coerceText(payload.firstName, 120) || null,
        last_name: coerceText(payload.lastName, 120) || null,
        inspector_name: coerceText(dimensions.inspector_name, 160) || null,
        location: coerceText(dimensions.location || payload.location, 120) || null,
        forklift_number: coerceText(dimensions.forklift_number || payload.forkliftNumber, 120) || null,
        asset_name: coerceText(dimensions.asset_name, 160) || null,
        total_checks: coerceInteger(metrics.total_checks) || 0,
        passed_checks: coerceInteger(metrics.passed_checks) || 0,
        failed_checks: coerceInteger(metrics.failed_checks) || 0,
        maintenance_orders_opened: coerceInteger(metrics.maintenance_orders_opened) || 0,
        attention_notes: notes || coerceText(payload.attentionNotes, 5000) || null,
        checks: getArray(payload.checks)
      }
    };
  }

  if (formKey === "crane_inspection") {
    return {
      table: "pro_crane_inspection_submissions",
      row: {
        ...base,
        inspection_date: coerceDateText(dimensions.inspection_date || dimensions.submission_date || payload.inspectionDate),
        first_name: coerceText(payload.firstName, 120) || null,
        last_name: coerceText(payload.lastName, 120) || null,
        inspector_name: coerceText(dimensions.inspector_name, 160) || null,
        crane_name: coerceText(dimensions.crane_name || payload.craneName, 160) || null,
        total_checks: coerceInteger(metrics.total_checks) || 0,
        passed_checks: coerceInteger(metrics.passed_checks) || 0,
        failed_checks: coerceInteger(metrics.failed_checks) || 0,
        maintenance_orders_opened: coerceInteger(metrics.maintenance_orders_opened) || 0,
        general_comments: notes || coerceText(payload.generalComments, 5000) || null,
        answers: getArray(payload.answers)
      }
    };
  }

  if (formKey === "operational_inspection") {
    return {
      table: "pro_operational_inspection_submissions",
      row: {
        ...base,
        check_date: coerceDateText(dimensions.check_date || dimensions.submission_date || payload.checkDate),
        first_name: coerceText(payload.firstName, 120) || null,
        last_name: coerceText(payload.lastName, 120) || null,
        inspector_name: coerceText(dimensions.inspector_name, 160) || null,
        area: coerceText(dimensions.area, 120) || null,
        asset_name: coerceText(dimensions.asset_name, 160) || null,
        current_psi: coerceNumber(metrics.current_psi ?? dimensions.current_psi ?? payload.currentPsi) || 0,
        total_checks: coerceInteger(metrics.total_checks) || 0,
        clear_checks: coerceInteger(metrics.clear_checks) || 0,
        issue_checks: coerceInteger(metrics.issue_checks) || 0,
        maintenance_orders_opened: coerceInteger(metrics.maintenance_orders_opened) || 0,
        general_notes: notes || coerceText(payload.generalNotes, 5000) || null,
        checks: getArray(payload.checks)
      }
    };
  }

  return null;
};

const buildMaintenanceOrderCode = () =>
  `MO-${new Date().toISOString().slice(0, 10).replace(/-/g, "")}-${crypto.randomBytes(3).toString("hex").toUpperCase()}`;

const getExternalBaseUrl = (req) => {
  if (PRO_MAINTENANCE_ACK_BASE_URL) return PRO_MAINTENANCE_ACK_BASE_URL;
  return `${req.protocol}://${req.get("host")}`;
};

const escapeHtml = (value) =>
  String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

const EMAIL_TEMPLATES_DIR = path.join(__dirname, "email-templates");
const emailTemplateCache = new Map();

const readEmailTemplate = (name) => {
  const fileName = `${name}.html`;
  const filePath = path.join(EMAIL_TEMPLATES_DIR, fileName);
  if (!filePath.startsWith(EMAIL_TEMPLATES_DIR)) return "";
  const shouldCacheTemplates = process.env.NODE_ENV === "production";
  if (shouldCacheTemplates && emailTemplateCache.has(fileName)) return emailTemplateCache.get(fileName);

  try {
    const template = fs.readFileSync(filePath, "utf8");
    if (shouldCacheTemplates) emailTemplateCache.set(fileName, template);
    return template;
  } catch (error) {
    console.warn(`Email template ${fileName} could not be loaded:`, error.message);
    if (shouldCacheTemplates) emailTemplateCache.set(fileName, "");
    return "";
  }
};

const renderStoredTemplate = (name, values) => {
  const template = readEmailTemplate(name);
  if (!template) return "";

  return template
    .replace(/\{\{\{\s*([a-zA-Z0-9_]+)\s*\}\}\}/g, (_, key) => String(values?.[key] ?? ""))
    .replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_, key) => escapeHtml(values?.[key] ?? ""));
};

const formatEmailLabel = (value) =>
  String(value || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());

const formatEmailValue = (value, fallback = "-") => {
  if (value === null || value === undefined || value === "") return fallback;
  return escapeHtml(value);
};

const formatEmailNumber = (value, suffix = "") => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "0";
  return `${parsed.toLocaleString("en-US", { maximumFractionDigits: 2 })}${suffix}`;
};

const buildEmailRows = (items) =>
  items
    .filter((item) => item && item.value !== null && item.value !== undefined && item.value !== "")
    .map((item) => `
      <tr>
        <td style="padding:9px 12px;border-bottom:1px solid #d7e1ef;color:#61708a;font-size:13px;">${escapeHtml(item.label)}</td>
        <td style="padding:9px 12px;border-bottom:1px solid #d7e1ef;color:#172742;font-size:13px;font-weight:700;text-align:right;">${formatEmailValue(item.value)}</td>
      </tr>
    `)
    .join("");

const buildEmailTable = (title, rows) => {
  const rowMarkup = buildEmailRows(rows);
  if (!rowMarkup) return "";
  return `
    <h3 style="margin:22px 0 8px;color:#172742;font-size:16px;">${escapeHtml(title)}</h3>
    <table class="detail-table" role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;border:1px solid #d7e1ef;border-radius:8px;overflow:hidden;">
      ${rowMarkup}
    </table>
  `;
};

const buildItemList = (title, items, emptyText = "") => {
  const cleanItems = (Array.isArray(items) ? items : []).filter(Boolean);
  if (!cleanItems.length && !emptyText) return "";
  const listMarkup = cleanItems.length
    ? cleanItems.map((item) => `<li style="margin:0 0 7px;">${escapeHtml(item)}</li>`).join("")
    : `<li style="margin:0;">${escapeHtml(emptyText)}</li>`;

  return `
    <h3 style="margin:22px 0 8px;color:#172742;font-size:16px;">${escapeHtml(title)}</h3>
    <ul style="margin:0 0 0 18px;padding:0;color:#233658;font-size:14px;line-height:1.45;">${listMarkup}</ul>
  `;
};

const clampPercent = (value) => Math.max(0, Math.min(100, value));

const buildEmailBarChart = (title, rows, { maxValue = null, color = "#2f61d3" } = {}) => {
  const cleanRows = (Array.isArray(rows) ? rows : [])
    .map((row) => ({
      label: row.label,
      value: Number(row.value) || 0,
      display: row.display || formatEmailNumber(row.value)
    }))
    .filter((row) => row.label && row.value >= 0);

  if (!cleanRows.length) return "";

  const max = Number(maxValue) > 0
    ? Number(maxValue)
    : Math.max(...cleanRows.map((row) => row.value), 1);

  const rowMarkup = cleanRows.map((row) => {
    const width = clampPercent((row.value / max) * 100);
    return `
      <tr>
        <td class="metric-label" style="padding:8px 0;width:118px;color:#61708a;font-size:13px;vertical-align:middle;">${escapeHtml(row.label)}</td>
        <td class="metric-bar" style="padding:8px 10px;vertical-align:middle;">
          <div style="background:#edf2fa;border-radius:999px;height:12px;line-height:12px;overflow:hidden;">
            <div style="background:${color};width:${width}%;height:12px;line-height:12px;">&nbsp;</div>
          </div>
        </td>
        <td class="metric-value" style="padding:8px 0;width:78px;color:#172742;font-size:13px;font-weight:700;text-align:right;vertical-align:middle;">${escapeHtml(row.display)}</td>
      </tr>
    `;
  }).join("");

  return `
    <h3 style="margin:22px 0 8px;color:#172742;font-size:16px;">${escapeHtml(title)}</h3>
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
      ${rowMarkup}
    </table>
  `;
};

const buildInspectionResultChart = ({ clearCount, issueCount, clearLabel = "Clear", issueLabel = "Issues" }) => {
  const clear = Number(clearCount) || 0;
  const issues = Number(issueCount) || 0;
  const total = Math.max(clear + issues, 1);
  const clearWidth = clampPercent((clear / total) * 100);
  const issueWidth = clampPercent((issues / total) * 100);

  return `
    <h3 style="margin:22px 0 8px;color:#172742;font-size:16px;">Result Breakdown</h3>
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
      <tr>
        <td colspan="2" style="padding:0 0 10px;">
          <div style="background:#edf2fa;border-radius:999px;height:16px;line-height:16px;overflow:hidden;">
            <div style="display:inline-block;background:#067647;width:${clearWidth}%;height:16px;line-height:16px;">&nbsp;</div><div style="display:inline-block;background:#b42318;width:${issueWidth}%;height:16px;line-height:16px;">&nbsp;</div>
          </div>
        </td>
      </tr>
      <tr>
        <td style="color:#067647;font-size:13px;font-weight:700;">${escapeHtml(clearLabel)}: ${formatEmailNumber(clear)}</td>
        <td style="color:#b42318;font-size:13px;font-weight:700;text-align:right;">${escapeHtml(issueLabel)}: ${formatEmailNumber(issues)}</td>
      </tr>
    </table>
  `;
};

const buildKpiGrid = (items) => {
  const cleanItems = (Array.isArray(items) ? items : []).filter((item) => item && item.label);
  if (!cleanItems.length) return "";

  const cells = cleanItems.map((item) => `
    <td class="kpi-cell" width="25%" style="padding:0 6px 12px;vertical-align:top;">
      <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="width:100%;background-color:#f7faff;border:1px solid #d7e1ef;border-radius:14px;">
        <tr>
          <td style="padding:13px 12px;min-height:86px;">
            <div style="font-size:10px;line-height:1.2;letter-spacing:.08em;text-transform:uppercase;color:#61708a;font-weight:bold;">${escapeHtml(item.label)}</div>
            <div style="margin-top:6px;font-size:24px;line-height:1.05;color:${escapeHtml(item.color || "#172742")};font-weight:bold;">${escapeHtml(item.value)}</div>
            ${item.note ? `<div style="margin-top:5px;font-size:11px;line-height:1.25;color:#61708a;">${escapeHtml(item.note)}</div>` : ""}
          </td>
        </tr>
      </table>
    </td>
  `);

  const rows = [];
  for (let index = 0; index < cells.length; index += 4) {
    rows.push(`<tr>${cells.slice(index, index + 4).join("")}</tr>`);
  }

  return `
    <table class="kpi-grid" role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;margin:2px -6px 10px;">
      ${rows.join("")}
    </table>
  `;
};

const buildEmailShell = ({ eyebrow, title, summary, submittedBy, submittedAt, body }) => {
  const submissionTable = buildEmailTable("Submission", [
    { label: "Submitted by", value: submittedBy || "Unknown user" },
    { label: "Submitted at", value: submittedAt }
  ]);
  const summaryBlock = summary
    ? `<span style="color:#172742;font-size:15px;font-weight:bold;line-height:1.45;">${escapeHtml(summary)}</span>`
    : "";
  const stored = renderStoredTemplate("layout", {
    eyebrow: eyebrow || "CSP Pro",
    title,
    summary_block: summaryBlock,
    submission_table: submissionTable,
    body: body || ""
  });

  if (stored) return stored;

  return `
    <div style="margin:0;padding:0;background:#eff4fb;">
      <div style="max-width:720px;margin:0 auto;padding:24px 14px;font-family:Arial,sans-serif;color:#233658;line-height:1.5;">
        <div style="background:#172742;color:#ffffff;border-radius:10px 10px 0 0;padding:20px 24px;">
          <div style="font-size:11px;letter-spacing:.16em;text-transform:uppercase;color:#f1a91e;font-weight:700;">${escapeHtml(eyebrow || "CSP Pro")}</div>
          <h2 style="margin:8px 0 0;font-size:24px;line-height:1.2;">${escapeHtml(title)}</h2>
        </div>
        <div style="background:#ffffff;border:1px solid #d7e1ef;border-top:0;border-radius:0 0 10px 10px;padding:22px 24px;">
          ${summaryBlock}
          ${submissionTable}
          ${body || ""}
        </div>
      </div>
    </div>
  `;
};

const buildGenericSubmissionEmail = ({ formLabel, submittedBy, submittedAt, dimensions, metrics, notes }) => {
  const dimensionLines = Object.entries(dimensions || {})
    .filter(([, value]) => value !== null && value !== undefined && value !== "")
    .map(([key, value]) => ({ label: formatEmailLabel(key), value }));
  const metricLines = Object.entries(metrics || {})
    .filter(([, value]) => value !== null && value !== undefined && value !== "")
    .map(([key, value]) => ({ label: formatEmailLabel(key), value }));

  return buildEmailShell({
    eyebrow: "CSP Pro Form",
    title: `${formLabel} submitted`,
    submittedBy,
    submittedAt,
    body: [
      buildEmailTable("Details", dimensionLines),
      buildEmailTable("Metrics", metricLines),
      notes ? `<h3 style="margin:22px 0 8px;color:#172742;font-size:16px;">Notes</h3><p style="margin:0;color:#233658;">${escapeHtml(notes)}</p>` : ""
    ].join("")
  });
};

const buildShiftReportEmail = ({ submittedBy, submittedAt, dimensions, metrics, notes, payload }) => {
  const values = {
    kpi_grid: buildKpiGrid([
      { label: "Tons", value: formatEmailNumber(metrics.tons), note: dimensions.shift || "Shift" },
      { label: "Linear Feet", value: formatEmailNumber(metrics.linear_feet), note: "Reported output" },
      { label: "Coils", value: formatEmailNumber(metrics.total_coils_ran), note: "Total ran" },
      {
        label: "Downtime",
        value: formatEmailNumber(metrics.total_downtime_minutes, "m"),
        note: "Planned + unplanned",
        color: Number(metrics.total_downtime_minutes) > 0 ? "#b42318" : "#067647"
      }
    ]),
    production_chart: buildEmailBarChart("Production Snapshot", [
      { label: "Tons", value: metrics.tons, display: formatEmailNumber(metrics.tons) },
      { label: "Coils", value: metrics.total_coils_ran, display: formatEmailNumber(metrics.total_coils_ran) },
      { label: "Downtime", value: metrics.total_downtime_minutes, display: formatEmailNumber(metrics.total_downtime_minutes, " min") }
    ], { color: "#f1a91e" }),
    shift_details_table: buildEmailTable("Shift Details", [
      { label: "Report date", value: dimensions.report_date || dimensions.submission_date },
      { label: "Operator", value: dimensions.operator },
      { label: "Shift", value: dimensions.shift },
      { label: "Had downtime", value: dimensions.had_downtime }
    ]),
    production_metrics_table: buildEmailTable("Production Metrics", [
      { label: "Hours worked", value: formatEmailNumber(metrics.hours_worked) },
      { label: "Tons", value: formatEmailNumber(metrics.tons) },
      { label: "Linear feet", value: formatEmailNumber(metrics.linear_feet) },
      { label: "Stroke count", value: formatEmailNumber(metrics.stroke_count) },
      { label: "Total coils ran", value: formatEmailNumber(metrics.total_coils_ran) },
      { label: "Total downtime", value: formatEmailNumber(metrics.total_downtime_minutes, " min") }
    ]),
    downtime_table: buildEmailTable("Downtime", [
      { label: "Planned downtime", value: formatEmailNumber(metrics.planned_downtime_minutes, " min") },
      { label: "Planned details", value: payload.plannedDowntimeDetails },
      { label: "Unplanned downtime", value: formatEmailNumber(metrics.unplanned_downtime_minutes, " min") },
      { label: "Unplanned details", value: payload.unplannedDowntimeDetails },
      { label: "Maintenance tech", value: dimensions.maintenance_tech }
    ]),
    skipped_orders_list: buildItemList(
      "Skipped Orders",
      getArray(payload.skippedOrders).map((row) => `${row.skippedOrderNumber || "Order"}: ${row.skippedOrderReason || "No reason provided"}`),
      "No skipped orders recorded."
    ),
    comments_block: notes ? `<h3 style="margin:22px 0 8px;color:#172742;font-size:16px;">Comments</h3><p style="margin:0;color:#233658;">${escapeHtml(notes)}</p>` : ""
  };
  const body = renderStoredTemplate("shift_report", values) || Object.values(values).join("");

  return buildEmailShell({
    eyebrow: "Shift Report",
    title: `Shift report submitted for ${formatEmailValue(dimensions.report_date || dimensions.submission_date)}`,
    summary: `${formatEmailValue(dimensions.operator, "An operator")} submitted ${formatEmailNumber(metrics.tons)} tons on ${formatEmailValue(dimensions.shift, "the selected shift")}.`,
    submittedBy,
    submittedAt,
    body
  });
};

const buildInspectionEmail = ({ formLabel, submittedBy, submittedAt, dimensions, metrics, notes, payload, itemField, issueCountKey, issueLabel }) => {
  const items = getArray(payload[itemField]);
  const issueItems = items.filter((item) => item.status === "Fail" || item.isIssue);
  const clearCount = metrics.passed_checks ?? metrics.clear_checks ?? 0;
  const issueCount = metrics[issueCountKey] ?? issueItems.length;

  const bodyValues = {
    kpi_grid: buildKpiGrid([
      { label: "Total Checks", value: formatEmailNumber(metrics.total_checks), note: "Submitted" },
      { label: "Clear/Pass", value: formatEmailNumber(clearCount), note: "Good responses", color: "#067647" },
      {
        label: issueLabel,
        value: formatEmailNumber(issueCount),
        note: issueCount > 0 ? "Needs attention" : "No issues",
        color: issueCount > 0 ? "#b42318" : "#067647"
      },
      {
        label: "Orders",
        value: formatEmailNumber(metrics.maintenance_orders_opened),
        note: "Maintenance opened",
        color: Number(metrics.maintenance_orders_opened) > 0 ? "#b42318" : "#172742"
      }
    ]),
    result_chart: buildInspectionResultChart({
      clearCount,
      issueCount,
      clearLabel: "Clear/Pass",
      issueLabel
    }),
    inspection_details_table: buildEmailTable("Inspection Details", [
      { label: "Date", value: dimensions.inspection_date || dimensions.check_date || dimensions.submission_date },
      { label: "Inspector", value: dimensions.inspector_name },
      { label: "Asset", value: dimensions.asset_name || dimensions.crane_name || dimensions.area },
      { label: "Location", value: dimensions.location },
      { label: "Forklift number", value: dimensions.forklift_number },
      { label: "Current PSI", value: dimensions.current_psi }
    ]),
    results_table: buildEmailTable("Results", [
      { label: "Total checks", value: formatEmailNumber(metrics.total_checks) },
      { label: "Clear/pass checks", value: formatEmailNumber(clearCount) },
      { label: issueLabel, value: formatEmailNumber(issueCount) },
      { label: "Maintenance orders opened", value: formatEmailNumber(metrics.maintenance_orders_opened) }
    ]),
    problem_items_list: buildItemList(
      "Problem Items",
      issueItems.map((item) => `${item.label || item.key}: ${item.notes || `Response: ${item.status || ""}`}`),
      "No problem items reported."
    ),
    notes_block: notes ? `<h3 style="margin:22px 0 8px;color:#172742;font-size:16px;">Notes</h3><p style="margin:0;color:#233658;">${escapeHtml(notes)}</p>` : ""
  };
  const templateName = formLabel.toLowerCase().replace(/\s+/g, "_");
  const body = renderStoredTemplate(templateName, bodyValues) || Object.values(bodyValues).join("");

  return buildEmailShell({
    eyebrow: formLabel,
    title: `${formLabel} submitted`,
    summary: `${formatEmailNumber(issueCount)} ${issueLabel} reported out of ${formatEmailNumber(metrics.total_checks)} checks.`,
    submittedBy,
    submittedAt,
    body
  });
};

const buildSubmissionEmail = ({ formKey, formLabel, submittedBy, submittedAt, dimensions, metrics, notes, payload }) => {
  if (formKey === "shift_report") {
    return buildShiftReportEmail({ submittedBy, submittedAt, dimensions, metrics, notes, payload });
  }

  if (formKey === "forklift_inspection") {
    return buildInspectionEmail({
      formLabel: "Forklift Inspection",
      submittedBy,
      submittedAt,
      dimensions,
      metrics,
      notes,
      payload,
      itemField: "checks",
      issueCountKey: "failed_checks",
      issueLabel: "failed checks"
    });
  }

  if (formKey === "crane_inspection") {
    return buildInspectionEmail({
      formLabel: "Crane Inspection",
      submittedBy,
      submittedAt,
      dimensions,
      metrics,
      notes,
      payload,
      itemField: "answers",
      issueCountKey: "failed_checks",
      issueLabel: "failed checks"
    });
  }

  if (formKey === "operational_inspection") {
    return buildInspectionEmail({
      formLabel: "Operational Inspection",
      submittedBy,
      submittedAt,
      dimensions,
      metrics,
      notes,
      payload,
      itemField: "checks",
      issueCountKey: "issue_checks",
      issueLabel: "issue checks"
    });
  }

  return buildGenericSubmissionEmail({ formLabel, submittedBy, submittedAt, dimensions, metrics, notes });
};

const ANALYZE_BATCH_SIZE = 1000;
const startOfDay = (value) => new Date(value.getFullYear(), value.getMonth(), value.getDate());
const addDays = (value, days) => {
  const next = new Date(value);
  next.setDate(next.getDate() + days);
  return next;
};
const startOfMonth = (value) => new Date(value.getFullYear(), value.getMonth(), 1);
const weekStartMonday = (value) => {
  const base = startOfDay(value);
  const day = base.getDay();
  const offset = day === 0 ? -6 : 1 - day;
  return addDays(base, offset);
};
const toYmd = (value) =>
  `${value.getFullYear()}-${String(value.getMonth() + 1).padStart(2, "0")}-${String(value.getDate()).padStart(2, "0")}`;
const parseDateValue = (value) => {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};
const toNumberSafe = (value) => {
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  const normalized = String(value ?? "").replace(/[^0-9.+-]/g, "");
  if (!normalized) return 0;
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : 0;
};
const roundMetric = (value, digits = 1) => {
  if (!Number.isFinite(value)) return 0;
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
};
const pctDelta = (current, baseline) => {
  if (!Number.isFinite(current) || !Number.isFinite(baseline) || baseline === 0) return null;
  return ((current - baseline) / baseline) * 100;
};
const average = (values) => {
  const valid = (Array.isArray(values) ? values : []).filter((value) => Number.isFinite(value));
  if (!valid.length) return null;
  return valid.reduce((sum, value) => sum + value, 0) / valid.length;
};
const sumBy = (rows, getter) => (Array.isArray(rows) ? rows : []).reduce((sum, row) => sum + (Number(getter(row)) || 0), 0);
const countTruthy = (rows, getter) => (Array.isArray(rows) ? rows : []).reduce((sum, row) => sum + (getter(row) ? 1 : 0), 0);
const normalizeLabel = (value, fallback = "Unknown") => {
  const text = String(value ?? "").trim();
  return text || fallback;
};
const topEntries = (map, limit = 5, sortGetter = (value) => value) =>
  Array.from(map.entries())
    .sort((a, b) => (sortGetter(b[1]) - sortGetter(a[1])) || String(a[0]).localeCompare(String(b[0])))
    .slice(0, limit)
    .map(([key, value]) => ({ key, value }));

async function fetchChartRows(table, select, {
  gte = null,
  gteColumn = null,
  lt = null,
  ltColumn = null,
  orderBy = null,
  ascending = true,
  limit = 50000
} = {}) {
  const rows = [];
  let from = 0;

  while (from < limit) {
    const to = Math.min(from + ANALYZE_BATCH_SIZE - 1, limit - 1);
    let query = chartSupabase.from(table).select(select);
    if (gte && gteColumn) query = query.gte(gteColumn, gte);
    if (lt && ltColumn) query = query.lt(ltColumn, lt);
    if (orderBy) query = query.order(orderBy, { ascending });

    const { data, error } = await query.range(from, to);
    if (error) throw error;

    const page = Array.isArray(data) ? data : [];
    rows.push(...page);
    if (page.length < ANALYZE_BATCH_SIZE) break;
    from += ANALYZE_BATCH_SIZE;
  }

  return rows;
}

function buildOperationalSnapshot({ productionRows, shippingRows, isoRows, focus = "" }) {
  const now = new Date();
  const today = startOfDay(now);
  const currentWeekStart = weekStartMonday(now);
  const lastCompletedWeekStart = addDays(currentWeekStart, -7);
  const priorFourWeekStart = addDays(lastCompletedWeekStart, -28);
  const trailing14Start = addDays(today, -14);
  const previous14Start = addDays(today, -28);
  const currentMonthStart = startOfMonth(now);
  const previousMonthStart = startOfMonth(addDays(currentMonthStart, -1));

  const normalizeProdDate = (row) => parseDateValue(row.processing_start_date);
  const normalizeShipDate = (row) => parseDateValue(row.ship_date);
  const normalizeIsoDate = (row) => parseDateValue(row.date_entered || row.complaint_date || row.date_opened);

  const prodCurrentWeek = productionRows.filter((row) => {
    const date = normalizeProdDate(row);
    return date && date >= lastCompletedWeekStart && date < currentWeekStart;
  });
  const prodPriorFourWeeks = productionRows.filter((row) => {
    const date = normalizeProdDate(row);
    return date && date >= priorFourWeekStart && date < lastCompletedWeekStart;
  });
  const prodTrailing14 = productionRows.filter((row) => {
    const date = normalizeProdDate(row);
    return date && date >= trailing14Start && date < now;
  });
  const prodPrevious14 = productionRows.filter((row) => {
    const date = normalizeProdDate(row);
    return date && date >= previous14Start && date < trailing14Start;
  });

  const productionWeekTons = sumBy(prodCurrentWeek, (row) => toNumberSafe(row.tag_tons));
  const productionPriorWeeklyAvg = sumBy(prodPriorFourWeeks, (row) => toNumberSafe(row.tag_tons)) / 4;
  const productionCurrentTph = average(prodTrailing14.map((row) => toNumberSafe(row.tons_per_hour)));
  const productionPreviousTph = average(prodPrevious14.map((row) => toNumberSafe(row.tons_per_hour)));
  const productionCurrentDaysToClose = average(prodTrailing14.map((row) => toNumberSafe(row.days_to_close)));
  const productionPreviousDaysToClose = average(prodPrevious14.map((row) => toNumberSafe(row.days_to_close)));

  const machine14Current = new Map();
  const machine14Previous = new Map();
  prodTrailing14.forEach((row) => {
    const key = normalizeLabel(row.machine_label, "Unassigned");
    machine14Current.set(key, (machine14Current.get(key) || 0) + toNumberSafe(row.tag_tons));
  });
  prodPrevious14.forEach((row) => {
    const key = normalizeLabel(row.machine_label, "Unassigned");
    machine14Previous.set(key, (machine14Previous.get(key) || 0) + toNumberSafe(row.tag_tons));
  });

  const shippingCurrentWeek = shippingRows.filter((row) => {
    const date = normalizeShipDate(row);
    return date && date >= lastCompletedWeekStart && date < currentWeekStart;
  });
  const shippingPriorFourWeeks = shippingRows.filter((row) => {
    const date = normalizeShipDate(row);
    return date && date >= priorFourWeekStart && date < lastCompletedWeekStart;
  });
  const shippingTrailing14 = shippingRows.filter((row) => {
    const date = normalizeShipDate(row);
    return date && date >= trailing14Start && date < now;
  });
  const shippingPrevious14 = shippingRows.filter((row) => {
    const date = normalizeShipDate(row);
    return date && date >= previous14Start && date < trailing14Start;
  });

  const shippingWeekTons = sumBy(shippingCurrentWeek, (row) => toNumberSafe(row.weight) / 2000);
  const shippingPriorWeeklyAvg = sumBy(shippingPriorFourWeeks, (row) => toNumberSafe(row.weight) / 2000) / 4;
  const shippingCurrentLoadCount = shippingCurrentWeek.length;
  const shippingPriorLoadAvg = shippingPriorFourWeeks.length / 4;
  const shippingCancelCurrent = countTruthy(shippingCurrentWeek, (row) => row.cancel_load === true || String(row.cancel_load).toLowerCase() === "true");
  const shippingCancelPrevious = countTruthy(shippingPriorFourWeeks, (row) => row.cancel_load === true || String(row.cancel_load).toLowerCase() === "true") / 4;

  const shippingCustomerCurrent = new Map();
  const shippingCustomerPrevious = new Map();
  shippingTrailing14.forEach((row) => {
    const key = normalizeLabel(row.customer_no || row.ship_to_customer_name, "Unknown customer");
    shippingCustomerCurrent.set(key, (shippingCustomerCurrent.get(key) || 0) + (toNumberSafe(row.weight) / 2000));
  });
  shippingPrevious14.forEach((row) => {
    const key = normalizeLabel(row.customer_no || row.ship_to_customer_name, "Unknown customer");
    shippingCustomerPrevious.set(key, (shippingCustomerPrevious.get(key) || 0) + (toNumberSafe(row.weight) / 2000));
  });

  const productionCustomerCurrentMonth = new Map();
  const productionCustomerPreviousMonth = new Map();
  productionRows.forEach((row) => {
    const date = normalizeProdDate(row);
    if (!date) return;
    const key = normalizeLabel(row.customer_number, "Unknown customer");
    if (date >= currentMonthStart && date < now) {
      productionCustomerCurrentMonth.set(key, (productionCustomerCurrentMonth.get(key) || 0) + toNumberSafe(row.tag_tons));
      return;
    }
    if (date >= previousMonthStart && date < currentMonthStart) {
      productionCustomerPreviousMonth.set(key, (productionCustomerPreviousMonth.get(key) || 0) + toNumberSafe(row.tag_tons));
    }
  });

  const isoTrailing30 = isoRows.filter((row) => {
    const date = normalizeIsoDate(row);
    return date && date >= addDays(today, -30) && date < now;
  });
  const isoPrevious30 = isoRows.filter((row) => {
    const date = normalizeIsoDate(row);
    return date && date >= addDays(today, -60) && date < addDays(today, -30);
  });
  const isoCostTrailing30 = sumBy(isoTrailing30, (row) => toNumberSafe(row.cost));
  const isoCostPrevious30 = sumBy(isoPrevious30, (row) => toNumberSafe(row.cost));
  const isoOpenTrailing30 = countTruthy(isoTrailing30, (row) => !String(row.status || "").toLowerCase().includes("closed"));

  const findings = [];
  const addFinding = (severity, area, title, detail, metrics = {}) => {
    findings.push({ severity, area, title, detail, metrics });
  };

  const prodWeekDelta = pctDelta(productionWeekTons, productionPriorWeeklyAvg);
  if (prodWeekDelta !== null && prodWeekDelta <= -15) {
    addFinding(
      prodWeekDelta <= -25 ? "high" : "medium",
      "production",
      "Production tons dropped versus recent baseline",
      `Last completed week produced ${roundMetric(productionWeekTons, 0)} tons versus a prior 4-week average of ${roundMetric(productionPriorWeeklyAvg, 0)} tons.`,
      { productionWeekTons: roundMetric(productionWeekTons, 0), priorAverageTons: roundMetric(productionPriorWeeklyAvg, 0), deltaPct: roundMetric(prodWeekDelta, 1) }
    );
  }

  const tphDelta = pctDelta(productionCurrentTph, productionPreviousTph);
  if (tphDelta !== null && tphDelta <= -10) {
    addFinding(
      tphDelta <= -20 ? "high" : "medium",
      "production",
      "Tons per hour efficiency is down",
      `Trailing 14-day average TPH is ${roundMetric(productionCurrentTph, 2)} versus ${roundMetric(productionPreviousTph, 2)} in the prior 14 days.`,
      { currentTph: roundMetric(productionCurrentTph, 2), previousTph: roundMetric(productionPreviousTph, 2), deltaPct: roundMetric(tphDelta, 1) }
    );
  }

  const daysToCloseDelta = pctDelta(productionCurrentDaysToClose, productionPreviousDaysToClose);
  if (productionCurrentDaysToClose && (productionCurrentDaysToClose >= 5 || (daysToCloseDelta !== null && daysToCloseDelta >= 15))) {
    addFinding(
      daysToCloseDelta !== null && daysToCloseDelta >= 25 ? "high" : "medium",
      "production",
      "Days-to-close is elevated",
      `Trailing 14-day average days-to-close is ${roundMetric(productionCurrentDaysToClose, 1)} versus ${roundMetric(productionPreviousDaysToClose, 1)} previously.`,
      { currentDaysToClose: roundMetric(productionCurrentDaysToClose, 1), previousDaysToClose: roundMetric(productionPreviousDaysToClose, 1), deltaPct: roundMetric(daysToCloseDelta ?? 0, 1) }
    );
  }

  const machineDrops = [];
  machine14Previous.forEach((previousTons, machine) => {
    const currentTons = machine14Current.get(machine) || 0;
    const delta = pctDelta(currentTons, previousTons);
    if (previousTons >= 50 && delta !== null && delta <= -20) {
      machineDrops.push({ machine, currentTons, previousTons, delta });
    }
  });
  machineDrops
    .sort((a, b) => a.delta - b.delta)
    .slice(0, 3)
    .forEach((entry) => {
      addFinding(
        entry.delta <= -35 ? "high" : "medium",
        "production",
        `Machine output drop on ${entry.machine}`,
        `${entry.machine} produced ${roundMetric(entry.currentTons, 0)} tons in the last 14 days versus ${roundMetric(entry.previousTons, 0)} in the prior 14 days.`,
        { machine: entry.machine, currentTons: roundMetric(entry.currentTons, 0), previousTons: roundMetric(entry.previousTons, 0), deltaPct: roundMetric(entry.delta, 1) }
      );
    });

  const shipWeekDelta = pctDelta(shippingWeekTons, shippingPriorWeeklyAvg);
  if (shipWeekDelta !== null && shipWeekDelta <= -15) {
    addFinding(
      shipWeekDelta <= -25 ? "high" : "medium",
      "shipping",
      "Shipping tons dropped versus recent baseline",
      `Last completed shipping week moved ${roundMetric(shippingWeekTons, 0)} tons versus a prior 4-week weekly average of ${roundMetric(shippingPriorWeeklyAvg, 0)} tons.`,
      { shippingWeekTons: roundMetric(shippingWeekTons, 0), priorAverageTons: roundMetric(shippingPriorWeeklyAvg, 0), deltaPct: roundMetric(shipWeekDelta, 1) }
    );
  }

  const shipLoadDelta = pctDelta(shippingCurrentLoadCount, shippingPriorLoadAvg);
  if (shipLoadDelta !== null && shipLoadDelta <= -15) {
    addFinding(
      shipLoadDelta <= -25 ? "high" : "medium",
      "shipping",
      "Shipped load count is down",
      `Last completed week shipped ${shippingCurrentLoadCount} loads versus an average of ${roundMetric(shippingPriorLoadAvg, 1)} loads across the prior four weeks.`,
      { shippingLoadCount: shippingCurrentLoadCount, priorAverageLoads: roundMetric(shippingPriorLoadAvg, 1), deltaPct: roundMetric(shipLoadDelta, 1) }
    );
  }

  const cancelDelta = pctDelta(shippingCancelCurrent, shippingCancelPrevious);
  if (shippingCancelCurrent >= 3 && (shippingCancelCurrent > shippingCancelPrevious + 1 || (cancelDelta !== null && cancelDelta >= 20))) {
    addFinding(
      "medium",
      "shipping",
      "Shipping cancellations increased",
      `Last completed week recorded ${shippingCancelCurrent} cancelled loads versus ${roundMetric(shippingCancelPrevious, 1)} per week across the prior four weeks.`,
      { currentCancelledLoads: shippingCancelCurrent, priorAverageCancelledLoads: roundMetric(shippingCancelPrevious, 1), deltaPct: roundMetric(cancelDelta ?? 0, 1) }
    );
  }

  const customerDrops = [];
  shippingCustomerPrevious.forEach((previousTons, customer) => {
    const currentTons = shippingCustomerCurrent.get(customer) || 0;
    const delta = pctDelta(currentTons, previousTons);
    if (previousTons >= 20 && delta !== null && delta <= -30) {
      customerDrops.push({ customer, currentTons, previousTons, delta });
    }
  });
  customerDrops
    .sort((a, b) => a.delta - b.delta)
    .slice(0, 4)
    .forEach((entry) => {
      addFinding(
        entry.delta <= -50 ? "high" : "medium",
        "customer-activity",
        `Customer shipping activity dropped for ${entry.customer}`,
        `${entry.customer} shipped ${roundMetric(entry.currentTons, 0)} tons in the last 14 days versus ${roundMetric(entry.previousTons, 0)} in the prior 14 days.`,
        { customer: entry.customer, currentTons: roundMetric(entry.currentTons, 0), previousTons: roundMetric(entry.previousTons, 0), deltaPct: roundMetric(entry.delta, 1) }
      );
    });

  const productionCustomerDrops = [];
  productionCustomerPreviousMonth.forEach((previousTons, customer) => {
    const currentTons = productionCustomerCurrentMonth.get(customer) || 0;
    const delta = pctDelta(currentTons, previousTons);
    if (previousTons >= 40 && delta !== null && delta <= -30) {
      productionCustomerDrops.push({ customer, currentTons, previousTons, delta });
    }
  });
  productionCustomerDrops
    .sort((a, b) => a.delta - b.delta)
    .slice(0, 4)
    .forEach((entry) => {
      addFinding(
        entry.delta <= -50 ? "high" : "medium",
        "customer-activity",
        `Production demand dropped for ${entry.customer}`,
        `${entry.customer} has ${roundMetric(entry.currentTons, 0)} tons month-to-date versus ${roundMetric(entry.previousTons, 0)} tons last month-to-date.`,
        { customer: entry.customer, currentMonthTons: roundMetric(entry.currentTons, 0), previousMonthTons: roundMetric(entry.previousTons, 0), deltaPct: roundMetric(entry.delta, 1) }
      );
    });

  const complaintDelta = pctDelta(isoTrailing30.length, isoPrevious30.length);
  if (isoTrailing30.length >= 3 && (complaintDelta !== null && complaintDelta >= 20)) {
    addFinding(
      complaintDelta >= 40 ? "high" : "medium",
      "quality",
      "Complaint volume increased",
      `ISO complaints are ${isoTrailing30.length} in the last 30 days versus ${isoPrevious30.length} in the prior 30 days.`,
      { complaintsTrailing30: isoTrailing30.length, complaintsPrevious30: isoPrevious30.length, deltaPct: roundMetric(complaintDelta, 1) }
    );
  }

  const isoCostDelta = pctDelta(isoCostTrailing30, isoCostPrevious30);
  if (isoCostTrailing30 >= 1000 && (isoCostDelta !== null && isoCostDelta >= 20)) {
    addFinding(
      isoCostDelta >= 40 ? "high" : "medium",
      "quality",
      "Refund cost increased",
      `Complaint-related cost is $${roundMetric(isoCostTrailing30, 0)} in the last 30 days versus $${roundMetric(isoCostPrevious30, 0)} previously.`,
      { costTrailing30: roundMetric(isoCostTrailing30, 0), costPrevious30: roundMetric(isoCostPrevious30, 0), deltaPct: roundMetric(isoCostDelta, 1) }
    );
  }

  if (isoOpenTrailing30 >= 3) {
    addFinding(
      "medium",
      "quality",
      "Open complaint backlog present",
      `${isoOpenTrailing30} complaints created in the last 30 days are still open.`,
      { openComplaintsTrailing30: isoOpenTrailing30 }
    );
  }

  const topShippingCustomers = topEntries(shippingCustomerCurrent, 5, (value) => value).map((entry) => ({
    customer: entry.key,
    tons: roundMetric(entry.value, 0)
  }));
  const topProductionMachines = topEntries(machine14Current, 5, (value) => value).map((entry) => ({
    machine: entry.key,
    tons: roundMetric(entry.value, 0)
  }));

  return {
    focus: String(focus || "").trim(),
    generatedAt: new Date().toISOString(),
    snapshots: {
      production: {
        lastCompletedWeekTons: roundMetric(productionWeekTons, 0),
        prior4WeekAverageTons: roundMetric(productionPriorWeeklyAvg, 0),
        trailing14AvgTph: roundMetric(productionCurrentTph ?? 0, 2),
        previous14AvgTph: roundMetric(productionPreviousTph ?? 0, 2),
        trailing14AvgDaysToClose: roundMetric(productionCurrentDaysToClose ?? 0, 1),
        previous14AvgDaysToClose: roundMetric(productionPreviousDaysToClose ?? 0, 1),
        topMachinesTrailing14: topProductionMachines
      },
      shipping: {
        lastCompletedWeekTons: roundMetric(shippingWeekTons, 0),
        prior4WeekAverageTons: roundMetric(shippingPriorWeeklyAvg, 0),
        lastCompletedWeekLoads: shippingCurrentLoadCount,
        prior4WeekAverageLoads: roundMetric(shippingPriorLoadAvg, 1),
        lastCompletedWeekCancelledLoads: shippingCancelCurrent,
        prior4WeekAverageCancelledLoads: roundMetric(shippingCancelPrevious, 1),
        topCustomersTrailing14: topShippingCustomers
      },
      quality: {
        complaintsTrailing30: isoTrailing30.length,
        complaintsPrevious30: isoPrevious30.length,
        refundCostTrailing30: roundMetric(isoCostTrailing30, 0),
        refundCostPrevious30: roundMetric(isoCostPrevious30, 0),
        openComplaintsTrailing30: isoOpenTrailing30
      }
    },
    findings: findings.slice(0, 12)
  };
}

function formatFallbackAnalysis(snapshot) {
  const findings = Array.isArray(snapshot?.findings) ? snapshot.findings : [];
  if (!findings.length) {
    return [
      "No major operational faults were detected from the current snapshot.",
      "",
      "Current checks reviewed:",
      `- Production last completed week tons: ${snapshot?.snapshots?.production?.lastCompletedWeekTons ?? 0}`,
      `- Shipping last completed week tons: ${snapshot?.snapshots?.shipping?.lastCompletedWeekTons ?? 0}`,
      `- Complaints last 30 days: ${snapshot?.snapshots?.quality?.complaintsTrailing30 ?? 0}`
    ].join("\n");
  }

  return findings.map((item, index) =>
    `${index + 1}. [${String(item.severity || "info").toUpperCase()}] ${item.title}\n${item.detail}`
  ).join("\n\n");
}

const sendSubmissionNotification = async ({
  formKey,
  formLabel,
  submittedBy,
  submittedAt,
  dimensions,
  metrics,
  notes,
  payload,
  recipients
}) => {
  const targetRecipients = Array.isArray(recipients) && recipients.length > 0
    ? recipients.map((value) => String(value).trim().toLowerCase()).filter(Boolean)
    : PRO_FORMS_RECIPIENTS;

  if (!RESEND_API_KEY || !PRO_FORMS_FROM_EMAIL || targetRecipients.length === 0) {
    return {
      sent: false,
      reason: "Email notifications skipped because RESEND_API_KEY, PRO_FORMS_FROM_EMAIL, or recipients were not configured."
    };
  }

  if (typeof fetch !== "function") {
    return {
      sent: false,
      reason: "Global fetch is not available in this Node runtime."
    };
  }

  const subject = `[CSP Pro] ${formLabel} submitted`;
  const html = buildSubmissionEmail({ formKey, formLabel, submittedBy, submittedAt, dimensions, metrics, notes, payload });

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      from: PRO_FORMS_FROM_EMAIL,
      to: targetRecipients,
      subject,
      html
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(errorText || "Email provider rejected the notification request.");
  }

  const providerPayload = await response.json().catch(() => ({}));
  return {
    sent: true,
    recipients: targetRecipients,
    providerId: providerPayload?.id || null
  };
};

const EMAIL_PREVIEW_SAMPLES = {
  shift_report: {
    formKey: "shift_report",
    formLabel: "Shift Report",
    submittedBy: "operator@coilsteelprocessing.com",
    dimensions: {
      submission_date: "2026-05-01",
      report_date: "2026-05-01",
      operator: "Operator 1",
      shift: "1st",
      had_downtime: "Yes",
      maintenance_tech: "Maintenance Tech 1"
    },
    metrics: {
      hours_worked: 8,
      tons: 124.35,
      linear_feet: 18500,
      stroke_count: 420,
      total_coils_ran: 18,
      planned_downtime_minutes: 15,
      unplanned_downtime_minutes: 22,
      total_downtime_minutes: 37
    },
    notes: "Line ran well after the coil change. Watch the entry sensor on the next shift.",
    payload: {
      plannedDowntimeDetails: "Scheduled coil change.",
      unplannedDowntimeDetails: "Entry sensor adjustment.",
      maintenanceTech: "Maintenance Tech 1",
      skippedOrders: [
        { skippedOrderNumber: "WO-10482", skippedOrderReason: "Material not staged." }
      ]
    }
  },
  forklift_inspection: {
    formKey: "forklift_inspection",
    formLabel: "Forklift Inspection",
    submittedBy: "inspector@coilsteelprocessing.com",
    dimensions: {
      submission_date: "2026-05-01",
      inspection_date: "2026-05-01",
      inspector_name: "Alex Inspector",
      location: "Plant 1",
      forklift_number: "FL-07",
      asset_name: "Plant 1 Forklift FL-07"
    },
    metrics: {
      total_checks: 12,
      passed_checks: 11,
      failed_checks: 1,
      maintenance_orders_opened: 1
    },
    notes: "Do not use until maintenance checks the brake pedal.",
    payload: {
      checks: [
        { key: "horn", label: "Horn", status: "Pass", notes: "" },
        { key: "brakes", label: "Brakes", status: "Fail", notes: "Brake pedal feels soft." }
      ]
    }
  },
  crane_inspection: {
    formKey: "crane_inspection",
    formLabel: "Crane Inspection",
    submittedBy: "inspector@coilsteelprocessing.com",
    dimensions: {
      submission_date: "2026-05-01",
      inspection_date: "2026-05-01",
      inspector_name: "Alex Inspector",
      crane_name: "Crane 3",
      asset_name: "Crane 3"
    },
    metrics: {
      total_checks: 10,
      passed_checks: 9,
      failed_checks: 1,
      maintenance_orders_opened: 1
    },
    notes: "Operator notified supervisor after inspection.",
    payload: {
      answers: [
        { key: "controls", label: "Controls", status: "Pass", notes: "" },
        { key: "hook_condition", label: "Hook condition", status: "Fail", notes: "Safety latch is sticking." }
      ]
    }
  },
  operational_inspection: {
    formKey: "operational_inspection",
    formLabel: "Operational Inspection",
    submittedBy: "operator@coilsteelprocessing.com",
    dimensions: {
      submission_date: "2026-05-01",
      check_date: "2026-05-01",
      inspector_name: "Alex Operator",
      area: "RBI",
      asset_name: "RBI",
      current_psi: 108
    },
    metrics: {
      total_checks: 8,
      clear_checks: 7,
      issue_checks: 1,
      current_psi: 108,
      maintenance_orders_opened: 1
    },
    notes: "Pressure is still within operating range, but lower than usual.",
    payload: {
      checks: [
        { key: "air_pressure", label: "Air pressure", status: "Yes", isIssue: false, notes: "" },
        { key: "leaks", label: "Visible leaks", status: "Yes", isIssue: true, notes: "Small leak near regulator." }
      ]
    }
  }
};

const sendTeamsMaintenanceNotification = async ({ req, order }) => {
  if (!PRO_MAINTENANCE_TEAMS_WEBHOOK_URL) {
    return {
      sent: false,
      reason: "Teams notification skipped because PRO_MAINTENANCE_TEAMS_WEBHOOK_URL is not configured."
    };
  }

  if (typeof fetch !== "function") {
    return {
      sent: false,
      reason: "Global fetch is not available in this Node runtime."
    };
  }

  const formUrl = `${getExternalBaseUrl(req)}/api/pro/maintenance-orders/${encodeURIComponent(order.public_token)}/form`;
  const facts = [
    { name: "Order", value: order.order_code },
    { name: "Asset", value: order.asset_name || "Unknown asset" },
    { name: "Issue", value: order.source_item_label || order.issue_category || "Inspection failure" },
    { name: "Reported By", value: order.reported_by_name || order.reported_by_email || "Unknown" },
    { name: "Reported At", value: order.reported_at || new Date().toISOString() },
    { name: "Priority", value: order.priority || "high" }
  ];

  const response = await fetch(PRO_MAINTENANCE_TEAMS_WEBHOOK_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      "@type": "MessageCard",
      "@context": "https://schema.org/extensions",
      summary: `Open maintenance order ${order.order_code}`,
      themeColor: "C0392B",
      title: `[CSP Pro] Open maintenance order ${order.order_code}`,
      sections: [
        {
          activityTitle: order.form_label || order.form_key || "Production form failure",
          activitySubtitle: order.source_item_key || "",
          facts,
          text: order.issue_notes || "No failure notes were provided."
        }
      ],
      potentialAction: [
        {
          "@type": "OpenUri",
          name: "Open maintenance form",
          targets: [{ os: "default", uri: formUrl }]
        }
      ]
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(errorText || "Teams rejected the maintenance notification.");
  }

  return {
    sent: true,
    maintenanceFormUrl: formUrl
  };
};

const renderMaintenanceOrderForm = (order, { message = "", error = "" } = {}) => {
  const isCompleted = order.status === "completed";
  const title = isCompleted ? "Maintenance Order Completed" : "Maintenance Response";
  const statusColor = isCompleted ? "#067647" : order.status === "acknowledged" ? "#b7791f" : "#b42318";

  return `
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>${escapeHtml(title)}</title>
      <style>
        :root { --navy:#172742; --steel:#233658; --gold:#f1a91e; --line:#d7e1ef; --mist:#eff4fb; --error:#b42318; --success:#067647; }
        * { box-sizing: border-box; }
        body { margin:0; font-family: Arial, sans-serif; background: var(--mist); color: var(--navy); }
        .wrap { max-width: 820px; margin: 0 auto; padding: 24px 14px 42px; }
        .header { background: var(--navy); color: #fff; padding: 22px; border-radius: 14px 14px 0 0; }
        .header small { display:block; color: var(--gold); text-transform: uppercase; letter-spacing: .14em; font-weight: 700; font-size: 11px; }
        .header h1 { margin: 8px 0 0; font-size: 28px; line-height: 1.15; }
        .panel { background: #fff; border: 1px solid var(--line); border-top: 0; border-radius: 0 0 14px 14px; padding: 22px; }
        .status { display:inline-block; padding: 6px 10px; border-radius: 999px; background: #f7faff; color: ${statusColor}; font-weight: 800; text-transform: uppercase; letter-spacing: .08em; font-size: 11px; }
        .grid { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; margin: 18px 0; }
        .card { border:1px solid var(--line); border-radius: 12px; padding: 13px; background:#f7faff; }
        .label { font-size: 11px; text-transform: uppercase; letter-spacing: .08em; color:#61708a; font-weight:700; margin-bottom:5px; }
        .value { font-size: 15px; font-weight: 800; color: var(--steel); line-height: 1.35; }
        label { display:block; margin: 14px 0 6px; font-weight: 800; color: var(--steel); }
        input, textarea, select { width:100%; border:1px solid #cfd8e8; border-radius: 8px; padding: 11px 12px; font: inherit; color: var(--navy); background:#fff; }
        textarea { min-height: 108px; resize: vertical; }
        .actions { display:flex; gap: 10px; flex-wrap: wrap; margin-top: 18px; }
        button { border:0; border-radius: 8px; padding: 12px 16px; font-weight: 800; cursor:pointer; }
        .complete { background: var(--success); color:#fff; }
        .ack { background: var(--gold); color: var(--navy); }
        .msg { margin: 0 0 16px; padding: 12px; border-radius: 10px; font-weight: 700; }
        .msg.success { background:#ecfdf3; color:#067647; border:1px solid #abefc6; }
        .msg.error { background:#fef3f2; color:#b42318; border:1px solid #fecdca; }
        @media (max-width: 640px) {
          .wrap { padding: 0 0 28px; }
          .header, .panel { border-radius: 0; }
          .grid { grid-template-columns: 1fr; }
          .actions button { width: 100%; }
        }
      </style>
    </head>
    <body>
      <main class="wrap">
        <section class="header">
          <small>CSP Pro Maintenance</small>
          <h1>${escapeHtml(order.order_code || "Maintenance Order")}</h1>
        </section>
        <section class="panel">
          ${message ? `<p class="msg success">${escapeHtml(message)}</p>` : ""}
          ${error ? `<p class="msg error">${escapeHtml(error)}</p>` : ""}
          <span class="status">${escapeHtml(order.status || "open")}</span>
          <div class="grid">
            <div class="card"><div class="label">Asset</div><div class="value">${formatEmailValue(order.asset_name, "Unknown asset")}</div></div>
            <div class="card"><div class="label">Issue</div><div class="value">${formatEmailValue(order.source_item_label || order.issue_category, "Inspection failure")}</div></div>
            <div class="card"><div class="label">Reported By</div><div class="value">${formatEmailValue(order.reported_by_name || order.reported_by_email, "Unknown")}</div></div>
            <div class="card"><div class="label">Reported At</div><div class="value">${formatEmailValue(order.reported_at, "-")}</div></div>
          </div>
          <div class="card">
            <div class="label">Failure Notes</div>
            <div class="value">${formatEmailValue(order.issue_notes, "No notes provided.")}</div>
          </div>

          <form method="post" action="/api/pro/maintenance-orders/${encodeURIComponent(order.public_token)}/form">
            <label for="completedBy">Maintenance Tech</label>
            <input id="completedBy" name="completedBy" type="text" value="${escapeHtml(order.completed_by || order.acknowledged_by || "")}" required />

            <label for="correctiveAction">Corrective Action</label>
            <textarea id="correctiveAction" name="correctiveAction" placeholder="What was done to correct or inspect the issue?">${escapeHtml(order.corrective_action || "")}</textarea>

            <label for="completionNotes">Maintenance Notes</label>
            <textarea id="completionNotes" name="completionNotes" placeholder="Add details, follow-up needs, or reason for acknowledge-only.">${escapeHtml(order.completion_notes || "")}</textarea>

            <label for="partsUsed">Parts Used</label>
            <input id="partsUsed" name="partsUsed" type="text" value="${escapeHtml(order.parts_used || "")}" placeholder="None, fuse, sensor, hose..." />

            <label for="downtimeMinutes">Maintenance Downtime Minutes</label>
            <input id="downtimeMinutes" name="downtimeMinutes" type="number" min="0" step="1" value="${escapeHtml(order.downtime_minutes || 0)}" />

            <div class="actions">
              <button class="complete" type="submit" name="action" value="complete">Complete Order</button>
              <button class="ack" type="submit" name="action" value="acknowledge">Acknowledge Only</button>
            </div>
          </form>
        </section>
      </main>
    </body>
    </html>
  `;
};

app.post("/api/create-user", async (req, res) => {
  const { email, password, roles } = req.body || {};

  if (!email || !password || !Array.isArray(roles) || roles.length === 0) {
    return res.status(400).json({ message: "Email, password, and roles are required." });
  }

  const cleanEmail = String(email).trim().toLowerCase();

  try {
    const resolvedRoleIds = await resolveRoleIds(roles);
    if (resolvedRoleIds.length === 0) {
      return res.status(400).json({ message: "No valid roles were selected." });
    }

    // 1️⃣ Create the user in Supabase Auth
    const { data: authUser, error: authError } = await supabase.auth.admin.createUser({
      email: cleanEmail,
      password,
      email_confirm: true,
    });

    if (authError || !authUser?.user) {
      console.error("Auth user creation failed:", authError);
      return res.status(400).json({ message: authError?.message || "Error creating auth user." });
    }

    // 2️⃣ Insert user record in 'users' table
    const { data: user, error: userError } = await supabase
      .from("users")
      .insert({
        id: authUser.user.id,
        email: cleanEmail
      })
      .select()
      .single();

    if (userError || !user) {
      console.error("User insert failed:", userError);
      return res.status(400).json({ message: userError?.message || "Error inserting user record." });
    }

    // 3️⃣ Assign roles
    const roleRows = resolvedRoleIds.map((role_id) => ({
      user_id: user.id,
      role_id,
    }));

    const { error: rolesError } = await supabase.from("user_roles").insert(roleRows);

    if (rolesError) {
      console.error("Role assignment failed:", rolesError);
      return res.status(400).json({ message: rolesError.message });
    }

    console.log(`✅ Created user ${email} with roles ${resolvedRoleIds.join(", ")}`);
    return res.status(200).json({ ok: true });
  } catch (err) {
    console.error("Unexpected error in create-user:", err);
    return res.status(500).json({ message: "Unexpected server error." });
  }
});

// --- USER MANAGEMENT ENDPOINTS ---

// Get all users with roles
app.get("/api/users", async (req, res) => {
  try {
    const { data, error } = await supabase
      .from("users")
      .select(`
        id,
        email,
        last_login,
        csv_download_count,
        user_roles (
          role_id,
          roles (name)
        )
      `);

    if (error) {
      console.error("User fetch failed:", error);
      return res.status(500).json({ error: error.message });
    }

    return res.json(data || []);
  } catch (err) {
    console.error("User endpoint error:", err);
    return res.status(500).json({ error: "Failed to fetch users" });
  }
});

// Delete user (removes roles + auth user)
app.delete("/api/users/:id", async (req, res) => {
  const { id } = req.params;

  try {
    // Delete role mappings first
    await supabase.from("user_roles").delete().eq("user_id", id);

    // Delete from users table
    await supabase.from("users").delete().eq("id", id);

    // Delete from Supabase Auth
    await supabase.auth.admin.deleteUser(id);

    return res.json({ success: true });
  } catch (err) {
    console.error("Delete user error:", err);
    return res.status(500).json({ error: "Failed to delete user" });
  }
});

// Update user roles
app.put("/api/users/:id/roles", async (req, res) => {
  const { id } = req.params;
  const { roles } = req.body || {};

  if (!Array.isArray(roles)) {
    return res.status(400).json({ error: "Roles must be an array" });
  }

  try {
    const resolvedRoleIds = await resolveRoleIds(roles);

    // Remove existing roles
    await supabase.from("user_roles").delete().eq("user_id", id);

    // Insert new roles
    const roleRows = resolvedRoleIds.map((role_id) => ({
      user_id: id,
      role_id,
    }));

    if (roleRows.length > 0) {
      await supabase.from("user_roles").insert(roleRows);
    }

    return res.json({ success: true });
  } catch (err) {
    console.error("Update roles error:", err);
    return res.status(500).json({ error: "Failed to update roles" });
  }
});

const buildTempPassword = (length = 12) => {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789!@#$%^&*";
  let value = "";
  for (let i = 0; i < length; i += 1) {
    const idx = Math.floor(Math.random() * chars.length);
    value += chars[idx];
  }
  return value;
};

const getUserAuthMetadata = async (id) => {
  const { data, error } = await supabase.auth.admin.getUserById(id);
  if (error || !data?.user) {
    throw new Error(error?.message || "User not found in auth.");
  }
  return data.user.user_metadata || {};
};

app.post("/api/users/:id/reset-password-temp", async (req, res) => {
  const { id } = req.params;
  const provided = String(req.body?.tempPassword || "").trim();
  const tempPassword = provided || buildTempPassword(14);

  if (tempPassword.length < 8) {
    return res.status(400).json({ error: "Temporary password must be at least 8 characters." });
  }

  try {
    const existingMetadata = await getUserAuthMetadata(id);
    const nextMetadata = { ...existingMetadata, force_password_change: true };

    const { error } = await supabase.auth.admin.updateUserById(id, {
      password: tempPassword,
      user_metadata: nextMetadata
    });

    if (error) {
      console.error("Temp password reset failed:", error);
      return res.status(400).json({ error: error.message || "Failed to reset password." });
    }

    return res.json({
      success: true,
      tempPassword,
      forcePasswordChange: true
    });
  } catch (err) {
    console.error("Temp password endpoint error:", err);
    return res.status(500).json({ error: err.message || "Failed to reset temporary password." });
  }
});

app.post("/api/users/:id/force-password-change", async (req, res) => {
  const { id } = req.params;
  const force = req.body?.force !== false;

  try {
    const existingMetadata = await getUserAuthMetadata(id);
    const nextMetadata = { ...existingMetadata, force_password_change: !!force };

    const { error } = await supabase.auth.admin.updateUserById(id, {
      user_metadata: nextMetadata
    });

    if (error) {
      console.error("Force password change update failed:", error);
      return res.status(400).json({ error: error.message || "Failed to update force-change flag." });
    }

    return res.json({ success: true, force_password_change: !!force });
  } catch (err) {
    console.error("Force password change endpoint error:", err);
    return res.status(500).json({ error: err.message || "Failed to update force-change flag." });
  }
});

app.post("/api/users/:id/send-reset-email", async (req, res) => {
  const { id } = req.params;
  const redirectTo =
    String(req.body?.redirectTo || "").trim() ||
    "https://bi.coilsteelprocessing.com/reset-password.html";

  try {
    const { data: user, error: userError } = await supabase
      .from("users")
      .select("email")
      .eq("id", id)
      .single();

    if (userError || !user?.email) {
      console.error("Reset email lookup failed:", userError);
      return res.status(404).json({ error: "User email not found." });
    }

    const { error: resetError } = await supabase.auth.resetPasswordForEmail(user.email, {
      redirectTo
    });

    if (resetError) {
      console.error("Reset email send failed:", resetError);
      return res.status(400).json({ error: resetError.message || "Failed to send reset email." });
    }

    return res.json({ success: true, email: user.email });
  } catch (err) {
    console.error("Send reset email endpoint error:", err);
    return res.status(500).json({ error: err.message || "Failed to send reset email." });
  }
});

app.get("/api/pro/maintenance-orders/:publicToken/acknowledge", async (req, res) => {
  const publicToken = coerceText(req.params.publicToken, 160);

  if (!publicToken) {
    return res.status(400).send("Missing maintenance order token.");
  }

  try {
    const { data: order, error: lookupError } = await chartSupabase
      .from("pro_maintenance_orders")
      .select("id,order_code,status,asset_name,source_item_label")
      .eq("public_token", publicToken)
      .maybeSingle();

    if (lookupError) {
      throw lookupError;
    }

    if (!order) {
      return res.status(404).send("Maintenance order not found.");
    }

    if (order.status === "open") {
      const { error: updateError } = await chartSupabase
        .from("pro_maintenance_orders")
        .update({
          status: "acknowledged",
          acknowledged_at: new Date().toISOString(),
          acknowledged_by: coerceText(req.query.by || "Maintenance Team", 160),
          acknowledged_via: "teams_link"
        })
        .eq("id", order.id);

      if (updateError) {
        throw updateError;
      }
    }

    return res.send(`
      <!DOCTYPE html>
      <html lang="en">
      <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Maintenance Order Acknowledged</title>
        <style>
          body { font-family: Arial, sans-serif; margin: 0; padding: 32px 20px; background: #f4f7fb; color: #172742; }
          .card { max-width: 640px; margin: 0 auto; background: #fff; border: 1px solid #d7e1ef; border-radius: 18px; padding: 28px; box-shadow: 0 18px 36px rgba(15,31,55,0.08); }
          h1 { margin: 0 0 12px; font-size: 28px; }
          p { margin: 0 0 10px; line-height: 1.55; }
          .meta { margin-top: 18px; padding: 16px; border-radius: 14px; background: #f7faff; border: 1px solid #d7e1ef; }
        </style>
      </head>
      <body>
        <div class="card">
          <h1>Maintenance order acknowledged</h1>
          <p><strong>${order.order_code}</strong> is now marked as acknowledged${order.asset_name ? ` for ${order.asset_name}` : ""}.</p>
          <div class="meta">
            <p><strong>Status:</strong> acknowledged</p>
            <p><strong>Issue:</strong> ${order.source_item_label || "Inspection failure"}</p>
          </div>
        </div>
      </body>
      </html>
    `);
  } catch (err) {
    console.error("Maintenance order acknowledge route error:", err);
    return res.status(500).send("Unable to acknowledge maintenance order.");
  }
});

app.get("/api/pro/maintenance-orders/:publicToken/form", async (req, res) => {
  const publicToken = coerceText(req.params.publicToken, 160);

  if (!publicToken) {
    return res.status(400).send("Missing maintenance order token.");
  }

  try {
    const { data: order, error } = await chartSupabase
      .from("pro_maintenance_orders")
      .select("*")
      .eq("public_token", publicToken)
      .maybeSingle();

    if (error) throw error;
    if (!order) return res.status(404).send("Maintenance order not found.");

    return res.status(200).type("html").send(renderMaintenanceOrderForm(order));
  } catch (err) {
    console.error("Maintenance order form route error:", err);
    return res.status(500).send("Unable to load maintenance order form.");
  }
});

app.post("/api/pro/maintenance-orders/:publicToken/form", async (req, res) => {
  const publicToken = coerceText(req.params.publicToken, 160);
  const action = coerceText(req.body?.action, 40);
  const completedBy = coerceText(req.body?.completedBy || req.body?.completed_by || "Maintenance Team", 160);
  const correctiveAction = coerceText(req.body?.correctiveAction || req.body?.corrective_action, 5000);
  const completionNotes = coerceText(req.body?.completionNotes || req.body?.completion_notes, 5000);
  const partsUsed = coerceText(req.body?.partsUsed || req.body?.parts_used, 1000);
  const downtimeMinutes = coerceInteger(req.body?.downtimeMinutes || req.body?.downtime_minutes) || 0;
  const now = new Date().toISOString();

  if (!publicToken) {
    return res.status(400).send("Missing maintenance order token.");
  }

  try {
    const updateRow = action === "complete"
      ? {
          status: "completed",
          acknowledged_at: now,
          acknowledged_by: completedBy,
          acknowledged_via: "maintenance_form",
          completed_at: now,
          completed_by: completedBy,
          completed_via: "maintenance_form",
          corrective_action: correctiveAction || null,
          completion_notes: completionNotes || null,
          parts_used: partsUsed || null,
          downtime_minutes: downtimeMinutes
        }
      : {
          status: "acknowledged",
          acknowledged_at: now,
          acknowledged_by: completedBy,
          acknowledged_via: "maintenance_form",
          corrective_action: correctiveAction || null,
          completion_notes: completionNotes || null,
          parts_used: partsUsed || null,
          downtime_minutes: downtimeMinutes
        };

    const { data: order, error } = await chartSupabase
      .from("pro_maintenance_orders")
      .update(updateRow)
      .eq("public_token", publicToken)
      .select("*")
      .maybeSingle();

    if (error) throw error;
    if (!order) return res.status(404).send("Maintenance order not found.");

    return res.status(200).type("html").send(renderMaintenanceOrderForm(order, {
      message: action === "complete"
        ? "Maintenance order completed."
        : "Maintenance order acknowledged."
    }));
  } catch (err) {
    console.error("Maintenance order form submit error:", err);
    return res.status(500).send("Unable to save maintenance response.");
  }
});

app.post("/api/pro/maintenance-orders/:publicToken/acknowledge", async (req, res) => {
  const publicToken = coerceText(req.params.publicToken, 160);
  const acknowledgedBy = coerceText(req.body?.acknowledgedBy || req.body?.acknowledged_by || "Maintenance Team", 160);

  if (!publicToken) {
    return res.status(400).json({ error: "Missing maintenance order token." });
  }

  try {
    const { data: order, error: updateError } = await chartSupabase
      .from("pro_maintenance_orders")
      .update({
        status: "acknowledged",
        acknowledged_at: new Date().toISOString(),
        acknowledged_by: acknowledgedBy,
        acknowledged_via: coerceText(req.body?.acknowledgedVia || req.body?.acknowledged_via || "api", 120)
      })
      .eq("public_token", publicToken)
      .select("id,order_code,status,acknowledged_at,acknowledged_by")
      .maybeSingle();

    if (updateError) {
      throw updateError;
    }

    if (!order) {
      return res.status(404).json({ error: "Maintenance order not found." });
    }

    return res.json({ success: true, order });
  } catch (err) {
    console.error("Maintenance order acknowledge API error:", err);
    return res.status(500).json({ error: err.message || "Unable to acknowledge maintenance order." });
  }
});

app.post("/api/pro/forms/submit", async (req, res) => {
  const body = req.body || {};
  const formKey = coerceText(body.formKey || body.form_key, 120);
  const formLabel = coerceText(body.formLabel || body.form_label || formKey, 160);
  const submittedBy = coerceText(body.submittedBy || body.submitted_by_email, 320).toLowerCase();
  const submittedAt = new Date().toISOString();
  const dimensions = sanitizePlainObject(body.dimensions);
  const metrics = sanitizePlainObject(body.metrics);
  const payload = sanitizePlainObject(body.payload);
  const notes = coerceText(body.notes, 5000);
  const rawChartRows = Array.isArray(body.chartRows) ? body.chartRows : [];
  const rawMaintenanceOrders = Array.isArray(body.maintenanceOrders) ? body.maintenanceOrders : [];
  const recipients = Array.isArray(body.notificationRecipients)
    ? body.notificationRecipients
    : Array.isArray(body.recipients)
      ? body.recipients
      : [];

  if (!formKey) {
    return res.status(400).json({ error: "formKey is required." });
  }

  if (!submittedBy) {
    return res.status(400).json({ error: "submittedBy is required." });
  }

  try {
    const submissionRow = {
      form_key: formKey,
      form_label: formLabel || formKey,
      submitted_at: submittedAt,
      submitted_by_email: submittedBy,
      submission_date: coerceText(dimensions.submission_date || dimensions.production_date || "", 20) || null,
      shift: coerceText(dimensions.shift, 80) || null,
      department: coerceText(dimensions.department, 120) || null,
      line: coerceText(dimensions.line, 120) || null,
      payload,
      dimensions,
      metrics,
      notes: notes || null
    };

    const { data: submission, error: submissionError } = await chartSupabase
      .from("pro_form_submissions")
      .insert(submissionRow)
      .select("id")
      .single();

    if (submissionError || !submission?.id) {
      console.error("Pro submission insert failed:", submissionError);
      return res.status(400).json({ error: submissionError?.message || "Failed to store form submission." });
    }

    const formSpecificSubmission = buildFormSpecificSubmission({
      submissionId: submission.id,
      formKey,
      submittedAt,
      submittedBy,
      dimensions,
      metrics,
      payload,
      notes
    });

    if (formSpecificSubmission) {
      const { error: formSpecificError } = await chartSupabase
        .from(formSpecificSubmission.table)
        .insert(formSpecificSubmission.row);

      if (formSpecificError) {
        console.error("Pro form-specific insert failed:", formSpecificError);
        return res.status(400).json({ error: formSpecificError.message || "Failed to store form-specific submission." });
      }
    }

    const chartRows = rawChartRows
      .filter((row) => row && typeof row === "object")
      .map((row) => ({
        submission_id: submission.id,
        form_key: formKey,
        chart_name: coerceText(row.chart_name || row.chartName, 160),
        chart_date: coerceText(row.chart_date || row.chartDate || dimensions.submission_date || dimensions.production_date || "", 20) || null,
        chart_shift: coerceText(row.chart_shift || row.chartShift || dimensions.shift, 80) || null,
        chart_department: coerceText(row.chart_department || row.chartDepartment || dimensions.department, 120) || null,
        chart_line: coerceText(row.chart_line || row.chartLine || dimensions.line, 120) || null,
        chart_series: coerceText(row.chart_series || row.chartSeries, 120) || null,
        chart_metric: coerceText(row.chart_metric || row.chartMetric, 120) || null,
        chart_bucket: coerceText(row.chart_bucket || row.chartBucket, 160) || null,
        chart_value: coerceNumber(row.chart_value ?? row.chartValue),
        payload: sanitizePlainObject(row.payload),
        submitted_at: submittedAt,
        submitted_by_email: submittedBy
      }))
      .filter((row) => row.chart_name && row.chart_metric && row.chart_value !== null);

    if (chartRows.length > 0) {
      const { error: chartRowsError } = await chartSupabase
        .from("pro_form_chart_rows")
        .insert(chartRows);

      if (chartRowsError) {
        console.error("Pro chart rows insert failed:", chartRowsError);
        return res.status(400).json({ error: chartRowsError.message || "Failed to store chart rows." });
      }
    }

    const maintenanceOrders = rawMaintenanceOrders
      .filter((row) => row && typeof row === "object")
      .map((row) => ({
        submission_id: submission.id,
        form_key: formKey,
        form_label: formLabel || formKey,
        order_code: buildMaintenanceOrderCode(),
        public_token: crypto.randomUUID(),
        status: coerceText(row.status, 40) || "open",
        priority: coerceText(row.priority, 40) || "high",
        asset_name: coerceText(row.asset_name || row.assetName || dimensions.asset_name || dimensions.crane_name, 160) || null,
        issue_category: coerceText(row.issue_category || row.issueCategory || "inspection_failure", 160) || null,
        source_item_key: coerceText(row.source_item_key || row.sourceItemKey, 160) || null,
        source_item_label: coerceText(row.source_item_label || row.sourceItemLabel, 500) || null,
        issue_notes: coerceText(row.issue_notes || row.issueNotes, 5000) || null,
        reported_at: submittedAt,
        reported_by_email: submittedBy,
        reported_by_name: coerceText(row.reported_by_name || row.reportedByName || dimensions.inspector_name, 160) || null,
        submission_date: coerceText(row.submission_date || row.submissionDate || dimensions.submission_date || "", 20) || null,
        metadata: sanitizePlainObject(row.metadata)
      }))
      .filter((row) => row.source_item_label || row.issue_notes);

    let insertedMaintenanceOrders = [];
    if (maintenanceOrders.length > 0) {
      const { data: orderRows, error: orderError } = await chartSupabase
        .from("pro_maintenance_orders")
        .insert(maintenanceOrders)
        .select("id,order_code,public_token,status,priority,asset_name,source_item_key,source_item_label,issue_notes,reported_at,reported_by_email,reported_by_name,form_key,form_label");

      if (orderError) {
        console.error("Pro maintenance order insert failed:", orderError);
        return res.status(400).json({ error: orderError.message || "Failed to store maintenance orders." });
      }

      insertedMaintenanceOrders = orderRows || [];
    }

    let notification = { sent: false, reason: "No notification attempt was made." };
    try {
      notification = await sendSubmissionNotification({
        formKey,
        formLabel: formLabel || formKey,
        submittedBy,
        submittedAt,
        dimensions,
        metrics,
        notes,
        payload,
        recipients
      });
    } catch (notificationError) {
      console.error("Pro submission notification failed:", notificationError);
      notification = {
        sent: false,
        reason: notificationError.message || "Notification send failed."
      };
    }

    const maintenanceNotifications = [];
    for (const order of insertedMaintenanceOrders) {
      try {
        const teamsNotification = await sendTeamsMaintenanceNotification({ req, order });
        maintenanceNotifications.push({
          orderCode: order.order_code,
          ...teamsNotification
        });
      } catch (teamsError) {
        console.error("Teams maintenance notification failed:", teamsError);
        maintenanceNotifications.push({
          orderCode: order.order_code,
          sent: false,
          reason: teamsError.message || "Teams notification failed."
        });
      }
    }

    return res.json({
      success: true,
      submissionId: submission.id,
      formSpecificTable: formSpecificSubmission?.table || null,
      chartRowsInserted: chartRows.length,
      maintenanceOrdersCreated: insertedMaintenanceOrders.length,
      maintenanceOrders: insertedMaintenanceOrders,
      notification,
      maintenanceNotifications
    });
  } catch (err) {
    console.error("Pro forms submit endpoint error:", err);
    return res.status(500).json({ error: err.message || "Failed to submit form." });
  }
});

app.get("/api/pro/forms/email-preview/:formKey", (req, res) => {
  const formKey = coerceText(req.params.formKey, 120);
  const sample = EMAIL_PREVIEW_SAMPLES[formKey];

  if (!sample) {
    return res.status(404).send(`
      <div style="font-family:Arial,sans-serif;padding:24px;color:#172742;">
        <h2>Email preview not found</h2>
        <p>Use one of these form keys:</p>
        <ul>
          ${Object.keys(EMAIL_PREVIEW_SAMPLES).map((key) => `<li><code>${escapeHtml(key)}</code></li>`).join("")}
        </ul>
      </div>
    `);
  }

  const html = buildSubmissionEmail({
    ...sample,
    submittedAt: new Date().toISOString()
  });

  return res
    .status(200)
    .type("html")
    .send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>${escapeHtml(sample.formLabel)} Email Preview</title></head><body style="margin:0;">${html}</body></html>`);
});

app.get("/api/pro/forms/email-preview", (req, res) => {
  const links = Object.entries(EMAIL_PREVIEW_SAMPLES)
    .map(([key, sample]) => `
      <li style="margin:0 0 10px;">
        <a href="/api/pro/forms/email-preview/${escapeHtml(key)}" style="color:#2f61d3;font-weight:700;">${escapeHtml(sample.formLabel)}</a>
        <code style="margin-left:8px;color:#61708a;">${escapeHtml(key)}</code>
      </li>
    `)
    .join("");

  return res.status(200).type("html").send(`
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="UTF-8">
        <title>CSP Pro Email Previews</title>
      </head>
      <body style="margin:0;background:#eff4fb;font-family:Arial,sans-serif;color:#172742;">
        <main style="max-width:720px;margin:0 auto;padding:32px 18px;">
          <h1 style="margin:0 0 10px;">CSP Pro Email Previews</h1>
          <p style="margin:0 0 22px;color:#61708a;">Open a template below to view the sample email HTML used by the backend.</p>
          <ul style="margin:0;padding-left:20px;">${links}</ul>
        </main>
      </body>
    </html>
  `);
});

app.post("/api/ai-chart", async (req, res) => {
  try {
    const openai = getOpenAIClient();
    if (!openai) return res.status(503).json({ error: "AI not configured on this server." });
    const prompt = req.body.prompt || "";
    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: "You are the CSP BI Assistant. Help users explore data but do not output SQL." },
        { role: "user", content: prompt }
      ]
    });
    res.json({ response: completion.choices[0].message.content });
  } catch (err) {
    console.error("AI endpoint error:", err);
    res.status(500).json({ error: "AI request failed" });
  }
});

app.post("/api/ai-analyze", async (req, res) => {
  try {
    const focus = String(req.body.focus || "").trim();

    const today = startOfDay(new Date());
    const productionWindowStart = toYmd(addDays(today, -120));
    const shippingWindowStart = toYmd(addDays(today, -120));
    const isoWindowStart = toYmd(addDays(today, -210));

    const [productionRows, shippingRows, isoRows] = await Promise.all([
      fetchChartRows(
        "psdata_production_tags_api",
        "processing_start_date,tag_tons,tons_per_hour,days_to_close,machine_label,customer_number",
        {
          gteColumn: "processing_start_date",
          gte: productionWindowStart,
          orderBy: "processing_start_date",
          ascending: false,
          limit: 40000
        }
      ),
      fetchChartRows(
        "psdata_loads_api",
        "shipDate,ship_date,weight,customer_no,ship_to_customer_name,carrier_number,cancel_load,bol_number,master_bol_number",
        {
          gteColumn: "shipDate",
          gte: shippingWindowStart,
          orderBy: "shipDate",
          ascending: false,
          limit: 40000
        }
      ),
      fetchChartRows(
        "v_iso_complaints",
        "log_number,date_entered,date_closed,status,customer,complaint_type,cost",
        {
          gteColumn: "date_entered",
          gte: isoWindowStart,
          orderBy: "date_entered",
          ascending: false,
          limit: 15000
        }
      )
    ]);

    const snapshot = buildOperationalSnapshot({
      productionRows,
      shippingRows,
      isoRows,
      focus
    });

    let summary = formatFallbackAnalysis(snapshot);
    const openai = getOpenAIClient();

    if (openai) {
      const completion = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        temperature: 0.2,
        messages: [
          {
            role: "system",
            content:
              "You are the CSP BI operations analyst. Review operational data and identify inefficiencies, productivity concerns, customer demand drops, and quality faults. Be concrete, concise, and actionable. Do not mention SQL or speculate beyond the metrics provided."
          },
          {
            role: "user",
            content: [
              focus ? `Focus area: ${focus}` : "Focus area: overall operations",
              "Write a concise analysis with:",
              "1. a short overall takeaway sentence",
              "2. up to 6 bullets ordered by severity",
              "3. a closing sentence on what to watch next",
              "",
              `Operational snapshot:\n${JSON.stringify(snapshot, null, 2)}`
            ].join("\n")
          }
        ]
      });

      const aiText = completion?.choices?.[0]?.message?.content;
      if (String(aiText || "").trim()) {
        summary = aiText.trim();
      }
    }

    return res.json({
      summary,
      findings: snapshot.findings,
      snapshot: snapshot.snapshots,
      focus: snapshot.focus,
      generatedAt: snapshot.generatedAt
    });
  } catch (err) {
    console.error("AI analyze endpoint error:", err);
    return res.status(500).json({ error: err.message || "AI analysis request failed." });
  }
});

// Production chart data endpoint for frontend chart rebuild pages
app.get("/api/chart-data", async (req, res) => {
  const allowedTables = new Set(["psdata_loads", "psdata_loads_api", "psdata_iso_complaints"]);
  const table = String(req.query.table || "psdata_loads_api").trim();
  const shipDateColumn = table === "psdata_loads_api" ? "shipDate" : "ship_date";
  const limitRaw = Number(req.query.limit || 3000);
  const limit = Math.min(Math.max(Number.isFinite(limitRaw) ? limitRaw : 3000, 50), 250000);
  const select = String(req.query.select || "*").trim() || "*";
  const order = String(req.query.order || "").trim();
  const shipDateGte = String(req.query.ship_date_gte || "").trim();
  const shipDateLt = String(req.query.ship_date_lt || "").trim();
  const shipDateLte = String(req.query.ship_date_lte || "").trim();

  if (!allowedTables.has(table)) {
    return res.status(400).json({ error: `Table not allowed: ${table}` });
  }

  try {
    const batchSize = 1000;
    const rows = [];
    let from = 0;

    while (from < limit) {
      const to = Math.min(from + batchSize - 1, limit - 1);
      let query = chartSupabase.from(table).select(select);

      if (table !== "psdata_iso_complaints") {
        if (shipDateGte) query = query.gte(shipDateColumn, shipDateGte);
        if (shipDateLt) query = query.lt(shipDateColumn, shipDateLt);
        if (shipDateLte) query = query.lte(shipDateColumn, shipDateLte);
      }

      if (order) {
        const [fieldRaw, dirRaw = "asc"] = order.split(".");
        const field = String(fieldRaw || "").trim();
        const ascending = String(dirRaw || "asc").trim().toLowerCase() !== "desc";
        if (/^[a-z_][a-z0-9_]*$/i.test(field)) {
          query = query.order(field, { ascending });
        }
      }

      const { data, error } = await query.range(from, to);

      if (error) {
        console.error("Chart data query failed:", error);
        return res.status(400).json({ error: error.message || "Chart data query failed." });
      }

      const page = Array.isArray(data) ? data : [];
      rows.push(...page);
      if (page.length < batchSize) break;
      from += batchSize;
    }

    return res.json({
      table,
      limit,
      order: order || null,
      ship_date_gte: shipDateGte || null,
      ship_date_lt: shipDateLt || null,
      ship_date_lte: shipDateLte || null,
      count: rows.length,
      rows
    });
  } catch (err) {
    console.error("Chart data endpoint error:", err);
    return res.status(500).json({ error: err.message || "Chart data endpoint failed." });
  }
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Tableau Auth Server running on port ${PORT}`);
});

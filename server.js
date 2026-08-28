const path = require("path");
const dotenv = require("dotenv");

dotenv.config();
if (!process.env.OPENAI_API_KEY) {
  dotenv.config({ path: path.resolve(__dirname, "../../cap-ai-server/ai-server/.env") });
}

const express = require("express");
const jwt = require("jsonwebtoken");
const cors = require("cors");
const crypto = require("crypto");
const fs = require("fs");
const OpenAI = require("openai");
function getOpenAIClient() {
  const key = process.env.OPENAI_API_KEY;
  if (!key) return null;
  return new OpenAI({ apiKey: key });
}

function getRequiredEnv(name) {
  const value = String(process.env[name] || "").trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function getOptionalEnv(name) {
  return String(process.env[name] || "").trim();
}

const app = express();
app.use(cors({
  origin: "*",
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
}));
app.use(express.json({ limit: "90mb" }));
app.use(express.urlencoded({ extended: false, limit: "90mb" }));

// Tableau Connected App Credentials are optional. The Tableau workflow is retired,
// so missing values must not prevent the auth/user-access server from starting.
const TABLEAU_CLIENT_ID = getOptionalEnv("TABLEAU_CLIENT_ID");
const TABLEAU_SECRET_ID = getOptionalEnv("TABLEAU_SECRET_ID");
const TABLEAU_SECRET_VALUE = getOptionalEnv("TABLEAU_SECRET_VALUE");
const TABLEAU_ENABLED = Boolean(TABLEAU_CLIENT_ID && TABLEAU_SECRET_ID && TABLEAU_SECRET_VALUE);

app.get("/", (req, res) => {
  res.send("Token server running");
});

app.get("/api/deploy-status", (_req, res) => {
  res.json({
    service: "cap-auth-server",
    roleUpdateMode: "hr-admin-v1",
    formSubmissionMode: "idempotent-v1",
    shiftReportDashboardMode: "current-week-shifts-v5",
    shiftReportAccessMode: "production-v1",
    driverSignupMode: "resend-otp-v4-dynamic-length",
    shippingAuthMode: "bi-master-v1",
    driverSignupConfigured: Boolean(
      process.env.RESEND_API_KEY
      && process.env.DRIVER_SUPABASE_URL
      && process.env.DRIVER_SUPABASE_SERVICE_ROLE_KEY
      && (process.env.SHIPPING_AUTH_FROM_EMAIL || process.env.PRO_FORMS_FROM_EMAIL)
    ),
    driverSignupDependencies: {
      resend: Boolean(process.env.RESEND_API_KEY),
      supabaseUrl: Boolean(process.env.DRIVER_SUPABASE_URL),
      supabaseServiceRole: Boolean(process.env.DRIVER_SUPABASE_SERVICE_ROLE_KEY),
      fromEmail: Boolean(process.env.SHIPPING_AUTH_FROM_EMAIL || process.env.PRO_FORMS_FROM_EMAIL)
    },
    node: process.version
  });
});

// Endpoint to generate Tableau Embed Token
app.get("/getTableauToken", (req, res) => {
  if (!TABLEAU_ENABLED) {
    return res.status(410).json({ error: "Tableau token generation is disabled." });
  }

  const now = Math.floor(Date.now() / 1000);
  const tableauUser = req.query.user || "todd@coilsteelprocessing.com";

  const payload = {
    iss: TABLEAU_CLIENT_ID,
    exp: now + 300,
    aud: "tableau",
    jti: crypto.randomUUID(),
    sub: tableauUser,
    scp: ["tableau:views:embed"]
  };

  const header = {
    kid: TABLEAU_SECRET_ID,
    alg: "HS256",
    iss: TABLEAU_CLIENT_ID
  };

  const token = jwt.sign(payload, TABLEAU_SECRET_VALUE, { algorithm: "HS256", header });

  res.json({ token });
});

// --- CREATE USER ENDPOINT ---
// Allows admin users to create new users and assign roles via Supabase
const { createClient } = require("@supabase/supabase-js");

const AUTH_SUPABASE_URL = getRequiredEnv("AUTH_SUPABASE_URL");
const AUTH_SERVICE_ROLE_KEY = getRequiredEnv("AUTH_SUPABASE_SERVICE_ROLE_KEY");
const CHART_SUPABASE_URL = getOptionalEnv("CHART_SUPABASE_URL");
const CHART_SERVICE_ROLE_KEY = getOptionalEnv("CHART_SUPABASE_SERVICE_ROLE_KEY");
const HR_INVITE_FROM_EMAIL = getOptionalEnv("HR_INVITE_FROM_EMAIL")
  || getOptionalEnv("PRO_FORMS_FROM_EMAIL");
const HR_INVITATION_TTL_DAYS = Number(getOptionalEnv("HR_INVITATION_TTL_DAYS") || 14);
const HR_PORTAL_BASE_URL = (
  getOptionalEnv("HR_PORTAL_BASE_URL")
  || "https://hr.coilsteelprocessing.com"
).replace(/\/+$/, "");

const supabase = createClient(AUTH_SUPABASE_URL, AUTH_SERVICE_ROLE_KEY);
const chartSupabase = CHART_SUPABASE_URL && CHART_SERVICE_ROLE_KEY
  ? createClient(CHART_SUPABASE_URL, CHART_SERVICE_ROLE_KEY)
  : null;
const ROLE_DEFINITIONS = [
  { id: 1, name: "admin" },
  { id: 3, name: "receiving" },
  { id: 4, name: "production" },
  { id: 7, name: "inventory" },
  { id: 9, name: "bonus_report" },
  { id: 10, name: "ai_assistant" },
  { id: 11, name: "shipping_overview" },
  { id: 12, name: "shipping_performance" },
  { id: 13, name: "customer_summary" },
  { id: 15, name: "iso" },
  { id: 16, name: "alarm_logs" },
  { id: 17, name: "quote_calculator" },
  { id: 18, name: "work_order_pricing" },
  { id: 19, name: "website_leads" },
  { id: 20, name: "employee" },
  { id: 21, name: "shift_reports" },
  { id: 22, name: "todd_requests" },
  { id: 23, name: "hr_admin" }
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

const getBearerToken = (req) => {
  const header = String(req.headers.authorization || "");
  const match = header.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : "";
};

const fetchUserRoleRows = async (userId) => {
  const { data, error } = await supabase
    .from("user_roles")
    .select("role_id, roles(name)")
    .eq("user_id", userId);

  if (error) throw error;
  return Array.isArray(data) ? data : [];
};

const roleNamesFromRows = (rows) =>
  (Array.isArray(rows) ? rows : [])
    .map((row) => row?.roles?.name)
    .filter(Boolean);

const requireRoleAccess = (requiredRoles, message, logLabel, allowedEmails = []) => async (req, res, next) => {
  try {
    const token = getBearerToken(req);
    if (!token) {
      return res.status(401).json({ error: "Missing Supabase access token." });
    }

    const { data, error } = await supabase.auth.getUser(token);
    const user = data?.user;
    if (error || !user?.id) {
      return res.status(401).json({ error: error?.message || "Invalid or expired Supabase session." });
    }

    const roleRows = await fetchUserRoleRows(user.id);
    const roles = roleNamesFromRows(roleRows);
    const email = String(user.email || "").trim().toLowerCase();
    if (!requiredRoles.some((role) => roles.includes(role)) && !allowedEmails.includes(email)) {
      return res.status(403).json({ error: message });
    }

    req.authUser = user;
    req.authRoles = roles;
    return next();
  } catch (error) {
    console.error(`${logLabel} authorization failed:`, error);
    return res.status(500).json({ error: `Unable to verify ${logLabel.toLowerCase()} access.` });
  }
};

const requireAdminAccess = requireRoleAccess(
  ["admin"],
  "Administrator access is required.",
  "Admin"
);

const requireHrAdminAccess = requireRoleAccess(
  ["admin", "hr_admin"],
  "HR administrator access is required.",
  "HR admin"
);

const requireWebsiteLeadAccess = requireRoleAccess(
  ["admin", "website_leads"],
  "Website traffic access is required.",
  "Website traffic",
  ["kyle@coilsteelprocessing.com", "josh@coilsteelprocessing.com"]
);

const requireShiftReportAccess = requireRoleAccess(
  ["admin", "production"],
  "Shift report dashboard access is required.",
  "Shift report dashboard"
);

const HR_INVITE_CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

const normalizeInvitationCode = (value) =>
  String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

const formatInvitationCode = (value) =>
  normalizeInvitationCode(value).replace(/(.{4})(?=.)/g, "$1-");

const hashInvitationCode = (value) =>
  crypto.createHash("sha256").update(normalizeInvitationCode(value)).digest("hex");

const generateHrInvitationCode = () => {
  const bytes = crypto.randomBytes(12);
  let code = "";
  for (const byte of bytes) {
    code += HR_INVITE_CODE_CHARS[byte % HR_INVITE_CODE_CHARS.length];
  }
  return formatInvitationCode(code);
};

const createHrInvitation = async (email, requestedRoles = []) => {
  const cleanEmail = String(email || "").trim().toLowerCase();
  const expiresAt = new Date(Date.now() + HR_INVITATION_TTL_DAYS * 24 * 60 * 60 * 1000).toISOString();
  const resolvedRoleIds = await resolveRoleIds(requestedRoles);
  const roleIds = Array.from(new Set([20, ...resolvedRoleIds]));

  await supabase
    .from("hr_invitations")
    .update({ status: "revoked" })
    .eq("email", cleanEmail)
    .eq("status", "pending");

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const code = generateHrInvitationCode();
    const { data, error } = await supabase
      .from("hr_invitations")
      .insert({
        email: cleanEmail,
        code_hash: hashInvitationCode(code),
        expires_at: expiresAt,
        metadata: {
          role_ids: roleIds
        }
      })
      .select("id,email,expires_at,metadata")
      .single();

    if (!error && data?.id) {
      return { ...data, code };
    }

    if (!/duplicate|unique/i.test(String(error?.message || ""))) {
      throw error || new Error("Unable to create the invitation code.");
    }
  }

  throw new Error("Unable to create a unique invitation code.");
};

const findValidHrInvitation = async (email, code) => {
  const normalizedCode = normalizeInvitationCode(code);
  if (!normalizedCode) return null;

  const { data, error } = await supabase
    .from("hr_invitations")
    .select("id,email,status,expires_at,metadata")
    .eq("code_hash", hashInvitationCode(normalizedCode))
    .maybeSingle();

  if (error) throw error;
  if (!data || data.status !== "pending") return null;
  if (String(data.email || "").trim().toLowerCase() !== String(email || "").trim().toLowerCase()) {
    return null;
  }

  if (new Date(data.expires_at).getTime() <= Date.now()) {
    await supabase
      .from("hr_invitations")
      .update({ status: "expired" })
      .eq("id", data.id)
      .eq("status", "pending");
    return null;
  }

  return data;
};

const acceptHrInvitation = async (invitationId, userId) => {
  const { data, error } = await supabase
    .from("hr_invitations")
    .update({
      status: "accepted",
      accepted_at: new Date().toISOString(),
      accepted_user_id: userId
    })
    .eq("id", invitationId)
    .eq("status", "pending")
    .select("id")
    .maybeSingle();

  if (error) throw error;
  if (!data?.id) {
    const usedError = new Error("The invitation code has already been used.");
    usedError.statusCode = 403;
    throw usedError;
  }
};

const restoreHrInvitation = async (invitationId, userId) => {
  if (!invitationId || !userId) return;
  const { error } = await supabase
    .from("hr_invitations")
    .update({
      status: "pending",
      accepted_at: null,
      accepted_user_id: null
    })
    .eq("id", invitationId)
    .eq("accepted_user_id", userId);

  if (error) {
    console.error("HR invitation restore failed:", error);
  }
};

const HR_REGISTRATION_WINDOW_MS = 15 * 60 * 1000;
const HR_REGISTRATION_MAX_ATTEMPTS = 8;
const hrRegistrationAttempts = new Map();

const consumeHrRegistrationAttempt = (req) => {
  const forwardedFor = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim();
  const key = forwardedFor || req.ip || "unknown";
  const now = Date.now();
  const current = hrRegistrationAttempts.get(key);

  if (!current || now - current.startedAt >= HR_REGISTRATION_WINDOW_MS) {
    hrRegistrationAttempts.set(key, { count: 1, startedAt: now });
    return true;
  }

  if (current.count >= HR_REGISTRATION_MAX_ATTEMPTS) return false;
  current.count += 1;
  return true;
};

const sendHrAccountConfirmation = async (email, confirmationUrl) => {
  if (!RESEND_API_KEY || !HR_INVITE_FROM_EMAIL) {
    throw new Error("HR confirmation email is not configured.");
  }

  if (!confirmationUrl) {
    throw new Error("Unable to generate the email confirmation link.");
  }

  const safeUrl = escapeHtml(confirmationUrl);
  const html = `
    <!DOCTYPE html>
    <html lang="en">
    <head><meta charset="UTF-8"><title>Confirm Your CSP Benefits Account</title></head>
    <body style="margin:0;background:#f3f6fb;font-family:Arial,sans-serif;color:#233658;">
      <div style="max-width:620px;margin:0 auto;padding:32px 18px;">
        <div style="background:#ffffff;border:1px solid #d5deec;border-radius:8px;padding:28px;">
          <h1 style="margin:0 0 14px;font-size:26px;color:#233658;">Confirm Your Employee Benefits Account</h1>
          <p style="margin:0 0 18px;line-height:1.55;">Your Coil Steel Processing employee benefits account has been created. Confirm your email address to finish setup.</p>
          <a href="${safeUrl}" style="display:inline-block;padding:12px 18px;border-radius:6px;background:#f1a91e;color:#ffffff;font-weight:bold;text-decoration:none;">Confirm Email and Sign In</a>
          <p style="margin:22px 0 0;color:#5b6a87;font-size:13px;line-height:1.5;">If you did not create this account, ignore this email or contact HR.</p>
        </div>
      </div>
    </body>
    </html>
  `;

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      from: HR_INVITE_FROM_EMAIL,
      to: [email],
      subject: "Confirm your CSP employee benefits account",
      html
    })
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload?.message || "The email provider rejected the confirmation email.");
  }

  return payload?.id || null;
};

const parseEmailRecipients = (value) => [...new Set(
  String(value || "")
    .split(",")
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean)
)];
const PRO_FORMS_RECIPIENTS = parseEmailRecipients(
  process.env.PRO_FORMS_RECIPIENTS || "todd@coilsteelprocessing.com"
);
const PRO_TEST_RECIPIENTS = parseEmailRecipients(
  process.env.PRO_TEST_RECIPIENTS || "todd@coilsteelprocessing.com"
);
const SHIFT_REPORT_RECIPIENTS = parseEmailRecipients(
  process.env.SHIFT_REPORT_RECIPIENTS
  || "todd@coilsteelprocessing.com,tino@coilsteelprocessing.com,josh@coilsteelprocessing.com,kim@coilsteelprocessing.com,jplace@coilsteelprocessing.com,jason@coilsteelprocessing.com,justin@coilsteelprocessing.com,kevin@coilsteelprocessing.com,mike@coilsteelprocessing.com,sal@coilsteelprocessing.com,scotty@coilsteelprocessing.com,tyrone@coilsteelprocessing.com,janet@coilsteelprocessing.com,rbi@coilsteelprocessing.com,brian@coilsteelprocessing.com,kyle@coilsteelprocessing.com"
);
const SHIFT_MAINTENANCE_RECIPIENTS = parseEmailRecipients(
  process.env.SHIFT_MAINTENANCE_RECIPIENTS
  || "sal@coilsteelprocessing.com,justin@coilsteelprocessing.com"
);
const CRANE_INSPECTION_RECIPIENTS = parseEmailRecipients(
  process.env.CRANE_INSPECTION_RECIPIENTS
  || "tino@coilsteelprocessing.com,jon@coilsteelprocessing.com,todd@coilsteelprocessing.com"
);
const CRANE_FAILURE_RECIPIENTS = parseEmailRecipients(
  process.env.CRANE_FAILURE_RECIPIENTS
  || "justin@coilsteelprocessing.com,tino@coilsteelprocessing.com,sal@coilsteelprocessing.com"
);
const FORKLIFT_INSPECTION_RECIPIENTS = parseEmailRecipients(
  process.env.FORKLIFT_INSPECTION_RECIPIENTS
  || "tino@coilsteelprocessing.com,jon@coilsteelprocessing.com,todd@coilsteelprocessing.com"
);
const FORKLIFT_FAILURE_RECIPIENTS = parseEmailRecipients(
  process.env.FORKLIFT_FAILURE_RECIPIENTS
  || "sal@coilsteelprocessing.com,justin@coilsteelprocessing.com"
);
const RESEND_API_KEY = String(process.env.RESEND_API_KEY || "").trim();
const PRO_FORMS_FROM_EMAIL = String(process.env.PRO_FORMS_FROM_EMAIL || "").trim();
const DRIVER_SUPABASE_URL = String(process.env.DRIVER_SUPABASE_URL || "").trim();
const DRIVER_SUPABASE_SERVICE_ROLE_KEY = String(process.env.DRIVER_SUPABASE_SERVICE_ROLE_KEY || "").trim();
const SHIPPING_AUTH_FROM_EMAIL = String(
  process.env.SHIPPING_AUTH_FROM_EMAIL
  || process.env.PRO_FORMS_FROM_EMAIL
  || process.env.HR_INVITE_FROM_EMAIL
  || ""
).trim();
const SHIPPING_PORTAL_BASE_URL = String(
  process.env.SHIPPING_PORTAL_BASE_URL || "https://shipping.coilsteelprocessing.com"
).trim().replace(/\/+$/, "");
const SHIPPING_AUTH_MEMBERS = new Map(
  String(process.env.SHIPPING_AUTH_ALLOWED_EMAILS || "todd@coilsteelprocessing.com:admin")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const [rawEmail, rawRole] = entry.split(":");
      const email = String(rawEmail || "").trim().toLowerCase();
      const role = String(rawRole || "shipping").trim().toLowerCase() === "admin" ? "admin" : "shipping";
      return [email, role];
    })
    .filter(([email]) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))
);
const SHIPPING_BI_ACCESS_ROLES = new Set(
  String(process.env.SHIPPING_BI_ACCESS_ROLES || "admin,shipping_overview,shipping_performance")
    .split(",")
    .map(normalizeRoleName)
    .filter(Boolean)
);
const driverSupabase = DRIVER_SUPABASE_URL && DRIVER_SUPABASE_SERVICE_ROLE_KEY
  ? createClient(DRIVER_SUPABASE_URL, DRIVER_SUPABASE_SERVICE_ROLE_KEY, {
      auth: { autoRefreshToken: false, persistSession: false }
    })
  : null;
const shippingAuthAttempts = new Map();
const EXPANSION_LEAD_RECIPIENTS = String(
  process.env.EXPANSION_LEAD_RECIPIENTS
  || "kyle@coilsteelprocessing.com,josh@coilsteelprocessing.com"
)
  .split(",")
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const FANTASY_FOOTBALL_ADMIN_RECIPIENTS = String(
  process.env.FANTASY_FOOTBALL_ADMIN_EMAILS
  || "todd@coilsteelprocessing.com"
)
  .split(",")
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const EXPANSION_LEAD_STORE_URL = String(
  process.env.EXPANSION_LEAD_STORE_URL
  || "https://wtjrucerrbzwxnwhqgma.supabase.co/functions/v1/expansion-lead-store"
).trim();
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

const normalizeUserProfile = (body = {}) => ({
  first_name: coerceText(body.first_name ?? body.firstName, 120) || "",
  last_name: coerceText(body.last_name ?? body.lastName, 120) || "",
  job_title: coerceText(body.job_title ?? body.jobTitle, 160) || ""
});

const profileFromAuthUser = (user = {}) => {
  const metadata = user.user_metadata || {};
  return {
    first_name: coerceText(metadata.first_name ?? metadata.firstName, 120) || "",
    last_name: coerceText(metadata.last_name ?? metadata.lastName, 120) || "",
    job_title: coerceText(metadata.job_title ?? metadata.jobTitle, 160) || ""
  };
};

const upsertPublicUserProfile = async ({ id, email, first_name = "", last_name = "", job_title = "" }) => {
  const baseRow = { id, email };
  const profileRow = {
    ...baseRow,
    first_name,
    last_name,
    job_title,
    updated_at: new Date().toISOString()
  };

  const { error } = await supabase
    .from("users")
    .upsert(profileRow, { onConflict: "id" });

  if (!error) return;

  const missingColumn = /column .* does not exist|schema cache|Could not find/i.test(String(error.message || ""));
  if (!missingColumn) throw error;

  const fallback = await supabase
    .from("users")
    .upsert(baseRow, { onConflict: "id" });
  if (fallback.error) throw fallback.error;
};

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
        linear_feet_per_ton: coerceNumber(metrics.linear_feet_per_ton) || 0,
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
const EMAIL_ASSETS_DIR = path.join(__dirname, "email-assets");
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

const consumeShippingAuthAttempt = (req, email) => {
  const now = Date.now();
  const windowMs = 15 * 60 * 1000;
  const key = `${String(req.ip || req.socket?.remoteAddress || "unknown")}:${email}`;
  const recent = (shippingAuthAttempts.get(key) || []).filter((value) => now - value < windowMs);
  if (recent.length >= 5) return false;
  recent.push(now);
  shippingAuthAttempts.set(key, recent);

  if (shippingAuthAttempts.size > 500) {
    for (const [attemptKey, values] of shippingAuthAttempts.entries()) {
      const active = values.filter((value) => now - value < windowMs);
      if (active.length) shippingAuthAttempts.set(attemptKey, active);
      else shippingAuthAttempts.delete(attemptKey);
    }
  }
  return true;
};

const DRIVER_LANGUAGES = new Set(["English", "Español", "Français", "Українська", "Русский"]);

const normalizeDriverSignup = (body = {}) => ({
  email: String(body.email || "").trim().toLowerCase(),
  password: String(body.password || ""),
  fullName: coerceText(body.fullName ?? body.name, 120),
  haulingFor: coerceText(body.haulingFor, 160),
  driverCompany: coerceText(body.driverCompany, 160),
  preferredLanguage: DRIVER_LANGUAGES.has(body.preferredLanguage)
    ? body.preferredLanguage
    : "English"
});

const sendDriverVerificationCode = async ({ email, password, profile }) => {
  let authUser = await findDriverAuthUser(email);
  if (authUser?.email_confirmed_at) {
    const error = new Error("An account already exists for this email. Sign in instead.");
    error.statusCode = 409;
    throw error;
  }

  const metadata = {
    full_name: profile.fullName,
    hauling_for: profile.haulingFor,
    driver_company: profile.driverCompany,
    preferred_language: profile.preferredLanguage
  };
  let verificationType = "signup";
  let linkData;

  if (authUser) {
    const { error: updateError } = await driverSupabase.auth.admin.updateUserById(authUser.id, {
      password,
      user_metadata: metadata
    });
    if (updateError) throw updateError;

    verificationType = "magiclink";
    const { data, error } = await driverSupabase.auth.admin.generateLink({
      type: "magiclink",
      email,
      options: { data: metadata }
    });
    if (error) throw error;
    linkData = data;
  } else {
    const { data, error } = await driverSupabase.auth.admin.generateLink({
      type: "signup",
      email,
      password,
      options: { data: metadata }
    });
    if (error) throw error;
    linkData = data;
  }

  const code = String(linkData?.properties?.email_otp || "").trim();
  if (!/^\d{6,10}$/.test(code)) {
    throw new Error("Supabase did not return a valid email verification code.");
  }

  const html = renderStoredTemplate("driver_verification_code", {
    driver_name: profile.fullName,
    verification_code: code,
    recipient_email: email
  });
  if (!html) throw new Error("Driver verification email template is unavailable.");

  const sendFrom = async (from) => {
    const response = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${RESEND_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        from,
        to: [email],
        subject: `${code} is your CSP Driver verification code`,
        html
      })
    });
    const payload = await response.json().catch(() => ({}));
    return { response, payload };
  };

  let delivery = await sendFrom(SHIPPING_AUTH_FROM_EMAIL);
  const fallbackFrom = String(PRO_FORMS_FROM_EMAIL || "").trim();
  if (!delivery.response.ok && fallbackFrom && fallbackFrom !== SHIPPING_AUTH_FROM_EMAIL) {
    console.warn(
      "Driver verification sender was rejected; retrying with the established forms sender:",
      delivery.payload?.message || delivery.response.status
    );
    delivery = await sendFrom(fallbackFrom);
  }
  if (!delivery.response.ok) {
    const error = new Error(delivery.payload?.message || "Resend rejected the driver verification email.");
    error.code = "driver_email_delivery_failed";
    throw error;
  }

  return {
    verificationType,
    verificationCodeLength: code.length
  };
};

app.post("/api/driver/auth/signup-code", async (req, res) => {
  res.set("Cache-Control", "no-store");
  const profile = normalizeDriverSignup(req.body);

  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(profile.email)) {
    return res.status(400).json({ error: "Enter a valid email address." });
  }
  if (profile.password.length < 8 || profile.password.length > 72) {
    return res.status(400).json({ error: "Use a password between 8 and 72 characters." });
  }
  if (!profile.fullName || !profile.haulingFor || !profile.driverCompany) {
    return res.status(400).json({ error: "Complete all driver information fields." });
  }
  if (!consumeShippingAuthAttempt(req, `driver-signup:${profile.email}`)) {
    return res.status(429).json({ error: "Too many verification requests. Wait 15 minutes and try again." });
  }
  if (!driverSupabase || !RESEND_API_KEY || !SHIPPING_AUTH_FROM_EMAIL) {
    return res.status(503).json({ error: "Driver verification email is not configured on the server." });
  }

  try {
    const verification = await sendDriverVerificationCode({
      email: profile.email,
      password: profile.password,
      profile
    });
    return res.json({
      ok: true,
      verificationType: verification.verificationType,
      verificationCodeLength: verification.verificationCodeLength,
      message: "Check your email for the CSP Driver verification code."
    });
  } catch (error) {
    console.error("Driver verification email failed:", error?.message || error);
    const statusCode = Number(error?.statusCode) || 500;
    const safeMessage = error?.code === "driver_email_delivery_failed"
      ? `Verification email could not be delivered: ${String(error.message || "Resend rejected the request.").slice(0, 240)}`
      : error?.message === "Driver verification email template is unavailable."
        ? error.message
        : `Verification failed: ${String(error?.message || "Unknown server error.").slice(0, 240)}`;
    return res.status(statusCode).json({
      error: statusCode === 409
        ? error.message
        : safeMessage
    });
  }
});

const findAuthUserByEmail = async (authClient, email) => {
  for (let page = 1; page <= 10; page += 1) {
    const { data, error } = await authClient.auth.admin.listUsers({ page, perPage: 1000 });
    if (error) throw error;
    const users = Array.isArray(data?.users) ? data.users : [];
    const match = users.find((user) => String(user?.email || "").trim().toLowerCase() === email);
    if (match || users.length < 1000) return match || null;
  }
  return null;
};

const findDriverAuthUser = (email) => findAuthUserByEmail(driverSupabase, email);

const resolveBiShippingRole = async (biUser) => {
  if (!biUser?.id) return null;
  const email = String(biUser.email || "").trim().toLowerCase();
  const roleRows = await fetchUserRoleRows(biUser.id);
  const roles = roleNamesFromRows(roleRows).map(normalizeRoleName);
  const allowlistedRole = SHIPPING_AUTH_MEMBERS.get(email) || null;
  const approved = roles.some((role) => SHIPPING_BI_ACCESS_ROLES.has(role)) || Boolean(allowlistedRole);
  if (!approved) return null;
  return roles.includes("admin") || allowlistedRole === "admin" ? "admin" : "shipping";
};

const issueDriverShippingSession = async (biUser, shippingRole) => {
  const email = String(biUser?.email || "").trim().toLowerCase();
  if (!email || !driverSupabase) throw new Error("Shipping authentication is not configured.");

  let driverUser = await findDriverAuthUser(email);
  const displayName = String(
    biUser?.user_metadata?.full_name
    || [biUser?.user_metadata?.first_name, biUser?.user_metadata?.last_name].filter(Boolean).join(" ")
    || email.split("@")[0]
    || "CSP Shipping"
  ).trim();

  if (!driverUser) {
    const { data, error } = await driverSupabase.auth.admin.createUser({
      email,
      email_confirm: true,
      app_metadata: { csp_role: shippingRole, bi_user_id: biUser.id },
      user_metadata: { full_name: displayName }
    });
    if (error || !data?.user) throw error || new Error("Unable to create the Shipping data account.");
    driverUser = data.user;
  } else {
    const currentRole = String(driverUser.app_metadata?.csp_role || "").trim().toLowerCase();
    const currentBiUserId = String(driverUser.app_metadata?.bi_user_id || "").trim();
    if (currentRole !== shippingRole || currentBiUserId !== biUser.id) {
      const { data, error } = await driverSupabase.auth.admin.updateUserById(driverUser.id, {
        app_metadata: {
          ...(driverUser.app_metadata || {}),
          csp_role: shippingRole,
          bi_user_id: biUser.id
        }
      });
      if (error) throw error;
      driverUser = data?.user || driverUser;
    }
  }

  const { data: linkData, error: linkError } = await driverSupabase.auth.admin.generateLink({
    type: "magiclink",
    email
  });
  const tokenHash = String(linkData?.properties?.hashed_token || "").trim();
  if (linkError || !tokenHash) {
    throw linkError || new Error("Unable to create the Shipping data session.");
  }

  const sessionClient = createClient(DRIVER_SUPABASE_URL, DRIVER_SUPABASE_SERVICE_ROLE_KEY, {
    auth: { autoRefreshToken: false, persistSession: false, detectSessionInUrl: false }
  });
  const { data: sessionData, error: sessionError } = await sessionClient.auth.verifyOtp({
    token_hash: tokenHash,
    type: "magiclink"
  });
  if (sessionError || !sessionData?.session?.access_token || !sessionData?.session?.refresh_token) {
    throw sessionError || new Error("Unable to establish the Shipping data session.");
  }

  return sessionData.session;
};

app.post("/api/shipping/auth/session", async (req, res) => {
  res.set("Cache-Control", "no-store");
  if (!driverSupabase) {
    return res.status(503).json({ error: "Shipping authentication is not configured on the server." });
  }

  const token = getBearerToken(req);
  if (!token) return res.status(401).json({ error: "Sign in with your BI account." });

  try {
    const { data, error } = await supabase.auth.getUser(token);
    const biUser = data?.user;
    if (error || !biUser?.id || !biUser?.email) {
      return res.status(401).json({ error: error?.message || "Your BI session is invalid or expired." });
    }

    const shippingRole = await resolveBiShippingRole(biUser);
    if (!shippingRole) {
      return res.status(403).json({ error: "Your BI account does not have CSP Shipping access." });
    }

    const driverSession = await issueDriverShippingSession(biUser, shippingRole);
    return res.json({
      ok: true,
      authSource: "bi",
      shippingRole,
      session: {
        access_token: driverSession.access_token,
        refresh_token: driverSession.refresh_token,
        expires_at: driverSession.expires_at || null,
        expires_in: driverSession.expires_in || null
      }
    });
  } catch (error) {
    console.error("BI-master Shipping session failed:", error?.message || error);
    return res.status(500).json({ error: "We could not establish the Shipping session. Try again shortly." });
  }
});

app.post("/api/shipping/auth/magic-link", async (req, res) => {
  res.set("Cache-Control", "no-store");
  const email = String(req.body?.email || "").trim().toLowerCase();

  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ error: "Enter a valid CSP employee email address." });
  }
  if (!consumeShippingAuthAttempt(req, email)) {
    return res.status(429).json({ error: "Too many sign-in requests. Wait 15 minutes and try again." });
  }
  if (!RESEND_API_KEY || !SHIPPING_AUTH_FROM_EMAIL) {
    return res.status(503).json({ error: "Shipping email sign-in is not configured on the server." });
  }

  try {
    const biUser = await findAuthUserByEmail(supabase, email);
    const shippingRole = await resolveBiShippingRole(biUser);
    if (!biUser || !shippingRole) {
      return res.status(403).json({ error: "This BI account does not have CSP Shipping access." });
    }

    const { data: linkData, error: linkError } = await supabase.auth.admin.generateLink({
      type: "magiclink",
      email
    });
    if (linkError || !linkData?.properties?.hashed_token) {
      throw linkError || new Error("Supabase did not return a secure sign-in token.");
    }

    const signInUrl = new URL(SHIPPING_PORTAL_BASE_URL);
    signInUrl.searchParams.set("bi_token_hash", linkData.properties.hashed_token);
    signInUrl.searchParams.set("type", "magiclink");
    const html = renderStoredTemplate("shipping_magic_link", {
      magic_link: signInUrl.toString(),
      recipient_email: email
    });
    if (!html) throw new Error("Shipping sign-in email template is unavailable.");

    const response = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${RESEND_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        from: SHIPPING_AUTH_FROM_EMAIL,
        to: [email],
        subject: "Sign in to CSP Shipping",
        html
      })
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload?.message || "Resend rejected the sign-in email.");

    return res.json({ ok: true, message: "Check your email for the CSP Shipping sign-in link." });
  } catch (error) {
    console.error("Shipping magic-link email failed:", error?.message || error);
    return res.status(500).json({ error: "We could not send the sign-in email. Try again shortly." });
  }
});

app.post("/api/shipping/close-stale-visit", async (req, res) => {
  res.set("Cache-Control", "no-store");
  if (!driverSupabase) {
    return res.status(503).json({ error: "Shipping visit management is not configured on the server." });
  }

  const token = getBearerToken(req);
  if (!token) return res.status(401).json({ error: "Sign in to manage driver visits." });

  const arrivalId = Number(req.body?.arrivalId);
  const conversationId = String(req.body?.conversationId || "").trim();
  if (!Number.isSafeInteger(arrivalId) || arrivalId < 1) {
    return res.status(400).json({ error: "Choose a valid driver visit." });
  }
  if (conversationId && !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(conversationId)) {
    return res.status(400).json({ error: "The conversation identifier is invalid." });
  }

  try {
    const { data: userData, error: userError } = await driverSupabase.auth.getUser(token);
    const staffUser = userData?.user;
    const staffRole = String(staffUser?.app_metadata?.csp_role || "").trim().toLowerCase();
    if (userError || !staffUser?.id) {
      return res.status(401).json({ error: "Your shipping session has expired. Sign in again." });
    }
    if (!new Set(["shipping", "admin"]).has(staffRole)) {
      return res.status(403).json({ error: "CSP Shipping access is required to close a visit." });
    }

    const { data: arrival, error: arrivalError } = await driverSupabase
      .from("driver_arrivals")
      .select("id,user_id,event_type,facility_id,facility_name,occurred_at,profile_snapshot")
      .eq("id", arrivalId)
      .maybeSingle();
    if (arrivalError) throw arrivalError;
    if (!arrival || !["geofence_enter", "manual_check_in"].includes(arrival.event_type)) {
      return res.status(404).json({ error: "This open driver visit could not be found." });
    }

    const closeTime = new Date();
    const enteredAt = new Date(arrival.occurred_at).getTime();
    const debouncedCloseTime = new Date(Math.min(
      closeTime.getTime(),
      Math.max(
        Number.isFinite(enteredAt) ? enteredAt + 1000 : 0,
        closeTime.getTime() - 125000
      )
    ));
    const { error: exitError } = await driverSupabase
      .from("driver_arrivals")
      .upsert({
        client_event_id: `shipping-close-${arrivalId}`,
        event_type: "geofence_exit",
        facility_id: arrival.facility_id,
        facility_name: arrival.facility_name,
        release_number: null,
        occurred_at: debouncedCloseTime.toISOString(),
        user_id: arrival.user_id,
        profile_snapshot: arrival.profile_snapshot || {}
      }, { onConflict: "client_event_id", ignoreDuplicates: true });
    if (exitError) throw exitError;

    if (conversationId) {
      const { error: conversationError } = await driverSupabase
        .from("driver_conversations")
        .update({ status: "closed", shipping_last_read_at: closeTime.toISOString() })
        .eq("id", conversationId)
        .eq("user_id", arrival.user_id)
        .eq("facility_id", arrival.facility_id)
        .eq("status", "open");
      if (conversationError) throw conversationError;
    }

    return res.json({
      ok: true,
      arrivalId,
      closedAt: closeTime.toISOString(),
      conversationClosed: Boolean(conversationId)
    });
  } catch (error) {
    console.error("Shipping stale visit close failed:", error?.message || error);
    return res.status(500).json({ error: "The stale driver visit could not be closed. Try again." });
  }
});

const formatEmailLabel = (value) =>
  String(value || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());

const formatEmailValue = (value, fallback = "-") => {
  if (value === null || value === undefined || value === "") return fallback;
  return escapeHtml(value);
};

const formatEmailDate = (value, fallback = "-") => {
  const input = String(value || "").trim();
  if (!input) return fallback;

  const date = /^\d{4}-\d{2}-\d{2}$/.test(input)
    ? new Date(`${input}T12:00:00Z`)
    : new Date(input);
  if (Number.isNaN(date.getTime())) return input;

  return new Intl.DateTimeFormat("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
    timeZone: "UTC"
  }).format(date);
};

const formatEmailEasternDateTime = (value, fallback = "-") => {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return fallback;

  return new Intl.DateTimeFormat("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZone: "America/New_York",
    timeZoneName: "short"
  }).format(date);
};

const formatEmailNumber = (value, suffix = "") => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "0";
  return `${parsed.toLocaleString("en-US", { maximumFractionDigits: 2 })}${suffix}`;
};

const formatEmailWholeNumber = (value, suffix = "") => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "0";
  return `${Math.round(parsed).toLocaleString("en-US")}${suffix}`;
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
    { label: "Submitted at", value: formatEmailEasternDateTime(submittedAt) }
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
  const reportDate = formatEmailDate(dimensions.report_date || dimensions.submission_date);
  const values = {
    kpi_grid: buildKpiGrid([
      { label: "Tons", value: formatEmailWholeNumber(metrics.tons), note: dimensions.shift || "Shift" },
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
      { label: "Tons", value: metrics.tons, display: formatEmailWholeNumber(metrics.tons) },
      { label: "Coils", value: metrics.total_coils_ran, display: formatEmailNumber(metrics.total_coils_ran) },
      { label: "Downtime", value: metrics.total_downtime_minutes, display: formatEmailNumber(metrics.total_downtime_minutes, " min") }
    ], { color: "#f1a91e" }),
    shift_details_table: buildEmailTable("Shift Details", [
      { label: "Report date", value: reportDate },
      { label: "Operator", value: dimensions.operator },
      { label: "Shift", value: dimensions.shift },
      { label: "Had downtime", value: dimensions.had_downtime }
    ]),
    production_metrics_table: buildEmailTable("Production Metrics", [
      { label: "Hours worked", value: formatEmailNumber(metrics.hours_worked) },
      { label: "Tons", value: formatEmailWholeNumber(metrics.tons) },
      { label: "Linear feet", value: formatEmailNumber(metrics.linear_feet) },
      { label: "Linear feet per ton", value: formatEmailNumber(metrics.linear_feet_per_ton) },
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
    maintenance_calls_list: buildItemList(
      "Maintenance Calls",
      getArray(payload.maintenanceTimes).map((row) => {
        const callTime = row.maintenanceCallTime || "n/a";
        const arrivalTime = row.maintenanceArrivalTime || "n/a";
        const completionTime = row.maintenanceCompletionTime || "n/a";
        return `Call: ${callTime} | Arrival: ${arrivalTime} | Completion: ${completionTime}`;
      }),
      "No maintenance calls recorded."
    ),
    comments_block: notes ? `<h3 style="margin:22px 0 8px;color:#172742;font-size:16px;">Comments</h3><p style="margin:0;color:#233658;">${escapeHtml(notes)}</p>` : ""
  };
  const body = renderStoredTemplate("shift_report", values) || Object.values(values).join("");

  return buildEmailShell({
    eyebrow: "Shift Report",
    title: `Shift report submitted for ${reportDate}`,
    summary: `${formatEmailValue(dimensions.operator, "An operator")} submitted ${formatEmailWholeNumber(metrics.tons)} tons on ${formatEmailValue(dimensions.shift, "the selected shift")}.`,
    submittedBy,
    submittedAt,
    body
  });
};

const buildInspectionEmail = ({ formLabel, submittedBy, submittedAt, dimensions, metrics, notes, payload, itemField, issueCountKey, issueLabel, includeOrders = true }) => {
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
      includeOrders ? {
        label: "Orders",
        value: formatEmailNumber(metrics.maintenance_orders_opened),
        note: "Maintenance opened",
        color: Number(metrics.maintenance_orders_opened) > 0 ? "#b42318" : "#172742"
      } : null
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
      includeOrders ? { label: "Maintenance orders opened", value: formatEmailNumber(metrics.maintenance_orders_opened) } : null
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
      issueLabel: "failed checks",
      includeOrders: false
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
      issueLabel: "failed checks",
      includeOrders: false
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

const buildShiftMaintenanceEmail = ({ submittedBy, submittedAt, dimensions, payload }) => {
  const calls = getArray(payload.maintenanceTimes).filter((call) =>
    call?.maintenanceCallTime || call?.maintenanceArrivalTime || call?.maintenanceCompletionTime
  );
  const callBlocks = calls.map((call) => buildKpiGrid([
    { label: "Call Time", value: call.maintenanceCallTime || "-" },
    { label: "Arrival", value: call.maintenanceArrivalTime || "-" },
    { label: "Completion", value: call.maintenanceCompletionTime || "-" }
  ])).join("");
  const callDetails = buildEmailTable("Maintenance Call Details", [
    { label: "Maintenance tech", value: dimensions.maintenance_tech || payload.maintenanceTech || "Not selected" },
    { label: "Reason / details", value: payload.maintenanceReason || "No details supplied" }
  ]);

  return buildEmailShell({
    eyebrow: "Maintenance Call",
    title: `${coerceText(dimensions.shift, 80) || "Shift"} Shift Maintenance`,
    summary: `${formatEmailValue(dimensions.operator, "Operator")} reported maintenance on ${formatEmailDate(dimensions.report_date || dimensions.submission_date)}.`,
    submittedBy,
    submittedAt,
    body: `${callBlocks}${callDetails}`
  });
};

const buildCraneFailureEmail = ({ submittedBy, submittedAt, dimensions, payload }) => {
  const failures = getArray(payload.answers).filter((answer) =>
    String(answer?.status || "").toLowerCase() === "fail"
  );
  return buildEmailShell({
    eyebrow: "Crane Failure Notice",
    title: `${coerceText(dimensions.crane_name, 160) || "Crane"} inspection failure`,
    summary: `${failures.length} failed inspection item${failures.length === 1 ? "" : "s"} reported by ${formatEmailValue(dimensions.inspector_name, "the inspector")}.`,
    submittedBy,
    submittedAt,
    body: buildItemList(
      "Failed Inspection Items",
      failures.map((failure) => `${failure.label || failure.key || "Inspection item"}: ${failure.notes || "No failure notes supplied."}`)
    )
  });
};

const buildForkliftFailureEmail = ({ submittedBy, submittedAt, dimensions, payload }) => {
  const failures = getArray(payload.checks).filter((check) =>
    String(check?.status || "").toLowerCase() === "fail"
  );
  const forkliftName = coerceText(dimensions.asset_name, 160)
    || `${coerceText(dimensions.location, 120) || "CSP"} Forklift ${coerceText(dimensions.forklift_number, 120) || ""}`.trim();
  return buildEmailShell({
    eyebrow: "Forklift Failure Notice",
    title: `${forkliftName || "Forklift"} inspection failure`,
    summary: `${failures.length} failed inspection item${failures.length === 1 ? "" : "s"} reported by ${formatEmailValue(dimensions.inspector_name, "the inspector")}.`,
    submittedBy,
    submittedAt,
    body: buildItemList(
      "Failed Inspection Items",
      failures.map((failure) => `${failure.label || failure.key || "Inspection item"}: ${failure.notes || "No failure notes supplied."}`)
    )
  });
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
  payload
}) => {
  const isTest = /\btest\b/i.test(String(formLabel || ""));
  const targetRecipients = isTest
    ? PRO_TEST_RECIPIENTS
    : formKey === "shift_report"
      ? SHIFT_REPORT_RECIPIENTS
      : formKey === "forklift_inspection"
        ? FORKLIFT_INSPECTION_RECIPIENTS
      : formKey === "crane_inspection"
        ? CRANE_INSPECTION_RECIPIENTS
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

  const subject = formKey === "shift_report"
    ? `${coerceText(dimensions.shift, 80) || "Shift"} Shift Report`
    : `[CSP Pro] ${formLabel} submitted`;
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

const sendSpecialFormNotification = async ({ recipients, subject, html }) => {
  const targetRecipients = parseEmailRecipients(getArray(recipients).join(","));
  if (!RESEND_API_KEY || !PRO_FORMS_FROM_EMAIL || targetRecipients.length === 0) {
    return { sent: false, reason: "Special email notification is not configured." };
  }

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ from: PRO_FORMS_FROM_EMAIL, to: targetRecipients, subject, html })
  });
  if (!response.ok) {
    throw new Error(await response.text() || "Email provider rejected the special notification request.");
  }
  const providerPayload = await response.json().catch(() => ({}));
  return { sent: true, recipients: targetRecipients, providerId: providerPayload?.id || null };
};

const EMAIL_PREVIEW_SAMPLES = {
  shift_report: {
    formKey: "shift_report",
    formLabel: "Shift Report",
    submittedBy: "operator@coilsteelprocessing.com",
    dimensions: {
      submission_date: "2026-05-01",
      report_date: "2026-05-01",
      operator: "Ronnell Gilmore",
      shift: "Third",
      had_downtime: "Yes",
      maintenance_tech: "Sal De La Cruz"
    },
    metrics: {
      hours_worked: 8,
      tons: 124.35,
      linear_feet: 18500,
      linear_feet_per_ton: 148.77,
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
      maintenanceTech: "Sal De La Cruz",
      maintenanceTimes: [
        {
          maintenanceCallTime: "10:15 PM",
          maintenanceArrivalTime: "10:28 PM",
          maintenanceCompletionTime: "10:52 PM"
        }
      ],
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

app.post("/api/create-user", requireAdminAccess, async (req, res) => {
  const { email, password, roles } = req.body || {};
  const profile = normalizeUserProfile(req.body || {});

  if (!email || !password || !Array.isArray(roles)) {
    return res.status(400).json({ message: "Email, password, and roles are required." });
  }

  const cleanEmail = String(email).trim().toLowerCase();

  try {
    const resolvedRoleIds = await resolveRoleIds([...roles, 20]);
    if (resolvedRoleIds.length === 0) {
      return res.status(400).json({ message: "No valid roles were selected." });
    }

    // 1️⃣ Create the user in Supabase Auth
    const { data: authUser, error: authError } = await supabase.auth.admin.createUser({
      email: cleanEmail,
      password,
      email_confirm: true,
      user_metadata: {
        ...profile,
        force_password_change: false
      }
    });

    if (authError || !authUser?.user) {
      console.error("Auth user creation failed:", authError);
      return res.status(400).json({ message: authError?.message || "Error creating auth user." });
    }

    // 2️⃣ Insert user record in 'users' table
    await upsertPublicUserProfile({
      id: authUser.user.id,
      email: cleanEmail,
      ...profile
    });

    // 3️⃣ Assign roles
    const roleRows = resolvedRoleIds.map((role_id) => ({
      user_id: authUser.user.id,
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

app.post("/api/hr/register", async (req, res) => {
  if (!RESEND_API_KEY || !HR_INVITE_FROM_EMAIL) {
    return res.status(503).json({ message: "HR confirmation email is not configured." });
  }

  if (!consumeHrRegistrationAttempt(req)) {
    return res.status(429).json({ message: "Too many registration attempts. Try again later." });
  }

  const { email, password, enrollmentCode } = req.body || {};
  const cleanEmail = String(email || "").trim().toLowerCase();
  const cleanPassword = String(password || "");

  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cleanEmail)) {
    return res.status(400).json({ message: "Enter a valid email address." });
  }

  if (cleanPassword.length < 8) {
    return res.status(400).json({ message: "Password must be at least 8 characters." });
  }

  let createdUserId = "";
  let invitationId = "";

  try {
    await ensureKnownRoles();

    const invitation = await findValidHrInvitation(cleanEmail, enrollmentCode);
    if (!invitation) {
      return res.status(403).json({
        message: "The invitation code is not valid for this email or has expired."
      });
    }
    invitationId = invitation.id;

    const { data: authData, error: authError } = await supabase.auth.admin.generateLink({
      type: "signup",
      email: cleanEmail,
      password: cleanPassword,
      options: {
        redirectTo: `${HR_PORTAL_BASE_URL}/login.html?confirmed=1`
      }
    });

    if (authError || !authData?.user?.id) {
      const alreadyExists = /already|registered|exists/i.test(String(authError?.message || ""));
      return res.status(400).json({
        message: alreadyExists
          ? "An account already exists for this email. Sign in or ask an administrator to add HR access."
          : authError?.message || "Unable to create the account."
      });
    }

    createdUserId = authData.user.id;

    const { error: userError } = await supabase
      .from("users")
      .upsert(
        { id: createdUserId, email: cleanEmail },
        { onConflict: "id" }
      );

    if (userError) throw userError;

    const invitedRoleIds = Array.isArray(invitation?.metadata?.role_ids)
      ? invitation.metadata.role_ids
      : [];
    const assignedRoleIds = Array.from(new Set([20, ...(await resolveRoleIds(invitedRoleIds))]));
    const roleRows = assignedRoleIds.map((role_id) => ({
      user_id: createdUserId,
      role_id
    }));

    const { error: roleError } = await supabase
      .from("user_roles")
      .upsert(roleRows, { onConflict: "user_id,role_id" });

    if (roleError) throw roleError;

    await acceptHrInvitation(invitationId, createdUserId);

    const confirmationUrl =
      authData?.properties?.action_link ||
      authData?.properties?.actionLink ||
      authData?.action_link ||
      "";

    await sendHrAccountConfirmation(cleanEmail, confirmationUrl);

    return res.status(201).json({ ok: true, emailConfirmationRequired: true });
  } catch (error) {
    console.error("HR registration failed:", error);

    if (createdUserId) {
      try {
        await restoreHrInvitation(invitationId, createdUserId);
        await supabase.from("users").delete().eq("id", createdUserId);
        await supabase.auth.admin.deleteUser(createdUserId);
      } catch (cleanupError) {
        console.error("HR registration cleanup failed:", cleanupError);
      }
    }

    const status = Number(error?.statusCode) || 500;
    return res.status(status).json({
      message: status === 403 ? error.message : "Unable to complete registration."
    });
  }
});

const listAllAuthUsers = async () => {
  const users = [];
  const perPage = 100;

  for (let page = 1; page <= 100; page += 1) {
    const { data, error } = await supabase.auth.admin.listUsers({ page, perPage });
    if (error) throw error;

    const pageUsers = Array.isArray(data?.users) ? data.users : [];
    users.push(...pageUsers);
    if (pageUsers.length < perPage) break;
  }

  return users;
};

const sendHrInvitation = async (email, code, expiresAt) => {
  if (!RESEND_API_KEY || !HR_INVITE_FROM_EMAIL) {
    throw new Error("HR invitation email is not configured.");
  }

  const registerUrl = `${HR_PORTAL_BASE_URL}/register.html?email=${encodeURIComponent(email)}`;
  const safeCode = escapeHtml(formatInvitationCode(code));
  const safeUrl = escapeHtml(registerUrl);
  const expirationLabel = new Intl.DateTimeFormat("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric"
  }).format(new Date(expiresAt));
  const html = `
    <!DOCTYPE html>
    <html lang="en">
    <head><meta charset="UTF-8"><title>CSP Employee Benefits Invitation</title></head>
    <body style="margin:0;background:#f3f6fb;font-family:Arial,sans-serif;color:#233658;">
      <div style="max-width:620px;margin:0 auto;padding:32px 18px;">
        <div style="background:#ffffff;border:1px solid #d5deec;border-radius:8px;padding:28px;">
          <h1 style="margin:0 0 14px;font-size:26px;color:#233658;">Employee Benefits Account</h1>
          <p style="margin:0 0 18px;line-height:1.55;">You have been invited to create a Coil Steel Processing employee benefits account.</p>
          <p style="margin:0 0 6px;font-size:13px;font-weight:bold;text-transform:uppercase;color:#5b6a87;">One-Time Invitation Code</p>
          <div style="margin:0 0 22px;padding:12px 14px;border:1px solid #d5deec;border-radius:6px;background:#f3f6fb;font-size:22px;font-weight:bold;letter-spacing:.04em;">${safeCode}</div>
          <a href="${safeUrl}" style="display:inline-block;padding:12px 18px;border-radius:6px;background:#f1a91e;color:#ffffff;font-weight:bold;text-decoration:none;">Create Employee Account</a>
          <p style="margin:22px 0 0;color:#5b6a87;font-size:13px;line-height:1.5;">This code can be used once and expires on ${escapeHtml(expirationLabel)}. Do not forward this invitation.</p>
        </div>
      </div>
    </body>
    </html>
  `;

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      from: HR_INVITE_FROM_EMAIL,
      to: [email],
      subject: "Create your CSP employee benefits account",
      html
    })
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload?.message || "The email provider rejected the invitation.");
  }

  return payload?.id || null;
};

app.post("/api/hr/admin/invitations", requireHrAdminAccess, async (req, res) => {
  const email = String(req.body?.email || "").trim().toLowerCase();
  const requestedRoles = Array.isArray(req.body?.roles) ? req.body.roles : [];
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ error: "Enter a valid employee email address." });
  }

  try {
    const authUsers = await listAllAuthUsers();
    const existingUser = authUsers.find(
      (user) => String(user.email || "").trim().toLowerCase() === email
    );
    if (existingUser) {
      return res.status(409).json({
        error: "An account already exists for this email. Grant HR access from the employee list instead."
      });
    }

    const invitation = await createHrInvitation(email, requestedRoles);
    try {
      const providerId = await sendHrInvitation(email, invitation.code, invitation.expires_at);
      await supabase
        .from("hr_invitations")
        .update({ provider_id: providerId })
        .eq("id", invitation.id);

      return res.json({
        success: true,
        email,
        providerId,
        expiresAt: invitation.expires_at
      });
    } catch (emailError) {
      await supabase
        .from("hr_invitations")
        .update({ status: "revoked" })
        .eq("id", invitation.id)
        .eq("status", "pending");
      throw emailError;
    }
  } catch (error) {
    console.error("HR invitation failed:", error);
    const status = /not configured/i.test(error?.message || "") ? 503 : 500;
    return res.status(status).json({ error: error?.message || "Unable to send the invitation." });
  }
});

app.get("/api/hr/admin/users", requireHrAdminAccess, async (req, res) => {
  const search = String(req.query.search || "").trim().toLowerCase();

  try {
    const [authUsers, roleResult] = await Promise.all([
      listAllAuthUsers(),
      supabase
        .from("user_roles")
        .select("user_id,role_id,roles(name)")
    ]);

    if (roleResult.error) throw roleResult.error;

    const rolesByUser = new Map();
    (Array.isArray(roleResult.data) ? roleResult.data : []).forEach((row) => {
      if (!rolesByUser.has(row.user_id)) rolesByUser.set(row.user_id, []);
      rolesByUser.get(row.user_id).push({
        id: Number(row.role_id),
        name: row?.roles?.name || ""
      });
    });

    const users = authUsers
      .filter((user) => !search || String(user.email || "").toLowerCase().includes(search))
      .map((user) => {
        const roles = rolesByUser.get(user.id) || [];
        return {
          id: user.id,
          email: user.email || "",
          created_at: user.created_at || null,
          last_sign_in_at: user.last_sign_in_at || null,
          email_confirmed_at: user.email_confirmed_at || null,
          force_password_change: user.user_metadata?.force_password_change === true,
          has_hr_access: roles.some((role) => role.id === 20 || role.name === "employee" || role.name === "hr"),
          roles
        };
      })
      .sort((left, right) => left.email.localeCompare(right.email));

    res.set("Cache-Control", "no-store");
    return res.json({ users });
  } catch (error) {
    console.error("HR admin user lookup failed:", error);
    return res.status(500).json({ error: error?.message || "Unable to load employees." });
  }
});

app.patch("/api/hr/admin/users/:id", requireHrAdminAccess, async (req, res) => {
  const id = String(req.params.id || "").trim();
  const requestedEmail = req.body?.email;
  const email = requestedEmail === undefined
    ? ""
    : String(requestedEmail || "").trim().toLowerCase();

  if (!id) return res.status(400).json({ error: "A user id is required." });
  if (!email) return res.status(400).json({ error: "Enter a valid email address." });
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ error: "Enter a valid email address." });
  }

  try {
    const { data, error } = await supabase.auth.admin.updateUserById(id, {
      email,
      email_confirm: true
    });
    if (error || !data?.user) throw error || new Error("User update did not return an account.");

    const { error: profileError } = await supabase
      .from("users")
      .upsert({ id, email }, { onConflict: "id" });
    if (profileError) throw profileError;

    return res.json({
      success: true,
      user: {
        id: data.user.id,
        email: data.user.email || email
      }
    });
  } catch (error) {
    console.error("HR admin email update failed:", error);
    return res.status(400).json({ error: error?.message || "Unable to update the employee email." });
  }
});

app.post("/api/hr/admin/users/:id/temporary-password", requireHrAdminAccess, async (req, res) => {
  const id = String(req.params.id || "").trim();
  if (!id) return res.status(400).json({ error: "A user id is required." });

  try {
    const tempPassword = buildTempPassword(14);
    const existingMetadata = await getUserAuthMetadata(id);
    const { error } = await supabase.auth.admin.updateUserById(id, {
      password: tempPassword,
      user_metadata: {
        ...existingMetadata,
        force_password_change: true
      }
    });
    if (error) throw error;

    res.set("Cache-Control", "no-store");
    return res.json({
      success: true,
      tempPassword,
      forcePasswordChange: true
    });
  } catch (error) {
    console.error("HR admin temporary password failed:", error);
    return res.status(400).json({ error: error?.message || "Unable to create a temporary password." });
  }
});

app.put("/api/hr/admin/users/:id/hr-access", requireHrAdminAccess, async (req, res) => {
  const id = String(req.params.id || "").trim();
  const enabled = req.body?.enabled === true;
  if (!id) return res.status(400).json({ error: "A user id is required." });

  try {
    await ensureKnownRoles();
    if (enabled) {
      const { error } = await supabase
        .from("user_roles")
        .upsert({ user_id: id, role_id: 20 }, { onConflict: "user_id,role_id" });
      if (error) throw error;
    } else {
      const { error } = await supabase
        .from("user_roles")
        .delete()
        .eq("user_id", id)
        .eq("role_id", 20);
      if (error) throw error;
    }

    return res.json({ success: true, enabled });
  } catch (error) {
    console.error("HR access update failed:", error);
    return res.status(500).json({ error: error?.message || "Unable to update HR access." });
  }
});

// --- USER MANAGEMENT ENDPOINTS ---

app.get("/api/auth/roles", async (req, res) => {
  try {
    const token = getBearerToken(req);
    if (!token) {
      return res.status(401).json({ error: "Missing Supabase access token." });
    }

    const { data: userData, error: userError } = await supabase.auth.getUser(token);
    const user = userData?.user;
    if (userError || !user?.id) {
      return res.status(401).json({ error: userError?.message || "Invalid or expired Supabase session." });
    }

    const roleRows = await fetchUserRoleRows(user.id);
    res.set("Cache-Control", "no-store");
    return res.json({
      user: {
        id: user.id,
        email: user.email || ""
      },
      roles: roleNamesFromRows(roleRows),
      user_roles: roleRows
    });
  } catch (err) {
    console.error("Auth role endpoint error:", err);
    return res.status(500).json({ error: err?.message || "Failed to load roles." });
  }
});

// Get all users with roles
app.get("/api/users", requireAdminAccess, async (req, res) => {
  const search = String(req.query.search || "").trim().toLowerCase();

  try {
    const [authUsers, roleResult] = await Promise.all([
      listAllAuthUsers(),
      supabase
        .from("user_roles")
        .select("user_id,role_id,roles(name)")
    ]);

    if (roleResult.error) throw roleResult.error;

    const rolesByUser = new Map();
    (Array.isArray(roleResult.data) ? roleResult.data : []).forEach((row) => {
      if (!rolesByUser.has(row.user_id)) rolesByUser.set(row.user_id, []);
      rolesByUser.get(row.user_id).push({
        role_id: Number(row.role_id),
        roles: {
          name: row?.roles?.name || ""
        }
      });
    });

    const users = authUsers
      .filter((user) => !search || String(user.email || "").toLowerCase().includes(search))
      .map((user) => {
        const profile = profileFromAuthUser(user);
        return {
          id: user.id,
          email: user.email || "",
          first_name: profile.first_name,
          last_name: profile.last_name,
          job_title: profile.job_title,
          created_at: user.created_at || null,
          last_sign_in_at: user.last_sign_in_at || null,
          email_confirmed_at: user.email_confirmed_at || null,
          force_password_change: user.user_metadata?.force_password_change === true,
          user_roles: rolesByUser.get(user.id) || []
        };
      })
      .sort((left, right) => left.email.localeCompare(right.email));

    res.set("Cache-Control", "no-store");
    return res.json(users);
  } catch (err) {
    console.error("User endpoint error:", err);
    return res.status(500).json({ error: err?.message || "Failed to fetch users" });
  }
});

// Delete user (removes roles + auth user)
const deleteUserHandler = async (req, res) => {
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
};

app.delete("/api/users/:id", requireAdminAccess, deleteUserHandler);
app.delete("/api/hr/admin/users/:id", requireHrAdminAccess, deleteUserHandler);

// Update user profile and login email
const updateUserProfileHandler = async (req, res) => {
  const id = String(req.params.id || "").trim();
  const email = String(req.body?.email || "").trim().toLowerCase();
  const profile = normalizeUserProfile(req.body || {});

  if (!id) return res.status(400).json({ error: "A user id is required." });
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ error: "Enter a valid email address." });
  }

  try {
    const existingMetadata = await getUserAuthMetadata(id);
    const nextMetadata = {
      ...existingMetadata,
      ...profile
    };

    const { data, error } = await supabase.auth.admin.updateUserById(id, {
      email,
      email_confirm: true,
      user_metadata: nextMetadata
    });
    if (error || !data?.user) throw error || new Error("User update did not return an account.");

    await upsertPublicUserProfile({
      id,
      email,
      ...profile
    });

    res.set("Cache-Control", "no-store");
    return res.json({
      success: true,
      user: {
        id,
        email: data.user.email || email,
        ...profile
      }
    });
  } catch (err) {
    console.error("User profile update error:", err);
    return res.status(400).json({ error: err?.message || "Failed to update user profile." });
  }
};

app.patch("/api/users/:id/profile", requireAdminAccess, updateUserProfileHandler);
app.post("/api/users/:id/profile", requireAdminAccess, updateUserProfileHandler);

// Update user roles
const updateUserRolesHandler = async (req, res) => {
  const { id } = req.params;
  const { roles } = req.body || {};

  if (!Array.isArray(roles)) {
    return res.status(400).json({ error: "Roles must be an array" });
  }

  try {
    const resolvedRoleIds = await resolveRoleIds([...roles, 20]);

    // Remove existing roles
    const { error: deleteError } = await supabase.from("user_roles").delete().eq("user_id", id);
    if (deleteError) throw deleteError;

    // Insert new roles
    const roleRows = resolvedRoleIds.map((role_id) => ({
      user_id: id,
      role_id,
    }));

    if (roleRows.length > 0) {
      const { error: insertError } = await supabase.from("user_roles").insert(roleRows);
      if (insertError) throw insertError;
    }

    return res.json({ success: true, roles: resolvedRoleIds });
  } catch (err) {
    console.error("Update roles error:", err);
    return res.status(500).json({ error: err?.message || "Failed to update roles" });
  }
};

app.put("/api/users/:id/roles", requireAdminAccess, updateUserRolesHandler);
app.post("/api/users/:id/roles", requireAdminAccess, updateUserRolesHandler);
app.put("/api/hr/admin/users/:id/roles", requireHrAdminAccess, updateUserRolesHandler);
app.post("/api/hr/admin/users/:id/roles", requireHrAdminAccess, updateUserRolesHandler);

app.post("/api/users/:id/roles/add", requireAdminAccess, async (req, res) => {
  const { id } = req.params;
  const role = req.body?.role ?? req.body?.role_id ?? req.body?.roleId;
  const roleId = Number(role);

  if (!Number.isInteger(roleId) || !ROLE_BY_ID.has(roleId)) {
    return res.status(400).json({ error: "Valid role id is required." });
  }

  try {
    const { data: existing, error: existingError } = await supabase
      .from("user_roles")
      .select("user_id, role_id")
      .eq("user_id", id)
      .eq("role_id", roleId)
      .maybeSingle();

    if (existingError) throw existingError;
    if (existing) {
      return res.json({ success: true, alreadyHadRole: true, role_id: roleId });
    }

    const { error: insertError } = await supabase
      .from("user_roles")
      .insert({ user_id: id, role_id: roleId });

    if (insertError) throw insertError;
    return res.json({ success: true, role_id: roleId });
  } catch (err) {
    console.error("Add role error:", err);
    return res.status(500).json({ error: err?.message || "Failed to add role" });
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

app.post("/api/users/:id/reset-password-temp", requireAdminAccess, async (req, res) => {
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

app.post("/api/users/:id/force-password-change", requireAdminAccess, async (req, res) => {
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

app.post("/api/users/:id/send-reset-email", requireAdminAccess, async (req, res) => {
  const { id } = req.params;
  const redirectTo =
    String(req.body?.redirectTo || "").trim() ||
    "https://bi.coilsteelprocessing.com/reset-password.html";

  try {
    const { data: publicUser, error: userError } = await supabase
      .from("users")
      .select("email")
      .eq("id", id)
      .maybeSingle();

    if (userError) {
      console.warn("Reset email public user lookup failed:", userError);
    }

    let email = publicUser?.email || "";
    if (!email) {
      const { data: authUser, error: authLookupError } = await supabase.auth.admin.getUserById(id);
      if (authLookupError || !authUser?.user?.email) {
        console.error("Reset email auth lookup failed:", authLookupError);
        return res.status(404).json({ error: "User email not found." });
      }
      email = authUser.user.email;
    }

    const { error: resetError } = await supabase.auth.resetPasswordForEmail(email, {
      redirectTo
    });

    if (resetError) {
      console.error("Reset email send failed:", resetError);
      return res.status(400).json({ error: resetError.message || "Failed to send reset email." });
    }

    return res.json({ success: true, email });
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

const TODD_REQUEST_SELECT = "id,project,requested_by,requested_by_email,date_requested,date_needed,priority,priority_rank,status,notes,created_at,updated_at,completed_at,metadata";
const TODD_PRIORITIES = new Set(["hot", "urgent", "high", "normal", "low"]);
const TODD_STATUSES = new Set(["not_started", "in_progress", "waiting", "on_hold", "long_term", "done"]);
const TODD_ATTACHMENT_BUCKET = "todd-request-attachments";
const TODD_ATTACHMENT_MAX_BYTES = 20 * 1024 * 1024;
const TODD_ATTACHMENT_EXTENSIONS = new Set([
  ".pdf", ".png", ".jpg", ".jpeg", ".webp", ".gif",
  ".doc", ".docx", ".xls", ".xlsx", ".csv", ".txt",
  ".zip", ".mp4", ".mov"
]);
const TODD_WORK_REQUEST_ADMIN_EMAILS = new Set(
  String(process.env.TODD_WORK_REQUEST_ADMIN_EMAILS || "todd@coilsteelprocessing.com,josh@coilsteelprocessing.com")
    .split(",")
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
);

function normalizeToddPriority(value) {
  const priority = coerceText(value, 20).toLowerCase().replace(/\s+/g, "_");
  return TODD_PRIORITIES.has(priority) ? priority : "normal";
}

function normalizeToddStatus(value) {
  const status = coerceText(value, 30).toLowerCase().replace(/[\s-]+/g, "_");
  return TODD_STATUSES.has(status) ? status : "not_started";
}

function normalizeToddAttachment(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const originalName = coerceText(value.name, 220);
  const contentType = coerceText(value.type, 160) || "application/octet-stream";
  const declaredSize = coerceInteger(value.size);
  const base64 = String(value.base64 || "").replace(/\s+/g, "");
  const extension = path.extname(originalName).toLowerCase();

  if (!originalName || !base64) {
    const error = new Error("The selected attachment is incomplete.");
    error.statusCode = 400;
    throw error;
  }
  if (!TODD_ATTACHMENT_EXTENSIONS.has(extension)) {
    const error = new Error("This attachment file type is not supported.");
    error.statusCode = 400;
    throw error;
  }
  if (!declaredSize || declaredSize < 1 || declaredSize > TODD_ATTACHMENT_MAX_BYTES) {
    const error = new Error("The attachment must be 20 MB or smaller.");
    error.statusCode = 400;
    throw error;
  }

  const buffer = Buffer.from(base64, "base64");
  if (!buffer.length || buffer.length !== declaredSize || buffer.length > TODD_ATTACHMENT_MAX_BYTES) {
    const error = new Error("The attachment could not be validated.");
    error.statusCode = 400;
    throw error;
  }

  return { originalName, contentType, extension, buffer };
}

async function ensureToddAttachmentBucket() {
  if (!chartSupabase) throw new Error("Work request storage is not configured.");
  const { data: buckets, error: listError } = await chartSupabase.storage.listBuckets();
  if (listError) throw listError;
  if ((buckets || []).some((bucket) => bucket.id === TODD_ATTACHMENT_BUCKET)) return;

  const { error: createError } = await chartSupabase.storage.createBucket(TODD_ATTACHMENT_BUCKET, {
    public: false,
    fileSizeLimit: TODD_ATTACHMENT_MAX_BYTES
  });
  if (createError && !/already exists|duplicate/i.test(String(createError.message || ""))) {
    throw createError;
  }
}

async function uploadToddAttachment(rawAttachment) {
  const attachment = normalizeToddAttachment(rawAttachment);
  if (!attachment) return null;
  await ensureToddAttachmentBucket();

  const safeBaseName = path.basename(attachment.originalName, attachment.extension)
    .replace(/[^a-z0-9_-]+/gi, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80) || "attachment";
  const folder = new Date().toISOString().slice(0, 7);
  const objectPath = `${folder}/${crypto.randomUUID()}-${safeBaseName}${attachment.extension}`;
  const { error } = await chartSupabase.storage
    .from(TODD_ATTACHMENT_BUCKET)
    .upload(objectPath, attachment.buffer, {
      contentType: attachment.contentType,
      cacheControl: "3600",
      upsert: false
    });

  if (error) throw error;
  return {
    bucket: TODD_ATTACHMENT_BUCKET,
    path: objectPath,
    name: attachment.originalName,
    type: attachment.contentType,
    size: attachment.buffer.length
  };
}

async function getToddRequestViewerEmail(req) {
  const authHeader = coerceText(req.headers.authorization, 1000);
  const bearerToken = authHeader.match(/^bearer\s+(.+)$/i)?.[1];
  if (bearerToken) {
    const { data, error } = await supabase.auth.getUser(bearerToken);
    if (!error && data?.user?.email) return String(data.user.email).trim().toLowerCase();
  }
  return "";
}

async function requireToddRequestAdmin(req, res) {
  const email = await getToddRequestViewerEmail(req);
  if (email && TODD_WORK_REQUEST_ADMIN_EMAILS.has(email)) return email;
  res.status(403).json({ error: "This request list is only available to Todd and Josh." });
  return "";
}

app.get("/api/todd-requests", async (req, res) => {
  const viewerEmail = await requireToddRequestAdmin(req, res);
  if (!viewerEmail) return;

  try {
    const includeDone = String(req.query.includeDone || req.query.include_done || "true").toLowerCase() !== "false";
    let query = chartSupabase
      .from("todd_work_requests")
      .select(TODD_REQUEST_SELECT)
      .order("priority_rank", { ascending: true })
      .order("created_at", { ascending: true });

    if (!includeDone) query = query.neq("status", "done");

    const { data, error } = await query;
    if (error) throw error;
    return res.json({ requests: data || [] });
  } catch (err) {
    console.error("Todd requests list error:", err);
    return res.status(500).json({ error: err.message || "Unable to load work requests." });
  }
});

app.get("/api/todd-requests/:id/attachment", async (req, res) => {
  const viewerEmail = await requireToddRequestAdmin(req, res);
  if (!viewerEmail) return;

  const id = coerceInteger(req.params.id);
  if (!id) return res.status(400).json({ error: "Valid request id is required." });

  try {
    const { data: requestRow, error: requestError } = await chartSupabase
      .from("todd_work_requests")
      .select("metadata")
      .eq("id", id)
      .maybeSingle();

    if (requestError) throw requestError;
    if (!requestRow) return res.status(404).json({ error: "Work request not found." });

    const attachment = requestRow.metadata?.attachment;
    if (!attachment?.path || attachment.bucket !== TODD_ATTACHMENT_BUCKET) {
      return res.status(404).json({ error: "This request does not have an attachment." });
    }

    const { data, error } = await chartSupabase.storage
      .from(TODD_ATTACHMENT_BUCKET)
      .createSignedUrl(attachment.path, 120, { download: attachment.name || true });

    if (error) throw error;
    return res.json({ url: data.signedUrl, name: attachment.name || "attachment" });
  } catch (err) {
    console.error("Todd request attachment error:", err);
    return res.status(500).json({ error: err.message || "Unable to open the attachment." });
  }
});

app.post("/api/todd-requests", async (req, res) => {
  const body = req.body || {};
  const project = coerceText(body.project, 300);
  const requestedBy = coerceText(body.requestedBy || body.requested_by, 160);
  const requestedByEmail = coerceText(body.requestedByEmail || body.requested_by_email, 320).toLowerCase();
  const dateRequested = coerceDateText(body.dateRequested || body.date_requested) || new Date().toISOString().slice(0, 10);
  const dateNeeded = coerceDateText(body.dateNeeded || body.date_needed);
  const priority = normalizeToddPriority(body.priority);
  const notes = coerceText(body.notes, 5000);
  let uploadedAttachment = null;

  if (!project) return res.status(400).json({ error: "Project is required." });
  if (!requestedBy) return res.status(400).json({ error: "Person that requested is required." });

  try {
    uploadedAttachment = await uploadToddAttachment(body.attachment);
    const { data: maxRows, error: maxError } = await chartSupabase
      .from("todd_work_requests")
      .select("priority_rank")
      .order("priority_rank", { ascending: false })
      .limit(1);

    if (maxError) throw maxError;
    const nextRank = (Number(maxRows?.[0]?.priority_rank || 0) || 0) + 100;
    const now = new Date().toISOString();
    const insertRow = {
      project,
      requested_by: requestedBy,
      requested_by_email: requestedByEmail || null,
      date_requested: dateRequested,
      date_needed: dateNeeded,
      priority,
      priority_rank: nextRank,
      status: "not_started",
      notes: notes || null,
      created_at: now,
      updated_at: now,
      metadata: {
        ...sanitizePlainObject(body.metadata),
        ...(uploadedAttachment ? { attachment: uploadedAttachment } : {})
      }
    };

    const { data, error } = await chartSupabase
      .from("todd_work_requests")
      .insert(insertRow)
      .select(TODD_REQUEST_SELECT)
      .single();

    if (error) throw error;
    return res.status(201).json({ request: data });
  } catch (err) {
    if (uploadedAttachment?.path) {
      await chartSupabase.storage
        .from(TODD_ATTACHMENT_BUCKET)
        .remove([uploadedAttachment.path])
        .catch(() => {});
    }
    console.error("Todd request create error:", err);
    return res.status(err.statusCode || 500).json({ error: err.message || "Unable to save work request." });
  }
});

app.put("/api/todd-requests/:id", async (req, res) => {
  const viewerEmail = await requireToddRequestAdmin(req, res);
  if (!viewerEmail) return;

  const id = coerceInteger(req.params.id);
  if (!id) return res.status(400).json({ error: "Valid request id is required." });

  const body = req.body || {};
  const updateRow = {
    updated_at: new Date().toISOString()
  };

  if (Object.prototype.hasOwnProperty.call(body, "project")) updateRow.project = coerceText(body.project, 300);
  if (Object.prototype.hasOwnProperty.call(body, "requestedBy") || Object.prototype.hasOwnProperty.call(body, "requested_by")) {
    updateRow.requested_by = coerceText(body.requestedBy || body.requested_by, 160);
  }
  if (Object.prototype.hasOwnProperty.call(body, "requestedByEmail") || Object.prototype.hasOwnProperty.call(body, "requested_by_email")) {
    updateRow.requested_by_email = coerceText(body.requestedByEmail || body.requested_by_email, 320).toLowerCase() || null;
  }
  if (Object.prototype.hasOwnProperty.call(body, "dateRequested") || Object.prototype.hasOwnProperty.call(body, "date_requested")) {
    updateRow.date_requested = coerceDateText(body.dateRequested || body.date_requested);
  }
  if (Object.prototype.hasOwnProperty.call(body, "dateNeeded") || Object.prototype.hasOwnProperty.call(body, "date_needed")) {
    updateRow.date_needed = coerceDateText(body.dateNeeded || body.date_needed);
  }
  if (Object.prototype.hasOwnProperty.call(body, "priority")) updateRow.priority = normalizeToddPriority(body.priority);
  if (Object.prototype.hasOwnProperty.call(body, "notes")) updateRow.notes = coerceText(body.notes, 5000) || null;
  if (Object.prototype.hasOwnProperty.call(body, "status")) {
    updateRow.status = normalizeToddStatus(body.status);
    updateRow.completed_at = updateRow.status === "done" ? updateRow.updated_at : null;
  }

  try {
    const { data, error } = await chartSupabase
      .from("todd_work_requests")
      .update(updateRow)
      .eq("id", id)
      .select(TODD_REQUEST_SELECT)
      .maybeSingle();

    if (error) throw error;
    if (!data) return res.status(404).json({ error: "Work request not found." });
    return res.json({ request: data });
  } catch (err) {
    console.error("Todd request update error:", err);
    return res.status(500).json({ error: err.message || "Unable to update work request." });
  }
});

app.post("/api/todd-requests/reorder", async (req, res) => {
  const viewerEmail = await requireToddRequestAdmin(req, res);
  if (!viewerEmail) return;

  const ids = Array.isArray(req.body?.ids)
    ? req.body.ids.map(coerceInteger).filter(Boolean)
    : [];

  if (!ids.length) return res.status(400).json({ error: "ids array is required." });

  try {
    const now = new Date().toISOString();
    const updates = ids.map((id, index) =>
      chartSupabase
        .from("todd_work_requests")
        .update({ priority_rank: (index + 1) * 100, updated_at: now })
        .eq("id", id)
    );

    const results = await Promise.all(updates);
    const failed = results.find((result) => result.error);
    if (failed?.error) throw failed.error;

    const { data, error } = await chartSupabase
      .from("todd_work_requests")
      .select(TODD_REQUEST_SELECT)
      .order("priority_rank", { ascending: true })
      .order("created_at", { ascending: true });

    if (error) throw error;
    return res.json({ requests: data || [] });
  } catch (err) {
    console.error("Todd request reorder error:", err);
    return res.status(500).json({ error: err.message || "Unable to reorder work requests." });
  }
});

app.post("/api/pro/forms/submit", async (req, res) => {
  if (!chartSupabase) {
    return res.status(503).json({
      error: "Pro form storage is not configured. Set CHART_SUPABASE_URL and CHART_SUPABASE_SERVICE_ROLE_KEY on the server."
    });
  }

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
  const isTestSubmission = /\btest\b/i.test(String(formLabel || ""));
  const clientSubmissionToken = coerceText(body.submissionToken || body.submission_token, 160);

  // Older hosted form bundles did not send a client token. Keep those safe by
  // deriving a short-lived, deterministic key from the request contents. This
  // collapses repeated clicks/browser retries in the same minute without
  // preventing a genuinely new report submitted later.
  const submissionMinute = new Date();
  submissionMinute.setUTCSeconds(0, 0);
  const automaticFingerprint = crypto
    .createHash("sha256")
    .update(JSON.stringify({
      formKey,
      formLabel,
      submittedBy,
      dimensions,
      metrics,
      payload,
      notes,
      submissionMinute: submissionMinute.toISOString()
    }))
    .digest("hex");
  const idempotencyKey = clientSubmissionToken
    ? `client:${clientSubmissionToken}`
    : `auto:${automaticFingerprint}`;

  if (!formKey) {
    return res.status(400).json({ error: "formKey is required." });
  }

  if (!submittedBy) {
    return res.status(400).json({ error: "submittedBy is required." });
  }

  try {
    const submissionRow = {
      idempotency_key: idempotencyKey,
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
      if (submissionError?.code === "23505") {
        const { data: existingSubmission } = await chartSupabase
          .from("pro_form_submissions")
          .select("id")
          .eq("idempotency_key", idempotencyKey)
          .maybeSingle();

        return res.json({
          success: true,
          duplicate: true,
          submissionId: existingSubmission?.id || null,
          chartRowsInserted: 0,
          maintenanceOrdersCreated: 0,
          maintenanceOrders: [],
          notification: {
            sent: false,
            duplicateSuppressed: true,
            reason: "Duplicate submission ignored; the original report was already saved and emailed."
          },
          maintenanceNotifications: []
        });
      }
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
        payload
      });
    } catch (notificationError) {
      console.error("Pro submission notification failed:", notificationError);
      notification = {
        sent: false,
        reason: notificationError.message || "Notification send failed."
      };
    }

    const maintenanceNotifications = [];

    if (formKey === "shift_report") {
      const maintenanceCalls = getArray(payload.maintenanceTimes).filter((call) =>
        call?.maintenanceCallTime || call?.maintenanceArrivalTime || call?.maintenanceCompletionTime
      );
      const hasMaintenanceDetails = Boolean(
        coerceText(payload.maintenanceReason, 1000) ||
        coerceText(dimensions.maintenance_tech || payload.maintenanceTech, 160)
      );
      if (maintenanceCalls.length > 0 || hasMaintenanceDetails) {
        try {
          maintenanceNotifications.push({
            type: "shift_maintenance_email",
            ...await sendSpecialFormNotification({
              recipients: isTestSubmission ? PRO_TEST_RECIPIENTS : SHIFT_MAINTENANCE_RECIPIENTS,
              subject: `${coerceText(dimensions.shift, 80) || "Shift"} Shift Maintenance Call`,
              html: buildShiftMaintenanceEmail({ submittedBy, submittedAt, dimensions, payload })
            })
          });
        } catch (maintenanceEmailError) {
          console.error("Shift maintenance email failed:", maintenanceEmailError);
          maintenanceNotifications.push({ type: "shift_maintenance_email", sent: false, reason: maintenanceEmailError.message });
        }
      }
    }

    if (formKey === "crane_inspection" && Number(metrics.failed_checks || 0) > 0) {
      try {
        maintenanceNotifications.push({
          type: "crane_failure_email",
          ...await sendSpecialFormNotification({
            recipients: isTestSubmission ? PRO_TEST_RECIPIENTS : CRANE_FAILURE_RECIPIENTS,
            subject: `Crane Failure Notice — ${coerceText(dimensions.crane_name, 160) || "Crane"}`,
            html: buildCraneFailureEmail({ submittedBy, submittedAt, dimensions, payload })
          })
        });
      } catch (craneFailureEmailError) {
        console.error("Crane failure email failed:", craneFailureEmailError);
        maintenanceNotifications.push({ type: "crane_failure_email", sent: false, reason: craneFailureEmailError.message });
      }
    }

    if (formKey === "forklift_inspection" && Number(metrics.failed_checks || 0) > 0) {
      try {
        maintenanceNotifications.push({
          type: "forklift_failure_email",
          ...await sendSpecialFormNotification({
            recipients: isTestSubmission ? PRO_TEST_RECIPIENTS : FORKLIFT_FAILURE_RECIPIENTS,
            subject: `Forklift Failure Notice — ${coerceText(dimensions.asset_name, 160) || coerceText(dimensions.forklift_number, 120) || "Forklift"}`,
            html: buildForkliftFailureEmail({ submittedBy, submittedAt, dimensions, payload })
          })
        });
      } catch (forkliftFailureEmailError) {
        console.error("Forklift failure email failed:", forkliftFailureEmailError);
        maintenanceNotifications.push({ type: "forklift_failure_email", sent: false, reason: forkliftFailureEmailError.message });
      }
    }

    if (!isTestSubmission) {
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
    }

    return res.json({
      success: true,
      duplicate: false,
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

const QUOTE_EXTRACTION_MODEL = process.env.OPENAI_QUOTE_MODEL || "gpt-4o";
const MIN_AUTO_QUOTE_WEIGHT = 1000;
const MAX_AUTO_QUOTE_WEIGHT = 120000;

function parseJsonObject(text) {
  const raw = String(text || "").trim();
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch (err) {
    const match = raw.match(/\{[\s\S]*\}/);
    if (!match) return {};
    try {
      return JSON.parse(match[0]);
    } catch (nestedErr) {
      return {};
    }
  }
}

function positiveNumberFromValue(value) {
  if (typeof value === "number") {
    return Number.isFinite(value) && value > 0 ? value : null;
  }
  if (typeof value === "string") {
    const cleaned = value.replace(/[^0-9.+-]/g, "");
    if (!cleaned) return null;
    const parsed = Number(cleaned);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
  }
  return null;
}

function normalizeWeightCandidates(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => {
      if (item && typeof item === "object" && !Array.isArray(item)) {
        const parsed = positiveNumberFromValue(item.value ?? item.weight ?? item.pounds ?? item.lbs ?? item.originalCoilQty ?? item.qty ?? item.quantity);
        if (!parsed) return null;
        return {
          value: parsed,
          label: cleanTextValue(item.label || item.section || item.source || item.text),
          evidence: cleanTextValue(item.evidence || item.sourceText || item.text || item.label)
        };
      }
      const parsed = positiveNumberFromValue(item);
      return parsed ? { value: parsed, label: "", evidence: "" } : null;
    })
    .filter(Boolean);
}

function cleanTextValue(value) {
  return String(value ?? "").trim();
}

function digitsOnly(value) {
  return cleanTextValue(value).replace(/\D/g, "");
}

function candidateHasExactWeightEvidence(candidate, sharedEvidence = "") {
  if (!candidate?.value) return false;
  const weightDigits = String(Math.round(Number(candidate.value)));
  if (!weightDigits) return false;
  const evidenceDigits = digitsOnly([candidate.label, candidate.evidence, sharedEvidence].filter(Boolean).join(" "));
  return evidenceDigits.includes(weightDigits);
}

function resolveQuoteWeight(data, warnings, mode = "quote") {
  const isWorkOrder = mode === "workOrder";
  const weightEvidence = cleanTextValue(data.weightEvidence || data.weightSource || data.weightBasis);
  const directCandidates = [
    { value: positiveNumberFromValue(data.masterCoilWeight), label: "masterCoilWeight" },
    { value: positiveNumberFromValue(data.weight), label: "weight" },
    { value: positiveNumberFromValue(data.pounds), label: "pounds" }
  ].filter((item) => item.value);
  const visibleCandidates = normalizeWeightCandidates(data.weightCandidates);
  const qtyCandidates = [
    { value: positiveNumberFromValue(data.originalCoilQty), label: "originalCoilQty" },
    { value: positiveNumberFromValue(data.qty), label: "qty" },
    { value: positiveNumberFromValue(data.quantity), label: "quantity" }
  ].filter((item) => item.value && item.value >= MIN_AUTO_QUOTE_WEIGHT);
  if (directCandidates.length && !weightEvidence && !visibleCandidates.length && !qtyCandidates.length) {
    warnings.push("Weight was not populated because no visible source text/evidence was provided by the extraction.");
    return "";
  }
  const allCandidates = [...qtyCandidates, ...visibleCandidates, ...directCandidates];
  const usable = allCandidates.find((item) => item.value >= MIN_AUTO_QUOTE_WEIGHT && item.value <= MAX_AUTO_QUOTE_WEIGHT);

  if (usable) {
    if (isWorkOrder && Math.round(usable.value) === 45000 && !candidateHasExactWeightEvidence(usable, weightEvidence)) {
      pushUniqueWarning(warnings, "Weight was not populated because 45,000 looked like an assumed/default value and no exact visible weight evidence was provided.");
      return "";
    }
    if (qtyCandidates.includes(usable)) {
      warnings.push(`Weight was populated from ${usable.label}; verify the Qty column is the original coil weight.`);
    }
    return usable.value;
  }

  const rejected = allCandidates.find((item) => item.value > MAX_AUTO_QUOTE_WEIGHT);
  if (rejected) {
    warnings.push(`Weight ${Math.round(rejected.value).toLocaleString("en-US")} was not populated because it is outside the automatic quote range; verify the production coil LBS manually.`);
    return "";
  }

  return "";
}

function normalizeIdentifierValue(value) {
  return cleanTextValue(value).toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function isWorkOrderLikeTagValue(value, referenceValues = []) {
  const normalized = normalizeIdentifierValue(value);
  if (!normalized) return false;
  if (referenceValues.some((reference) => normalizeIdentifierValue(reference) === normalized)) return true;
  return /^(?:PO|WO|SO|PP|PR)\d{4,}$/.test(normalized);
}

function pushUniqueWarning(warnings, warning) {
  if (!warnings.includes(warning)) warnings.push(warning);
}

function isInternalCspCustomerName(value) {
  const normalized = cleanTextValue(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) return false;
  return [
    "csp",
    "csp coil steel prs",
    "csp coil steel processing",
    "coil steel prs",
    "coil steel processing",
    "warehouse process"
  ].includes(normalized);
}

function firstArrayValue(...values) {
  for (const value of values) {
    if (Array.isArray(value) && value.length) return value;
  }
  return [];
}

function firstScalarValue(...values) {
  for (const value of values) {
    if (Array.isArray(value)) {
      const first = value.find((item) => item !== undefined && item !== null && cleanTextValue(item) !== "");
      if (first !== undefined) return first;
    } else if (value !== undefined && value !== null && cleanTextValue(value) !== "") {
      return value;
    }
  }
  return "";
}

function arrayItem(values, index) {
  return Array.isArray(values) && values[index] !== undefined && values[index] !== null ? values[index] : "";
}

function normalizeExtractedCoil(coil = {}) {
  const shortTons = positiveNumberFromValue(coil.shortTons ?? coil.shortTonsRfq ?? coil.shortTonsPerRfq);
  const weight = positiveNumberFromValue(coil.weight ?? coil.pounds ?? coil.lbs) || (shortTons ? shortTons * 2000 : "");
  return {
    weight,
    gauge: firstScalarValue(coil.gauge, coil.gaugeMin, coil.gaugeMax),
    width: firstScalarValue(coil.width),
    length: firstScalarValue(coil.length),
    grade: firstScalarValue(coil.grade, coil.alloyGrade),
    coating: firstScalarValue(coil.coating, coil.coatingWeight),
    materialType: firstScalarValue(coil.materialType),
    finishedGoodType: firstScalarValue(coil.finishedGoodType),
    quality: firstScalarValue(coil.quality, coil.qualityType),
    shortTons: shortTons || "",
    maxLift: firstScalarValue(coil.maxLift),
    liftUnitIndicator: firstScalarValue(coil.liftUnitIndicator)
  };
}

function coilsFromParallelExtractionArrays(data = {}) {
  const gauges = firstArrayValue(data.gauges, data.gauge, data.gaugeMin, data.gaugeMins);
  const widths = firstArrayValue(data.widths, data.width);
  const lengths = firstArrayValue(data.lengths, data.length);
  const grades = firstArrayValue(data.grades, data.grade, data.alloyGrade, data.alloyGrades);
  const coatings = firstArrayValue(data.coatings, data.coating, data.coatingWeight, data.coatingWeights);
  const materialTypes = firstArrayValue(data.materialTypes, data.materialType);
  const finishedGoodTypes = firstArrayValue(data.finishedGoodTypes, data.finishedGoodType);
  const qualities = firstArrayValue(data.qualities, data.quality, data.qualityType, data.qualityTypes);
  const weights = firstArrayValue(data.weights, data.weight, data.pounds, data.lbs);
  const shortTons = firstArrayValue(data.shortTons, data.shortTonsRfq, data.shortTonsPerRfq);
  const maxLifts = firstArrayValue(data.maxLifts, data.maxLift);
  const liftUnits = firstArrayValue(data.liftUnitIndicators, data.liftUnitIndicator);
  const rowCount = Math.max(
    gauges.length,
    widths.length,
    lengths.length,
    grades.length,
    coatings.length,
    materialTypes.length,
    finishedGoodTypes.length,
    qualities.length,
    weights.length,
    shortTons.length,
    maxLifts.length,
    liftUnits.length
  );
  if (rowCount < 2) return [];
  return Array.from({ length: rowCount }, (_, index) => normalizeExtractedCoil({
    weight: arrayItem(weights, index),
    gauge: arrayItem(gauges, index),
    width: arrayItem(widths, index),
    length: arrayItem(lengths, index),
    grade: arrayItem(grades, index),
    coating: arrayItem(coatings, index),
    materialType: arrayItem(materialTypes, index),
    finishedGoodType: arrayItem(finishedGoodTypes, index),
    quality: arrayItem(qualities, index),
    shortTons: arrayItem(shortTons, index),
    maxLift: arrayItem(maxLifts, index),
    liftUnitIndicator: arrayItem(liftUnits, index)
  })).filter((coil) => coil.gauge || coil.width || coil.length || coil.weight);
}

function normalizeQuoteExtraction(value, mode = "quote") {
  const isWorkOrder = mode === "workOrder";
  const data = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const packaging = data.packaging && typeof data.packaging === "object" && !Array.isArray(data.packaging)
    ? data.packaging
    : {};
  const warnings = Array.isArray(data.warnings) ? data.warnings.map(String).filter(Boolean) : [];
  const normalizedWeight = resolveQuoteWeight(data, warnings, mode);
  let customer = cleanTextValue(data.customer);
  let customerLogoText = cleanTextValue(data.customerLogoText);
  const customerEvidence = cleanTextValue(data.customerEvidence || data.customerSource || data.customerBasis);
  const weightEvidence = cleanTextValue(data.weightEvidence || data.weightSource || data.weightBasis);
  const productionPoNumber = cleanTextValue(data.productionPoNumber || data.productionPo || data.jobPoNumber);
  const packagingPoNumber = cleanTextValue(data.packagingPoNumber || data.packagingPo);
  const poLikeValues = [
    productionPoNumber,
    packagingPoNumber,
    cleanTextValue(data.poNumber),
    cleanTextValue(data.poRef),
    cleanTextValue(data.workOrder)
  ].filter(Boolean);
  let cspTag = cleanTextValue(data.cspTag);
  let tag = cleanTextValue(data.tag);
  const coilIds = Array.isArray(data.coilIds)
    ? data.coilIds.map(cleanTextValue).filter(Boolean)
    : [];
  const coilCount = positiveNumberFromValue(data.coilCount) || (coilIds.length ? coilIds.length : "");
  const directCoils = (Array.isArray(data.coils) ? data.coils : [])
    .map((coil) => normalizeExtractedCoil(coil && typeof coil === "object" && !Array.isArray(coil) ? coil : {}))
    .filter((coil) => coil.gauge || coil.width || coil.length || coil.weight);
  const parallelCoils = coilsFromParallelExtractionArrays(data);
  const extractedCoils = parallelCoils.length > directCoils.length ? parallelCoils : directCoils;

  if (isWorkOrder) {
    if (cspTag || tag) {
      pushUniqueWarning(warnings, "CSP Tag # was left blank because it must be entered manually for work-order uploads.");
    }
    cspTag = "";
    tag = "";
  } else if (isWorkOrderLikeTagValue(cspTag, poLikeValues)) {
    cspTag = "";
    pushUniqueWarning(warnings, "CSP Tag # was not populated because the extracted value matched or appeared to be a PO/work-order number.");
  }
  if (isWorkOrderLikeTagValue(tag, poLikeValues)) {
    tag = "";
    pushUniqueWarning(warnings, "Tag was ignored because the extracted value matched or appeared to be a PO/work-order number.");
  }

  if (isInternalCspCustomerName(customer)) {
    customer = "";
    warnings.push("Customer was not populated because the visible name appears to be CSP/internal header text.");
  }
  if (isInternalCspCustomerName(customerLogoText)) {
    customerLogoText = "";
    warnings.push("Customer logo text was ignored because it appears to be CSP/internal header text.");
  }
  if (!customer && !customerLogoText) {
    warnings.push("Customer name was not explicitly visible; verify manually.");
  }
  if ((customer || customerLogoText) && !customerEvidence) {
    customer = "";
    customerLogoText = "";
    warnings.push("Customer name was not populated because no visible customer evidence was provided by the extraction.");
  }

  return {
    customer,
    customerLogoText,
    customerEvidence,
    customerAddress: data.customerAddress || "",
    productionPoNumber,
    packagingPoNumber,
    poNumber: productionPoNumber || data.poNumber || data.poRef || data.workOrder || "",
    poRef: productionPoNumber || data.poRef || data.poNumber || data.workOrder || "",
    workOrder: data.workOrder || "",
    quoteDate: data.quoteDate || data.reqDate || data.poDate || "",
    reqDate: data.reqDate || data.quoteDate || "",
    poDate: data.poDate || "",
    process: data.process === "stretched" ? "stretched" : data.process === "leveled" ? "leveled" : "",
    line: data.line === "rbi" ? "rbi" : data.line === "herr" ? "herr" : "",
    cspTag: cspTag || tag || "",
    tag: tag || cspTag || "",
    materialType: data.materialType || "",
    finishedGoodItemNum: data.finishedGoodItemNum || data.goodItemNum || "",
    finishedGoodType: data.finishedGoodType || "",
    coating: data.coating || data.coatingWeight || "",
    coatingWeight: data.coatingWeight || data.coating || "",
    grade: data.grade || data.alloyGrade || "",
    alloyGrade: data.alloyGrade || data.grade || "",
    quality: data.quality || data.qualityType || "",
    qualityType: data.qualityType || data.quality || "",
    gauge: firstScalarValue(data.gauge, data.gaugeMin, data.gaugeMax),
    gaugeMin: firstScalarValue(data.gaugeMin),
    gaugeMax: firstScalarValue(data.gaugeMax),
    tolerance: data.tolerance ?? data.gaugeTolerance ?? "",
    gaugeTolerance: data.gaugeTolerance ?? data.tolerance ?? "",
    width: firstScalarValue(data.width),
    length: firstScalarValue(data.length),
    lengths: Array.isArray(data.lengths) ? data.lengths : [],
    coils: extractedCoils,
    weight: normalizedWeight,
    pounds: normalizedWeight,
    masterCoilWeight: normalizedWeight,
    weightEvidence,
    weightCandidates: normalizeWeightCandidates(data.weightCandidates),
    coilCount,
    coilIds,
    perPieceWeight: data.perPieceWeight ?? "",
    pieces: data.pieces ?? "",
    maxLift: firstScalarValue(data.maxLift, extractedCoils.find((coil) => coil.maxLift)?.maxLift),
    skidQty: data.skidQty ?? "",
    boardQty: data.boardQty ?? "",
    liftUnitIndicator: data.liftUnitIndicator || "",
    flatnessSpec: data.flatnessSpec || "",
    quoteLeadTime: data.quoteLeadTime || data.leadTime || "",
    actualYield: data.actualYield || "",
    packaging: {
      blocks3x4: Boolean(packaging.blocks3x4),
      paperWrap: Boolean(packaging.paperWrap),
      paperTop: Boolean(packaging.paperTop),
      banding4way: Boolean(packaging.banding4way),
      stenciling: Boolean(packaging.stenciling),
      fullBoard3x4: Boolean(packaging.fullBoard3x4),
      skids: Boolean(packaging.skids)
    },
    needsSkids: Boolean(data.needsSkids || packaging.skids || /skid/i.test(cleanTextValue(data.liftUnitIndicator)) || extractedCoils.some((coil) => /skid/i.test(cleanTextValue(coil.liftUnitIndicator)))),
    needsFullBoards: Boolean(data.needsFullBoards || packaging.fullBoard3x4),
    notes: data.notes || "",
    billingNotes: data.billingNotes || "",
    warnings: [...new Set(warnings)],
    confidence: Number(data.confidence || 0) || null
  };
}

function quoteExtractionPrompt(mode = "quote") {
  const isWorkOrder = mode === "workOrder";
  return [
    isWorkOrder
      ? "Extract PO and work-order data from the uploaded CSP customer document screenshot or image."
      : "Extract quote request data from the uploaded customer email screenshot or quote request image.",
    isWorkOrder
      ? "This is for PO/work-order intake and work-order pricing sheet review, not the quote calculator."
      : "This is for the quote pricing calculator.",
    "Return only a JSON object. Do not include markdown.",
    "",
    "Use these exact keys when present:",
    "customer, customerLogoText, customerEvidence, customerAddress, productionPoNumber, packagingPoNumber, poNumber, poRef, workOrder, quoteDate, reqDate, poDate, process, line, cspTag, tag, materialType, finishedGoodItemNum, finishedGoodType, coating, coatingWeight, grade, alloyGrade, quality, qualityType, gauge, gaugeMin, gaugeMax, tolerance, gaugeTolerance, width, length, lengths, coils, weight, pounds, masterCoilWeight, originalCoilQty, qty, quantity, weightEvidence, weightCandidates, coilCount, coilIds, perPieceWeight, pieces, maxLift, skidQty, boardQty, liftUnitIndicator, flatnessSpec, quoteLeadTime, actualYield, packaging, needsSkids, needsFullBoards, notes, billingNotes, warnings, confidence.",
    "",
    "Only populate customer or customerLogoText from an explicit visible external customer name, customer logo, bill-to/sold-to/ship-to/customer-name field, email sender organization, or customer address block.",
    "Put a short description of the visible source text used for the customer in customerEvidence.",
    "If the customer name is not explicitly visible, leave customer and customerLogoText empty.",
    "Do not infer the customer from a reusable template, document layout, prior examples, PO/work-order/SO/job/order numbers, item numbers, dimensions, packaging, process, or line.",
    "Do not use CSP, Coil Steel Processing, CSP Coil Steel Prs, Warehouse Process, or similar internal CSP header/logo text as the customer.",
    "If the same template could belong to another customer, leave customer blank and add a warning that the customer is ambiguous.",
    "If the visible email signature/domain/logo says WorthingtonSteel.com or Worthington Steel, customer should be Worthington Steel. Do not use CIRAL unless CIRAL is explicitly visible in the request.",
    isWorkOrder
      ? "For RCP/vendor setup or PO forms, prefer the company/location in a visible From, Vendor, PO issuer, sold-to, bill-to, or ship-from field as the customer. Example: if the form says \"From: RCP - Hamilton\" or \"Hamilton, OH\", customer should be \"Hamilton\" and customerEvidence should cite that visible From text."
      : "For RFQ/email documents, prefer the sender company or explicit customer/bill-to/sold-to name as the customer.",
    "For pasted email headers, ignore internal CSP recipients in To/Cc. If From uses sttxna.com, customer should be Steel Technologies LLC and customerEvidence should cite the From email domain.",
    isWorkOrder
      ? "On RCP forms, do not treat an \"RCP Customer\" line as the CSP quote customer when a separate From/Vendor/issuer field is visible; that line may be an end-customer or downstream reference."
      : "Do not use downstream customer/reference lines when a direct sender or bill-to customer is visible.",
    "Normalize process to either \"leveled\" or \"stretched\". If text says stretcher leveled or stretched leveled, use \"stretched\".",
    "Normalize line to \"herr\" for Herr-Voss and \"rbi\" for RBI.",
    isWorkOrder
      ? "For work-order uploads, leave cspTag and tag empty. CSP Tag # is not printed on these orders and must be entered manually. Do not use Rel Tag #, Mill Tag Number, Sales Order, Process Order, Customer PO, coil tag, or material tag as CSP Tag #."
      : "Only populate cspTag/tag from a visible CSP tag, material tag, coil tag, or tag/lot field. Do not use PO numbers, work-order numbers, sales-order numbers, customer part numbers, setup IDs, or values like PP044370 or PR423214 as cspTag/tag.",
    "Numbers should be numbers when possible. Strip commas, #, lbs, inches, and quote marks from numeric values.",
    "For multiple lengths, put the first length in length and all lengths in lengths.",
    "For RFQ tables with multiple material rows, return every quoted row in coils. Each coils item should include gauge, width, length, grade/alloyGrade, coating/coatingWeight, materialType, finishedGoodType, quality/qualityType, shortTons, weight/pounds, maxLift, and liftUnitIndicator when visible.",
    "Customer RFQ tables may use simple headers such as Gauge, Width, Length, Type, Tyoe, Grade, Quantity. Treat Type/Tyoe as materialType, and when Quantity/Qty is a large material amount such as 45,000 in an RFQ row, use it as weight/pounds, not piece count.",
    "Treat packaging language such as lift, lifts, bundle, bundle weight, or standard pack - 4000# lifts as maxLift when paired with a pounds/# value. Do not use that lift value as coil weight.",
    "Some pasted RFQ tables list all headers first, then each row's values below the headers. Reconstruct those rows in header order.",
    "If an RFQ table has 4 visible item rows, coils must contain 4 items. If it has 2 visible item rows, coils must contain 2 items. Never return only the last row, thickest row, or a representative row.",
    "Example: rows with gauges 0.0785, 0.164, 0.100, and 0.118 must become four separate coils items, preserving each row's grade, coating, width, length, Short Tons / RFQ, Lift Unit Indicator, and Max Lift.",
    "If a row has Short Tons / RFQ, convert it to pounds by multiplying by 2000 and put that pounds value on that coils item. Do not collapse multiple RFQ rows into one coil.",
    "If Lift Unit Indicator says Skid on any row, set needsSkids true.",
    isWorkOrder
      ? "Use weight/pounds/masterCoilWeight for the inbound coil or order weight, not per-piece weight."
      : "Use weight/pounds/masterCoilWeight for the inbound coil or RFQ weight, not per-piece weight.",
    isWorkOrder
      ? "For Paragon Steel Process Order images, the Original Coil Detail row has columns like Rel Tag #, Heat #, Qty, Item Description, Product Type, Quality, PIW, Grade. In that row, Qty is the original coil weight in pounds; put that Qty value in weight, pounds, masterCoilWeight, originalCoilQty, and weightCandidates, and cite it in weightEvidence."
      : "If a quote/RFQ row labels the coil pounds as Qty, use it as weight only when the context clearly shows it is coil weight, not piece count.",
    isWorkOrder
      ? "When both production and packaging PO/work-order documents are uploaded, use the production document's master coil, consumption coil, inbound coil, or order LBS as weight/pounds/masterCoilWeight. Do not sum planned production rows, packaging rows, SO line items, produced-sheet rows, or package/bundle weights."
      : "When multiple quote/package documents are uploaded, use the RFQ/inbound coil/order LBS as weight/pounds/masterCoilWeight. Do not sum packaging rows, downstream line items, produced-sheet rows, or package/bundle weights.",
    "For CSP Job Work Order images with a Consumption or Cons ID section, use the LBS value from the consumption/HR Coil row as the quote weight. For this template, prefer the top production coil LBS over Planned Production rows, Order Information rows, Produced Sheets rows, Scrapped rows, or Packaging tables.",
    isWorkOrder
      ? "For process/order forms, prefer printed coil-row weights in the 40,000 to 50,000 lb range over handwritten notes, rounded estimates, bundle/max-lift notes, or default-looking values such as exactly 45,000."
      : "Prefer printed coil-row weights over handwritten notes, rounded estimates, or default-looking values.",
    "Never use exactly 45,000 as weight unless the image visibly prints exactly 45,000 in a weight, Qty, or LBS field. If uncertain, leave weight blank.",
    "If the source, inventory, application, consumption, or input-coil section lists multiple material coils for the same job, set coilCount and coilIds, then use the sum of only those input/material coil Weight/LBS values as weight/pounds/masterCoilWeight.",
    "For multiple input coils, set pieces to the number of input coils only when no better finished-piece count is visible. Add each coil weight to weightCandidates and cite the sum in weightEvidence.",
    "Do not sum output rows, planned production rows, customer order rows, scrapped rows, package rows, or repeated references to the same coil.",
    "Do not use values from rows whose identifiers begin with SO- as the quote weight unless that is the only explicit customer quote weight visible.",
    "Do not join an SO number with a nearby weight. For example, never turn SO-240312-1-1 plus 26,014 lbs into 261014.",
    "If a visible weight is over 120000 lbs, assume it may be OCR/field-merge error, leave weight/pounds/masterCoilWeight empty, and add a warning.",
    "If no visible weight or valid weight source text is found, leave weight, pounds, and masterCoilWeight empty. Never use an assumed/default weight.",
    "Put the exact visible text used for weight in weightEvidence, such as \"Consumption HR Coil LBS 44,840\" or \"Original Coil Detail Qty 43,190\".",
    "When there are several visible LBS values, include weightCandidates as an array of objects with value and label/source text.",
    "When production and packaging documents have separate POs, put the production PO in productionPoNumber and the packaging PO in packagingPoNumber. Use productionPoNumber for poNumber/poRef.",
    "If a packaging document shows different child/package weights than the production PO, keep the production/RFQ coil weight and add a warning describing the difference.",
    "Packaging must be an object with booleans for blocks3x4, paperWrap, paperTop, banding4way, stenciling, fullBoard3x4, and skids.",
    "Map lumber, 3x4 cross-blocks, cross blocks, and 3x4 blocks to packaging.blocks3x4.",
    "Map full-width boards, full width boards, and 3x4 full boards to packaging.fullBoard3x4.",
    "Map paper tops to packaging.paperTop, paper wrap to packaging.paperWrap, skid mentions to packaging.skids.",
    "Add warnings for fields that are unclear, missing, or inferred.",
    "confidence should be a number from 0 to 1."
  ].join("\n");
}

app.get("/api/quotes/status", (req, res) => {
  res.json({
    aiConfigured: Boolean(process.env.OPENAI_API_KEY),
    model: QUOTE_EXTRACTION_MODEL
  });
});

app.post("/api/quotes/extract", async (req, res) => {
  try {
    const openai = getOpenAIClient();
    if (!openai) return res.status(503).json({ error: "AI extraction is not configured on this server." });

    const mode = req.body?.mode === "workOrder" ? "workOrder" : "quote";
    const files = Array.isArray(req.body?.files) ? req.body.files : [];
    const pastedText = String(req.body?.text || req.body?.emailText || "").trim();
    if (!files.length && !pastedText) return res.status(400).json({ error: "Upload at least one screenshot image or paste quote request text." });
    if (files.length > 6) return res.status(400).json({ error: "Upload 6 images or fewer at a time." });
    if (pastedText.length > 30000) return res.status(413).json({ error: "Pasted text is too long. Keep it under 30,000 characters." });

    const imageFiles = files.filter((file) => {
      const type = String(file?.type || "").toLowerCase();
      const dataUrl = String(file?.dataUrl || "");
      return /^image\/(png|jpe?g|webp)$/.test(type) && /^data:image\/(png|jpe?g|webp);base64,/i.test(dataUrl);
    });

    if (files.length && imageFiles.length !== files.length) {
      return res.status(400).json({ error: "Only PNG, JPG, and WebP screenshot images are supported for AI extraction." });
    }

    const totalBytes = imageFiles.reduce((sum, file) => sum + Buffer.byteLength(String(file.dataUrl || ""), "utf8"), 0);
    if (totalBytes > 80 * 1024 * 1024) {
      return res.status(413).json({ error: "Upload is too large. Try fewer screenshots at once." });
    }

    const content = [
      { type: "text", text: quoteExtractionPrompt(mode) },
      ...(pastedText ? [{ type: "text", text: `Pasted quote request email/text:\n${pastedText}` }] : []),
      ...imageFiles.map((file) => ({
        type: "image_url",
        image_url: {
          url: file.dataUrl,
          detail: "high"
        }
      }))
    ];

    const completion = await openai.chat.completions.create({
      model: QUOTE_EXTRACTION_MODEL,
      temperature: 0,
      response_format: { type: "json_object" },
      messages: [
        {
          role: "system",
          content: mode === "workOrder"
            ? "You extract structured CSP steel processing PO and work-order fields from screenshots. Return valid JSON only."
            : "You extract structured CSP steel processing quote request fields from screenshots. Return valid JSON only."
        },
        { role: "user", content }
      ],
      max_tokens: 3000
    });

    const raw = completion.choices?.[0]?.message?.content || "{}";
    const extracted = normalizeQuoteExtraction(parseJsonObject(raw), mode);
    return res.json({
      extracted,
      model: completion.model || QUOTE_EXTRACTION_MODEL
    });
  } catch (err) {
    console.error("Quote extraction endpoint error:", err);
    return res.status(500).json({ error: err.message || "AI quote extraction failed." });
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

const SAD_MAX_EVENTS = 15000;
const SAD_MAX_FINDINGS = 20;
const SAD_BASELINE_DAYS = 7;
const SAD_MODEL = process.env.OPENAI_SAD_MODEL || "gpt-4o-mini";
const SAD_AI_PROXY_ENDPOINT = process.env.SAD_AI_PROXY_ENDPOINT ||
  "https://cap-auth-server-1.onrender.com/api/ai-chart";

function sadClamp(value, min, max) {
  return Math.min(max, Math.max(min, Number(value) || 0));
}

function sadRound(value, digits = 1) {
  const factor = 10 ** digits;
  return Math.round((Number(value) || 0) * factor) / factor;
}

function sadMean(values) {
  if (!Array.isArray(values) || !values.length) return 0;
  return values.reduce((sum, value) => sum + (Number(value) || 0), 0) / values.length;
}

function sadStdDev(values, mean = sadMean(values)) {
  if (!Array.isArray(values) || values.length < 2) return 0;
  const variance = values.reduce((sum, value) => {
    const delta = (Number(value) || 0) - mean;
    return sum + (delta * delta);
  }, 0) / values.length;
  return Math.sqrt(variance);
}

function sadDayKey(timestamp) {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return "";
  return date.toISOString().slice(0, 10);
}

function normalizeSadEvent(row, index) {
  const timestamp = new Date(row?.timestamp || row?.eventTimestamp || row?.event_timestamp || "");
  if (Number.isNaN(timestamp.getTime())) return null;
  const alarmType = coerceText(row?.alarmType || row?.alarm_type || row?.type, 20).toUpperCase() || "OTHER";
  const plcTag = coerceText(row?.plcTag || row?.plc_tag || row?.plc, 300) || "Unknown PLC";
  const message = coerceText(row?.message || row?.description, 800) || "No alarm message";
  return {
    id: coerceText(row?.id, 200) || `event-${index}`,
    timestamp: timestamp.toISOString(),
    timestampMs: timestamp.getTime(),
    alarmType,
    plcTag,
    message,
    cause: coerceText(row?.cause, 800),
    actions: coerceText(row?.actions, 1200)
  };
}

function sadGroupKey({ alarmType, plcTag, message }) {
  return `${String(alarmType).toUpperCase()}|${String(plcTag).toLowerCase()}|${String(message).toLowerCase()}`;
}

function sadBurstCount(timestamps, windowMs = 10 * 60 * 1000) {
  if (!Array.isArray(timestamps) || !timestamps.length) return 0;
  const sorted = timestamps.slice().sort((a, b) => a - b);
  let left = 0;
  let max = 1;
  for (let right = 0; right < sorted.length; right += 1) {
    while (sorted[right] - sorted[left] > windowMs) left += 1;
    max = Math.max(max, right - left + 1);
  }
  return max;
}

function buildSadStatisticalFindings(rawEvents, rawCommonRows) {
  const events = (Array.isArray(rawEvents) ? rawEvents : [])
    .slice(0, SAD_MAX_EVENTS)
    .map(normalizeSadEvent)
    .filter(Boolean)
    .sort((a, b) => a.timestampMs - b.timestampMs);

  if (!events.length) {
    return {
      findings: [],
      stats: {
        eventsAnalyzed: 0,
        patternsAnalyzed: 0,
        anomaliesFound: 0,
        baselineDays: SAD_BASELINE_DAYS
      },
      analysisEnd: new Date().toISOString()
    };
  }

  const analysisEndMs = events[events.length - 1].timestampMs;
  const currentStartMs = analysisEndMs - (24 * 60 * 60 * 1000);
  const sixHourStartMs = analysisEndMs - (6 * 60 * 60 * 1000);
  const baselineWindows = [];
  for (let dayOffset = SAD_BASELINE_DAYS; dayOffset >= 1; dayOffset -= 1) {
    const bucketEnd = currentStartMs - ((dayOffset - 1) * 24 * 60 * 60 * 1000);
    baselineWindows.push({
      start: bucketEnd - (24 * 60 * 60 * 1000),
      end: bucketEnd
    });
  }
  const baselineSampleTotals = baselineWindows.map((window) =>
    events.filter((event) => event.timestampMs >= window.start && event.timestampMs < window.end).length
  );
  const groups = new Map();

  events.forEach((event) => {
    const key = sadGroupKey(event);
    if (!groups.has(key)) {
      groups.set(key, {
        key,
        alarmType: event.alarmType,
        plcTag: event.plcTag,
        message: event.message,
        cause: event.cause,
        actions: event.actions,
        events: []
      });
    }
    const group = groups.get(key);
    group.events.push(event);
    if (!group.cause && event.cause) group.cause = event.cause;
    if (!group.actions && event.actions) group.actions = event.actions;
  });

  const commonByKey = new Map();
  (Array.isArray(rawCommonRows) ? rawCommonRows : []).forEach((row) => {
    const normalized = {
      alarmType: coerceText(row?.alarm_type || row?.alarmType, 20).toUpperCase() || "OTHER",
      plcTag: coerceText(row?.plc_tag || row?.plcTag || row?.plc, 300) || "Unknown PLC",
      message: coerceText(row?.message, 800) || "No alarm message"
    };
    commonByKey.set(sadGroupKey(normalized), {
      count: Math.max(0, Number(row?.qty_last_24h) || 0),
      isIssue: Boolean(row?.is_issue)
    });
  });
  const currentCommonTotal = Array.from(commonByKey.values())
    .reduce((sum, row) => sum + (Number(row?.count) || 0), 0);

  const findings = [];
  groups.forEach((group) => {
    const currentEvents = group.events.filter((event) => event.timestampMs >= currentStartMs);
    const common = commonByKey.get(group.key);
    const currentCount = Math.max(currentEvents.length, common?.count || 0);
    if (!currentCount) return;

    const baselineCounts = baselineWindows.map((window) =>
      group.events.filter((event) =>
        event.timestampMs >= window.start && event.timestampMs < window.end
      ).length);

    const baselineShares = baselineCounts.map((count, index) => {
      const sampleTotal = baselineSampleTotals[index] || 0;
      return sampleTotal ? count / sampleTotal : 0;
    });
    const hasShareBaseline = currentCommonTotal > 0 && baselineSampleTotals.some((count) => count > 0);
    const rawBaselineMean = sadMean(baselineCounts);
    const rawBaselineStdDev = sadStdDev(baselineCounts, rawBaselineMean);
    const baselineShareMean = sadMean(baselineShares);
    const baselineShareStdDev = sadStdDev(baselineShares, baselineShareMean);
    const baselineMean = hasShareBaseline ? baselineShareMean * currentCommonTotal : rawBaselineMean;
    const baselineStdDev = hasShareBaseline ? baselineShareStdDev * currentCommonTotal : rawBaselineStdDev;
    const denominator = Math.max(baselineStdDev, Math.sqrt(baselineMean + 0.5), 1);
    const zScore = (currentCount - baselineMean) / denominator;
    const ratio = currentCount / Math.max(baselineMean, 0.5);
    const last6hCount = currentEvents.filter((event) => event.timestampMs >= sixHourStartMs).length;
    const prior18hCount = Math.max(0, currentEvents.length - last6hCount);
    const recentHourlyRate = last6hCount / 6;
    const priorHourlyRate = prior18hCount / 18;
    const rawRateRatio = recentHourlyRate / Math.max(priorHourlyRate, 0.05);
    const burst10m = sadBurstCount(currentEvents.map((event) => event.timestampMs));
    const baselineRateRatios = baselineWindows.map((window) => {
      const latest6Start = window.end - (6 * 60 * 60 * 1000);
      const latest6Count = group.events.filter((event) =>
        event.timestampMs >= latest6Start && event.timestampMs < window.end
      ).length;
      const earlier18Count = group.events.filter((event) =>
        event.timestampMs >= window.start && event.timestampMs < latest6Start
      ).length;
      return (latest6Count / 6) / Math.max(earlier18Count / 18, 0.05);
    });
    const baselineRateRatio = Math.max(sadMean(baselineRateRatios), 1);
    const rateRatio = rawRateRatio / baselineRateRatio;
    const baselineBurstCounts = baselineWindows.map((window) =>
      sadBurstCount(group.events
        .filter((event) => event.timestampMs >= window.start && event.timestampMs < window.end)
        .map((event) => event.timestampMs))
    );
    const baselineBurstMean = sadMean(baselineBurstCounts);
    const burstRatio = burst10m / Math.max(baselineBurstMean, 1);
    const isIssue = Boolean(common?.isIssue);

    let score = 0;
    score += Math.min(38, Math.max(0, zScore) * 9);
    score += Math.min(22, Math.max(0, Math.log2(Math.max(1, ratio))) * 8);
    score += Math.min(14, Math.log10(currentCount + 1) * 8);
    score += Math.min(12, Math.max(0, Math.log2(Math.max(1, rateRatio))) * 5);
    score += Math.min(8, Math.max(0, burstRatio - 1) * 3);
    if (isIssue) score += 12;
    if (group.alarmType === "AL") score += 6;
    score = sadClamp(score, 0, 100);

    const unusual = score >= 35 || zScore >= 2 || ratio >= 2.5 || rateRatio >= 3 || burstRatio >= 3 || isIssue;
    if (!unusual) return;

    let severity = "Low";
    if (score >= 75) severity = "Critical";
    else if (score >= 58) severity = "High";
    else if (score >= 42) severity = "Medium";

    const evidenceParts = [
      `${currentCount} occurrence${currentCount === 1 ? "" : "s"} in the latest 24h`,
      `${sadRound(baselineMean)} expected from the 7-day baseline`,
      `${sadRound(ratio)}× baseline`,
      `z-score ${sadRound(zScore)}`
    ];
    if (rateRatio >= 1.5) evidenceParts.push(`${sadRound(rateRatio)}× recent rate change`);
    if (burstRatio >= 1.5) {
      evidenceParts.push(`${burst10m} events within 10 minutes vs ${sadRound(baselineBurstMean)} baseline`);
    }

    findings.push({
      id: crypto.createHash("sha1").update(group.key).digest("hex").slice(0, 12),
      severity,
      statisticalScore: Math.round(score),
      alarmType: group.alarmType,
      plcTag: group.plcTag,
      message: group.message,
      cause: group.cause || "",
      existingActions: group.actions || "",
      current24hCount: currentCount,
      baselineDailyMean: sadRound(baselineMean),
      baselineStdDev: sadRound(baselineStdDev),
      baselineMethod: hasShareBaseline ? "historical alarm-share baseline" : "daily occurrence baseline",
      zScore: sadRound(zScore),
      baselineRatio: sadRound(ratio),
      recentRateRatio: sadRound(rateRatio),
      burst10m,
      baselineBurst10m: sadRound(baselineBurstMean),
      burstRatio: sadRound(burstRatio),
      isIssue,
      evidence: evidenceParts.join(" • "),
      lastSeen: group.events[group.events.length - 1]?.timestamp || new Date(analysisEndMs).toISOString()
    });
  });

  findings.sort((a, b) =>
    b.statisticalScore - a.statisticalScore ||
    b.current24hCount - a.current24hCount ||
    String(a.plcTag).localeCompare(String(b.plcTag))
  );

  const limited = findings.slice(0, SAD_MAX_FINDINGS);
  return {
    findings: limited,
    stats: {
      eventsAnalyzed: events.length,
      patternsAnalyzed: groups.size,
      anomaliesFound: limited.length,
      baselineDays: SAD_BASELINE_DAYS
    },
    analysisEnd: new Date(analysisEndMs).toISOString()
  };
}

function sadActionsText(value) {
  if (Array.isArray(value)) {
    return value.map((item) => coerceText(item, 500)).filter(Boolean).join(" • ");
  }
  if (value && typeof value === "object") {
    return Object.values(value).map((item) => coerceText(item, 500)).filter(Boolean).join(" • ");
  }
  return coerceText(value, 1200);
}

function buildSadStatisticalFindingsFromStats(rawRows, analysisEnd = new Date().toISOString()) {
  const rows = Array.isArray(rawRows) ? rawRows : [];
  const findings = [];
  let eventsAnalyzed = 0;

  rows.forEach((row) => {
    const currentCount = Math.max(0, Number(row?.current_24h_count) || 0);
    if (!currentCount) return;
    eventsAnalyzed += currentCount;

    const baselineMean = Math.max(0, Number(row?.baseline_daily_mean) || 0);
    const baselineStdDev = Math.max(0, Number(row?.baseline_daily_stddev) || 0);
    const denominator = Math.max(baselineStdDev, Math.sqrt(baselineMean + 0.5), 1);
    const zScore = (currentCount - baselineMean) / denominator;
    const ratio = currentCount / Math.max(baselineMean, 0.5);
    const last6hCount = Math.max(0, Number(row?.last_6h_count) || 0);
    const prior18hCount = Math.max(0, Number(row?.prior_18h_count) || 0);
    const recentHourlyRate = last6hCount / 6;
    const priorHourlyRate = prior18hCount / 18;
    const rateRatio = recentHourlyRate / Math.max(priorHourlyRate, 0.05);
    const burst10m = Math.max(0, Number(row?.current_burst_10m) || 0);
    const baselineBurstMean = Math.max(0, Number(row?.baseline_burst_10m_mean) || 0);
    const burstRatio = burst10m / Math.max(baselineBurstMean, 1);
    const alarmType = coerceText(row?.alarm_type, 20).toUpperCase() || "OTHER";
    const plcTag = coerceText(row?.plc_tag, 300) || "Unknown PLC";
    const message = coerceText(row?.message, 800) || "No alarm message";
    const cause = coerceText(row?.cause, 800);
    const actions = sadActionsText(row?.actions);
    const isIssue = alarmType === "AL";

    let score = 0;
    score += Math.min(38, Math.max(0, zScore) * 9);
    score += Math.min(22, Math.max(0, Math.log2(Math.max(1, ratio))) * 8);
    score += Math.min(14, Math.log10(currentCount + 1) * 8);
    score += Math.min(16, Math.max(0, Math.log2(Math.max(1, rateRatio))) * 6);
    score += Math.min(10, Math.max(0, burstRatio - 1) * 4);
    if (isIssue) score += 8;
    score = sadClamp(score, 0, 100);

    const unusual =
      score >= 35 ||
      zScore >= 2 ||
      ratio >= 2.5 ||
      (last6hCount >= 5 && rateRatio >= 3) ||
      (burst10m >= 3 && burstRatio >= 3);
    if (!unusual) return;

    let severity = "Low";
    if (score >= 75) severity = "Critical";
    else if (score >= 58) severity = "High";
    else if (score >= 42) severity = "Medium";

    const evidenceParts = [
      `${currentCount} occurrence${currentCount === 1 ? "" : "s"} in the latest 24h`,
      `${sadRound(baselineMean)} expected from the prior 7 days`,
      `${sadRound(ratio)}× baseline`,
      `z-score ${sadRound(zScore)}`
    ];
    if (last6hCount >= 3 && rateRatio >= 1.5) {
      evidenceParts.push(`${sadRound(rateRatio)}× recent rate change`);
    }
    if (burst10m >= 2 && burstRatio >= 1.5) {
      evidenceParts.push(`${burst10m} events in a 10-minute interval vs ${sadRound(baselineBurstMean)} baseline`);
    }

    const key = sadGroupKey({ alarmType, plcTag, message });
    findings.push({
      id: crypto.createHash("sha1").update(key).digest("hex").slice(0, 12),
      severity,
      statisticalScore: Math.round(score),
      alarmType,
      plcTag,
      message,
      cause,
      existingActions: actions,
      current24hCount: currentCount,
      baselineDailyMean: sadRound(baselineMean),
      baselineStdDev: sadRound(baselineStdDev),
      baselineMethod: "Supabase rolling seven-day RBI alarm baseline",
      zScore: sadRound(zScore),
      baselineRatio: sadRound(ratio),
      recentRateRatio: sadRound(rateRatio),
      burst10m,
      baselineBurst10m: sadRound(baselineBurstMean),
      burstRatio: sadRound(burstRatio),
      isIssue,
      evidence: evidenceParts.join(" • "),
      lastSeen: new Date(row?.last_seen || analysisEnd).toISOString()
    });
  });

  findings.sort((a, b) =>
    b.statisticalScore - a.statisticalScore ||
    b.current24hCount - a.current24hCount ||
    String(a.plcTag).localeCompare(String(b.plcTag))
  );

  const limited = findings.slice(0, SAD_MAX_FINDINGS);
  return {
    findings: limited,
    stats: {
      eventsAnalyzed,
      patternsAnalyzed: rows.length,
      anomaliesFound: limited.length,
      baselineDays: SAD_BASELINE_DAYS
    },
    analysisEnd
  };
}

async function fetchSadSupabaseAnalysis() {
  if (!chartSupabase) return null;
  const analysisEnd = new Date().toISOString();
  const { data, error } = await chartSupabase.rpc("get_rbi_sad_alarm_stats", {
    p_analysis_end: analysisEnd
  });
  if (error) {
    throw new Error(`Unable to read RBI alarm statistics from Supabase: ${error.message}`);
  }
  return buildSadStatisticalFindingsFromStats(data, analysisEnd);
}

function sadFallbackInterpretation(finding) {
  const dominantSignal = finding.burstRatio >= 1.5
    ? "a concentrated short-window burst"
    : finding.recentRateRatio >= 2
      ? "a recent acceleration in occurrence rate"
      : "a statistically unusual increase above its recent baseline";
  return {
    id: finding.id,
    isAnomaly: true,
    anomalyType: finding.burstRatio >= 1.5
      ? "burst_cluster"
      : (finding.recentRateRatio >= 2 ? "rate_acceleration" : "frequency_spike"),
    relatedAlarmIds: [],
    potentialProblem: finding.message,
    aiAssessment: `This alarm shows ${dominantSignal}. Treat it as an early-warning signal and confirm the machine state before assigning a root cause.`,
    recommendedInspection: finding.existingActions || `Inspect the ${finding.plcTag} condition, related sensors, interlocks, and recent operator sequence.`,
    confidence: sadClamp(Math.round(45 + (finding.statisticalScore * 0.5)), 45, 95)
  };
}

async function getSadAiInterpretations(openai, findings) {
  if (!openai || !findings.length) return null;
  const schema = {
    type: "object",
    properties: {
      summary: { type: "string" },
      findings: {
        type: "array",
        maxItems: SAD_MAX_FINDINGS,
        items: {
          type: "object",
          properties: {
            id: { type: "string" },
            isAnomaly: { type: "boolean" },
            anomalyType: {
              type: "string",
              enum: [
                "frequency_spike",
                "rate_acceleration",
                "burst_cluster",
                "sequence_or_cooccurrence",
                "new_pattern",
                "not_anomalous",
                "other"
              ]
            },
            relatedAlarmIds: {
              type: "array",
              maxItems: 5,
              items: { type: "string" }
            },
            potentialProblem: { type: "string" },
            aiAssessment: { type: "string" },
            recommendedInspection: { type: "string" },
            confidence: { type: "integer", minimum: 0, maximum: 100 }
          },
          required: [
            "id",
            "isAnomaly",
            "anomalyType",
            "relatedAlarmIds",
            "potentialProblem",
            "aiAssessment",
            "recommendedInspection",
            "confidence"
          ],
          additionalProperties: false
        }
      }
    },
    required: ["summary", "findings"],
    additionalProperties: false
  };

  const response = await openai.responses.create({
    model: SAD_MODEL,
    temperature: 0.2,
    input: [
      {
        role: "system",
        content: [
          "You are S.A.D., CSP's Steel Alarm Diagnostics analyst.",
          "The application has already detected statistical anomalies in industrial alarm logs.",
          "Independently decide whether each candidate is an operational anomaly, including unusual frequency, acceleration, bursts, sequences, or co-occurring alarms.",
          "Use only the supplied alarm messages, PLC tags, existing causes/actions, and measured evidence.",
          "Find relationships among alarms when the evidence supports them, but do not invent a root cause.",
          "Describe each item as a potential problem or inspection focus, never as a confirmed diagnosis.",
          "Do not recommend bypassing guards, interlocks, lockout/tagout, or other safety controls.",
          "Keep each assessment and inspection recommendation concise and useful to maintenance personnel."
        ].join(" ")
      },
      {
        role: "user",
        content: `Analyze these statistically detected alarm anomalies:\n${JSON.stringify(findings, null, 2)}`
      }
    ],
    text: {
      format: {
        type: "json_schema",
        name: "sad_alarm_analysis",
        strict: true,
        schema
      }
    }
  });

  const parsed = parseJsonObject(response?.output_text || "");
  return parsed && Array.isArray(parsed.findings) ? parsed : null;
}

async function getSadAiInterpretationsViaProxy(findings) {
  if (!findings.length || !SAD_AI_PROXY_ENDPOINT) return null;
  const prompt = [
    "You are S.A.D., CSP's Steel Alarm Diagnostics analyst.",
    "Independently assess each statistically detected industrial alarm candidate.",
    "Use only the supplied PLC tags, messages, causes, actions, and statistical evidence.",
    "Do not invent a root cause. Describe potential problems and safe inspection priorities only.",
    "Never recommend bypassing guards, interlocks, lockout/tagout, or other safety controls.",
    "Return JSON only with this exact shape:",
    '{"summary":"string","findings":[{"id":"string","isAnomaly":true,"anomalyType":"frequency_spike|rate_acceleration|burst_cluster|sequence_or_cooccurrence|new_pattern|not_anomalous|other","relatedAlarmIds":[],"potentialProblem":"string","aiAssessment":"string","recommendedInspection":"string","confidence":0}]}',
    "Include one result for every supplied id. Confidence must be an integer from 0 to 100.",
    `Candidates:\n${JSON.stringify(findings, null, 2)}`
  ].join("\n");

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 45000);
  try {
    const response = await fetch(SAD_AI_PROXY_ENDPOINT, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ prompt, fresh: true }),
      signal: controller.signal
    });
    if (!response.ok) {
      throw new Error(`AI proxy returned ${response.status}`);
    }
    const payload = await response.json();
    const parsed = parseJsonObject(payload?.response || payload?.summary || payload?.output || "");
    return parsed && Array.isArray(parsed.findings) ? parsed : null;
  } finally {
    clearTimeout(timeoutId);
  }
}

app.post("/api/alarm-sad-analyze", async (req, res) => {
  res.set("Cache-Control", "no-store");
  try {
    const supabaseAnalysis = await fetchSadSupabaseAnalysis();
    const analysis = supabaseAnalysis ||
      buildSadStatisticalFindings(req.body?.events, req.body?.commonRows);
    const dataSource = supabaseAnalysis ? "supabase_rbi_alarm_all" : "request_payload";
    const fallbackFindings = analysis.findings.map(sadFallbackInterpretation);
    let aiResult = null;
    let aiStatus = "fallback";

    const aiRequested = req.body?.useAi !== false;
    const openai = aiRequested ? getOpenAIClient() : null;
    if (aiRequested && analysis.findings.length) {
      try {
        aiResult = openai
          ? await getSadAiInterpretations(openai, analysis.findings)
          : await getSadAiInterpretationsViaProxy(analysis.findings);
        if (aiResult) aiStatus = "analyzed";
      } catch (aiError) {
        console.error("S.A.D. AI interpretation error:", aiError);
      }
      if (!aiResult) aiStatus = openai ? "fallback" : "not_configured";
    } else if (aiRequested && !analysis.findings.length) {
      aiStatus = "no_candidates";
    } else if (aiRequested && !openai) {
      aiStatus = "not_configured";
    } else if (!aiRequested) {
      aiStatus = "disabled";
    }

    const aiById = new Map((aiResult?.findings || []).map((item) => [String(item.id), item]));
    const fallbackById = new Map(fallbackFindings.map((item) => [String(item.id), item]));
    const findings = analysis.findings.map((finding) => {
      const interpretation = aiById.get(String(finding.id)) || fallbackById.get(String(finding.id));
      return {
        ...finding,
        aiAnomaly: interpretation?.isAnomaly !== false,
        anomalyType: coerceText(interpretation?.anomalyType, 80) || "other",
        relatedAlarmIds: Array.isArray(interpretation?.relatedAlarmIds)
          ? interpretation.relatedAlarmIds.map((id) => coerceText(id, 80)).filter(Boolean).slice(0, 5)
          : [],
        detectionMethod: aiById.has(String(finding.id)) && interpretation?.isAnomaly !== false
          ? "AI + statistical"
          : "Statistical",
        potentialProblem: coerceText(interpretation?.potentialProblem, 500) || finding.message,
        aiAssessment: coerceText(interpretation?.aiAssessment, 1200),
        recommendedInspection: coerceText(interpretation?.recommendedInspection, 1200),
        confidence: sadClamp(interpretation?.confidence, 0, 100)
      };
    }).sort((a, b) =>
      Number(b.aiAnomaly) - Number(a.aiAnomaly) ||
      b.statisticalScore - a.statisticalScore
    );

    const aiAnomalyCount = findings.filter((finding) => finding.aiAnomaly).length;

    return res.json({
      success: true,
      summary: coerceText(aiResult?.summary, 1200) ||
        (findings.length
          ? `${findings.length} statistically unusual alarm patterns were identified for review.`
          : "No statistically unusual alarm patterns were identified in the available data."),
      findings,
      stats: {
        ...analysis.stats,
        statisticalCandidates: analysis.stats.anomaliesFound,
        aiAnomalies: aiStatus === "analyzed" ? aiAnomalyCount : null
      },
      analysisEnd: analysis.analysisEnd,
      generatedAt: new Date().toISOString(),
      dataSource,
      aiStatus,
      model: aiStatus === "analyzed" ? SAD_MODEL : null,
      advisory:
        "S.A.D. findings are early-warning indicators, not confirmed diagnoses. Qualified personnel must verify conditions and follow all required safety procedures."
    });
  } catch (err) {
    console.error("S.A.D. analysis endpoint error:", err);
    return res.status(500).json({ error: err.message || "S.A.D. analysis failed." });
  }
});

const expansionLeadText = (value, maxLength) =>
  String(value ?? "").trim().slice(0, maxLength);

const expansionLeadIp = (req) => {
  const forwarded = String(req.headers["x-forwarded-for"] || "")
    .split(",")[0]
    .trim();
  return expansionLeadText(forwarded || req.ip || "", 100);
};

const sendFantasyFootballEmail = async ({
  to,
  replyTo,
  subject,
  html,
  attachments = [],
  tags = [],
  idempotencyKey = ""
}) => {
  if (!RESEND_API_KEY || !PRO_FORMS_FROM_EMAIL) {
    throw new Error("Fantasy football email delivery is not configured.");
  }

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      "Content-Type": "application/json",
      ...(idempotencyKey ? { "Idempotency-Key": idempotencyKey } : {})
    },
    body: JSON.stringify({
      from: PRO_FORMS_FROM_EMAIL,
      to: Array.isArray(to) ? to : [to],
      ...(replyTo ? { reply_to: replyTo } : {}),
      subject,
      html,
      ...(attachments.length ? { attachments } : {}),
      ...(tags.length ? { tags } : {})
    })
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload?.message || "The email provider rejected the fantasy football confirmation.");
  }
  return payload?.id || null;
};

const FANTASY_FOOTBALL_INVITE_TESTS = {
  blue: {
    leagueName: "CSP Blue",
    templateName: "fantasy_football_invite_blue",
    artworkName: "fantasy-blue.png"
  },
  gold: {
    leagueName: "CSP Gold",
    templateName: "fantasy_football_invite_gold",
    artworkName: "fantasy-gold.png"
  }
};

app.post("/api/fantasy-football-invite-test", async (req, res) => {
  res.set("Cache-Control", "no-store");
  const leagueKey = expansionLeadText(req.body?.league, 20).toLowerCase();
  const testRunId = expansionLeadText(req.body?.testRunId, 60);
  const config = FANTASY_FOOTBALL_INVITE_TESTS[leagueKey];

  if (!config) return res.status(400).json({ error: "Choose the Blue or Gold test email." });
  if (!/^[0-9a-f-]{20,60}$/i.test(testRunId)) {
    return res.status(400).json({ error: "A valid test run ID is required." });
  }
  if (!consumeShippingAuthAttempt(req, `fantasy-invite-test:${leagueKey}`)) {
    return res.status(429).json({ error: "Too many fantasy invite tests. Wait 15 minutes and try again." });
  }

  const recipient = FANTASY_FOOTBALL_ADMIN_RECIPIENTS[0];
  if (!recipient || !RESEND_API_KEY || !PRO_FORMS_FROM_EMAIL) {
    return res.status(503).json({ error: "Fantasy football test email delivery is not configured." });
  }

  try {
    const html = renderStoredTemplate(config.templateName, { first_name: "Todd" });
    if (!html) throw new Error(`${config.leagueName} email template is unavailable.`);

    const artworkPath = path.join(EMAIL_ASSETS_DIR, config.artworkName);
    const artwork = fs.readFileSync(artworkPath).toString("base64");
    const providerId = await sendFantasyFootballEmail({
      to: recipient,
      replyTo: recipient,
      subject: `[RESEND TEST] Your ${config.leagueName} Fantasy Football Invite`,
      html,
      attachments: [{
        content: artwork,
        filename: config.artworkName,
        content_id: "fantasy-helmet"
      }],
      tags: [
        { name: "campaign", value: "csp-fantasy-2026" },
        { name: "league", value: leagueKey },
        { name: "delivery", value: "test" }
      ],
      idempotencyKey: `fantasy-invite-test-${leagueKey}-${testRunId}`
    });

    return res.json({ ok: true, league: config.leagueName, recipient, providerId });
  } catch (error) {
    console.error("Fantasy football invite test failed:", error?.message || error);
    return res.status(502).json({ error: error?.message || "The fantasy invite test could not be sent." });
  }
});

app.post("/api/fantasy-football-signups", async (req, res) => {
  res.set("Cache-Control", "no-store");

  // Quietly accept bot submissions that fill the hidden website field.
  if (expansionLeadText(req.body?.website, 200)) {
    return res.json({ success: true });
  }
  const signup = {
    submission_token: expansionLeadText(req.body?.submissionToken, 50),
    season: 2026,
    name: expansionLeadText(req.body?.name, 120),
    email: expansionLeadText(req.body?.email, 254).toLowerCase(),
    phone: expansionLeadText(req.body?.phone, 60),
    submitted_at: new Date().toISOString(),
    page_url: expansionLeadText(req.body?.pageUrl, 1200) || null,
    referrer: expansionLeadText(req.body?.referrer, 1200) || null,
    ip_address: expansionLeadIp(req) || null,
    user_agent: expansionLeadText(req.get("user-agent"), 1000) || null
  };

  if (!signup.name || !signup.email || !signup.phone) {
    return res.status(400).json({ error: "Complete all required fields." });
  }
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(signup.email)) {
    return res.status(400).json({ error: "Enter a valid email address." });
  }
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(signup.submission_token)) {
    return res.status(400).json({ error: "The signup could not be identified. Refresh the page and try again." });
  }

  try {
    let stored;
    if (chartSupabase) {
      const { data, error: insertError } = await chartSupabase
        .from("fantasy_football_signups")
        .insert(signup)
        .select("id,submitted_at")
        .single();

      if (insertError) {
        if (insertError.code === "23505") {
          return res.status(409).json({ error: "This email has already been signed up." });
        }
        throw insertError;
      }
      stored = data;
    } else {
      const payload = await expansionLeadStoreRequest({
        action: "fantasy_signup_store",
        signup
      });
      stored = {
        id: payload.id,
        submitted_at: payload.submittedAt || signup.submitted_at
      };
    }

    const detailsRows = [
      ["Name", signup.name],
      ["Email", signup.email],
      ["Phone", signup.phone],
      ["Season", "2026"]
    ].map(([label, value]) => `
      <tr>
        <td style="padding:9px 12px;border-bottom:1px solid #d8dee9;color:#66748a;font-size:13px;font-weight:700;">${escapeHtml(label)}</td>
        <td style="padding:9px 12px;border-bottom:1px solid #d8dee9;color:#172132;font-size:14px;">${escapeHtml(value)}</td>
      </tr>
    `).join("");

    const emailShell = (eyebrow, title, content) => `
      <div style="margin:0;padding:28px;background:#f4f7fb;font-family:Arial,sans-serif;color:#172132;">
        <div style="max-width:680px;margin:0 auto;overflow:hidden;border:1px solid #d8dee9;border-radius:10px;background:#ffffff;">
          <div style="padding:24px 28px;background:#142033;color:#ffffff;">
            <div style="color:#f1a91e;font-size:12px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;">${escapeHtml(eyebrow)}</div>
            <h1 style="margin:8px 0 0;font-size:26px;line-height:1.2;">${escapeHtml(title)}</h1>
          </div>
          <div style="padding:24px 28px;">${content}</div>
        </div>
      </div>
    `;

    const adminHtml = emailShell(
      "CSP Fantasy Football",
      "New league signup",
      `<table role="presentation" style="width:100%;border-collapse:collapse;border:1px solid #d8dee9;">${detailsRows}</table>
       <p style="margin:22px 0 0;color:#66748a;font-size:13px;">Submitted ${escapeHtml(new Date(stored.submitted_at).toLocaleString("en-US", { timeZone: "America/New_York", timeZoneName: "short" }))}</p>`
    );
    const participantHtml = emailShell(
      "CSP Fantasy Football",
      "You're signed up for the 2026 season",
      `<p style="margin:0;color:#172132;font-size:16px;line-height:1.6;">Hi ${escapeHtml(signup.name)},</p>
       <p style="margin:14px 0 0;color:#4f5f75;font-size:15px;line-height:1.65;">Your spot in the CSP fantasy football league is reserved. The league will be on Sleeper again, and we’ll send your league invite before the draft.</p>
       <p style="margin:18px 0 0;color:#4f5f75;font-size:15px;line-height:1.65;"><strong>Draft:</strong> Sunday, August 30, 2026 at 1:00 PM ET.</p>
       <p style="margin:12px 0 0;color:#4f5f75;font-size:15px;line-height:1.65;"><strong>League fee:</strong> $20. Please give payment to Todd to confirm your place in the league.</p>
       <p style="margin:12px 0 0;color:#4f5f75;font-size:15px;line-height:1.65;"><strong>Deadline:</strong> Sign up and submit the $20 league fee by Tuesday, August 25, 2026.</p>
       <p style="margin:22px 0 0;"><a href="https://sleeper.com/download" style="display:inline-block;padding:11px 18px;border-radius:6px;background:#f1a91e;color:#142033;font-size:15px;font-weight:700;text-decoration:none;">Download Sleeper</a></p>`
    );

    const [adminResult, participantResult] = await Promise.allSettled([
      sendFantasyFootballEmail({
        to: FANTASY_FOOTBALL_ADMIN_RECIPIENTS,
        replyTo: signup.email,
        subject: `[Fantasy Football] New signup — ${signup.name}`,
        html: adminHtml
      }),
      sendFantasyFootballEmail({
        to: signup.email,
        replyTo: FANTASY_FOOTBALL_ADMIN_RECIPIENTS[0] || null,
        subject: "You're signed up for CSP Fantasy Football",
        html: participantHtml
      })
    ]);

    const emailErrors = [adminResult, participantResult]
      .filter((result) => result.status === "rejected")
      .map((result) => expansionLeadText(result.reason?.message, 500))
      .filter(Boolean);
    const emailStatus = {
      admin_email_sent_at: adminResult.status === "fulfilled" ? new Date().toISOString() : null,
      participant_email_sent_at: participantResult.status === "fulfilled" ? new Date().toISOString() : null,
      admin_email_provider_id: adminResult.status === "fulfilled" ? adminResult.value : null,
      participant_email_provider_id: participantResult.status === "fulfilled" ? participantResult.value : null,
      email_error: emailErrors.join(" | ") || null,
      updated_at: new Date().toISOString()
    };
    if (chartSupabase) {
      const { error: updateError } = await chartSupabase
        .from("fantasy_football_signups")
        .update(emailStatus)
        .eq("id", stored.id);
      if (updateError) console.error("Fantasy football email status update failed:", updateError);
    } else {
      try {
        await expansionLeadStoreRequest({
          action: "fantasy_signup_email_status",
          id: stored.id,
          submissionToken: signup.submission_token,
          emailStatus
        });
      } catch (updateError) {
        console.error("Fantasy football email status update failed:", updateError);
      }
    }

    if (emailErrors.length > 0) {
      console.error("Fantasy football confirmation email failed:", emailErrors.join(" | "));
      return res.status(502).json({
        error: "Your signup was recorded, but one of the confirmation emails could not be sent.",
        recorded: true,
        id: stored.id
      });
    }

    return res.status(201).json({ success: true, recorded: true, emailed: true, id: stored.id });
  } catch (error) {
    console.error("Fantasy football signup failed:", error);
    return res.status(500).json({ error: "We could not complete your signup. Please try again." });
  }
});

app.post("/api/expansion-events", async (req, res) => {
  res.set("Cache-Control", "no-store");
  try {
    const clientIp = expansionLeadIp(req);
    const payload = await expansionLeadStoreRequest({
      action: "traffic_store",
      clientIp,
      userAgent: expansionLeadText(req.get("user-agent"), 1200),
      event: req.body || {}
    });
    return res.status(201).json(payload);
  } catch (error) {
    console.error("Expansion traffic storage failed:", error);
    const statusCode = Number(error?.statusCode) || 502;
    return res.status(statusCode).json({ error: error?.message || "Unable to record this visit." });
  }
});

const sendExpansionLeadEmail = async (lead) => {
  if (!RESEND_API_KEY || !PRO_FORMS_FROM_EMAIL || EXPANSION_LEAD_RECIPIENTS.length === 0) {
    throw new Error("Expansion lead email delivery is not configured.");
  }

  const rows = [
    ["Name", lead.name],
    ["Company", lead.company],
    ["Email", lead.email],
    ["Phone", lead.phone],
    ["Processing need", lead.material_need],
    ["Opportunity timing", lead.opportunity_timing]
  ].map(([label, value]) => `
    <tr>
      <td style="padding:9px 12px;border-bottom:1px solid #d8dee9;color:#5f6c80;font-size:13px;font-weight:700;">${escapeHtml(label)}</td>
      <td style="padding:9px 12px;border-bottom:1px solid #d8dee9;color:#172132;font-size:14px;">${escapeHtml(value)}</td>
    </tr>
  `).join("");

  const html = `
    <div style="margin:0;padding:28px;background:#f6f8fb;font-family:Arial,sans-serif;color:#172132;">
      <div style="max-width:680px;margin:0 auto;overflow:hidden;border:1px solid #d8dee9;border-radius:10px;background:#ffffff;">
        <div style="padding:24px 28px;background:#142033;color:#ffffff;">
          <div style="color:#f1a91e;font-size:12px;font-weight:700;letter-spacing:.14em;text-transform:uppercase;">Expansion Landing Page</div>
          <h1 style="margin:8px 0 0;font-size:26px;line-height:1.2;">New processing inquiry</h1>
        </div>
        <div style="padding:24px 28px;">
          <table role="presentation" style="width:100%;border-collapse:collapse;border:1px solid #d8dee9;">${rows}</table>
          <h2 style="margin:24px 0 8px;font-size:17px;color:#233658;">Message</h2>
          <p style="margin:0;white-space:pre-wrap;color:#5f6c80;font-size:14px;line-height:1.55;">${escapeHtml(lead.message || "No additional message provided.")}</p>
          <p style="margin:24px 0 0;color:#7a8597;font-size:12px;">Submitted ${escapeHtml(new Date(lead.submitted_at).toLocaleString("en-US", { timeZone: "America/New_York", timeZoneName: "short" }))}</p>
        </div>
      </div>
    </div>
  `;

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      from: PRO_FORMS_FROM_EMAIL,
      to: EXPANSION_LEAD_RECIPIENTS,
      reply_to: lead.email,
      subject: `[Website Lead] ${lead.company} — ${lead.material_need}`,
      html
    })
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload?.message || "Resend rejected the expansion lead email.");
  }

  return payload?.id || null;
};

const expansionLeadStoreRequest = async (payload, accessToken = "") => {
  const response = await fetch(EXPANSION_LEAD_STORE_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {})
    },
    body: JSON.stringify(payload)
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(result?.error || "Supabase rejected the lead record.");
    error.statusCode = response.status;
    throw error;
  }
  return result;
};

app.post("/api/expansion-leads", async (req, res) => {
  res.set("Cache-Control", "no-store");

  // Quietly accept bot submissions that fill the hidden website field.
  if (expansionLeadText(req.body?.website, 200)) {
    return res.json({ success: true });
  }

  const lead = {
    submission_token: expansionLeadText(req.body?.submissionToken, 50),
    submitted_at: new Date().toISOString(),
    name: expansionLeadText(req.body?.name, 120),
    company: expansionLeadText(req.body?.company, 160),
    email: expansionLeadText(req.body?.email, 254).toLowerCase(),
    phone: expansionLeadText(req.body?.phone, 60),
    material_need: expansionLeadText(req.body?.material, 180),
    opportunity_timing: expansionLeadText(req.body?.opportunity, 140),
    message: expansionLeadText(req.body?.message, 5000) || null,
    page_url: expansionLeadText(req.body?.pageUrl, 1200) || null,
    referrer: expansionLeadText(req.body?.referrer, 1200) || null,
    visitor_id: expansionLeadText(req.body?.visitorId, 160) || null,
    session_id: expansionLeadText(req.body?.sessionId, 160) || null,
    ip_address: expansionLeadIp(req) || null,
    user_agent: expansionLeadText(req.get("user-agent"), 1000) || null,
    metadata: sanitizePlainObject(req.body?.metadata)
  };

  const required = ["name", "company", "email", "phone", "material_need", "opportunity_timing"];
  const missing = required.filter((key) => !lead[key]);
  if (missing.length > 0) {
    return res.status(400).json({ error: "Complete all required fields." });
  }
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(lead.email)) {
    return res.status(400).json({ error: "Enter a valid email address." });
  }
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(lead.submission_token)) {
    return res.status(400).json({ error: "The submission could not be identified. Refresh the page and try again." });
  }

  try {
    const stored = await expansionLeadStoreRequest({ action: "store", lead });
    if (stored.emailStatus === "sent") {
      return res.json({ success: true, recorded: true, emailed: true, id: stored.id });
    }

    try {
      const providerId = await sendExpansionLeadEmail({ ...lead, id: stored.id });
      await expansionLeadStoreRequest({
        action: "email_status",
        id: stored.id,
        submissionToken: lead.submission_token,
        emailStatus: "sent",
        resendEmailId: providerId
      });

      return res.status(201).json({
        success: true,
        recorded: true,
        emailed: true,
        id: stored.id
      });
    } catch (emailError) {
      console.error("Expansion lead email failed:", emailError);
      await expansionLeadStoreRequest({
        action: "email_status",
        id: stored.id,
        submissionToken: lead.submission_token,
        emailStatus: "failed",
        emailError: expansionLeadText(emailError?.message, 1000)
      }).catch((statusError) => console.error("Expansion lead failure status update failed:", statusError));

      return res.status(502).json({
        error: "Your request was recorded, but the notification email could not be sent. Please call CSP if your need is urgent.",
        recorded: true
      });
    }
  } catch (error) {
    console.error("Expansion lead submission failed:", error);
    const statusCode = Number(error?.statusCode) || 500;
    return res.status(statusCode).json({ error: error?.message || "We could not submit your request. Please try again or call CSP." });
  }
});

app.get("/api/expansion-leads/admin", requireWebsiteLeadAccess, async (req, res) => {
  res.set("Cache-Control", "no-store");
  try {
    const payload = await expansionLeadStoreRequest({ action: "list" }, getBearerToken(req));
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    return res.json({
      rows,
      count: rows.length,
      generatedAt: payload.generatedAt || new Date().toISOString()
    });
  } catch (error) {
    console.error("Expansion lead admin endpoint failed:", error);
    const statusCode = Number(error?.statusCode) || 500;
    return res.status(statusCode).json({ error: error?.message || "Unable to load form responses." });
  }
});

app.get("/api/expansion-traffic/admin", requireWebsiteLeadAccess, async (req, res) => {
  res.set("Cache-Control", "no-store");
  try {
    const daysRaw = Number(req.query.days || 30);
    const days = Math.min(Math.max(Number.isFinite(daysRaw) ? Math.round(daysRaw) : 30, 1), 3650);
    const limitRaw = Number(req.query.limit || 5000);
    const limit = Math.min(Math.max(Number.isFinite(limitRaw) ? Math.round(limitRaw) : 5000, 100), 10000);
    const payload = await expansionLeadStoreRequest(
      { action: "traffic_list", days, limit },
      getBearerToken(req)
    );
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    return res.json({
      days,
      limit,
      rows,
      count: rows.length,
      generatedAt: payload.generatedAt || new Date().toISOString()
    });
  } catch (error) {
    console.error("Expansion traffic admin endpoint failed:", error);
    const statusCode = Number(error?.statusCode) || 500;
    return res.status(statusCode).json({ error: error?.message || "Unable to load ad traffic." });
  }
});

// Protected website traffic feed. IP and location data never receives a public
// browser database key; it is returned only after the BI Supabase session and
// the admin/website_leads role have both been verified.
app.get("/api/web-visits/admin", requireWebsiteLeadAccess, async (req, res) => {
  if (!chartSupabase) {
    return res.status(503).json({ error: "Website traffic storage is not configured." });
  }

  const daysRaw = Number(req.query.days || 30);
  const days = Math.min(Math.max(Number.isFinite(daysRaw) ? Math.round(daysRaw) : 30, 1), 3650);
  const limitRaw = Number(req.query.limit || 5000);
  const limit = Math.min(Math.max(Number.isFinite(limitRaw) ? Math.round(limitRaw) : 5000, 100), 10000);
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
  const columns = [
    "id", "visited_at", "site", "page_url", "page_path", "pathname", "page_title",
    "referrer", "source_host", "visitor_id", "session_id", "event_name", "campaign_name",
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "gclid",
    "gbraid", "wbraid", "fbclid", "msclkid", "li_fat_id", "landing_page", "ip_address",
    "ip_hash", "user_agent", "device_type", "browser", "os", "timezone", "viewport_width",
    "viewport_height", "screen_width", "screen_height", "scroll_depth", "organization",
    "reverse_dns", "network_name", "ip_city", "ip_region", "ip_country", "ip_latitude",
    "ip_longitude", "is_steel_company", "steel_company_name", "steel_company_segment",
    "steel_confidence", "steel_reason", "metadata"
  ].join(",");

  try {
    const { data, error } = await chartSupabase
      .from("web_visitor_events")
      .select(columns)
      .gte("visited_at", since)
      .or("site.ilike.%expansion%,pathname.eq./expansion.html,page_path.ilike.%expansion%")
      .order("visited_at", { ascending: false })
      .limit(limit);

    if (error) {
      console.error("Website traffic query failed:", error);
      return res.status(400).json({ error: error.message || "Website traffic query failed." });
    }

    return res.json({
      days,
      limit,
      count: Array.isArray(data) ? data.length : 0,
      rows: Array.isArray(data) ? data : [],
      generatedAt: new Date().toISOString()
    });
  } catch (error) {
    console.error("Website traffic endpoint failed:", error);
    return res.status(500).json({ error: error.message || "Website traffic endpoint failed." });
  }
});

const easternDateKey = (value = new Date()) => {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(value);
  const get = (type) => parts.find((part) => part.type === type)?.value || "";
  return `${get("year")}-${get("month")}-${get("day")}`;
};

const normalizeShiftReportShift = (value) => {
  const text = String(value || "").trim();
  const normalized = text.toLowerCase();
  if (normalized === "1" || normalized.startsWith("first") || normalized.startsWith("1st")) return "First";
  if (normalized === "2" || normalized.startsWith("second") || normalized.startsWith("2nd")) return "Second";
  if (normalized === "3" || normalized.startsWith("third") || normalized.startsWith("3rd")) return "Third";
  return text || "Unassigned";
};

const shiftReportWeekKey = (dateKey) => {
  const match = String(dateKey || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return "";
  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  const day = date.getUTCDay();
  date.setUTCDate(date.getUTCDate() - (day === 0 ? 6 : day - 1));
  return date.toISOString().slice(0, 10);
};

app.get("/api/shift-report-dashboard", requireShiftReportAccess, async (_req, res) => {
  if (!chartSupabase) {
    return res.status(503).json({ error: "Shift report dashboard storage is not configured." });
  }

  try {
    const sourceRows = await fetchChartRows(
      "pro_shift_report_submissions",
      "submission_id,submitted_at,report_date,operator,shift,hours_worked,tons,linear_feet,stroke_count,total_coils_ran,planned_downtime_minutes,planned_downtime_details,unplanned_downtime_minutes,unplanned_downtime_details",
      { orderBy: "report_date", ascending: true, limit: 50000 }
    );

    const latestByReport = new Map();
    sourceRows.forEach((row) => {
      const reportDate = String(row.report_date || "").slice(0, 10);
      const operator = String(row.operator || "").trim();
      if (!reportDate || !operator || /\btest\b/i.test(operator)) return;

      const shift = normalizeShiftReportShift(row.shift);
      const key = `${reportDate}|${shift.toLowerCase()}|${operator.toLowerCase()}`;
      const previous = latestByReport.get(key);
      const currentTime = Date.parse(row.submitted_at || "") || 0;
      const previousTime = Date.parse(previous?.submitted_at || "") || 0;
      if (!previous || currentTime >= previousTime) {
        latestByReport.set(key, { ...row, report_date: reportDate, operator, shift });
      }
    });

    const rows = Array.from(latestByReport.values());
    const today = easternDateKey();
    const todayRows = rows.filter((row) => row.report_date === today);
    const sum = (list, field) => list.reduce((total, row) => total + toNumberSafe(row[field]), 0);
    const employeeTotals = new Map();
    const employeeMonthlyTotals = new Map();
    const shiftTotals = new Map();
    const weeklyShiftTotals = new Map();
    const downtimeReasonTotals = new Map();

    rows.forEach((row) => {
      const employees = Array.from(new Set(
        String(row.operator || "")
          .split("/")
          .map((name) => name.trim())
          .filter(Boolean)
          .map((name) => /^matt$/i.test(name) ? "Matt Bocanegra" : name)
      ));
      const tonsPerEmployee = employees.length ? toNumberSafe(row.tons) / employees.length : 0;
      employees.forEach((employee) => {
        employeeTotals.set(employee, (employeeTotals.get(employee) || 0) + tonsPerEmployee);
        const month = String(row.report_date || "").slice(0, 7);
        if (month) {
          const monthlyKey = `${month}|${employee}`;
          employeeMonthlyTotals.set(monthlyKey, (employeeMonthlyTotals.get(monthlyKey) || 0) + tonsPerEmployee);
        }
      });
      shiftTotals.set(row.shift, (shiftTotals.get(row.shift) || 0) + toNumberSafe(row.tons));

      const week = shiftReportWeekKey(row.report_date);
      if (week) {
        const weeklyKey = `${week}|${row.shift}`;
        weeklyShiftTotals.set(weeklyKey, (weeklyShiftTotals.get(weeklyKey) || 0) + toNumberSafe(row.tons));
      }

      [
        {
          type: "Planned",
          minutes: toNumberSafe(row.planned_downtime_minutes),
          reason: String(row.planned_downtime_details || "").trim()
        },
        {
          type: "Unplanned",
          minutes: toNumberSafe(row.unplanned_downtime_minutes),
          reason: String(row.unplanned_downtime_details || "").trim()
        }
      ].forEach((item) => {
        if (!week || (!item.minutes && !item.reason)) return;
        const reason = item.reason || "No reason entered";
        const key = `${week}|${row.shift}|${item.type}|${reason.toLowerCase()}`;
        const previous = downtimeReasonTotals.get(key) || {
          week,
          shift: row.shift,
          type: item.type,
          reason,
          minutes: 0,
          occurrences: 0
        };
        previous.minutes += item.minutes;
        previous.occurrences += 1;
        downtimeReasonTotals.set(key, previous);
      });
    });

    const employeeTons = Array.from(employeeTotals.entries())
      .map(([employee, tons]) => ({ employee, tons: roundMetric(tons, 0) }))
      .sort((a, b) => b.tons - a.tons || a.employee.localeCompare(b.employee))
      .slice(0, 12);
    const employeeMonths = Array.from(new Set(rows.map((row) => String(row.report_date || "").slice(0, 7)).filter(Boolean)))
      .sort()
      .slice(-8);
    const employeeMonthRank = new Map();
    employeeMonthlyTotals.forEach((tons, key) => {
      const separator = key.indexOf("|");
      const month = key.slice(0, separator);
      const employee = key.slice(separator + 1);
      if (employeeMonths.includes(month)) {
        employeeMonthRank.set(employee, (employeeMonthRank.get(employee) || 0) + tons);
      }
    });
    const monthlyEmployees = Array.from(employeeMonthRank.entries())
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, 6)
      .map(([employee]) => employee);
    const employeeTonsByMonth = {
      months: employeeMonths,
      series: monthlyEmployees.map((employee) => ({
        employee,
        tons: employeeMonths.map((month) => roundMetric(employeeMonthlyTotals.get(`${month}|${employee}`) || 0, 0))
      }))
    };
    const currentMonth = today.slice(0, 7);
    const employeeTonsCurrentMonth = Array.from(employeeTotals.keys())
      .map((employee) => ({
        employee,
        tons: roundMetric(employeeMonthlyTotals.get(`${currentMonth}|${employee}`) || 0, 0)
      }))
      .filter((row) => row.tons > 0)
      .sort((a, b) => b.tons - a.tons || a.employee.localeCompare(b.employee))
      .slice(0, 8);
    const shiftOrder = ["First", "Second", "Third"];
    const shiftTons = Array.from(shiftTotals.entries())
      .map(([shift, tons]) => ({ shift, tons: roundMetric(tons, 0) }))
      .sort((a, b) => {
        const aIndex = shiftOrder.indexOf(a.shift);
        const bIndex = shiftOrder.indexOf(b.shift);
        return (aIndex < 0 ? 99 : aIndex) - (bIndex < 0 ? 99 : bIndex) || a.shift.localeCompare(b.shift);
      });
    const todayShiftKpis = shiftOrder.map((shift) => {
      const shiftRows = todayRows.filter((row) => row.shift === shift);
      return {
        shift,
        reports: shiftRows.length,
        tons: roundMetric(sum(shiftRows, "tons"), 0),
        downtimeMinutes: Math.round(
          sum(shiftRows, "planned_downtime_minutes") + sum(shiftRows, "unplanned_downtime_minutes")
        )
      };
    });
    const allWeeks = Array.from(new Set(rows.map((row) => shiftReportWeekKey(row.report_date)).filter(Boolean))).sort();
    const visibleWeeks = allWeeks.slice(-12);
    const weeklyShiftTons = visibleWeeks.map((week) => ({
      week,
      First: roundMetric(weeklyShiftTotals.get(`${week}|First`) || 0, 0),
      Second: roundMetric(weeklyShiftTotals.get(`${week}|Second`) || 0, 0),
      Third: roundMetric(weeklyShiftTotals.get(`${week}|Third`) || 0, 0)
    }));
    const currentWeek = shiftReportWeekKey(today);
    const currentWeekShiftTons = shiftOrder.map((shift) => ({
      shift,
      tons: roundMetric(weeklyShiftTotals.get(`${currentWeek}|${shift}`) || 0, 0)
    }));
    const downtimeReasons = Array.from(downtimeReasonTotals.values())
      .filter((item) => visibleWeeks.includes(item.week))
      .map((item) => ({ ...item, minutes: Math.round(item.minutes) }))
      .sort((a, b) => b.week.localeCompare(a.week) || shiftOrder.indexOf(a.shift) - shiftOrder.indexOf(b.shift) || b.minutes - a.minutes);
    const reportDates = rows.map((row) => row.report_date).filter(Boolean).sort();

    return res.json({
      today,
      kpis: {
        reports: todayRows.length,
        tons: roundMetric(sum(todayRows, "tons"), 0),
        coils: Math.round(sum(todayRows, "total_coils_ran")),
        linearFeet: Math.round(sum(todayRows, "linear_feet")),
        downtimeMinutes: Math.round(
          sum(todayRows, "planned_downtime_minutes") + sum(todayRows, "unplanned_downtime_minutes")
        )
      },
      todayShiftKpis,
      employeeTons,
      employeeTonsByMonth,
      employeeTonsCurrentMonth,
      shiftTons,
      weeklyShiftTons,
      currentWeek,
      currentWeekShiftTons,
      downtimeReasons,
      history: {
        reports: rows.length,
        sourceRows: sourceRows.length,
        firstDate: reportDates[0] || null,
        lastDate: reportDates[reportDates.length - 1] || null
      },
      generatedAt: new Date().toISOString()
    });
  } catch (error) {
    console.error("Shift report dashboard endpoint failed:", error);
    return res.status(500).json({ error: error.message || "Unable to load shift report dashboard data." });
  }
});

// Production chart data endpoint for frontend chart rebuild pages
app.get("/api/chart-data", async (req, res) => {
  const allowedTables = new Set(["psdata_loads", "psdata_loads_api", "psdata_production_tags_api", "psdata_iso_complaints", "psdata_appt_in", "psdata_cust_inv", "psdata_cust_inv_active_api"]);
  const table = String(req.query.table || "psdata_loads_api").trim();
  const shipDateColumn = table === "psdata_loads_api" ? "shipDate" : "ship_date";
  const receivingDateColumn = "arrival_date";
  const limitRaw = Number(req.query.limit || 3000);
  const limit = Math.min(Math.max(Number.isFinite(limitRaw) ? limitRaw : 3000, 50), 250000);
  const select = String(req.query.select || "*").trim() || "*";
  const order = String(req.query.order || "").trim();
  const shipDateGte = String(req.query.ship_date_gte || "").trim();
  const shipDateLt = String(req.query.ship_date_lt || "").trim();
  const shipDateLte = String(req.query.ship_date_lte || "").trim();
  const arrivalDateGte = String(req.query.arrival_date_gte || "").trim();
  const arrivalDateLt = String(req.query.arrival_date_lt || "").trim();
  const arrivalDateLte = String(req.query.arrival_date_lte || "").trim();

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

      if (table === "psdata_appt_in") {
        if (arrivalDateGte) query = query.gte(receivingDateColumn, arrivalDateGte);
        if (arrivalDateLt) query = query.lt(receivingDateColumn, arrivalDateLt);
        if (arrivalDateLte) query = query.lte(receivingDateColumn, arrivalDateLte);
      } else if (table === "psdata_loads" || table === "psdata_loads_api") {
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
      arrival_date_gte: arrivalDateGte || null,
      arrival_date_lt: arrivalDateLt || null,
      arrival_date_lte: arrivalDateLte || null,
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

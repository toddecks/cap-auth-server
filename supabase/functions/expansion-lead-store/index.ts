import "jsr:@supabase/functions-js/edge-runtime.d.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Content-Type": "application/json"
};

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: corsHeaders });

const text = (value: unknown, maxLength: number) =>
  String(value ?? "").trim().slice(0, maxLength);

const isUuid = (value: string) =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);

const safeMetadata = (value: unknown) => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  const entries = Object.entries(value as Record<string, unknown>)
    .slice(0, 30)
    .map(([key, item]) => [text(key, 80), text(item, 300)])
    .filter(([key]) => Boolean(key));
  return Object.fromEntries(entries);
};

const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
const authSupabaseUrl = "https://pxydsxadvmuffniluokk.supabase.co";
const authSupabaseAnonKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB4eWRzeGFkdm11ZmZuaWx1b2trIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjUzNDc4NzIsImV4cCI6MjA4MDkyMzg3Mn0.pyR6evjkvXIItcBuf8qIXBYOtOZIZIaE_0NEVGShFrI";
const legacySecret = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const secretDictionary = (() => {
  try {
    return JSON.parse(Deno.env.get("SUPABASE_SECRET_KEYS") || "{}");
  } catch {
    return {};
  }
})();
const secretKey = legacySecret
  || secretDictionary.default
  || Object.values(secretDictionary).find((value) => typeof value === "string")
  || "";

const restRequest = async (path: string, init: RequestInit = {}) => {
  if (!supabaseUrl || !secretKey) throw new Error("Supabase server credentials are unavailable.");
  const response = await fetch(`${supabaseUrl}/rest/v1/${path}`, {
    ...init,
    headers: {
      apikey: String(secretKey),
      Authorization: `Bearer ${secretKey}`,
      "Content-Type": "application/json",
      ...(init.headers || {})
    }
  });
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    throw new Error(payload?.message || payload?.hint || `Supabase request failed (${response.status}).`);
  }
  return payload;
};

const authorizeLeadViewer = async (req: Request) => {
  const authorization = text(req.headers.get("authorization"), 5000);
  if (!/^Bearer\s+\S+/i.test(authorization)) {
    return { error: "Missing Supabase access token.", status: 401 };
  }

  const authHeaders = {
    apikey: authSupabaseAnonKey,
    Authorization: authorization,
    "Content-Type": "application/json"
  };
  const userResponse = await fetch(`${authSupabaseUrl}/auth/v1/user`, { headers: authHeaders });
  const user = await userResponse.json().catch(() => null);
  if (!userResponse.ok || !user?.id) {
    return { error: "Invalid or expired Supabase session.", status: 401 };
  }

  const rolesResponse = await fetch(
    `${authSupabaseUrl}/rest/v1/user_roles?user_id=eq.${encodeURIComponent(user.id)}&select=roles(name)`,
    { headers: authHeaders }
  );
  const roleRows = await rolesResponse.json().catch(() => []);
  if (!rolesResponse.ok) {
    return { error: "Unable to verify Website Traffic access.", status: 500 };
  }

  const roles = (Array.isArray(roleRows) ? roleRows : [])
    .map((row) => row?.roles?.name)
    .filter(Boolean);
  const email = text(user.email, 254).toLowerCase();
  if (!roles.some((role) => role === "admin" || role === "website_leads")
      && email !== "kyle@coilsteelprocessing.com") {
    return { error: "Website traffic access is required.", status: 403 };
  }

  return { user, roles };
};

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  if (req.method !== "POST") return json({ error: "Method not allowed." }, 405);

  try {
    const body = await req.json();
    const action = text(body?.action, 30);

    if (action === "list") {
      const authorization = await authorizeLeadViewer(req);
      if (authorization.error) return json({ error: authorization.error }, authorization.status);

      const rows = await restRequest(
        "expansion_leads?select=id,submitted_at,name,company,email&order=submitted_at.desc&limit=250"
      );
      return json({
        rows: Array.isArray(rows) ? rows : [],
        count: Array.isArray(rows) ? rows.length : 0,
        generatedAt: new Date().toISOString()
      });
    }

    if (action === "email_status") {
      const id = Number(body?.id);
      const submissionToken = text(body?.submissionToken, 50);
      const emailStatus = text(body?.emailStatus, 20);
      if (!Number.isInteger(id) || id < 1 || !isUuid(submissionToken)) {
        return json({ error: "Invalid lead status request." }, 400);
      }
      if (!new Set(["sent", "failed"]).has(emailStatus)) {
        return json({ error: "Invalid email status." }, 400);
      }

      const update = emailStatus === "sent"
        ? {
            email_status: "sent",
            email_sent_at: new Date().toISOString(),
            resend_email_id: text(body?.resendEmailId, 300) || null,
            email_error: null
          }
        : {
            email_status: "failed",
            email_error: text(body?.emailError, 1000) || "Email delivery failed."
          };

      const rows = await restRequest(
        `expansion_leads?id=eq.${id}&submission_token=eq.${encodeURIComponent(submissionToken)}&select=id,email_status`,
        {
          method: "PATCH",
          headers: { Prefer: "return=representation" },
          body: JSON.stringify(update)
        }
      );
      if (!Array.isArray(rows) || rows.length !== 1) {
        return json({ error: "Lead record was not found." }, 404);
      }
      return json({ success: true, id: rows[0].id, emailStatus: rows[0].email_status });
    }

    if (action !== "store") return json({ error: "Invalid action." }, 400);

    const incoming = body?.lead || {};
    const lead = {
      submission_token: text(incoming.submission_token, 50),
      submitted_at: new Date().toISOString(),
      name: text(incoming.name, 120),
      company: text(incoming.company, 160),
      email: text(incoming.email, 254).toLowerCase(),
      phone: text(incoming.phone, 60),
      material_need: text(incoming.material_need, 180),
      opportunity_timing: text(incoming.opportunity_timing, 140),
      message: text(incoming.message, 5000) || null,
      page_url: text(incoming.page_url, 1200) || null,
      referrer: text(incoming.referrer, 1200) || null,
      visitor_id: text(incoming.visitor_id, 160) || null,
      session_id: text(incoming.session_id, 160) || null,
      ip_address: text(incoming.ip_address, 100) || null,
      user_agent: text(incoming.user_agent, 1000) || null,
      metadata: safeMetadata(incoming.metadata)
    };

    const required = ["name", "company", "email", "phone", "material_need", "opportunity_timing"] as const;
    if (required.some((key) => !lead[key])) return json({ error: "Complete all required fields." }, 400);
    if (!isUuid(lead.submission_token)) return json({ error: "Invalid submission token." }, 400);
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(lead.email)) return json({ error: "Enter a valid email address." }, 400);

    const existing = await restRequest(
      `expansion_leads?submission_token=eq.${encodeURIComponent(lead.submission_token)}&select=id,email_status&limit=1`
    );
    if (Array.isArray(existing) && existing.length > 0) {
      return json({ id: existing[0].id, emailStatus: existing[0].email_status, recorded: true });
    }

    if (lead.ip_address) {
      const since = new Date(Date.now() - 15 * 60 * 1000).toISOString();
      const recent = await restRequest(
        `expansion_leads?ip_address=eq.${encodeURIComponent(lead.ip_address)}&submitted_at=gte.${encodeURIComponent(since)}&select=id&limit=5`
      );
      if (Array.isArray(recent) && recent.length >= 5) {
        return json({ error: "Too many requests. Please call CSP or try again later." }, 429);
      }
    }

    const inserted = await restRequest("expansion_leads?select=id,email_status", {
      method: "POST",
      headers: { Prefer: "return=representation" },
      body: JSON.stringify({ ...lead, email_status: "pending" })
    });
    if (!Array.isArray(inserted) || inserted.length !== 1) {
      throw new Error("Supabase did not return the new lead record.");
    }

    return json({ id: inserted[0].id, emailStatus: inserted[0].email_status, recorded: true }, 201);
  } catch (error) {
    console.error("Expansion lead store failed:", error);
    return json({ error: error instanceof Error ? error.message : "Lead storage failed." }, 500);
  }
});

require("dotenv").config();
const express = require("express");
const jwt = require("jsonwebtoken");
const cors = require("cors");
const crypto = require("crypto");
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

const SUPABASE_URL = "https://pxydsxadvmuffniluokk.supabase.co";
const SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB4eWRzeGFkdm11ZmZuaWx1b2trIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NTM0Nzg3MiwiZXhwIjoyMDgwOTIzODcyfQ.jTtNfZUlS6Ue0W7SWpmQLDLRerNGP7tlPxzZlJfuxPc";

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY);

app.post("/api/create-user", async (req, res) => {
  const { email, password, roles } = req.body || {};

  if (!email || !password || !Array.isArray(roles) || roles.length === 0) {
    return res.status(400).json({ message: "Email, password, and roles are required." });
  }

  const cleanEmail = String(email).trim().toLowerCase();

  try {
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
    const roleRows = roles.map((role_id) => ({
      user_id: user.id,
      role_id: Number(role_id),
    }));

    const { error: rolesError } = await supabase.from("user_roles").insert(roleRows);

    if (rolesError) {
      console.error("Role assignment failed:", rolesError);
      return res.status(400).json({ message: rolesError.message });
    }

    console.log(`✅ Created user ${email} with roles ${roles.join(", ")}`);
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
    // Remove existing roles
    await supabase.from("user_roles").delete().eq("user_id", id);

    // Insert new roles
    const roleRows = roles.map((role_id) => ({
      user_id: id,
      role_id: Number(role_id),
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
    "https://csp-bi-website.onrender.com/change-password.html";

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

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Tableau Auth Server running on port ${PORT}`);
});

const express = require("express");
const jwt = require("jsonwebtoken");
const cors = require("cors");
const crypto = require("crypto");

const app = express();
app.use(cors({
  origin: "*",
  methods: ["GET"],
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
    // 1️⃣ Create the user
    const { data: user, error: userError } = await supabase
      .from("users")
      .insert({ email: cleanEmail, password })
      .select()
      .single();

    if (userError || !user) {
      console.error("User creation failed:", userError);
      return res.status(400).json({ message: userError?.message || "Error creating user." });
    }

    // 2️⃣ Assign roles
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

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Tableau Auth Server running on port ${PORT}`);
});
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

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Tableau Auth Server running on port ${PORT}`);
});
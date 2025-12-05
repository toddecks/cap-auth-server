const express = require("express");
const jwt = require("jsonwebtoken");
const cors = require("cors");

const app = express();
app.use(cors());
app.use(express.json());

// Tableau Connected App Credentials
const CLIENT_ID = "b06194b0-af65-43d0-907d-bcfd33ddd1ed";
const SECRET = "wb86WFtB3DDaA+mLoxfBzfkcN63zYGUk7GjCAfNskXxY=";

// Endpoint to generate Tableau Embed Token
app.get("/getTableauToken", (req, res) => {
  const now = Math.floor(Date.now() / 1000);

  const payload = {
    iss: CLIENT_ID,
    exp: now + 300,
    aud: "tableau",
    sub: "todd@coilsteelprocessing.com",
    stid: "csp",
    scp: [
      "tableau:views:embed",
      "tableau:metadata:read"
    ]
  };

  const token = jwt.sign(payload, SECRET, { algorithm: "HS256" });

  res.json({ token });
});

// Start server
app.listen(3000, () => {
  console.log("Tableau Auth Server running on http://localhost:3000");
});
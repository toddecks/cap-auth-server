(() => {
  const parseRolesFromStorage = () => {
    const roles = new Set();
    const add = (val) => {
      if (!val) return;
      if (Array.isArray(val)) {
        val.forEach((r) => {
          if (typeof r === "string") roles.add(r.toLowerCase());
          if (r?.roles?.name) roles.add(String(r.roles.name).toLowerCase());
        });
      }
    };
    try {
      add(JSON.parse(localStorage.getItem("cspUserRoles")));
    } catch (err) {
      console.warn("Parse cspUserRoles failed", err);
    }
    try {
      add(JSON.parse(localStorage.getItem("userRoles")));
    } catch (err) {
      console.warn("Parse userRoles failed", err);
    }
    return Array.from(roles);
  };

  const isAdmin = parseRolesFromStorage().includes("admin");

  const form = document.getElementById("createUserForm");
  const messageEl = document.getElementById("message");

  const setMessage = (text, isError = true) => {
    if (!messageEl) return;
    messageEl.style.display = text ? "block" : "none";
    messageEl.style.color = isError ? "red" : "green";
    messageEl.textContent = text || "";
  };

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!isAdmin) {
      setMessage("Admin access required to create users.", true);
      return;
    }

    const email = document.getElementById("newUserEmail")?.value.trim().toLowerCase();
    const password = document.getElementById("newUserPassword")?.value.trim();
    const roleCheckboxes = Array.from(
      form.querySelectorAll('input[type="checkbox"][data-role-id]')
    );
    const selectedRoleIds = roleCheckboxes
      .filter((cb) => cb.checked)
      .map((cb) => Number(cb.getAttribute("data-role-id")))
      .filter(Boolean);

    if (!email || !password) {
      setMessage("Email and password are required.", true);
      return;
    }
    if (selectedRoleIds.length === 0) {
      setMessage("Select at least one role.", true);
      return;
    }

    setMessage("");

    const API_BASE =
      window.CREATE_USER_API_BASE ||
      "https://cap-auth-server.onrender.com"; // TODO: set to your Render base URL

    try {
      const response = await fetch(`${API_BASE}/api/create-user`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          email,
          password,
          roles: selectedRoleIds,
        }),
      });

      const result = await response.json().catch(() => ({}));

      if (!response.ok) {
        const errorText =
          result?.message ||
          result?.error ||
          "Error creating user. Please try again.";
        setMessage(errorText, true);
        return;
      }

      setMessage("User created successfully.", false);
      form.reset();
    } catch (err) {
      console.error("Create user request failed:", err);
      setMessage("Network or server error creating user.", true);
    }
  };

  if (form) {
    if (!isAdmin) {
      setMessage("Admin access required to create users.", true);
    }
    form.addEventListener("submit", handleSubmit);
  }
})();

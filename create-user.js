document.addEventListener("DOMContentLoaded", () => {
  console.log("create-user.js loaded");

  const parseRolesFromStorage = () => {
    const roles = new Set();
    const add = (val) => {
      if (!val) return;

      if (Array.isArray(val)) {
        val.forEach((r) => {
          if (typeof r === "string") roles.add(r.toLowerCase());
          if (r?.roles?.name) roles.add(String(r.roles.name).toLowerCase());
          if (r?.role?.name) roles.add(String(r.role.name).toLowerCase());
          if (r?.name) roles.add(String(r.name).toLowerCase());
        });
      }

      if (typeof val === "string") {
        roles.add(val.toLowerCase());
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

  const roles = parseRolesFromStorage();
  const isAdmin = roles.includes("admin");

  // *** CORRECT ROLE LABELS (matches Supabase roles table) ***
  const ROLE_LABELS = {
    1: "Admin",
    2: "Shipping",
    3: "Receiving",
    4: "Production",
    5: "Live",
    6: "Finance",
    7: "Inventory"
  };

  const normalizeRoleCheckboxes = () => {
    const checkboxes = document.querySelectorAll('input[type="checkbox"][data-role-id]');
    checkboxes.forEach(cb => {
      const roleId = Number(cb.getAttribute("data-role-id"));
      const label = ROLE_LABELS[roleId];

      let labelEl = cb.closest("label");
      if (!labelEl && cb.id) {
        labelEl = document.querySelector(`label[for="${cb.id}"]`);
      }

      if (label && labelEl) {
        labelEl.childNodes.forEach(node => {
          if (node.nodeType === Node.TEXT_NODE) node.textContent = ` ${label}`;
        });
      }
    });
  };

  const form = document.getElementById("createUserForm");
  const messageEl = document.getElementById("message");

  const setMessage = (text, isError = true) => {
    if (!messageEl) {
      console.warn("Message element not found");
      return;
    }
    messageEl.style.display = text ? "block" : "none";
    messageEl.style.color = isError ? "red" : "green";
    messageEl.textContent = text || "";
  };

  if (!form) {
    console.error("Create User form not found in DOM");
    setMessage("Form not found. Page did not load correctly.", true);
    return;
  }

  normalizeRoleCheckboxes();

  if (!isAdmin) {
    setMessage("Admin access required to create users.", true);
  }

  const handleSubmit = async (e) => {
    e.preventDefault();
    console.log("Create user submit fired");

    if (!isAdmin) {
      setMessage("Admin access required to create users.", true);
      return;
    }

    const emailInput = document.getElementById("newUserEmail");
    const passwordInput = document.getElementById("newUserPassword");

    if (!emailInput || !passwordInput) {
      setMessage("Form fields not found.", true);
      return;
    }

    const email = emailInput.value.trim().toLowerCase();
    const password = passwordInput.value.trim();

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

    setMessage("Creating user...", false);

    const API_BASE =
      window.CREATE_USER_API_BASE ||
      "https://cap-auth-server.onrender.com";

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

      let result = {};
      try {
        result = await response.json();
      } catch (e) {
        console.warn("Non-JSON response from server");
      }

      if (!response.ok) {
        const errorText =
          result?.message ||
          result?.error ||
          "Error creating user. Please try again.";
        console.error("Create user failed:", errorText);
        setMessage(errorText, true);
        return;
      }

      console.log("User created successfully:", result);
      setMessage("✅ User created successfully.", false);
      form.reset();

    } catch (err) {
      console.error("Create user request failed:", err);
      setMessage("Network or server error creating user.", true);
    }
  };

  form.addEventListener("submit", handleSubmit);
});
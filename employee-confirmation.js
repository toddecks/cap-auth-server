// Link directly to a confirmation-only page; never use the Auth action-link redirect.
function employeeConfirmationUrl(authData, baseUrl) {
  const tokenHash = authData?.properties?.hashed_token;
  if (!tokenHash) throw new Error("Unable to generate the email confirmation token.");
  const url = new URL("confirm.html", baseUrl.replace(/\/+$/, "") + "/");
  url.searchParams.set("token_hash", tokenHash);
  url.searchParams.set("type", "email");
  return url.toString();
}
module.exports = { employeeConfirmationUrl };

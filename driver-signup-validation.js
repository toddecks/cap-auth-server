"use strict";
function validateDriverSignup(profile, body = {}) {
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(profile.email)) return 'Enter a valid email address.';
  if (profile.password.length < 8 || profile.password.length > 72) return 'Use a password between 8 and 72 characters.';
  const missing = [
    ['fullName', 'full name'], ['haulingFor', 'company you are hauling for'], ['driverCompany', 'driver company']
  ].filter(([key]) => !profile[key]).map(([,label]) => label);
  if (missing.length) return `Enter your ${missing.join(', ')}.`;
  // Released iPhone/Android builds predate the phone field. Keep email-verified
  // signup compatible; never invent a phone number or block those clients.
  if (Object.prototype.hasOwnProperty.call(body || {}, 'phone') && !profile.phone) {
    return 'Enter a valid mobile phone number, including the area code.';
  }
  return null;
}
module.exports = { validateDriverSignup };

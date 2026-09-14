const { test } = require('node:test');
const assert = require('node:assert/strict');
const { validateDriverSignup } = require('./driver-signup-validation');
const profile = { email:'driver@example.com', password:'example-test-password', fullName:'Test Driver', haulingFor:'Customer', driverCompany:'Carrier', phone:'' };
test('released app signup without a phone field remains accepted', () => {
  assert.equal(validateDriverSignup(profile, {fullName:profile.fullName}), null);
});
test('new app missing or invalid phone gets a specific correction', () => {
  assert.match(validateDriverSignup(profile,{phone:''}), /valid mobile phone/);
  assert.match(validateDriverSignup(profile,{phone:'123'}), /area code/);
  assert.equal(validateDriverSignup({...profile,phone:'+14195550123'},{phone:'(419) 555-0123'}),null);
});
test('required profile information is named and email/password rules still apply', () => {
  assert.equal(validateDriverSignup({...profile,haulingFor:''},{}),'Enter your company you are hauling for.');
  assert.match(validateDriverSignup({...profile,email:'invalid'},{}),/valid email/);
  assert.match(validateDriverSignup({...profile,password:'short'},{}),/8 and 72/);
});

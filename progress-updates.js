const express = require('express');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');

// Resend owns the durable subscriber list. This router never sends a broadcast.
module.exports = function progressUpdates({ env = process.env, request = fetch } = {}) {
  const router = express.Router();
  const limits = new Map();
  const key = env.RESEND_API_KEY;
  const segment = env.RESEND_PROGRESS_SEGMENT_ID || 'f79254b7-9583-458d-a981-ff76849308db';
  // Domain-separated signing key remains stable across deploys without another
  // credential. An explicit confirmation secret can override it for rotation.
  const secret = env.PROGRESS_CONFIRM_SECRET || (key
    ? crypto.createHmac('sha256', key).update('csp-expansion-confirmation-v1').digest('hex')
    : '');
  const from = env.PROGRESS_FROM_EMAIL || env.PRO_FORMS_FROM_EMAIL
    || 'CSP Updates <updates@pro.coilsteelprocessing.com>';
  const origin = 'https://cap-auth-server.onrender.com';
  const escape = value => String(value).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const ready = () => Boolean(key && segment && secret?.length >= 32 && from);
  const message = 'Check your inbox to confirm your subscription.';
  function limited(id, max) {
    const now = Date.now();
    for (const [k, v] of limits) if (v.until < now) limits.delete(k);
    const value = limits.get(id) || { count: 0, until: now + 3600000 };
    value.count++; limits.set(id, value);
    return value.count > max;
  }
  async function resend(path, method = 'GET', body, headers = {}) {
    const response = await request('https://api.resend.com' + path, {
      method, headers: {Authorization: 'Bearer ' + key, 'Content-Type': 'application/json', ...headers},
      ...(body ? {body: JSON.stringify(body)} : {}), signal: AbortSignal.timeout(15000)
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) { const error = new Error('Email service unavailable'); error.status = response.status; throw error; }
    return data;
  }
  function verify(token) {
    return jwt.verify(token, secret, { algorithms: ['HS256'], audience: 'csp-progress-updates', issuer: 'csp' });
  }
  function page(res, content, status = 200) {
    return res.status(status).type('html').send('<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>CSP Progress Updates</title><body style="background:#233658;color:#233658;font-family:Arial;padding:32px"><main style="max-width:520px;margin:60px auto;background:white;padding:32px;border-radius:12px"><h1>CSP Progress Updates</h1>' + content + '</main></body></html>');
  }
  router.use((req, res, next) => {
    res.set({'Cache-Control':'no-store','Referrer-Policy':'no-referrer','X-Content-Type-Options':'nosniff'});
    next();
  });
  router.get('/status', (_req, res) => res.status(ready() ? 200 : 503).json({ready:ready(), confirmationRequired:true}));
  router.post('/subscribe', async (req, res) => {
    if (!ready()) return res.status(503).json({error:'Progress updates are not available yet. Please try again later.'});
    if (req.body?.website) return res.json({message});
    const name = String(req.body?.name || '').trim();
    const email = String(req.body?.email || '').trim().toLowerCase();
    if (!name || name.length > 120 || email.length > 254 || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || req.body?.consent !== true) {
      return res.status(400).json({error:'Enter your name, a valid email, and agree to receive progress updates.'});
    }
    const emailHash = crypto.createHash('sha256').update(email).digest('hex');
    if (limited('ip:' + req.ip, 100) || limited('email:' + emailHash, 3)) {
      return res.status(429).json({error:'Please wait before requesting another confirmation email.'});
    }
    try {
      const token = jwt.sign({name, email}, secret, {algorithm:'HS256', expiresIn:'48h', audience:'csp-progress-updates', issuer:'csp'});
      const link = origin + '/api/progress-updates/confirm?token=' + encodeURIComponent(token);
      await resend('/emails', 'POST', {
        from, to: [email], subject: 'Confirm your CSP expansion updates',
        text: 'Confirm that you want CSP expansion progress emails: ' + link + '\nIf you did not request this, ignore this email. The link expires in 48 hours.',
        html: '<h1>Follow our progress</h1><p>Confirm that you want occasional CSP expansion updates, new photos, and project news.</p><p><a href="' + escape(link) + '">Confirm my subscription</a></p><p>If you did not request this, ignore this email. This link expires in 48 hours.</p>'
      }, {'Idempotency-Key':'progress-confirm-' + emailHash + '-' + Math.floor(Date.now()/300000)});
      return res.json({message});
    } catch { return res.status(502).json({error:'We could not send the confirmation. Please try again later.'}); }
  });
  // GET does not subscribe anyone: email-security link scanners must not opt users in.
  router.get('/confirm', (req, res) => {
    if (!ready()) return page(res, '<p>Subscriptions are temporarily unavailable.</p>', 503);
    try {
      verify(String(req.query.token || ''));
      return page(res, '<p>Receive occasional new-line progress photos and a link to the project page. You can unsubscribe from any update.</p><form method="post"><input type="hidden" name="token" value="' + escape(req.query.token) + '"><button style="background:#f1a91e;border:0;padding:14px;cursor:pointer">Confirm my subscription</button></form>');
    } catch { return page(res, '<p>This link is invalid or expired. Please sign up again on the project page.</p>', 400); }
  });
  router.post('/confirm', async (req, res) => {
    if (!ready()) return page(res, '<p>Subscriptions are temporarily unavailable.</p>', 503);
    let person;
    try { person = verify(String(req.body?.token || '')); }
    catch { return page(res, '<p>This link is invalid or expired. Please sign up again.</p>', 400); }
    if (limited('confirm:' + req.ip, 100)) return page(res, '<p>Please try again later.</p>', 429);
    try {
      const contactPath = '/contacts/' + encodeURIComponent(person.email);
      let contact;
      try { contact = await resend(contactPath); }
      catch (error) { if (error.status !== 404) throw error; }
      // Never override a prior global unsubscribe, even when an old link is replayed.
      if (contact?.unsubscribed) return page(res, '<p>This address previously unsubscribed. Your email preferences have not been changed. Contact CSP if you want to rejoin.</p>');
      if (!contact) {
        const [first, ...last] = person.name.split(/\s+/);
        await resend('/contacts', 'POST', {email:person.email, first_name:first, last_name:last.join(' '), unsubscribed:false});
      }
      await resend(contactPath + '/segments/' + encodeURIComponent(segment), 'POST');
      return page(res, '<p>You’re subscribed! We’ll email you when there’s new progress to share.</p><p><a href="https://coilsteelprocessing.com/new-line/">See the latest progress</a></p>');
    } catch { return page(res, '<p>We could not finish your subscription. Please try this confirmation again later.</p>', 502); }
  });
  return router;
};

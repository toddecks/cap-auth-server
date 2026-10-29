# Expansion progress subscriptions

## Configuration

1. In Resend create a dedicated **CSP Expansion Updates** segment. Do not import inquiry contacts without their consent.
2. The CSP Expansion Updates segment is configured by default. Optional Render overrides: `RESEND_PROGRESS_SEGMENT_ID`, `PROGRESS_CONFIRM_SECRET` (at least 32 characters), and `PROGRESS_FROM_EMAIL`. By default, confirmation tokens use a domain-separated HMAC key derived from the existing Resend secret; rotating that key invalidates outstanding links. Sender defaults to `PRO_FORMS_FROM_EMAIL`, then `updates@pro.coilsteelprocessing.com` on the verified domain. `RESEND_API_KEY` must permit Contacts and Segments, not only sending.
3. Deploy `progress-updates.js` and the route registration in `server.js`. Preserve unrelated uncommitted changes.
4. Test with an owner-controlled email: subscribe, explicitly confirm, verify the contact is in the segment. Invalid/expired links and unsubscribed contacts must not be added. Do not send a campaign during setup.
5. Only then upload the updated expansion.html, new-line/index.html and assets/progress-signup.{css,js}. Replace the homepage module in WordPress separately.

## Sending a progress update

Create a draft Broadcast in Resend, select only the CSP Expansion Updates segment, and use the prepared `email-templates/expansion-progress-update.html`. Add the business mailing address where marked. Preview/test it to yourself, then explicitly approve sending. Use Resend’s Broadcast unsubscribe link; do not send this through the transactional `/emails` endpoint. No automatic sending is configured.

## Safeguards

Subscribers must check the consent box and confirm ownership by email within 48 hours. Confirmation requires a POST button, not a GET scanned by email security tools. Existing globally unsubscribed contacts are never automatically resubscribed. Resend stores the contact and segment membership; no Supabase migration is involved. Rate limits are process-local: add shared limits/CAPTCHA if the service scales to multiple instances or receives abuse. Confirmation URLs are bearer credentials: keep query strings out of application/access logs.

References: https://resend.com/docs/api-reference/contacts/create-contact and https://resend.com/docs/api-reference/contacts/add-contact-to-segment

Health check: `GET /api/progress-updates/status` returns readiness and confirms that email ownership is required. It does not send mail or expose configuration.

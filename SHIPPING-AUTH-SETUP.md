# CSP Shipping email sign-in

The shipping portal requests branded magic-link email from:

`POST /api/shipping/auth/magic-link`

The endpoint uses the existing Resend configuration and a separate service-role
connection to the CSP Driver App Supabase project. It creates approved Auth users,
assigns their `csp_role`, generates a one-time token hash, and emails a link to
`https://shipping.coilsteelprocessing.com`.

## Render environment variables

Keep the existing `RESEND_API_KEY` and `PRO_FORMS_FROM_EMAIL` variables. Add:

- `DRIVER_SUPABASE_URL=https://djdhhmotrzahexhlnwvs.supabase.co`
- `DRIVER_SUPABASE_SERVICE_ROLE_KEY=<Driver App project service-role key>`
- `SHIPPING_PORTAL_BASE_URL=https://shipping.coilsteelprocessing.com`
- `SHIPPING_AUTH_FROM_EMAIL=CSP Shipping <shipping@coilsteelprocessing.com>`
- `SHIPPING_AUTH_ALLOWED_EMAILS=todd@coilsteelprocessing.com:admin`

Additional employees use comma-separated `email:role` entries. Supported roles
are `shipping` and `admin`.

Never put the service-role key in the website's `config.js` or any browser file.

The same variables also power `POST /api/driver/auth/signup-code`, which creates
driver Auth accounts and sends branded six-digit verification codes through
Resend. No additional secret is required for mobile signup.

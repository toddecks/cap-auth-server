# CSP Shipping BI-master sign-in

The shipping portal authenticates passwords and magic links against the BI
Supabase project. After the BI account and roles are verified, the backend
creates a short-lived session for the separate CSP Driver App data project.
Shipping never stores or copies the BI password.

The browser exchanges its authenticated BI session at:

`POST /api/shipping/auth/session`

The portal requests a branded BI magic-link email from:

`POST /api/shipping/auth/magic-link`

BI users with `admin`, `shipping_overview`, or `shipping_performance` are
authorized by default. `SHIPPING_AUTH_ALLOWED_EMAILS` remains a fallback
allowlist for explicitly approved employees.

## Render environment variables

Keep the existing `RESEND_API_KEY` and `PRO_FORMS_FROM_EMAIL` variables. Add:

- `DRIVER_SUPABASE_URL=https://djdhhmotrzahexhlnwvs.supabase.co`
- `DRIVER_SUPABASE_SERVICE_ROLE_KEY=<Driver App project service-role key>`
- `SHIPPING_PORTAL_BASE_URL=https://shipping.coilsteelprocessing.com`
- `SHIPPING_AUTH_FROM_EMAIL=CSP Shipping <shipping@coilsteelprocessing.com>`
- `SHIPPING_BI_ACCESS_ROLES=admin,shipping_overview,shipping_performance`
- `SHIPPING_AUTH_ALLOWED_EMAILS=todd@coilsteelprocessing.com:admin`

Additional fallback employees use comma-separated `email:role` entries.
Supported fallback roles are `shipping` and `admin`.

Never put the service-role key in the website's `config.js` or any browser file.

The same variables also power `POST /api/driver/auth/signup-code`, which creates
driver Auth accounts and sends branded six-digit verification codes through
Resend. No additional secret is required for mobile signup.

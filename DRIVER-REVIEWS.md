# Driver departure reviews

Review URL: https://g.page/r/CS0GZ0amOC9REAE/review

## Text drivers (live)

Completing a Shipping SMS arrival sets departed_at. A server worker checks every 30 seconds, claims one request per arrival, saves the outgoing transcript, and submits the text using the existing Twilio Messaging Service. Only departures after driver_review_settings.enabled_at are eligible; open and historical visits are ignored. A staff browser does not have to stay open.

The request primary key and atomic SKIP LOCKED claim prevent repeated close clicks, multiple server instances, and timer overlap from duplicating a text. Provider rejections are failed; uncertain responses and interrupted sends are held for manual investigation, never automatically resent. Existing Twilio delivery callbacks update the message transcript. Deleting a visit/thread cascades to its review record.

No real driver messages were sent for testing. Mock transport tests cover one send, overlapping polls, timeouts, and opt-out errors. Transaction-rolled-back database fixtures verified open/closed/historical behavior and repeated claims. Tables are RLS enabled, inaccessible to client roles; RPC is service_role only and SECURITY INVOKER.

## App drivers

Native source: /Users/toddyarberry/Documents/CSP-Driver-App

Confirmed geofence departures schedule a local notification after persisting the departure event. The existing active-visit guard prevents repeated boundary callbacks from requesting another review. It works without a network connection at departure. Tapping opens the fixed Google review URL. Notification permission is required. Manual check-out/Shipping stale-visit cleanup does not generate this geofence notification.

New app installation is required; the current app has no OTA update configuration.
Android build 14: 1d3e36ba-f6fe-4c65-9425-8261f5e2f716
Apple build 19: 285666bf-61ea-4bb3-bb66-031a03d1ee07

Validation: npm run typecheck, native cloud builds. Physical geofence testing requires a device on a real or simulated visit.

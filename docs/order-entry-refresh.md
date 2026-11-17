# Order-entry reporting refresh

Source: Azure SQL dbo.SetupHdr (SETUPNUMBER, ENTEREDBY, INPUTDATE). Clarion dates use 1800-12-28 as epoch. Count distinct setup headers, not production cuts or audit changes.

Current refresh: this Mac's launch agent com.csp.order-entry-sync runs daily at 05:20 local time and at login. It requires the Mac to be awake, signed in, and on an allowed CSP network. launchd runs a missed calendar job after wake. Source and runner are in ~/Library/Application Support/CSP/order-entry-sync. SQL password and purpose-specific ingest token are in login Keychain, services com.csp.order-entry.azure and com.csp.order-entry.ingest. No credentials are stored in the job or plist.

The Mac sends a complete validated snapshot to the authenticated order-entry-ingest Edge Function. The endpoint stores batches under a new sync run and atomically publishes only after exact row-count verification. Reporting reads the latest complete run and retains previous data after failed refreshes. Tables have RLS and service-role-only access. The ingest function uses its own random shared-secret authentication; its source stores only the SHA-256 digest.

Logs: ~/Library/Logs/CSP/order-entry-sync.log and order-entry-sync-error.log. Manual run: /usr/local/bin/node "$HOME/Library/Application Support/CSP/order-entry-sync/run.cjs".

Cloud workflow sync-azure-order-entries.yml is currently DISABLED because Azure firewall rejects Render IP 74.220.49.7. No Azure firewall rule was changed: saved SQL account lacks permission. Re-enable only after allowing stable cloud egress and verifying a complete remote sync.

UI: https://bi.coilsteelprocessing.com/newperformance.html under Orders Entered. Daily Monday-Sunday selected week and last-four-weeks line chart. CSV and employee totals available.

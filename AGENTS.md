# BMA Jastip Bot — Manual Health Check

## Commands
- `Bestie cek status tick` / `cek status tick`: read-only audit of production Evolution API, Jastip Worker and Google Sheets ORDER. Default window: last 30 minutes, extend for unresolved cases. Report actual time window and log limits.
- `cek Evolution`: audit connection transitions, disconnect reasons, latest inbound and dispatch events, deployment status.
- `recovery tick`: recover only verified missing reactions through supported mechanisms, preserving deletion and allocation status; never duplicate orders. Report unavailable mechanisms rather than inventing them.
- `restart Evolution`: explicit approval to restart Evolution API only.
- `deploy`: explicit approval to deploy prepared changes for the identified application only; verify build and runtime separately.

## Health check procedure
Railway project 17796c36-2457-49b2-91c9-06feb2895f3b, production environment 7bdc80f9-323a-4746-aaac-1670754f9bca. Evolution API service 4400361b-e856-4210-96ff-f475b12c76a6; Jastip Worker service 4148ad86-c59d-4adb-8d3a-bd0fa5cb8145.
Correlate inbound/dispatch, worker receipt, SHEET ACCEPTED and INITIAL REACTION SENT by message ID. Verify unresolved orders against the correct Sheet tab. Report queue depth only if actually observable. Distinguish accepted reaction API request from visible WhatsApp reaction. Missing logs, truncated logs and silent periods are not proof of missing orders or healthy connection. Compare actual group activity when available. Never claim complete coverage of messages not received by Evolution.

No additional periodic scans or runtime changes for this manual SOP. No restart, logout, deployment, WhatsApp send or data mutation during read-only checks. If fresh FIX messages repeatedly fail to reach Evolution despite an open connection, recommend checking duplicate sessions and pairing again with owner present and explicit approval. Never delete instance, database or Redis. Preserve existing order and reaction rules.

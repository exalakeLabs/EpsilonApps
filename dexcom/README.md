# Dexcom sandbox callback

Working directory: `/Users/alex.gauthier/git/EpsilonApps/dexcom`.

Run `.venv/bin/python app.py` from this directory, then open http://localhost:8080 and choose Connect Dexcom sandbox. Select the G7 simulated user when Dexcom prompts.

The registered redirect URI must be exactly http://localhost:8080/callback.

The server binds only to 127.0.0.1. Credentials are read from credentials.json; OAuth state is checked once, and authorization codes are exchanged immediately. Credentials and tokens stay on the backend. Generated token and data files use mode 0600. Do not commit or share credentials.json or tokens.json.

After authorization the app retrieves dataRange and the last available day of estimated glucose values, saving data-range.json, readings.json, and status.json. /status reports a summary without tokens. This app only targets the sandbox; it cannot retrieve real patient data.

To retry with updated credentials, copy the current values from the Sigma app in Dexcom My Apps into credentials.json locally, then start a new connection from http://localhost:8080. The app reloads credentials for each new connection and keeps that pair for its callback. No restart is needed after editing credentials. Stop the server with Ctrl+C.

A regenerated Dexcom secret invalidates the previous secret. If Dexcom returns `invalid_client` or `unauthorized_client`, verify the local credential pair against My Apps. An `invalid_grant` response means the authorization code was rejected; start a new connection instead of reusing a callback link. Dexcom authorization codes are single-use and expire after one minute.

Failures show an actionable message on the home page and a safe error code at /status. Callback URLs are cleared after token exchange succeeds or fails, and provider error bodies, secrets and tokens are not logged.

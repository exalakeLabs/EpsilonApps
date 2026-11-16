# Dexcom sandbox callback

Working directory: `/Users/alex.gauthier/git/EpsilonApps/dexcom`.

Run `python3 app.py` from this directory, then open http://localhost:8080 and choose Connect Dexcom sandbox. Select the G7 simulated user when Dexcom prompts.

The registered redirect URI must be exactly http://localhost:8080/callback.

The server binds only to 127.0.0.1. Credentials are read from credentials.json; OAuth state is checked once, and authorization codes are exchanged immediately. Credentials and tokens stay on the backend. Generated token and data files use mode 0600. Do not commit or share credentials.json or tokens.json.

After authorization the app retrieves dataRange and the last available day of estimated glucose values, saving data-range.json, readings.json, and status.json. /status reports a summary without tokens. This app only targets the sandbox; it cannot retrieve real patient data.

To retry with updated credentials, edit credentials.json locally and restart the server. Stop with Ctrl+C.

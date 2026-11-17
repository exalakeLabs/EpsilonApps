"""Offline regression tests for the local OAuth callback (no Dexcom calls)."""
import contextlib
import io
import json
import tempfile
import threading
import unittest
from http.client import HTTPConnection
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

import app


class CallbackTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root_patch = patch.object(app, 'ROOT', Path(self.temp.name))
        self.root_patch.start()
        app.states.clear()
        app.status.clear()
        app.status.update(connected=False, environment='sandbox')
        self.set_credentials('initial-test-secret')
        self.server = app.ThreadingHTTPServer(('127.0.0.1', 0), app.Handler)
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.worker.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.worker.join()
        self.root_patch.stop()
        self.temp.cleanup()

    def set_credentials(self, secret):
        (app.ROOT / 'credentials.json').write_text(json.dumps(
            dict(client_id='test-client', client_secret=secret)))

    def request(self, path):
        conn = HTTPConnection('127.0.0.1', self.server.server_port, timeout=3)
        conn.request('GET', path, headers={'Host': 'localhost:8080'})
        response = conn.getresponse()
        result = response.status, dict(response.getheaders()), response.read().decode()
        conn.close()
        return result

    def begin(self):
        code, headers, _ = self.request('/connect')
        self.assertEqual(code, 302)
        query = parse_qs(urlsplit(headers['Location']).query)
        self.assertEqual(query['redirect_uri'], [app.REDIRECT])
        return query['state'][0]

    def complete(self, state):
        return self.request('/callback?state=' + state + '&code=fake-code')

    def test_new_connections_reload_credentials_and_keep_their_pair(self):
        first = self.begin()
        self.set_credentials('updated-test-secret')
        second = self.begin()
        used = []

        def exchange(request, **kwargs):
            used.append(parse_qs(request.data.decode())['client_secret'][0])
            return io.BytesIO(json.dumps(dict(access_token='test-token', refresh_token='test-refresh')).encode())

        ranges = {'egvs': {'end': {'systemTime': '2026-11-17T12:00:00'}}}
        readings = {'records': [{'value': 100, 'transmitterGeneration': 'g7'}]}
        with patch.object(app, 'urlopen', side_effect=exchange), patch.object(app, 'api', side_effect=[ranges, readings, ranges, readings]):
            for state in [first, second]:
                code, headers, _ = self.complete(state)
                self.assertEqual(code, 303)
                self.assertEqual(headers['Location'], '/')
                self.assertEqual(headers['Cache-Control'], 'no-store')
        self.assertEqual(used, ['initial-test-secret', 'updated-test-secret'])
        self.assertTrue(app.status['connected'])
        self.assertEqual(app.status['reading_count'], 1)
        self.assertEqual((app.ROOT / 'tokens.json').stat().st_mode & 0o777, 0o600)

    def test_invalid_credentials_are_actionable_without_secret_leaks(self):
        state = self.begin()
        body = json.dumps({'error': 'unauthorized_client', 'error_description': 'initial-test-secret fake-code <script>echo</script>'}).encode()
        error = HTTPError('https://sandbox-api.dexcom.com/v3/oauth2/token', 401, 'Unauthorized', {}, io.BytesIO(body))
        logs = io.StringIO()
        with patch.object(app, 'urlopen', side_effect=error) as exchange, contextlib.redirect_stdout(logs):
            code, headers, _ = self.complete(state)
            self.assertEqual(code, 303)
            self.assertEqual(headers['Location'], '/')
            self.assertEqual(self.complete(state)[0], 400)
            exchange.assert_called_once()
        status = json.loads(self.request('/status')[2])
        home = self.request('/')[2]
        self.assertEqual(status['oauth_error'], 'unauthorized_client')
        self.assertEqual(status['http_status'], 401)
        self.assertIn('current Client ID', home)
        for secret in ('initial-test-secret', 'fake-code', '<script>'):
            self.assertNotIn(secret, home + json.dumps(status) + logs.getvalue())
        self.assertFalse((app.ROOT / 'tokens.json').exists())

    def test_expired_code_has_separate_recovery_message(self):
        body = io.BytesIO(b'{"error":"invalid_grant","error_description":"Code not valid"}')
        details = app.error_details(HTTPError(app.BASE, 400, 'Bad request', {}, body), 'token_exchange')
        self.assertIn('expired or already', details['message'])
        self.assertNotIn('app credentials', details['message'])

    def test_unknown_provider_error_body_is_not_echoed(self):
        body = io.BytesIO(b'{"error":"secret-value","error_description":"secret-value"}')
        details = app.error_details(HTTPError(app.BASE, 500, 'Error', {}, body), 'token_exchange')
        self.assertIsNone(details['oauth_error'])
        self.assertNotIn('secret-value', json.dumps(details))

    def test_invalid_state_never_attempts_exchange(self):
        with patch.object(app, 'urlopen') as exchange:
            self.assertEqual(self.complete('unknown-state')[0], 400)
            exchange.assert_not_called()


if __name__ == '__main__':
    unittest.main()

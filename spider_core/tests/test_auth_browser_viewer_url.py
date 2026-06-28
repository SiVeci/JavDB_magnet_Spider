import os
import sys
import unittest
from unittest.mock import patch


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from auth_browser import main as auth_browser_main  # noqa: E402


class AuthBrowserViewerUrlTest(unittest.TestCase):
    def tearDown(self):
        auth_browser_main.sessions.clear()

    def test_viewer_url_is_empty_without_public_base_url(self):
        session_id = "s1"
        auth_browser_main.sessions[session_id] = {
            "status": "waiting_login",
            "created_at": 1,
            "expires_at": 2,
            "error": "",
            "error_code": "",
        }

        with patch.object(auth_browser_main, "PUBLIC_BASE_URL", ""), patch.object(
            auth_browser_main,
            "JAVDB_LOGIN_URL",
            "https://javdb.com/login",
        ):
            data = auth_browser_main._response(session_id)

        self.assertEqual(data["login_url"], "https://javdb.com/login")
        self.assertEqual(data["viewer_url"], "")

    def test_viewer_url_uses_public_base_url_when_configured(self):
        session_id = "s2"
        auth_browser_main.sessions[session_id] = {
            "status": "waiting_login",
            "created_at": 1,
            "expires_at": 2,
            "error": "",
            "error_code": "",
        }

        with patch.object(auth_browser_main, "PUBLIC_BASE_URL", "http://127.0.0.1:8090"):
            data = auth_browser_main._response(session_id)

        self.assertEqual(data["viewer_url"], "http://127.0.0.1:8090/sessions/s2/viewer")


if __name__ == "__main__":
    unittest.main()

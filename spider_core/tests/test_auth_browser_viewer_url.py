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

        with patch.object(auth_browser_main, "VNC_MODE", False), patch.object(
            auth_browser_main, "PUBLIC_BASE_URL", ""
        ), patch.object(
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

        with patch.object(auth_browser_main, "VNC_MODE", False), patch.object(
            auth_browser_main, "PUBLIC_BASE_URL", "http://127.0.0.1:8090"
        ):
            data = auth_browser_main._response(session_id)

        self.assertEqual(data["viewer_url"], "http://127.0.0.1:8090/sessions/s2/viewer")

    def test_viewer_url_uses_relative_novnc_path_in_vnc_mode(self):
        session_id = "s3"
        auth_browser_main.sessions[session_id] = {
            "status": "waiting_login",
            "created_at": 1,
            "expires_at": 2,
            "error": "",
            "error_code": "",
        }

        # VNC 模式优先于 PUBLIC_BASE_URL，返回相对 noVNC 入口供前端拼 origin。
        with patch.object(auth_browser_main, "VNC_MODE", True), patch.object(
            auth_browser_main, "PUBLIC_BASE_URL", "http://127.0.0.1:8090"
        ), patch.object(
            auth_browser_main,
            "VNC_VIEWER_PATH",
            "/auth-viewer/vnc.html?path=auth-viewer/websockify&autoconnect=true&resize=remote",
        ):
            data = auth_browser_main._response(session_id)

        self.assertTrue(data["viewer_url"].startswith("/auth-viewer/vnc.html"))
        self.assertIn("path=auth-viewer/websockify", data["viewer_url"])


if __name__ == "__main__":
    unittest.main()

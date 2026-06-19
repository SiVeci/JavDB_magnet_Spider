import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from storage_utils import (  # noqa: E402
    UnsafeFilenameError,
    atomic_write_json,
    get_safe_csv_path,
    normalize_csv_filename,
    read_json_file,
)


class FilenameValidationTest(unittest.TestCase):
    def test_normalizes_plain_csv_names(self):
        self.assertEqual(normalize_csv_filename("output"), "output.csv")
        self.assertEqual(normalize_csv_filename("天川空, 天川そら.csv"), "天川空, 天川そら.csv")

    def test_allows_empty_when_requested(self):
        self.assertEqual(normalize_csv_filename("", allow_empty=True), "")

    def test_rejects_path_traversal_and_absolute_paths(self):
        for value in ("../x.csv", "..\\x.csv", "/tmp/x.csv", "C:\\tmp\\x.csv"):
            with self.subTest(value=value):
                with self.assertRaises(UnsafeFilenameError):
                    normalize_csv_filename(value)

    def test_rejects_control_chars_and_hidden_names(self):
        for value in ("bad\nname.csv", ".secret.csv", ".."):
            with self.subTest(value=value):
                with self.assertRaises(UnsafeFilenameError):
                    normalize_csv_filename(value)

    def test_rejects_non_csv_suffixes(self):
        with self.assertRaises(UnsafeFilenameError):
            normalize_csv_filename("output.txt")

    def test_safe_path_stays_in_data_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path, safe_name = get_safe_csv_path(tmpdir, "safe")
            self.assertEqual(safe_name, "safe.csv")
            self.assertEqual(os.path.dirname(path), os.path.abspath(tmpdir))


class AtomicJsonTest(unittest.TestCase):
    def test_atomic_write_json_can_be_read_and_overwritten(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "state.json")
            atomic_write_json(path, {"state": "running"}, indent=2)
            self.assertEqual(read_json_file(path), {"state": "running"})

            atomic_write_json(path, {"state": "finished"}, indent=2)
            with open(path, "r", encoding="utf-8") as f:
                self.assertEqual(json.load(f), {"state": "finished"})

    def test_read_json_file_returns_default_for_invalid_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "broken.json")
            with open(path, "w", encoding="utf-8") as f:
                f.write("{not-json")

            self.assertEqual(read_json_file(path, default={"state": "idle"}), {"state": "idle"})


class AuthTokenTest(unittest.TestCase):
    def test_auth_disabled_without_env(self):
        with patch.dict(os.environ, {"JAVDB_AUTH_TOKEN": "", "JAVDB_AUTH_REQUIRED": ""}):
            import main

            self.assertFalse(main.is_auth_required())
            self.assertTrue(main.is_api_authorized(None))

    def test_auth_enabled_by_token(self):
        with patch.dict(os.environ, {"JAVDB_AUTH_TOKEN": "secret", "JAVDB_AUTH_REQUIRED": ""}):
            import main

            self.assertTrue(main.is_auth_required())
            self.assertTrue(main.is_api_authorized("secret"))
            self.assertFalse(main.is_api_authorized("wrong"))
            self.assertFalse(main.is_api_authorized(None))

    def test_required_without_token_rejects_all_api_tokens(self):
        with patch.dict(os.environ, {"JAVDB_AUTH_TOKEN": "", "JAVDB_AUTH_REQUIRED": "1"}):
            import main

            self.assertTrue(main.is_auth_required())
            self.assertFalse(main.is_api_authorized("anything"))

    def test_middleware_protects_private_api_but_allows_version(self):
        with patch.dict(os.environ, {"JAVDB_AUTH_TOKEN": "secret", "JAVDB_AUTH_REQUIRED": ""}):
            import main

            client = TestClient(main.app)
            self.assertEqual(client.get("/api/version").status_code, 200)
            self.assertEqual(client.get("/api/status").status_code, 401)
            self.assertEqual(client.get("/api/status", headers={"X-JavDB-Token": "wrong"}).status_code, 401)
            self.assertEqual(client.get("/api/status", headers={"X-JavDB-Token": "secret"}).status_code, 200)

    def test_ensure_zh_locale_preserves_query_params(self):
        import main

        url = main.ensure_zh_locale("https://javdb.com/actors/QDvG?page=3&sort_type=0&t=s%2Cd")
        self.assertIn("locale=zh", url)
        self.assertIn("page=3", url)
        self.assertIn("sort_type=0", url)
        self.assertIn("t=s%2Cd", url)


if __name__ == "__main__":
    unittest.main()

import csv
import io
import os
import tempfile
import unittest
from unittest.mock import patch

import sys


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import db_store  # noqa: E402
from schemas import TaskConfig  # noqa: E402
from services import auth_browser_service  # noqa: E402
from services import cookie_validation_service  # noqa: E402
from services import queue_service, task_service  # noqa: E402
import spider_engine  # noqa: E402


class MockResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code


class RuntimeConfigTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_store.configure(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_task_schema_no_longer_stores_runtime_config(self):
        with db_store.connect() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}
        self.assertNotIn("cookie", columns)
        self.assertNotIn("user_agent", columns)
        self.assertNotIn("proxies", columns)

    def test_cookie_persistence_follows_remember_flag(self):
        db_store.save_runtime_config(cookie="session-cookie", remember_cookie=False, user_agent="ua", proxies="proxy")
        self.assertEqual(db_store.get_runtime_config()["cookie"], "session-cookie")
        with db_store.connect() as conn:
            row = conn.execute("SELECT cookie, remember_cookie FROM runtime_config WHERE id = 1").fetchone()
        self.assertEqual(row["cookie"], "")
        self.assertEqual(row["remember_cookie"], 0)

        db_store.save_runtime_config(cookie="stored-cookie", remember_cookie=True, user_agent="ua", proxies="proxy")
        with db_store.connect() as conn:
            row = conn.execute("SELECT cookie, remember_cookie FROM runtime_config WHERE id = 1").fetchone()
        self.assertEqual(row["cookie"], "stored-cookie")
        self.assertEqual(row["remember_cookie"], 1)

    def test_empty_settings_save_preserves_session_cookie(self):
        db_store.save_runtime_config(
            cookie="session-cookie",
            remember_cookie=False,
            user_agent="ua",
            proxies="proxy",
            cookie_source="manual",
            cookie_status="valid",
        )

        db_store.save_runtime_config(cookie=None, remember_cookie=False, user_agent="ua2", proxies="")

        runtime = db_store.get_runtime_config()
        self.assertEqual(runtime["cookie"], "session-cookie")
        self.assertEqual(runtime["user_agent"], "ua2")
        self.assertEqual(runtime["cookie_source"], "manual")
        self.assertEqual(runtime["cookie_status"], "valid")

    def test_cookie_lifecycle_fields_are_saved_with_runtime_config(self):
        db_store.save_runtime_config(
            cookie="manual-cookie",
            remember_cookie=False,
            user_agent="ua",
            proxies="",
            cookie_source="manual",
            cookie_status="unverified",
        )

        runtime = db_store.get_runtime_config()
        self.assertEqual(runtime["cookie_source"], "manual")
        self.assertEqual(runtime["cookie_status"], "unverified")
        self.assertGreater(runtime["cookie_captured_at"], 0)
        self.assertEqual(runtime["cookie_validated_at"], 0)

    def test_task_cookie_failure_count_defaults_to_zero(self):
        task_id = db_store.create_task("https://javdb.com/actors/QDvG")

        task = db_store.get_task(task_id)

        self.assertEqual(task["task_cookie_failure_count"], 0)

    def test_missing_cookie_validation_updates_runtime_status(self):
        result = cookie_validation_service.validate_runtime_cookie(update_runtime=True)

        self.assertFalse(result["valid"])
        self.assertEqual(result["status"], "missing")
        runtime = db_store.get_runtime_config(include_cookie=False)
        self.assertEqual(runtime["cookie_status"], "missing")
        self.assertGreater(runtime["cookie_validated_at"], 0)

    def test_cloudflare_analytics_script_is_not_blocked(self):
        html = '<script src="https://static.cloudflareinsights.com/beacon.min.js"></script>'

        blocked = cookie_validation_service._is_blocked_response(MockResponse(html, 200), html)

        self.assertFalse(blocked)

    def test_cloudflare_challenge_markers_are_blocked(self):
        html = '<html><title>Just a moment...</title><div class="cf-challenge">Checking browser</div></html>'

        blocked = cookie_validation_service._is_blocked_response(MockResponse(html, 200), html)

        self.assertTrue(blocked)

    def test_auth_browser_login_saves_source_and_validation_status(self):
        def fake_validate(update_runtime=True):
            if update_runtime:
                db_store.update_cookie_validation_status("valid", 123, "")
            return {"valid": True, "status": "valid", "message": "ok", "validated_at": 123}

        class FakeJar:
            def items(self):
                return [("_jdb_session", "abc"), ("locale", "zh")]

        class FakeResp:
            status_code = 200
            url = "https://javdb.com/"
            text = "<html><body>logged in</body></html>"

        class FakeCurlSession:
            def __init__(self):
                self.cookies = FakeJar()
                self.headers = {"User-Agent": "ua"}

            def post(self, url, data=None, allow_redirects=True):
                return FakeResp()

            def get(self, url):
                # 登录成功后不应再取验证码，但保底返回空。
                class _R:
                    status_code = 200
                    content = b""
                    headers = {"content-type": "image/gif"}
                return _R()

        session_id = "s1"
        auth_browser_service._sessions[session_id] = {
            "curl_session": FakeCurlSession(),
            "token": "tok",
            "created_at": 1,
            "expires_at": auth_browser_service._now() + 600,
            "status": "waiting_login",
        }

        with patch.object(
            auth_browser_service,
            "validate_runtime_cookie",
            side_effect=fake_validate,
        ) as validate:
            data = auth_browser_service.submit_login(
                session_id, "user@example.com", "pw", "abcde", remember_cookie=False
            )

        validate.assert_called_once_with(update_runtime=True)
        self.assertTrue(data["has_cookie"])
        self.assertEqual(data["cookie_validation"]["status"], "valid")
        runtime = db_store.get_runtime_config()
        self.assertIn("_jdb_session=abc", runtime["cookie"])
        self.assertEqual(runtime["cookie_source"], "auth_browser")
        self.assertEqual(runtime["cookie_status"], "valid")
        self.assertEqual(runtime["cookie_validated_at"], 123)
        # 登录成功后会话应被销毁。
        self.assertNotIn(session_id, auth_browser_service._sessions)


class TaskEnqueueTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_store.configure(self.tmpdir.name)
        import main

        # import main 模块级会 configure(spider_core/data)，import 后再次 configure 确保 DB 隔离在 tempdir。
        db_store.configure(self.tmpdir.name)

        self.main = main
        self.old_data_dir = main.DATA_DIR
        main.DATA_DIR = self.tmpdir.name
        main.QUEUE_THREAD = None
        queue_service.QUEUE_THREAD = None
        db_store.save_runtime_config(cookie="cookie", remember_cookie=False, user_agent="ua", proxies="")

    def tearDown(self):
        self.main.DATA_DIR = self.old_data_dir
        self.main.QUEUE_THREAD = None
        queue_service.QUEUE_THREAD = None
        self.tmpdir.cleanup()

    def actor_html(self, actor_name="Actor Name"):
        return f"<html><body><h1 class='actor-section-name'>{actor_name}</h1></body></html>"

    def test_create_task_preprocesses_name_but_does_not_start_queue(self):
        with patch.object(task_service, "fetch_html", return_value=MockResponse(self.actor_html("Queue Actor"))):
            result = task_service.create_task_from_config(
                TaskConfig(start_url="https://javdb.com/actors/QDvG", user_agent="ua")
            )

        self.assertEqual(result["code"], 200)
        task = db_store.get_task(result["data"]["task_id"])
        self.assertEqual(task["state"], "pending")
        self.assertEqual(task["final_filename"], "Queue Actor.csv")
        self.assertFalse(queue_service.is_queue_running())

    def test_existing_collection_requires_mode_before_enqueue(self):
        db_store.ensure_collection("Queue Actor.csv")

        with patch.object(task_service, "fetch_html", return_value=MockResponse(self.actor_html("Queue Actor"))):
            with self.assertRaises(task_service.TaskConfigError) as ctx:
                task_service.create_task_from_config(
                    TaskConfig(start_url="https://javdb.com/actors/QDvG", user_agent="ua")
                )
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(db_store.list_tasks(), [])

        with patch.object(task_service, "fetch_html", return_value=MockResponse(self.actor_html("Queue Actor"))):
            result = task_service.create_task_from_config(
                TaskConfig(
                    start_url="https://javdb.com/actors/QDvG",
                    user_agent="ua",
                    crawl_mode="incremental",
                )
            )
        self.assertEqual(result["code"], 200)
        self.assertEqual(db_store.get_task(result["data"]["task_id"])["crawl_mode"], "incremental")

    def test_runtime_cookie_expiry_pauses_running_task(self):
        task_id = db_store.create_task("https://javdb.com/actors/QDvG", filename="out.csv")

        with patch.object(spider_engine, "fetch_html", return_value=MockResponse("<a href='/login'>login</a><input type='password'>", 401)):
            spider_engine.run_spider(
                "https://javdb.com/actors/QDvG",
                "cookie",
                "ua",
                "out.csv",
                task_id=task_id,
            )

        task = db_store.get_task(task_id)
        runtime = db_store.get_runtime_config(include_cookie=False)
        self.assertEqual(task["state"], "waiting_cookie")
        self.assertEqual(task["task_cookie_failure_count"], 1)
        self.assertEqual(runtime["cookie_status"], "expired")
        self.assertIn("登录态", task["error_message"])

    def test_runtime_network_error_pauses_without_invalidating_cookie(self):
        class FakeNetworkError(Exception):
            pass

        task_id = db_store.create_task("https://javdb.com/actors/QDvG", filename="out.csv")

        with patch.object(spider_engine, "fetch_html", side_effect=FakeNetworkError("proxy unavailable")):
            spider_engine.run_spider(
                "https://javdb.com/actors/QDvG",
                "cookie",
                "ua",
                "out.csv",
                task_id=task_id,
            )

        task = db_store.get_task(task_id)
        runtime = db_store.get_runtime_config(include_cookie=False)
        self.assertEqual(task["state"], "waiting_cookie")
        self.assertEqual(task["task_cookie_failure_count"], 1)
        self.assertEqual(runtime["cookie_status"], "network_error")
        self.assertIn("网络或代理", runtime["cookie_last_error"])

    def test_resume_resets_cookie_failure_count_after_validation(self):
        task_id = db_store.create_task("https://javdb.com/actors/QDvG", filename="out.csv")
        db_store.update_task_status(task_id, state="waiting_cookie", task_cookie_failure_count=2)

        with patch("routers.tasks.ensure_queue_worker", return_value=None), patch("routers.tasks.validate_runtime_cookie", return_value={"valid": True, "status": "valid", "message": "ok"}):
            from routers.tasks import resume_task_by_id

            result = resume_task_by_id(task_id)

        self.assertEqual(result["code"], 200)
        task = db_store.get_task(task_id)
        self.assertEqual(task["state"], "pending")
        self.assertEqual(task["task_cookie_failure_count"], 0)


class MagnetSelectionTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_store.configure(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_select_magnet_updates_exported_csv_and_copy_links(self):
        db_store.ensure_collection("output.csv")
        movie = {"code": "ABC-001", "title": "Title", "url": "https://example.test/v/1"}
        first = {"name": "first.torrent", "link": "magnet:?xt=urn:btih:first", "rank": 1, "date": "2026-01-01", "size_mb": 100}
        second = {"name": "second.torrent", "link": "magnet:?xt=urn:btih:second", "rank": 2, "date": "2026-01-02", "size_mb": 200}
        db_store.save_movie_result("output.csv", movie, first, [first, second])

        movies = db_store.get_collection_movies("output.csv")["movies"]
        magnets = db_store.get_movie_magnets(movies[0]["id"])
        second_id = next(row["id"] for row in magnets if row["link"] == second["link"])

        self.assertTrue(db_store.select_movie_magnet(movies[0]["id"], second_id))
        self.assertEqual(db_store.get_magnet_links("output.csv"), [second["link"]])

        csv_bytes, _ = db_store.export_collection_to_csv_bytes("output.csv")
        rows = list(csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig"))))
        self.assertEqual(rows[0][db_store.CSV_FIELDNAMES[3]], second["name"])
        self.assertEqual(rows[0][db_store.CSV_FIELDNAMES[4]], second["link"])

    def test_check_result_selects_viable_highest_score(self):
        db_store.ensure_collection("output.csv")
        movie = {"code": "ABC-001", "title": "Title", "url": "https://example.test/v/1"}
        first = {"name": "first.torrent", "link": "magnet:?xt=urn:btih:first", "rank": 300, "date": "2026-01-01", "size_mb": 100}
        second = {"name": "second.torrent", "link": "magnet:?xt=urn:btih:second", "rank": 250, "date": "2026-01-02", "size_mb": 200}
        db_store.save_movie_result("output.csv", movie, first, [first, second])
        movie_id = db_store.get_collection_movies("output.csv")["movies"][0]["id"]
        magnets = db_store.get_movie_magnets(movie_id)
        first_id = next(row["id"] for row in magnets if row["link"] == first["link"])
        second_id = next(row["id"] for row in magnets if row["link"] == second["link"])

        db_store.update_magnet_check_result(first_id, "dead", 0, 0)
        db_store.update_magnet_check_result(second_id, "weak", 0, 3)

        selected = next(row for row in db_store.get_movie_magnets(movie_id) if row["is_selected"])
        self.assertEqual(selected["link"], second["link"])
        self.assertEqual(db_store.get_magnet_links("output.csv"), [second["link"]])

    def test_all_failed_or_dead_selects_penalized_highest_score(self):
        db_store.ensure_collection("output.csv")
        movie = {"code": "ABC-001", "title": "Title", "url": "https://example.test/v/1"}
        first = {"name": "first.torrent", "link": "magnet:?xt=urn:btih:first", "rank": 300, "date": "2026-01-01", "size_mb": 100}
        second = {"name": "second.torrent", "link": "magnet:?xt=urn:btih:second", "rank": 250, "date": "2026-01-02", "size_mb": 200}
        db_store.save_movie_result("output.csv", movie, first, [first, second])
        movie_id = db_store.get_collection_movies("output.csv")["movies"][0]["id"]
        magnets = db_store.get_movie_magnets(movie_id)
        first_id = next(row["id"] for row in magnets if row["link"] == first["link"])
        second_id = next(row["id"] for row in magnets if row["link"] == second["link"])

        db_store.update_magnet_check_result(first_id, "dead", 0, 0)
        db_store.update_magnet_check_result(first_id, "dead", 0, 0)
        db_store.update_magnet_check_result(second_id, None, 0, 0, "检测超时")

        rows = db_store.get_movie_magnets(movie_id)
        selected = next(row for row in rows if row["is_selected"])
        self.assertEqual(selected["link"], first["link"])
        self.assertEqual(selected["priority_score"], 100)
        self.assertEqual(next(row for row in rows if row["id"] == second_id)["priority_score"], 50)


if __name__ == "__main__":
    unittest.main()


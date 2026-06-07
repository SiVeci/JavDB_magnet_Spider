import csv
import io
import os
import tempfile
import unittest
from unittest.mock import patch

import sys


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import db_store  # noqa: E402


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


class TaskEnqueueTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_store.configure(self.tmpdir.name)
        import main

        self.main = main
        self.old_data_dir = main.DATA_DIR
        main.DATA_DIR = self.tmpdir.name
        main.QUEUE_THREAD = None
        db_store.save_runtime_config(cookie="cookie", remember_cookie=False, user_agent="ua", proxies="")

    def tearDown(self):
        self.main.DATA_DIR = self.old_data_dir
        self.main.QUEUE_THREAD = None
        self.tmpdir.cleanup()

    def actor_html(self, actor_name="Actor Name"):
        return f"<html><body><h1 class='actor-section-name'>{actor_name}</h1></body></html>"

    def test_create_task_preprocesses_name_but_does_not_start_queue(self):
        with patch.object(self.main, "fetch_html", return_value=MockResponse(self.actor_html("Queue Actor"))):
            result = self.main.create_task_from_config(
                self.main.TaskConfig(start_url="https://javdb.com/actors/QDvG", user_agent="ua")
            )

        self.assertEqual(result["code"], 200)
        task = db_store.get_task(result["task_id"])
        self.assertEqual(task["state"], "pending")
        self.assertEqual(task["final_filename"], "Queue Actor.csv")
        self.assertFalse(self.main.is_queue_running())

    def test_existing_collection_requires_mode_before_enqueue(self):
        db_store.ensure_collection("Queue Actor.csv")

        with patch.object(self.main, "fetch_html", return_value=MockResponse(self.actor_html("Queue Actor"))):
            response = self.main.create_task_from_config(
                self.main.TaskConfig(start_url="https://javdb.com/actors/QDvG", user_agent="ua")
            )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(db_store.list_tasks(), [])

        with patch.object(self.main, "fetch_html", return_value=MockResponse(self.actor_html("Queue Actor"))):
            result = self.main.create_task_from_config(
                self.main.TaskConfig(
                    start_url="https://javdb.com/actors/QDvG",
                    user_agent="ua",
                    crawl_mode="incremental",
                )
            )
        self.assertEqual(result["code"], 200)
        self.assertEqual(db_store.get_task(result["task_id"])["crawl_mode"], "incremental")


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

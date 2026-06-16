import os
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db_store  # noqa: E402
from ranking_utils import parse_ranking_url, ranking_url  # noqa: E402


class MockResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code


class RankingDataSplitTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_store.configure(self.tmpdir.name)
        import main

        db_store.configure(self.tmpdir.name)
        self.main = main
        self.old_data_dir = main.DATA_DIR
        main.DATA_DIR = self.tmpdir.name
        main.QUEUE_THREAD = None
        db_store.save_runtime_config(cookie="cookie", remember_cookie=False, user_agent="ua", proxies="")
        self.client = TestClient(main.app)

    def tearDown(self):
        self.main.DATA_DIR = self.old_data_dir
        self.main.QUEUE_THREAD = None
        db_store.configure(self.old_data_dir)
        self.tmpdir.cleanup()

    def seed_ranking_movie(self):
        url = ranking_url("censored", "daily")
        db_store.ensure_collection("ranking_censored_daily.csv", url, "ranking", "censored", "daily")
        best = {
            "name": "rank.torrent",
            "link": "magnet:?xt=urn:btih:rank",
            "rank": 100,
            "date": "2026-01-01",
            "size_mb": 2048,
        }
        db_store.save_movie_result(
            "ranking_censored_daily.csv",
            {"code": "RANK-001", "title": "Ranked Movie", "url": "https://example.test/v/rank", "tags": ["tag-a"]},
            best,
            [best],
        )

    def test_parse_ranking_url_detects_category_and_period(self):
        meta = parse_ranking_url("https://javdb.com/rankings/movies?locale=zh&t=fc2&p=weekly")
        self.assertIsNotNone(meta)
        self.assertEqual(meta["collection_type"], "ranking")
        self.assertEqual(meta["ranking_category"], "fc2")
        self.assertEqual(meta["ranking_period"], "weekly")
        self.assertEqual(meta["filename"], "ranking_fc2_weekly.csv")

    def test_legacy_source_url_collection_migrates_to_ranking(self):
        now = time.time()
        with db_store.connect() as conn:
            conn.execute(
                """
                INSERT INTO collections(
                    filename, source_url, collection_type, ranking_category, ranking_period,
                    tags_json, created_at, updated_at
                )
                VALUES (?, ?, 'actor', '', '', '[]', ?, ?)
                """,
                ("legacy-ranking.csv", ranking_url("western", "monthly"), now, now),
            )

        db_store._migrate_collection_type_columns()

        self.assertEqual(db_store.get_history(), [])
        self.assertEqual(db_store.get_ranking_collection_filename("western", "monthly"), "legacy-ranking.csv")

    def test_actor_history_excludes_ranking_collections(self):
        db_store.ensure_collection("actor.csv")
        db_store.ensure_collection("ranking_censored_daily.csv", ranking_url("censored", "daily"), "ranking", "censored", "daily")

        r = self.client.get("/api/history")
        self.assertEqual(r.status_code, 200)
        names = [item["name"] for item in r.json()["data"]]
        self.assertEqual(names, ["actor.csv"])

    def test_ranking_movies_endpoint_returns_collection_movie_shape(self):
        self.seed_ranking_movie()

        r = self.client.get("/api/rankings/censored/daily/movies")
        self.assertEqual(r.status_code, 200)
        data = r.json()["data"]
        self.assertEqual(data["total_count"], 1)
        self.assertEqual(data["available_tags"], ["tag-a"])
        self.assertEqual(data["movies"][0]["code"], "RANK-001")
        self.assertEqual(data["movies"][0]["candidate_count"], 1)

    def test_update_ranking_endpoint_creates_overwrite_task(self):
        with patch.object(self.main, "fetch_html", return_value=MockResponse("<html></html>")):
            r = self.client.post("/api/rankings/censored/daily/update")

        self.assertEqual(r.status_code, 200)
        payload = r.json()
        self.assertEqual(payload["code"], 200)
        task = db_store.get_task(payload["task_id"])
        self.assertEqual(task["final_filename"], "ranking_censored_daily.csv")
        self.assertEqual(task["crawl_mode"], "overwrite")
        self.assertEqual(task["collection_type"], "ranking")
        self.assertEqual(task["ranking_category"], "censored")
        self.assertEqual(task["ranking_period"], "daily")


if __name__ == "__main__":
    unittest.main(verbosity=2)

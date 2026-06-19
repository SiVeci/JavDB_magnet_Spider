import os
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db_store  # noqa: E402
from ranking_utils import parse_ranking_url, parse_top250_options, ranking_url  # noqa: E402
from services import queue_service, task_service  # noqa: E402


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
        queue_service.QUEUE_THREAD = None
        db_store.save_runtime_config(cookie="cookie", remember_cookie=False, user_agent="ua", proxies="")
        self.client = TestClient(main.app)

    def tearDown(self):
        self.main.DATA_DIR = self.old_data_dir
        self.main.QUEUE_THREAD = None
        queue_service.QUEUE_THREAD = None
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

    def test_playback_ranking_url_uses_playback_path(self):
        self.assertEqual(ranking_url("playback", "daily"), "https://javdb.com/rankings/playback?p=daily")

    def test_parse_playback_ranking_url_ignores_t_param(self):
        meta = parse_ranking_url("https://javdb.com/rankings/playback?locale=zh&p=monthly&t=high_score")
        self.assertIsNotNone(meta)
        self.assertEqual(meta["collection_type"], "ranking")
        self.assertEqual(meta["ranking_category"], "playback")
        self.assertEqual(meta["ranking_period"], "monthly")
        self.assertEqual(meta["filename"], "ranking_playback_monthly.csv")

    def test_top250_ranking_url_uses_top_path(self):
        self.assertEqual(ranking_url("top250", "all"), "https://javdb.com/rankings/top")
        self.assertEqual(ranking_url("top250", "0"), "https://javdb.com/rankings/top?t=0")
        self.assertEqual(ranking_url("top250", "y2026"), "https://javdb.com/rankings/top?t=y2026")

    def test_parse_top250_ranking_url_detects_option(self):
        meta = parse_ranking_url("https://javdb.com/rankings/top?locale=zh&t=y2026")
        self.assertIsNotNone(meta)
        self.assertEqual(meta["collection_type"], "ranking")
        self.assertEqual(meta["ranking_category"], "top250")
        self.assertEqual(meta["ranking_period"], "y2026")
        self.assertEqual(meta["filename"], "ranking_top250_y2026.csv")

        all_meta = parse_ranking_url("https://javdb.com/rankings/top")
        self.assertIsNotNone(all_meta)
        self.assertEqual(all_meta["ranking_period"], "all")

    def test_parse_top250_options_from_select(self):
        html = """
        <select name="t" id="t" data-url="/rankings/top?t=%25s">
            <option value="">全部</option>
            <option value="0">有碼</option>
            <option value="y2026">2026</option>
        </select>
        """
        self.assertEqual(
            parse_top250_options(html),
            [
                {"key": "all", "label": "全部"},
                {"key": "0", "label": "有碼"},
                {"key": "y2026", "label": "2026"},
            ],
        )

    def test_top250_options_api_caches_remote_options(self):
        html = """
        <select name="t" id="t">
            <option value="">全部</option>
            <option value="y2026">2026</option>
        </select>
        """
        with patch("routers.rankings.fetch_html", return_value=MockResponse(html)):
            r = self.client.get("/api/rankings/top250/options?refresh=1")

        self.assertEqual(r.status_code, 200)
        payload = r.json()
        self.assertEqual(payload["code"], 200)
        self.assertEqual(payload["data"]["options"][0], {"key": "all", "label": "全部"})
        cached = db_store.get_ranking_option_cache("top250")
        self.assertEqual(cached["options"][1], {"key": "y2026", "label": "2026"})

    def test_top250_options_api_does_not_fetch_without_refresh(self):
        with patch("routers.rankings.fetch_html", side_effect=AssertionError("should not fetch")):
            r = self.client.get("/api/rankings/top250/options")

        self.assertEqual(r.status_code, 200)
        payload = r.json()
        self.assertEqual(payload["code"], 200)
        self.assertEqual(payload["data"]["options"], [])

    def test_top250_options_api_merges_local_collections(self):
        db_store.save_ranking_option_cache("top250", [{"key": "all", "label": "全部"}], "https://javdb.com/rankings/top")
        db_store.ensure_collection(
            "ranking_top250_y2026.csv",
            ranking_url("top250", "y2026"),
            "ranking",
            "top250",
            "y2026",
        )

        r = self.client.get("/api/rankings/top250/options")

        self.assertEqual(r.status_code, 200)
        keys = [item["key"] for item in r.json()["data"]["options"]]
        self.assertEqual(keys, ["all", "y2026"])

    def test_top250_options_api_reports_auth_error(self):
        with patch("routers.rankings.fetch_html", return_value=MockResponse("", 403)):
            r = self.client.get("/api/rankings/top250/options?refresh=1")

        self.assertEqual(r.status_code, 502)
        payload = r.json()
        self.assertEqual(payload["error_type"], "auth")
        self.assertIn("Cookie", payload["msg"])

    def test_top250_options_api_reports_network_error(self):
        with patch("routers.rankings.fetch_html", side_effect=TimeoutError("timeout")):
            r = self.client.get("/api/rankings/top250/options?refresh=1")

        self.assertEqual(r.status_code, 502)
        payload = r.json()
        self.assertEqual(payload["error_type"], "network")
        self.assertIn("网络", payload["msg"])

    def test_top250_options_api_reports_parse_error(self):
        with patch("routers.rankings.fetch_html", return_value=MockResponse("<html></html>")):
            r = self.client.get("/api/rankings/top250/options?refresh=1")

        self.assertEqual(r.status_code, 502)
        payload = r.json()
        self.assertEqual(payload["error_type"], "parse")
        self.assertIn("未找到", payload["msg"])

    def test_top250_options_api_uses_cache_after_refresh_error(self):
        db_store.save_ranking_option_cache("top250", [{"key": "all", "label": "全部"}], "https://javdb.com/rankings/top")
        with patch("routers.rankings.fetch_html", return_value=MockResponse("", 403)):
            r = self.client.get("/api/rankings/top250/options?refresh=1")

        self.assertEqual(r.status_code, 200)
        payload = r.json()
        self.assertEqual(payload["data"]["error_type"], "auth")
        self.assertTrue(payload["data"]["stale"])
        self.assertEqual(payload["data"]["options"], [{"key": "all", "label": "全部"}])

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
        with patch.object(task_service, "fetch_html", return_value=MockResponse("<html></html>")):
            r = self.client.post("/api/rankings/censored/daily/update")

        self.assertEqual(r.status_code, 200)
        payload = r.json()
        self.assertEqual(payload["code"], 200)
        task = db_store.get_task(payload["data"]["task_id"])
        self.assertEqual(task["final_filename"], "ranking_censored_daily.csv")
        self.assertEqual(task["crawl_mode"], "overwrite")
        self.assertEqual(task["collection_type"], "ranking")
        self.assertEqual(task["ranking_category"], "censored")
        self.assertEqual(task["ranking_period"], "daily")

    def test_update_playback_ranking_endpoint_creates_overwrite_task(self):
        with patch.object(task_service, "fetch_html", return_value=MockResponse("<html></html>")):
            r = self.client.post("/api/rankings/playback/daily/update")

        self.assertEqual(r.status_code, 200)
        payload = r.json()
        self.assertEqual(payload["code"], 200)
        task = db_store.get_task(payload["data"]["task_id"])
        self.assertEqual(task["start_url"], "https://javdb.com/rankings/playback?p=daily&locale=zh")
        self.assertEqual(task["final_filename"], "ranking_playback_daily.csv")
        self.assertEqual(task["crawl_mode"], "overwrite")
        self.assertEqual(task["collection_type"], "ranking")
        self.assertEqual(task["ranking_category"], "playback")
        self.assertEqual(task["ranking_period"], "daily")

    def test_update_top250_ranking_endpoint_creates_overwrite_task(self):
        with patch.object(task_service, "fetch_html", return_value=MockResponse("<html></html>")):
            r = self.client.post("/api/rankings/top250/y2026/update")

        self.assertEqual(r.status_code, 200)
        payload = r.json()
        self.assertEqual(payload["code"], 200)
        task = db_store.get_task(payload["data"]["task_id"])
        self.assertEqual(task["start_url"], "https://javdb.com/rankings/top?t=y2026&locale=zh")
        self.assertEqual(task["final_filename"], "ranking_top250_y2026.csv")
        self.assertEqual(task["crawl_mode"], "overwrite")
        self.assertEqual(task["collection_type"], "ranking")
        self.assertEqual(task["ranking_category"], "top250")
        self.assertEqual(task["ranking_period"], "y2026")


if __name__ == "__main__":
    unittest.main(verbosity=2)


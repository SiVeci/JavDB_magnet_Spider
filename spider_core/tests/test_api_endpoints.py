"""test_api_endpoints — API 端点集成测试（TestClient）。

覆盖原先零覆盖的 HTTP 层：
- 只读端点（history / collections / movies / magnets / version / runtime_config）
- 错误码路径（404 / 400 / 502）
- CSV 导出与 tags / exclude_tags 过滤
- 本轮新增的网络异常收口（create_task / get_tags TLS 失败 → 502）

数据准备走 db_store 公共接口（与 test_v14 一致），不依赖网络。
鉴权默认关闭（不设 JAVDB_AUTH_* 环境变量），故无需带 token。
"""

import csv
import io
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db_store  # noqa: E402
from services import task_service  # noqa: E402


def _seed_collection():
    """建一个含 2 部影片的集合，标签各异，便于过滤测试。"""
    db_store.ensure_collection("api.csv")
    m1_best = {"name": "a.torrent", "link": "magnet:?xt=urn:btih:aaa", "rank": 100, "date": "2026-01-01", "size_mb": 1024}
    db_store.save_movie_result(
        "api.csv",
        {"code": "API-001", "title": "标题一", "url": "https://example.test/v/1", "tags": ["巨乳", "中出"]},
        m1_best, [m1_best],
    )
    m2_best = {"name": "b.torrent", "link": "magnet:?xt=urn:btih:bbb", "rank": 90, "date": "2026-01-02", "size_mb": 2048}
    db_store.save_movie_result(
        "api.csv",
        {"code": "API-002", "title": "标题二", "url": "https://example.test/v/2", "tags": ["巨乳", "騎乘"]},
        m2_best, [m2_best],
    )


class ApiEndpointTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_store.configure(self.tmpdir.name)
        import main
        # import main 模块级会 configure(spider_core/data)，import 后再次 configure 确保 DB 隔离在 tempdir。
        db_store.configure(self.tmpdir.name)
        self.main = main
        self.old_data_dir = main.DATA_DIR
        main.DATA_DIR = self.tmpdir.name
        self.client = TestClient(main.app)
        _seed_collection()

    def tearDown(self):
        self.main.DATA_DIR = self.old_data_dir
        self.tmpdir.cleanup()

    # ---------- 只读端点 ----------
    def test_version_is_public(self):
        r = self.client.get("/api/version")
        self.assertEqual(r.status_code, 200)
        self.assertIn("version", r.json()["data"])

    def test_history_lists_collection(self):
        r = self.client.get("/api/history")
        self.assertEqual(r.status_code, 200)
        data = r.json()["data"]
        self.assertEqual(data[0]["name"], "api.csv")
        self.assertEqual(data[0]["count"], 2)

    def test_runtime_config_roundtrip(self):
        r = self.client.post("/api/runtime_config", json={
            "cookie": "", "remember_cookie": False, "user_agent": "UA/1", "proxies": "", "trackers": ["udp://t:80"]
        })
        self.assertEqual(r.json()["code"], 200)
        got = self.client.get("/api/runtime_config").json()["data"]
        self.assertEqual(got["user_agent"], "UA/1")
        self.assertEqual(got["trackers"], ["udp://t:80"])
        self.assertNotIn("cookie", got)  # include_cookie=False

    def test_collection_movies_and_magnets(self):
        movies = self.client.get("/api/collections/api.csv/movies").json()["data"]["movies"]
        self.assertEqual(len(movies), 2)
        mid = movies[0]["id"]
        mags = self.client.get(f"/api/movies/{mid}/magnets").json()["data"]
        self.assertTrue(any(m["link"].startswith("magnet:") for m in mags))

    # ---------- 错误码路径 ----------
    def test_collection_movies_404(self):
        r = self.client.get("/api/collections/nope.csv/movies")
        self.assertEqual(r.status_code, 404)
        self.assertEqual(r.json()["code"], 404)

    def test_magnets_missing_name_400(self):
        r = self.client.get("/api/magnets")
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()["code"], 400)

    def test_download_unknown_404(self):
        r = self.client.get("/api/download", params={"name": "nope.csv"})
        self.assertEqual(r.status_code, 404)

    def test_delete_all_fail_returns_400(self):
        r = self.client.post("/api/delete", json={"filenames": ["nope.csv"]})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()["code"], 400)

    def test_missing_frontend_responses_do_not_expose_paths(self):
        with patch.object(self.main.os.path, "exists", return_value=False):
            root = self.main.read_root()
            favicon = self.main.get_favicon()

        self.assertEqual(root.status_code, 404)
        self.assertNotIn(self.tmpdir.name, root.body.decode("utf-8"))
        self.assertNotIn("frontend", root.body.decode("utf-8"))
        self.assertEqual(favicon.status_code, 404)
        self.assertNotIn("frontend", favicon.body.decode("utf-8"))

    # ---------- 导出 + tags / exclude_tags ----------
    def test_magnet_links_filter_by_tags(self):
        # 仅 API-001 含「中出」
        both = self.client.get("/api/magnets", params={"name": "api.csv"}).json()["data"]
        self.assertEqual(len(both), 2)
        only1 = self.client.get("/api/magnets", params={"name": "api.csv", "tags": "中出"}).json()["data"]
        self.assertEqual(only1, ["magnet:?xt=urn:btih:aaa"])

    def test_magnet_links_exclude_tags(self):
        # 排除「中出」应只剩 API-002
        rest = self.client.get("/api/magnets", params={"name": "api.csv", "exclude_tags": "中出"}).json()["data"]
        self.assertEqual(rest, ["magnet:?xt=urn:btih:bbb"])

    def test_download_csv_contents_and_exclude(self):
        r = self.client.get("/api/download", params={"name": "api.csv"})
        self.assertEqual(r.status_code, 200)
        self.assertIn("filename*=UTF-8''api.csv", r.headers["content-disposition"])
        rows = list(csv.DictReader(io.StringIO(r.content.decode("utf-8-sig"))))
        self.assertEqual({row[db_store.CSV_FIELDNAMES[0]] for row in rows}, {"API-001", "API-002"})

        # exclude_tags=巨乳 → 两部都含巨乳 → 0 行
        r2 = self.client.get("/api/download", params={"name": "api.csv", "exclude_tags": "巨乳"})
        rows2 = list(csv.DictReader(io.StringIO(r2.content.decode("utf-8-sig"))))
        self.assertEqual(len(rows2), 0)

    # ---------- 网络异常收口回归 ----------
    def test_create_task_network_failure_returns_502(self):
        class FakeTLSError(Exception):
            pass
        with patch.object(task_service, "fetch_html", side_effect=FakeTLSError("TLS connect error")), \
             patch.object(task_service, "get_runtime_for_request", return_value={"cookie": "x", "user_agent": "ua", "proxies": ""}), \
             patch.object(task_service, "save_runtime_from_payload", return_value=None):
            r = self.client.post("/api/tasks", json={"start_url": "https://javdb.com/actors/x"})
        self.assertEqual(r.status_code, 502)
        self.assertEqual(r.json()["code"], 502)

    def test_get_tags_network_failure_returns_502(self):
        from routers import settings as st

        class FakeTLSError(Exception):
            pass
        with patch.object(st, "fetch_html", side_effect=FakeTLSError("TLS connect error")), \
             patch.object(st, "get_runtime_for_request", return_value={"cookie": "x", "user_agent": "ua", "proxies": ""}):
            r = self.client.post("/api/get_tags", json={"url": "https://javdb.com/actors/x"})
        self.assertEqual(r.status_code, 502)
        self.assertEqual(r.json()["code"], 502)


if __name__ == "__main__":
    unittest.main(verbosity=2)

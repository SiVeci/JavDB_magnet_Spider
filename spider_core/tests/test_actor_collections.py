import os
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import db_store  # noqa: E402
from bs4 import BeautifulSoup  # noqa: E402
from ranking_utils import ranking_url  # noqa: E402
from routers.actors import build_actor_crawl_url  # noqa: E402
from services import actor_collection_service as svc  # noqa: E402


class _Resp:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code


def _card(actor_id, name):
    """构造真实的 .actor-box 卡片：演员主链接 + 删除按钮（均为 /actors/ 链接）。"""
    return (
        f'<div class="box actor-box" id="actor-{actor_id}">'
        f'<a href="/actors/{actor_id}" title="{name} 别名">'
        f'<figure class="image"><img class="avatar" src="x.jpg"></figure>'
        f'<strong>{name}</strong></a>'
        f'<a class="button is-danger" data-method="post" href="/actors/{actor_id}/uncollect">刪除</a>'
        f'</div>'
    )


def _page(cards_html, nav=True):
    """包裹成收藏演员页：#actors 容器内放卡片，容器外放分类导航链接。"""
    nav_html = (
        '<div class="toolbar">'
        '<a href="/actors/censored">有碼</a>'
        '<a href="/actors/uncensored">無碼</a>'
        '<a href="/actors/western">歐美</a>'
        '</div>'
        if nav
        else ''
    )
    return f'<html><body>{nav_html}<div id="actors" class="actors">{cards_html}</div></body></html>'


class ActorSchemaTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_store.configure(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _collection_actor_id(self, filename):
        with db_store.connect() as conn:
            row = conn.execute("SELECT actor_id FROM collections WHERE filename = ?", (filename,)).fetchone()
        return row["actor_id"] if row else None

    def test_table_and_column_created(self):
        with db_store.connect() as conn:
            cols = [r["name"] for r in conn.execute("PRAGMA table_info(collections)").fetchall()]
            self.assertIn("actor_id", cols)
            tbl = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='collection_actors'"
            ).fetchone()
            self.assertTrue(tbl)

    def test_collection_id_records_actor_id_from_source_url(self):
        db_store.ensure_collection("yui.csv", "https://javdb.com/actors/ABC123?locale=zh&t=1", "actor")
        self.assertEqual(self._collection_actor_id("yui.csv"), "ABC123")

    def test_actor_id_preserved_on_empty_source_update(self):
        db_store.ensure_collection("yui.csv", "https://javdb.com/actors/ABC123", "actor")
        # 后续无 source_url 的写入（如 save_movie_result 内部）不应清空 actor_id。
        db_store.ensure_collection("yui.csv")
        self.assertEqual(self._collection_actor_id("yui.csv"), "ABC123")

    def test_ranking_collection_has_no_actor_id(self):
        db_store.ensure_collection(
            "ranking_censored_daily.csv", ranking_url("censored", "daily"), "ranking", "censored", "daily"
        )
        self.assertEqual(self._collection_actor_id("ranking_censored_daily.csv"), "")

    def test_lookup_by_actor_id(self):
        db_store.ensure_collection("yui.csv", "https://javdb.com/actors/ABC123", "actor")
        self.assertEqual(db_store.get_actor_collection_filename_by_actor_id("ABC123"), "yui.csv")
        self.assertEqual(db_store.get_actor_collection_filename_by_actor_id("NOPE"), "")

    def test_migration_backfills_actor_id(self):
        now = time.time()
        with db_store.connect() as conn:
            conn.execute(
                """
                INSERT INTO collections(
                    filename, source_url, collection_type, ranking_category, ranking_period,
                    actor_id, tags_json, created_at, updated_at
                )
                VALUES (?, ?, 'actor', '', '', '', '[]', ?, ?)
                """,
                ("legacy.csv", "https://javdb.com/actors/LEG999?x=1", now, now),
            )
        db_store._migrate_actor_id_column()
        self.assertEqual(self._collection_actor_id("legacy.csv"), "LEG999")


class ActorSnapshotTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_store.configure(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _actor(self, aid, name):
        return {"actor_id": aid, "actor_name": name, "actor_url": f"https://javdb.com/actors/{aid}"}

    def test_first_seen_category_wins_and_dedup(self):
        db_store.replace_category_snapshot("g0t0", "u0", [self._actor("A1", "甲")])
        db_store.replace_category_snapshot("g0t2", "u2", [self._actor("A1", "甲改名")])
        rows = db_store.list_collection_actors()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["category"], "g0t0")  # 首见分类保留
        self.assertEqual(rows[0]["actor_name"], "甲改名")  # 名称随刷新更新

    def test_last_task_tags_preserved_across_refresh(self):
        db_store.replace_category_snapshot("g0t0", "u0", [self._actor("A1", "甲")])
        db_store.set_actor_last_task_tags("A1", [{"name": "巨乳", "value": "3"}])
        db_store.replace_category_snapshot("g0t0", "u0", [self._actor("A1", "甲")])
        rows = db_store.list_collection_actors()
        self.assertEqual(rows[0]["last_task_tags"], [{"name": "巨乳", "value": "3"}])

    def test_unfavorited_actor_removed_on_refresh(self):
        db_store.replace_category_snapshot("g0t0", "u0", [self._actor("A1", "甲"), self._actor("A2", "乙")])
        db_store.replace_category_snapshot("g0t0", "u0", [self._actor("A1", "甲")])
        ids = {r["actor_id"] for r in db_store.list_collection_actors()}
        self.assertEqual(ids, {"A1"})

    def test_has_collection_matched_by_actor_id(self):
        db_store.replace_category_snapshot("g0t0", "u0", [self._actor("A1", "甲")])
        db_store.ensure_collection("jia.csv", "https://javdb.com/actors/A1", "actor")
        row = next(r for r in db_store.list_collection_actors() if r["actor_id"] == "A1")
        self.assertTrue(row["has_collection"])
        self.assertEqual(row["collection_filename"], "jia.csv")


class ActorParseTest(unittest.TestCase):
    def test_name_from_strong_not_title(self):
        soup = BeautifulSoup(_page(_card("k4MbY", "小松空")), "html.parser")
        cards = svc.parse_actor_cards(soup)
        self.assertEqual(cards, [
            {"actor_id": "k4MbY", "actor_name": "小松空", "actor_url": "https://javdb.com/actors/k4MbY"}
        ])

    def test_nav_and_uncollect_links_ignored(self):
        # 容器外的分类导航链接与卡片内的删除按钮都不应进入结果（也不应误报）。
        page = _page(_card("k4MbY", "小松空") + _card("k470e", "林芽依"), nav=True)
        soup = BeautifulSoup(page, "html.parser")
        ids = [c["actor_id"] for c in svc.parse_actor_cards(soup)]
        self.assertEqual(ids, ["k4MbY", "k470e"])
        self.assertNotIn("censored", ids)
        self.assertNotIn("uncensored", ids)

    def test_missing_strong_skipped_no_title_fallback(self):
        html = _page('<div class="actor-box"><a href="/actors/xx" title="标题名"><figure></figure></a></div>')
        soup = BeautifulSoup(html, "html.parser")
        self.assertEqual(svc.parse_actor_cards(soup), [])

    def test_name_whitespace_cleaned(self):
        html = _page('<div class="actor-box"><a href="/actors/z9"><strong>  小松\n  空 </strong></a></div>')
        soup = BeautifulSoup(html, "html.parser")
        self.assertEqual(svc.parse_actor_cards(soup)[0]["actor_name"], "小松 空")

    def test_dedup_by_actor_id(self):
        soup = BeautifulSoup(_page(_card("dup", "甲") + _card("dup", "甲")), "html.parser")
        self.assertEqual(len(svc.parse_actor_cards(soup)), 1)

    def test_max_page_parsing_and_entity_decode(self):
        html = (
            '<ul class="pagination-list">'
            '<li><a class="pagination-link is-current" href="/users/collection_actors?g=0&amp;t=0">1</a></li>'
            '<li><a rel="next" class="pagination-link" href="/users/collection_actors?g=0&amp;t=0&amp;page=2">2</a></li>'
            '<li><a class="pagination-link" href="/users/collection_actors?g=0&amp;t=0&amp;page=7">7</a></li>'
            '</ul>'
        )
        soup = BeautifulSoup(html, "html.parser")
        self.assertEqual(svc._parse_max_page(soup), 7)

    def test_no_pagination_single_page(self):
        soup = BeautifulSoup(_card("a", "甲"), "html.parser")
        self.assertEqual(svc._parse_max_page(soup), 1)


class ActorRefreshTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_store.configure(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _runtime(self):
        return {"cookie": "ck", "user_agent": "ua", "proxies": ""}

    def test_refresh_all_partial_failure_does_not_wipe(self):
        # 预置 g1t0 已有快照；刷新时 g1t0 返回 403（auth），不应被清空。
        db_store.replace_category_snapshot(
            "g1t0", "u", [{"actor_id": "OLD", "actor_name": "旧", "actor_url": "https://javdb.com/actors/OLD"}]
        )

        def fake_fetch(url, headers=None, proxies=None):
            if "g=1&t=0" in url:
                return _Resp("blocked", status_code=403)
            return _Resp(_page(_card("NEW", "新")), status_code=200)

        with patch.object(svc, "get_runtime_for_request", return_value=self._runtime()), \
             patch.object(svc, "fetch_html", side_effect=fake_fetch):
            result = svc.refresh_all()

        failed_cats = {f["category"] for f in result["failed"]}
        self.assertIn("g1t0", failed_cats)
        ids = {r["actor_id"] for r in db_store.list_collection_actors()}
        self.assertIn("OLD", ids)   # 失败分类旧数据保留（PRD §12.2）
        self.assertIn("NEW", ids)   # 成功分类已写入

    def test_logged_out_page_raises_auth(self):
        login_html = '<form action="/users/sign_in"></form><a href="/login">登入</a>'

        def fake_fetch(url, headers=None, proxies=None):
            return _Resp(login_html, status_code=200)

        with patch.object(svc, "fetch_html", side_effect=fake_fetch):
            with self.assertRaises(svc.ActorFetchError) as ctx:
                svc.fetch_category(svc.ACTOR_CATEGORIES[0], self._runtime())
        self.assertEqual(ctx.exception.kind, "auth")


class ActorUrlBuildTest(unittest.TestCase):
    def test_build_crawl_url_with_tags(self):
        url = build_actor_crawl_url("https://javdb.com/actors/k4MbY", ["1", "3"])
        self.assertIn("/actors/k4MbY?", url)
        self.assertIn("locale=zh", url)
        self.assertIn("sort_type=0", url)
        self.assertIn("t=1%2C3", url)  # urlencode 逗号

    def test_build_crawl_url_without_tags(self):
        url = build_actor_crawl_url("https://javdb.com/actors/k4MbY", [])
        self.assertNotIn("&t=", url)
        self.assertNotIn("?t=", url)


if __name__ == "__main__":
    unittest.main()

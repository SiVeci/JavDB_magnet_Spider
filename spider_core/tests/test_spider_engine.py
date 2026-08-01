"""test_spider_engine — spider_engine 纯解析函数单元测试（零网络）。

覆盖系统中最脆弱、原先零覆盖的 HTML 解析逻辑：
parse_size / evaluate_magnet / parse_movie_tags。
HTML fixture 内联为代表性片段，贴合 JavDB 真实 DOM 结构。
导入 spider_engine 不触发网络（fetch_html 仅在被调用时才用 curl_cffi/WebView）。
"""

import os
import sys
import tempfile
import types
import unittest
from urllib.parse import parse_qs, urlparse
from unittest.mock import Mock, patch

from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import spider_engine  # noqa: E402
import db_store  # noqa: E402


def soup(html):
    return BeautifulSoup(html, "html.parser")


class FetchHtmlTest(unittest.TestCase):
    def test_pc_fetch_uses_current_chrome_impersonation(self):
        # 注入假 curl_cffi（venv-test 不装），既验调用参数又不依赖真实库。
        response = object()
        headers = {"User-Agent": "UA", "Cookie": "cookie=value"}
        proxies = {"https": "http://127.0.0.1:7890"}
        mock_get = Mock(return_value=response)
        fake = types.ModuleType("curl_cffi")
        fake.requests = types.SimpleNamespace(get=mock_get)

        with patch.object(spider_engine, "IS_ANDROID", False), \
             patch.dict(sys.modules, {"curl_cffi": fake}):
            result = spider_engine.fetch_html("https://example.test", headers=headers, proxies=proxies)

        self.assertIs(result, response)
        mock_get.assert_called_once_with(
            "https://example.test",
            headers=headers,
            proxies=proxies,
            impersonate="chrome",
            timeout=15,
        )


class FetchHtmlRetryTest(unittest.TestCase):
    """覆盖 PC 分支(A4)的网络瞬时异常有限重试 + 指数退避。

    venv-test 故意不装 curl_cffi，故注入一个假 curl_cffi 模块，让
    `from curl_cffi import requests` 取到受控的 get()，无需真实网络/依赖。
    """

    def _install_fake_curl(self, side_effects):
        calls = {"n": 0}

        def fake_get(*args, **kwargs):
            index = calls["n"]
            calls["n"] += 1
            effect = side_effects[min(index, len(side_effects) - 1)]
            if isinstance(effect, Exception):
                raise effect
            return effect

        fake_module = types.ModuleType("curl_cffi")
        fake_module.requests = types.SimpleNamespace(get=fake_get)
        return fake_module, calls

    def test_retries_then_succeeds(self):
        ok = object()
        fake, calls = self._install_fake_curl([ConnectionError("reset"), ok])
        with patch.object(spider_engine, "IS_ANDROID", False), \
             patch.dict(sys.modules, {"curl_cffi": fake}), \
             patch.object(spider_engine.time, "sleep") as mock_sleep:
            result = spider_engine.fetch_html("https://example.test")
        self.assertIs(result, ok)
        self.assertEqual(calls["n"], 2)          # 第一次失败 + 第二次成功
        self.assertEqual(mock_sleep.call_count, 1)  # 两次尝试间退避一次

    def test_raises_after_exhausting_retries(self):
        boom = ConnectionError("connection down")
        fake, calls = self._install_fake_curl([boom])  # 每次都抛
        with patch.object(spider_engine, "IS_ANDROID", False), \
             patch.dict(sys.modules, {"curl_cffi": fake}), \
             patch.object(spider_engine.time, "sleep") as mock_sleep:
            with self.assertRaises(ConnectionError):
                spider_engine.fetch_html("https://example.test")
        # HTTP_MAX_RETRIES=2 → 总尝试 3 次、退避 2 次
        self.assertEqual(calls["n"], spider_engine.HTTP_MAX_RETRIES + 1)
        self.assertEqual(mock_sleep.call_count, spider_engine.HTTP_MAX_RETRIES)

    def test_success_first_try_no_backoff(self):
        ok = object()
        fake, calls = self._install_fake_curl([ok])
        with patch.object(spider_engine, "IS_ANDROID", False), \
             patch.dict(sys.modules, {"curl_cffi": fake}), \
             patch.object(spider_engine.time, "sleep") as mock_sleep:
            result = spider_engine.fetch_html("https://example.test")
        self.assertIs(result, ok)
        self.assertEqual(calls["n"], 1)
        mock_sleep.assert_not_called()


class RuntimeFetchIssueTest(unittest.TestCase):
    def test_normal_detail_page_with_login_link_is_not_expired(self):
        response = types.SimpleNamespace(
            status_code=200,
            url="https://javdb.com/v/demo",
            text="""
            <html>
              <body>
                <nav><a href="/login">Login</a></nav>
                <section class="movie-panel-info">
                  <div class="panel-block"><strong>類別:</strong><span class="value"><a>美乳</a></span></div>
                </section>
              </body>
            </html>
            """,
        )

        self.assertIsNone(spider_engine.classify_runtime_fetch_issue(response, stage_label="详情页请求"))

    def test_login_form_is_expired(self):
        response = types.SimpleNamespace(
            status_code=200,
            url="https://javdb.com/login",
            text='<form action="/login"><input type="password" /></form>',
        )

        issue = spider_engine.classify_runtime_fetch_issue(response, stage_label="详情页请求")
        self.assertEqual(issue["cookie_status"], "expired")


class ParseSizeTest(unittest.TestCase):
    def test_gb_converts_to_mb(self):
        self.assertEqual(spider_engine.parse_size("3.5GB"), 3.5 * 1024)

    def test_mb_passthrough(self):
        self.assertEqual(spider_engine.parse_size("700MB"), 700.0)

    def test_kb_converts_to_mb(self):
        self.assertAlmostEqual(spider_engine.parse_size("512KB"), 512 / 1024)

    def test_with_surrounding_text_and_spaces(self):
        # .meta 文本常形如 "ABC-123 2.0 GB · 5 个文件"
        self.assertEqual(spider_engine.parse_size("ABP-001 2.0 GB · 3 files"), 2.0 * 1024)

    def test_empty_and_unparsable(self):
        self.assertEqual(spider_engine.parse_size(""), 0.0)
        self.assertEqual(spider_engine.parse_size(None), 0.0)
        self.assertEqual(spider_engine.parse_size("未知大小"), 0.0)


class EvaluateMagnetTest(unittest.TestCase):
    def _item(self, name, tags_html="", meta="1.0GB", date="2026-01-01", with_magnet=True):
        href = 'href="magnet:?xt=urn:btih:demo"' if with_magnet else ""
        return soup(f"""
            <div class="item">
                <a {href}>下载</a>
                <span class="name">{name}</span>
                <div class="tags">{tags_html}</div>
                <span class="meta">{meta}</span>
                <span class="date"><span class="time">{date}</span></span>
            </div>
        """).select_one(".item")

    def test_returns_none_without_magnet_link(self):
        self.assertIsNone(spider_engine.evaluate_magnet(self._item("foo", with_magnet=False)))

    def test_basic_fields_extracted(self):
        result = spider_engine.evaluate_magnet(self._item("Movie-001-1080p", meta="2.0GB", date="2026-02-03"))
        self.assertEqual(result["link"], "magnet:?xt=urn:btih:demo")
        self.assertEqual(result["name"], "Movie-001-1080p")  # 保留原始大小写
        self.assertEqual(result["date"], "2026-02-03")
        self.assertEqual(result["size_mb"], 2.0 * 1024)

    def test_parses_uncensored_hd_sub_flags(self):
        # name 含 uc(无码) + 1080p(高清)，tags 含 字幕(有字幕)
        item = self._item("ABP-001-UC-1080p", tags_html='<span class="tag">字幕</span>')
        result = spider_engine.evaluate_magnet(item)
        self.assertTrue(result["has_uncensored"])
        self.assertTrue(result["has_hd"])
        self.assertTrue(result["has_subtitle"])
        self.assertNotIn("rank", result)

    def test_parses_hd_via_tag(self):
        item = self._item("plain-name", tags_html='<span class="tag">高清</span>')
        result = spider_engine.evaluate_magnet(item)
        self.assertFalse(result["has_uncensored"])
        self.assertTrue(result["has_hd"])
        self.assertFalse(result["has_subtitle"])

    def test_parses_plain_candidate_without_flags(self):
        result = spider_engine.evaluate_magnet(self._item("plain-name"))
        self.assertFalse(result["has_uncensored"])
        self.assertFalse(result["has_hd"])
        self.assertFalse(result["has_subtitle"])

    def test_missing_name_defaults_unknown(self):
        item = soup('<div class="item"><a href="magnet:?xt=urn:btih:x">d</a></div>').select_one(".item")
        result = spider_engine.evaluate_magnet(item)
        self.assertEqual(result["name"], "Unknown")
        self.assertEqual(result["date"], "1970-01-01")
        self.assertEqual(result["size_mb"], 0.0)

    def test_default_group_scoring_preserves_111_10_0(self):
        parsed = [
            spider_engine.evaluate_magnet(
                self._item("ABP-001-UC-1080p", tags_html='<span class="tag">字幕</span>')
            ),
            spider_engine.evaluate_magnet(
                self._item("plain-name", tags_html='<span class="tag">高清</span>')
            ),
            spider_engine.evaluate_magnet(self._item("plain-name")),
        ]

        scored = spider_engine.score_magnet_candidates(parsed)

        self.assertEqual([item["rank"] for item in scored], [111, 10, 0])

    def test_custom_group_mapping_scores_largest_and_subtitle_hd(self):
        parsed = [
            spider_engine.evaluate_magnet(self._item("4GB plain", meta="4.0GB")),
            spider_engine.evaluate_magnet(
                self._item("2GB subtitle+HD", tags_html='<span class="tag">字幕</span><span class="tag">高清</span>', meta="2.0GB")
            ),
        ]

        scored = spider_engine.score_magnet_candidates(parsed, {
            "magnet_score_100_condition": "largest_size",
            "magnet_score_10_condition": "subtitle",
            "magnet_score_1_condition": "hd",
        })

        self.assertEqual([item["rank"] for item in scored], [100, 11])

    def test_equal_largest_group_candidates_both_match(self):
        parsed = [
            spider_engine.evaluate_magnet(self._item("first", meta="4.0GB")),
            spider_engine.evaluate_magnet(self._item("second", meta="4.0GB")),
        ]

        scored = spider_engine.score_magnet_candidates(parsed, {
            "magnet_score_100_condition": "largest_size",
            "magnet_score_10_condition": "subtitle",
            "magnet_score_1_condition": "hd",
        })

        self.assertEqual([item["rank"] for item in scored], [100, 100])

    def test_unknown_sizes_do_not_get_largest_group_score(self):
        parsed = [
            spider_engine.evaluate_magnet(self._item("first", meta="未知大小")),
            spider_engine.evaluate_magnet(self._item("second", meta="0MB")),
        ]

        scored = spider_engine.score_magnet_candidates(parsed, {
            "magnet_score_100_condition": "largest_size",
            "magnet_score_10_condition": "subtitle",
            "magnet_score_1_condition": "hd",
        })

        self.assertEqual([item["rank"] for item in scored], [0, 0])


class RunSpiderScoringTest(unittest.TestCase):
    def test_run_spider_scores_all_detail_candidates_as_a_group(self):
        list_html = """
            <div class="movie-list">
              <a class="box" href="/v/demo-001" title="DEMO-001">
                <div class="video-title"><strong>DEMO-001</strong></div>
              </a>
            </div>
        """
        detail_html = """
            <div id="magnets-content">
              <div class="item">
                <a href="magnet:?xt=urn:btih:large">下载</a>
                <span class="name">4GB plain</span>
                <span class="meta">4.0GB</span>
                <span class="date"><span class="time">2026-01-01</span></span>
              </div>
              <div class="item">
                <a href="magnet:?xt=urn:btih:small">下载</a>
                <span class="name">2GB subtitle+HD</span>
                <div class="tags"><span class="tag">字幕</span><span class="tag">高清</span></div>
                <span class="meta">2.0GB</span>
                <span class="date"><span class="time">2026-01-02</span></span>
              </div>
            </div>
        """
        responses = [
            Mock(text=list_html, status_code=200, url="https://javdb.com/actors/demo"),
            Mock(text=detail_html, status_code=200, url="https://javdb.com/v/demo-001"),
        ]

        old_data_dir = os.path.dirname(db_store.get_db_path())
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                db_store.configure(tmpdir)
                with patch.object(spider_engine, "DATA_DIR", tmpdir), \
                        patch.object(spider_engine, "CHECKPOINT_FILE", os.path.join(tmpdir, "checkpoint.json")), \
                        patch.object(spider_engine, "fetch_html", side_effect=responses), \
                        patch.object(spider_engine, "update_status"), \
                        patch.object(spider_engine.time, "sleep"), \
                        patch.object(db_store, "save_movie_result") as save_result:
                    spider_engine.run_spider(
                        "https://javdb.com/actors/demo",
                        "",
                        "UA",
                        "demo.csv",
                        crawl_mode="overwrite",
                        score_conditions={
                            "magnet_score_100_condition": "largest_size",
                            "magnet_score_10_condition": "subtitle",
                            "magnet_score_1_condition": "hd",
                        },
                    )
        finally:
            db_store.configure(old_data_dir)

        save_result.assert_called_once()
        best = save_result.call_args.args[2]
        all_candidates = save_result.call_args.args[3]
        self.assertEqual(best["link"], "magnet:?xt=urn:btih:large")
        self.assertEqual(best["rank"], 100)
        self.assertEqual([item["rank"] for item in all_candidates], [100, 11])


class RunSpiderLocaleTest(unittest.TestCase):
    def test_run_spider_normalizes_initial_pagination_and_detail_urls(self):
        list_html = """
            <div class="movie-list">
              <a class="box" href="/v/demo-001?source=list&locale=en" title="DEMO-001">
                <div class="video-title"><strong>DEMO-001</strong></div>
              </a>
            </div>
            <nav class="pagination"><a class="pagination-next" href="/actors/demo?page=2&locale=en">Next</a></nav>
        """
        final_list_html = """
            <div class="movie-list"></div>
        """
        detail_html = """
            <div id="magnets-content">
              <div class="item">
                <a href="magnet:?xt=urn:btih:locale">下载</a>
                <span class="name">1GB plain</span>
                <span class="meta">1.0GB</span>
                <span class="date"><span class="time">2026-01-01</span></span>
              </div>
            </div>
        """
        seen = []

        def fake_fetch(url, headers=None, proxies=None):
            seen.append((url, headers))
            if "/v/demo-001" in url:
                return Mock(text=detail_html, status_code=200, url=url)
            if "page=2" in url:
                return Mock(text=final_list_html, status_code=200, url=url)
            return Mock(text=list_html, status_code=200, url=url)

        with tempfile.TemporaryDirectory() as tmpdir:
            old_data_dir = os.path.dirname(db_store.get_db_path())
            try:
                db_store.configure(tmpdir)
                with patch.object(spider_engine, "DATA_DIR", tmpdir), \
                        patch.object(spider_engine, "CHECKPOINT_FILE", os.path.join(tmpdir, "checkpoint.json")), \
                        patch.object(spider_engine, "fetch_html", side_effect=fake_fetch), \
                        patch.object(spider_engine, "update_status"), \
                        patch.object(spider_engine.time, "sleep"), \
                        patch.object(db_store, "save_movie_result"):
                    spider_engine.run_spider(
                        "https://javdb.com/actors/demo?page=1&locale=en&sort_type=0",
                        "session=abc; locale=en",
                        "UA",
                        "locale.csv",
                        crawl_mode="overwrite",
                    )
            finally:
                db_store.configure(old_data_dir)

        self.assertEqual(len(seen), 3)
        for url, headers in seen:
            self.assertEqual(parse_qs(urlparse(url).query)["locale"], ["zh"])
            self.assertIn("locale=en", headers["Cookie"])
        detail_query = parse_qs(urlparse(seen[-1][0]).query)
        self.assertEqual(detail_query["source"], ["list"])

    def test_run_spider_normalizes_old_checkpoint_detail_url(self):
        detail_html = """
            <div id="magnets-content">
              <div class="item">
                <a href="magnet:?xt=urn:btih:resume">下载</a>
                <span class="name">1GB plain</span>
                <span class="meta">1.0GB</span>
                <span class="date"><span class="time">2026-01-01</span></span>
              </div>
            </div>
        """
        seen = []

        def fake_fetch(url, headers=None, proxies=None):
            seen.append(url)
            return Mock(text=detail_html, status_code=200, url=url)

        with tempfile.TemporaryDirectory() as tmpdir:
            old_data_dir = os.path.dirname(db_store.get_db_path())
            try:
                db_store.configure(tmpdir)
                with patch.object(spider_engine, "DATA_DIR", tmpdir), \
                        patch.object(spider_engine, "CHECKPOINT_FILE", os.path.join(tmpdir, "checkpoint.json")), \
                        patch.object(
                            spider_engine,
                            "load_checkpoint",
                            return_value={
                                "phase": 2,
                                "movie_links": [
                                    {
                                        "code": "DEMO-001",
                                        "url": "https://javdb.com/v/demo-001?locale=en",
                                        "title": "DEMO-001",
                                    }
                                ],
                                "current_index": 0,
                            },
                        ), \
                        patch.object(spider_engine, "fetch_html", side_effect=fake_fetch), \
                        patch.object(spider_engine, "update_status"), \
                        patch.object(spider_engine.time, "sleep"), \
                        patch.object(db_store, "save_movie_result"):
                    spider_engine.run_spider(
                        "https://javdb.com/actors/demo?locale=en",
                        "locale=en",
                        "UA",
                        "resume.csv",
                        is_resume=True,
                        crawl_mode="overwrite",
                    )
            finally:
                db_store.configure(old_data_dir)

        self.assertEqual(parse_qs(urlparse(seen[0]).query)["locale"], ["zh"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

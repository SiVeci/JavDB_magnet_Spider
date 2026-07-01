"""test_spider_engine — spider_engine 纯解析函数单元测试（零网络）。

覆盖系统中最脆弱、原先零覆盖的 HTML 解析逻辑：
parse_size / evaluate_magnet / parse_movie_tags。
HTML fixture 内联为代表性片段，贴合 JavDB 真实 DOM 结构。
导入 spider_engine 不触发网络（fetch_html 仅在被调用时才用 curl_cffi/WebView）。
"""

import os
import sys
import types
import unittest
from unittest.mock import Mock, patch

from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import spider_engine  # noqa: E402


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

    def test_rank_uncensored_hd_sub(self):
        # name 含 uc(无码) + 1080p(高清)，tags 含 字幕(有字幕)
        item = self._item("ABP-001-UC-1080p", tags_html='<span class="tag">字幕</span>')
        result = spider_engine.evaluate_magnet(item)
        self.assertEqual(result["rank"], 100 + 10 + 1)

    def test_rank_hd_via_tag(self):
        item = self._item("plain-name", tags_html='<span class="tag">高清</span>')
        result = spider_engine.evaluate_magnet(item)
        self.assertEqual(result["rank"], 10)

    def test_rank_zero_for_plain(self):
        result = spider_engine.evaluate_magnet(self._item("plain-name"))
        self.assertEqual(result["rank"], 0)

    def test_missing_name_defaults_unknown(self):
        item = soup('<div class="item"><a href="magnet:?xt=urn:btih:x">d</a></div>').select_one(".item")
        result = spider_engine.evaluate_magnet(item)
        self.assertEqual(result["name"], "Unknown")
        self.assertEqual(result["date"], "1970-01-01")
        self.assertEqual(result["size_mb"], 0.0)



if __name__ == "__main__":
    unittest.main(verbosity=2)

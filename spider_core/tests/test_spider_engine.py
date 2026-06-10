"""test_spider_engine — spider_engine 纯解析函数单元测试（零网络）。

覆盖系统中最脆弱、原先零覆盖的 HTML 解析逻辑：
parse_size / evaluate_magnet / parse_movie_tags。
HTML fixture 内联为代表性片段，贴合 JavDB 真实 DOM 结构。
导入 spider_engine 不触发网络（fetch_html 仅在被调用时才用 curl_cffi/WebView）。
"""

import os
import sys
import unittest

from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import spider_engine  # noqa: E402


def soup(html):
    return BeautifulSoup(html, "html.parser")


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


class ParseMovieTagsTest(unittest.TestCase):
    PANEL = """
    <div class="movie-panel-info">
        <div class="panel-block">
            <strong>番號:</strong><span class="value">ABP-001</span>
        </div>
        <div class="panel-block">
            <strong>類別:</strong>
            <span class="value">
                <a href="/tags?c=1">巨乳</a>
                <a href="/tags?c=2">中出</a>
                <a href="/tags?c=2">中出</a>
            </span>
        </div>
    </div>
    """

    def test_extracts_category_tags_deduped_in_order(self):
        tags = spider_engine.parse_movie_tags(soup(self.PANEL))
        self.assertEqual(tags, ["巨乳", "中出"])

    def test_simplified_label_supported(self):
        html = self.PANEL.replace("類別", "类别")
        self.assertEqual(spider_engine.parse_movie_tags(soup(html)), ["巨乳", "中出"])

    def test_returns_empty_when_no_category_block(self):
        html = '<div class="movie-panel-info"><div class="panel-block"><strong>番號:</strong><span class="value">X</span></div></div>'
        self.assertEqual(spider_engine.parse_movie_tags(soup(html)), [])

    def test_returns_empty_on_unrelated_html(self):
        self.assertEqual(spider_engine.parse_movie_tags(soup("<div>nothing</div>")), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)

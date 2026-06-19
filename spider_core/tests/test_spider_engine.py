"""test_spider_engine 鈥?spider_engine 绾В鏋愬嚱鏁板崟鍏冩祴璇曪紙闆剁綉缁滐級銆?

瑕嗙洊绯荤粺涓渶鑴嗗急銆佸師鍏堥浂瑕嗙洊鐨?HTML 瑙ｆ瀽閫昏緫锛?
parse_size / evaluate_magnet / parse_movie_tags銆?
HTML fixture 鍐呰仈涓轰唬琛ㄦ€х墖娈碉紝璐村悎 JavDB 鐪熷疄 DOM 缁撴瀯銆?
瀵煎叆 spider_engine 涓嶈Е鍙戠綉缁滐紙fetch_html 浠呭湪琚皟鐢ㄦ椂鎵嶇敤 curl_cffi/WebView锛夈€?
"""

import os
import sys
import unittest
from unittest.mock import patch

from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import spider_engine  # noqa: E402


def soup(html):
    return BeautifulSoup(html, "html.parser")


class FetchHtmlTest(unittest.TestCase):
    def test_pc_fetch_uses_current_chrome_impersonation(self):
        response = object()
        headers = {"User-Agent": "UA", "Cookie": "cookie=value"}
        proxies = {"https": "http://127.0.0.1:7890"}

        with patch.object(spider_engine, "IS_ANDROID", False), \
             patch("curl_cffi.requests.get", return_value=response) as mock_get:
            result = spider_engine.fetch_html("https://example.test", headers=headers, proxies=proxies)

        self.assertIs(result, response)
        mock_get.assert_called_once_with(
            "https://example.test",
            headers=headers,
            proxies=proxies,
            impersonate="chrome",
            timeout=15,
        )


class ParseSizeTest(unittest.TestCase):
    def test_gb_converts_to_mb(self):
        self.assertEqual(spider_engine.parse_size("3.5GB"), 3.5 * 1024)

    def test_mb_passthrough(self):
        self.assertEqual(spider_engine.parse_size("700MB"), 700.0)

    def test_kb_converts_to_mb(self):
        self.assertAlmostEqual(spider_engine.parse_size("512KB"), 512 / 1024)

    def test_with_surrounding_text_and_spaces(self):
        # .meta 鏂囨湰甯稿舰濡?"ABC-123 2.0 GB 路 5 涓枃浠?
        self.assertEqual(spider_engine.parse_size("ABP-001 2.0 GB 路 3 files"), 2.0 * 1024)

    def test_empty_and_unparsable(self):
        self.assertEqual(spider_engine.parse_size(""), 0.0)
        self.assertEqual(spider_engine.parse_size(None), 0.0)
        self.assertEqual(spider_engine.parse_size("鏈煡澶у皬"), 0.0)


class EvaluateMagnetTest(unittest.TestCase):
    def _item(self, name, tags_html="", meta="1.0GB", date="2026-01-01", with_magnet=True):
        href = 'href="magnet:?xt=urn:btih:demo"' if with_magnet else ""
        return soup(f"""
            <div class="item">
                <a {href}>涓嬭浇</a>
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
        self.assertEqual(result["name"], "Movie-001-1080p")  # 淇濈暀鍘熷澶у皬鍐?
        self.assertEqual(result["date"], "2026-02-03")
        self.assertEqual(result["size_mb"], 2.0 * 1024)

    def test_rank_uncensored_hd_sub(self):
        # name 鍚?uc(鏃犵爜) + 1080p(楂樻竻)锛宼ags 鍚?瀛楀箷(鏈夊瓧骞?
        item = self._item("ABP-001-UC-1080p", tags_html='<span class="tag">瀛楀箷</span>')
        result = spider_engine.evaluate_magnet(item)
        self.assertEqual(result["rank"], 100 + 10 + 1)

    def test_rank_hd_via_tag(self):
        item = self._item("plain-name", tags_html='<span class="tag">楂樻竻</span>')
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

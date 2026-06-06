import os
import unittest

import sys
from bs4 import BeautifulSoup


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from spider_engine import parse_movie_tags  # noqa: E402


class MovieTagParseTest(unittest.TestCase):
    def test_parse_movie_tags_from_detail_panel(self):
        html = """
        <nav class="panel movie-panel-info">
          <div class="panel-block">
            <strong>類別:</strong>
            &nbsp;<span class="value"><a href="/tags?c4=91">美乳</a>,&nbsp;<a href="/tags?c2=201">女檢察官</a>,&nbsp;<a href="/tags?c7=28">單體作品</a>,&nbsp;<a href="/tags?c5=18">中出</a>,&nbsp;<a href="/tags?c5=68">潮吹</a></span>
          </div>
        </nav>
        """
        self.assertEqual(
            parse_movie_tags(BeautifulSoup(html, "html.parser")),
            ["美乳", "女檢察官", "單體作品", "中出", "潮吹"],
        )

    def test_parse_movie_tags_is_safe_when_missing(self):
        html = '<nav class="panel movie-panel-info"><div class="panel-block"><strong>片商:</strong><span class="value">Maker</span></div></nav>'
        self.assertEqual(parse_movie_tags(BeautifulSoup(html, "html.parser")), [])

    def test_parse_movie_tags_accepts_simplified_label(self):
        html = '<div class="movie-panel-info"><div class="panel-block"><strong>类别：</strong><span class="value"><a>标签A</a><a>标签A</a><a>标签B</a></span></div></div>'
        self.assertEqual(parse_movie_tags(BeautifulSoup(html, "html.parser")), ["标签A", "标签B"])


if __name__ == "__main__":
    unittest.main()

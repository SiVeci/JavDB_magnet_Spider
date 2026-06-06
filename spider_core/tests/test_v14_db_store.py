import csv
import io
import os
import tempfile
import unittest

import sys


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import db_store  # noqa: E402


class DbStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_store.configure(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def write_csv(self, filename, rows):
        path = os.path.join(self.tmpdir.name, filename)
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=db_store.CSV_FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_import_existing_csvs_is_idempotent(self):
        self.write_csv(
            "history.csv",
            [
                {
                    "影片番号": "ABC-001",
                    "原始标题": "Title 1",
                    "影片链接": "https://example.test/v/1",
                    "最佳资源文件名": "abc.torrent",
                    "磁力链接": "magnet:?xt=urn:btih:abc",
                    "优先级得分": "110",
                    "日期": "2026-01-01",
                    "文件大小(MB)": "1024",
                }
            ],
        )

        csv_path = os.path.join(self.tmpdir.name, "history.csv")
        self.assertEqual(db_store.import_existing_csvs(self.tmpdir.name), 1)
        self.assertFalse(os.path.exists(csv_path))
        self.assertEqual(db_store.import_existing_csvs(self.tmpdir.name), 0)

        history = db_store.get_history()
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["name"], "history.csv")
        self.assertEqual(history[0]["count"], 1)
        self.assertEqual(history[0]["tags"], [])

    def test_import_csv_tolerates_malformed_numeric_fields(self):
        self.write_csv(
            "bad-number.csv",
            [
                {
                    "影片番号": "BAD-001",
                    "原始标题": "Bad Number",
                    "影片链接": "https://example.test/v/bad",
                    "最佳资源文件名": "bad.torrent",
                    "磁力链接": "magnet:?xt=urn:btih:bad",
                    "优先级得分": "not-a-number",
                    "日期": "2026-01-01",
                    "文件大小(MB)": "unknown",
                }
            ],
        )

        csv_path = os.path.join(self.tmpdir.name, "bad-number.csv")
        self.assertEqual(db_store.import_existing_csvs(self.tmpdir.name), 1)
        self.assertFalse(os.path.exists(csv_path))
        self.assertEqual(db_store.get_magnet_links("bad-number.csv"), ["magnet:?xt=urn:btih:bad"])

    def test_save_movie_result_stores_candidates_and_selected_magnet(self):
        db_store.ensure_collection("output.csv")
        movie = {"code": "ABC-001", "title": "Title 1", "url": "https://example.test/v/1"}
        candidates = [
            {
                "name": "low.torrent",
                "link": "magnet:?xt=urn:btih:low",
                "rank": 1,
                "date": "2026-01-01",
                "size_mb": 100,
            },
            {
                "name": "best.torrent",
                "link": "magnet:?xt=urn:btih:best",
                "rank": 110,
                "date": "2026-01-02",
                "size_mb": 2048,
            },
        ]

        db_store.save_movie_result("output.csv", movie, candidates[1], candidates)

        self.assertEqual(db_store.get_existing_codes("output.csv"), {"ABC-001"})
        self.assertEqual(db_store.get_magnet_links("output.csv"), ["magnet:?xt=urn:btih:best"])
        with db_store.connect() as conn:
            total = conn.execute("SELECT COUNT(*) AS count FROM magnets").fetchone()["count"]
            selected = conn.execute("SELECT COUNT(*) AS count FROM magnets WHERE is_selected = 1").fetchone()["count"]
        self.assertEqual(total, 2)
        self.assertEqual(selected, 1)

    def test_export_collection_to_csv_bytes_without_physical_csv(self):
        db_store.ensure_collection("output.csv")
        movie = {"code": "ABC-001", "title": "Title 1", "url": "https://example.test/v/1"}
        best = {
            "name": "best.torrent",
            "link": "magnet:?xt=urn:btih:best",
            "rank": 110,
            "date": "2026-01-02",
            "size_mb": 2048,
        }
        db_store.save_movie_result("output.csv", movie, best, [best])

        path = os.path.join(self.tmpdir.name, "output.csv")
        if os.path.exists(path):
            os.remove(path)

        csv_bytes, safe_name = db_store.export_collection_to_csv_bytes("output.csv")
        self.assertEqual(safe_name, "output.csv")
        self.assertFalse(os.path.exists(path))
        rows = list(csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig"))))
        self.assertEqual(rows[0]["影片番号"], "ABC-001")
        self.assertEqual(rows[0]["磁力链接"], "magnet:?xt=urn:btih:best")

    def test_movie_tags_are_stored_and_filter_exports(self):
        db_store.ensure_collection("output.csv")
        first = {
            "name": "first.torrent",
            "link": "magnet:?xt=urn:btih:first",
            "rank": 110,
            "date": "2026-01-01",
            "size_mb": 100,
        }
        second = {
            "name": "second.torrent",
            "link": "magnet:?xt=urn:btih:second",
            "rank": 110,
            "date": "2026-01-02",
            "size_mb": 200,
        }
        db_store.save_movie_result(
            "output.csv",
            {"code": "ABC-001", "title": "Title 1", "url": "https://example.test/v/1", "tags": ["美乳", "中出"]},
            first,
            [first],
        )
        db_store.save_movie_result(
            "output.csv",
            {"code": "ABC-002", "title": "Title 2", "url": "https://example.test/v/2", "tags": ["美乳", "潮吹"]},
            second,
            [second],
        )

        history = db_store.get_history()
        self.assertEqual(history[0]["tags"], ["美乳", "中出", "潮吹"])

        collection = db_store.get_collection_movies("output.csv")
        self.assertEqual(collection["available_tags"], ["美乳", "中出", "潮吹"])
        self.assertEqual(collection["movies"][0]["tags"], ["美乳", "中出"])

        self.assertEqual(db_store.get_magnet_links("output.csv", ["美乳"]), [first["link"], second["link"]])
        self.assertEqual(db_store.get_magnet_links("output.csv", ["美乳", "中出"]), [first["link"]])

        csv_bytes, _ = db_store.export_collection_to_csv_bytes("output.csv", ["潮吹"])
        rows = list(csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig"))))
        self.assertEqual([row["影片番号"] for row in rows], ["ABC-002"])
        self.assertNotIn("标签", rows[0])

    def test_replacing_movie_tags_rebuilds_collection_union(self):
        db_store.ensure_collection("output.csv")
        best = {
            "name": "best.torrent",
            "link": "magnet:?xt=urn:btih:best",
            "rank": 110,
            "date": "2026-01-02",
            "size_mb": 2048,
        }
        db_store.save_movie_result(
            "output.csv",
            {"code": "ABC-001", "title": "Title", "url": "https://example.test/v/1", "tags": ["旧标签"]},
            best,
            [best],
        )
        db_store.save_movie_result(
            "output.csv",
            {"code": "ABC-001", "title": "Title", "url": "https://example.test/v/1", "tags": ["新标签"]},
            best,
            [best],
        )

        self.assertEqual(db_store.get_history()[0]["tags"], ["新标签"])
        db_store.clear_collection("output.csv")
        self.assertEqual(db_store.get_history()[0]["tags"], [])

    def test_delete_collections_removes_db_and_physical_csv(self):
        self.write_csv("history.csv", [])
        db_store.ensure_collection("history.csv")

        deleted, missing = db_store.delete_collections(["history.csv"], self.tmpdir.name)

        self.assertEqual(deleted, ["history.csv"])
        self.assertEqual(missing, [])
        self.assertFalse(db_store.collection_exists("history.csv"))
        self.assertFalse(os.path.exists(os.path.join(self.tmpdir.name, "history.csv")))


class DbBackedApiTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_store.configure(self.tmpdir.name)

        import main

        self.main = main
        self.old_data_dir = main.DATA_DIR
        main.DATA_DIR = self.tmpdir.name
        db_store.ensure_collection("api.csv")
        best = {
            "name": "best.torrent",
            "link": "magnet:?xt=urn:btih:api",
            "rank": 110,
            "date": "2026-01-02",
            "size_mb": 2048,
        }
        db_store.save_movie_result(
            "api.csv",
            {"code": "API-001", "title": "API Title", "url": "https://example.test/v/api", "tags": ["美乳", "中出"]},
            best,
            [best],
        )

    def tearDown(self):
        self.main.DATA_DIR = self.old_data_dir
        self.tmpdir.cleanup()

    def test_history_download_and_magnets_are_db_backed(self):
        history = self.main.get_history()
        self.assertEqual(history["code"], 200)
        self.assertEqual(history["data"][0]["name"], "api.csv")
        self.assertEqual(history["data"][0]["count"], 1)

        magnets = self.main.get_magnets("api.csv")
        self.assertEqual(magnets, {"code": 200, "data": ["magnet:?xt=urn:btih:api"]})
        self.assertEqual(self.main.get_magnets("api.csv", tags="美乳,中出"), magnets)
        self.assertEqual(self.main.get_magnets("api.csv", tags="潮吹"), {"code": 200, "data": []})

        path = os.path.join(self.tmpdir.name, "api.csv")
        if os.path.exists(path):
            os.remove(path)
        response = self.main.download_csv("api.csv")
        self.assertEqual(response.status_code, 200)
        self.assertFalse(os.path.exists(path))
        self.assertIn("filename*=UTF-8''api.csv", response.headers["content-disposition"])
        rows = list(csv.DictReader(io.StringIO(response.body.decode("utf-8-sig"))))
        self.assertEqual(rows[0][db_store.CSV_FIELDNAMES[0]], "API-001")


if __name__ == "__main__":
    unittest.main()

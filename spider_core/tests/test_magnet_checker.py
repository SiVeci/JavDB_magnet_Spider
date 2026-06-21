import os
import struct
import sys
import time
import unittest
from unittest.mock import patch


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import magnet_checker  # noqa: E402


class MagnetCheckerTest(unittest.TestCase):
    def test_extracts_info_hash_and_merges_trackers(self):
        link = (
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
            "&tr=http%3A%2F%2Ftracker.example%2Fannounce"
        )

        info_hash = magnet_checker.extract_info_hash(link)
        trackers = magnet_checker.get_trackers_for_magnet(
            link,
            ["http://tracker.example/announce", "udp://tracker.user:80/announce"],
        )

        self.assertEqual(info_hash, bytes.fromhex("0123456789abcdef0123456789abcdef01234567"))
        self.assertEqual(trackers[0], "http://tracker.example/announce")
        self.assertEqual(trackers[1], "udp://tracker.user:80/announce")
        self.assertEqual(trackers.count("http://tracker.example/announce"), 1)

    def test_classifies_tracker_results(self):
        self.assertEqual(magnet_checker.classify_result(1, 0), "active")
        self.assertEqual(magnet_checker.classify_result(0, 2), "weak")
        self.assertEqual(magnet_checker.classify_result(0, 0), "dead")

    def test_invalid_magnet_is_dead(self):
        result = magnet_checker.check_magnet("not-a-magnet")

        self.assertEqual(result["check_status"], "dead")
        self.assertEqual(result["seeders"], 0)
        self.assertIn("无效", result["check_error"])

    def test_uses_best_successful_tracker_result(self):
        link = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"

        def fake_query(tracker, _info_hash, _peer_id, _timeout):
            if "first" in tracker:
                return 0, 4
            return 7, 1

        with patch.object(magnet_checker, "DEFAULT_TRACKERS", []), patch.object(
            magnet_checker, "query_tracker", side_effect=fake_query
        ):
            result = magnet_checker.check_magnet(link, ["http://first/announce", "http://second/announce"])

        self.assertEqual(result["check_status"], "active")
        self.assertEqual(result["seeders"], 7)
        self.assertEqual(result["leechers"], 4)

    def test_returns_active_without_waiting_for_slow_tracker(self):
        link = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"

        def fake_query(tracker, _info_hash, _peer_id, _timeout):
            if "slow" in tracker:
                time.sleep(0.2)
                return 0, 0
            return 2, 1

        with patch.object(magnet_checker, "DEFAULT_TRACKERS", []), patch.object(
            magnet_checker, "query_tracker", side_effect=fake_query
        ):
            started = time.monotonic()
            result = magnet_checker.check_magnet(link, ["http://slow/announce", "http://fast/announce"])
            elapsed = time.monotonic() - started

        self.assertEqual(result["check_status"], "active")
        self.assertEqual(result["seeders"], 2)
        self.assertLess(elapsed, 0.15)

    def test_uses_best_non_active_tracker_result(self):
        link = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"

        def fake_query(tracker, _info_hash, _peer_id, _timeout):
            if "weak" in tracker:
                return 0, 5
            return 0, 1

        with patch.object(magnet_checker, "DEFAULT_TRACKERS", []), patch.object(
            magnet_checker, "query_tracker", side_effect=fake_query
        ):
            result = magnet_checker.check_magnet(link, ["http://weak/announce", "http://dead/announce"])

        self.assertEqual(result["check_status"], "weak")
        self.assertEqual(result["seeders"], 0)
        self.assertEqual(result["leechers"], 5)

    def test_all_tracker_failures_returns_error(self):
        link = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"

        with patch.object(magnet_checker, "DEFAULT_TRACKERS", []), patch.object(
            magnet_checker, "query_tracker", side_effect=RuntimeError("down")
        ):
            result = magnet_checker.check_magnet(link, ["http://tracker/announce"])

        self.assertIsNone(result["check_status"])
        self.assertEqual(result["seeders"], 0)
        self.assertEqual(result["leechers"], 0)
        self.assertEqual(result["check_error"], "down")

    def test_bdecode_tracker_payload(self):
        payload = b"d8:completei15e10:incompletei3ee"

        data = magnet_checker.bdecode(payload)

        self.assertEqual(data[b"complete"], 15)
        self.assertEqual(data[b"incomplete"], 3)



if __name__ == "__main__":
    unittest.main()

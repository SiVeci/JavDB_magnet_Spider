import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from magnet_scoring import infer_magnet_conditions, score_magnet_candidates


def candidate(name, size_mb=1024.0, uncensored=False, hd=False, subtitle=False):
    return {
        "name": name,
        "size_mb": size_mb,
        "has_uncensored": uncensored,
        "has_hd": hd,
        "has_subtitle": subtitle,
    }


class MagnetScoringTest(unittest.TestCase):
    def test_infer_conditions_from_legacy_filename_tokens(self):
        self.assertEqual(
            infer_magnet_conditions("JUR-750-U.torrent"),
            {
                "has_uncensored": True,
                "has_hd": False,
                "has_subtitle": False,
            },
        )
        self.assertTrue(
            infer_magnet_conditions("JUR-750-C-4K.torrent")["has_subtitle"]
        )
        self.assertTrue(
            infer_magnet_conditions("JUR-750-C-4K.torrent")["has_hd"]
        )

    def test_infer_conditions_uses_tags_when_available(self):
        result = infer_magnet_conditions("plain-name", ["字幕", "高清"])
        self.assertTrue(result["has_subtitle"])
        self.assertTrue(result["has_hd"])
        self.assertFalse(result["has_uncensored"])

    def test_infer_conditions_does_not_match_letters_inside_words(self):
        result = infer_magnet_conditions("documentary-cut.torrent")
        self.assertFalse(result["has_uncensored"])
        self.assertFalse(result["has_subtitle"])

    def test_default_mapping_preserves_100_10_1_scores(self):
        results = score_magnet_candidates([
            candidate("uncensored", uncensored=True),
            candidate("hd", hd=True),
            candidate("subtitle", subtitle=True),
        ])

        self.assertEqual([item["rank"] for item in results], [100, 10, 1])

    def test_custom_mapping_assigns_fixed_level_scores(self):
        results = score_magnet_candidates([
            candidate("4GB plain", size_mb=4096.0),
            candidate("2GB subtitle+HD", size_mb=2048.0, hd=True, subtitle=True),
        ], {
            "magnet_score_100_condition": "largest_size",
            "magnet_score_10_condition": "subtitle",
            "magnet_score_1_condition": "hd",
        })

        self.assertEqual([item["rank"] for item in results], [100, 11])

    def test_largest_known_candidate_gets_configured_score(self):
        results = score_magnet_candidates([
            candidate("large", size_mb=2048.0),
            candidate("small", size_mb=1024.0),
        ], {
            "magnet_score_100_condition": "largest_size",
            "magnet_score_10_condition": "uncensored",
            "magnet_score_1_condition": "hd",
        })

        self.assertEqual(results[0]["rank"], 100)
        self.assertEqual(results[1]["rank"], 0)

    def test_equal_largest_candidates_both_match(self):
        results = score_magnet_candidates([
            candidate("first", size_mb=4096.0),
            candidate("second", size_mb=4096.0),
            candidate("small", size_mb=1024.0),
        ], {
            "magnet_score_100_condition": "largest_size",
            "magnet_score_10_condition": "uncensored",
            "magnet_score_1_condition": "hd",
        })

        self.assertEqual([item["rank"] for item in results], [100, 100, 0])

    def test_all_unknown_sizes_do_not_match_largest_size(self):
        results = score_magnet_candidates([
            candidate("unknown", size_mb=None),
            candidate("zero", size_mb=0.0),
        ], {
            "magnet_score_100_condition": "largest_size",
            "magnet_score_10_condition": "uncensored",
            "magnet_score_1_condition": "hd",
        })

        self.assertEqual([item["rank"] for item in results], [0, 0])

    def test_duplicate_conditions_are_rejected(self):
        with self.assertRaises(ValueError):
            score_magnet_candidates([], {
                "magnet_score_100_condition": "uncensored",
                "magnet_score_10_condition": "uncensored",
                "magnet_score_1_condition": "subtitle",
            })

    def test_unknown_condition_is_rejected(self):
        with self.assertRaises(ValueError):
            score_magnet_candidates([], {
                "magnet_score_100_condition": "date",
                "magnet_score_10_condition": "hd",
                "magnet_score_1_condition": "subtitle",
            })

    def test_lower_levels_cannot_outscore_level_100(self):
        results = score_magnet_candidates([
            candidate("level-100", uncensored=True),
            candidate("lower-levels", hd=True, subtitle=True),
        ])

        self.assertEqual(results[0]["rank"], 100)
        self.assertEqual(results[1]["rank"], 11)
        self.assertGreater(results[0]["rank"], results[1]["rank"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

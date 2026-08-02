from collections.abc import Mapping
import re


SCORE_LEVELS = (
    ("magnet_score_100_condition", 100),
    ("magnet_score_10_condition", 10),
    ("magnet_score_1_condition", 1),
)

ALLOWED_CONDITIONS = {"uncensored", "hd", "subtitle", "largest_size"}

DEFAULT_SCORE_CONDITIONS = {
    "magnet_score_100_condition": "uncensored",
    "magnet_score_10_condition": "hd",
    "magnet_score_1_condition": "subtitle",
}


def infer_magnet_conditions(name, tags=None):
    """Infer the three filename or HTML tag conditions used by magnet scoring."""
    normalized_name = str(name or "").lower()
    normalized_tags = {str(tag).strip().casefold() for tag in (tags or [])}
    return {
        "has_uncensored": bool(
            re.search(r"\b(uc|uncensored|u)\b", normalized_name)
            or normalized_tags.intersection({"无码", "無碼", "uncensored"})
        ),
        "has_hd": bool(
            normalized_tags.intersection({"高清", "hd"})
            or re.search(r"\b(1080p|4k|2160p)\b", normalized_name)
        ),
        "has_subtitle": bool(
            normalized_tags.intersection({"字幕", "subtitle", "subtitles"})
            or re.search(r"\b(c|chs)\b", normalized_name)
        ),
    }


def validate_score_conditions(config):
    """Return a normalized three-level condition mapping."""
    if config is None:
        config = DEFAULT_SCORE_CONDITIONS
    if not isinstance(config, Mapping):
        raise ValueError("磁力评分条件配置必须是映射")

    normalized = {}
    for key, _score in SCORE_LEVELS:
        condition = config.get(key)
        if condition not in ALLOWED_CONDITIONS:
            raise ValueError("磁力评分条件必须从四个支持项中选择三个且不能重复")
        normalized[key] = condition

    if len(set(normalized.values())) != len(SCORE_LEVELS):
        raise ValueError("磁力评分条件必须从四个支持项中选择三个且不能重复")
    return normalized


def _has_positive_size(value):
    try:
        return value > 0
    except TypeError:
        return False


def score_magnet_candidates(candidates, config=None):
    """Return scored candidate copies without modifying the input list."""
    score_conditions = validate_score_conditions(config)
    scored = [dict(candidate) for candidate in candidates]

    known_sizes = [item.get("size_mb") for item in scored if _has_positive_size(item.get("size_mb"))]
    max_size = max(known_sizes) if known_sizes else None

    for item in scored:
        condition_matches = {
            "uncensored": bool(item.get("has_uncensored")),
            "hd": bool(item.get("has_hd")),
            "subtitle": bool(item.get("has_subtitle")),
            "largest_size": (
                max_size is not None
                and _has_positive_size(item.get("size_mb"))
                and item.get("size_mb") == max_size
            ),
        }
        item["rank"] = sum(
            score
            for key, score in SCORE_LEVELS
            if condition_matches[score_conditions[key]]
        )
    return scored

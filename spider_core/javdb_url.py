from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


def ensure_zh_locale(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["locale"] = ["zh"]
    return urlunparse(parsed._replace(query=urlencode(params, doseq=True)))

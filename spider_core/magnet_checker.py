import base64
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
import os
import random
import socket
import struct
import time
from urllib.parse import parse_qs, quote_from_bytes, urlencode, urlparse
from urllib.request import Request, urlopen


DEFAULT_TRACKERS = [
    "http://tracker.opentrackr.org:1337/announce",
    "http://tracker.openbittorrent.com:6969/announce",
    "http://tracker.internetwarriors.net:1337/announce",
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
]

CONCURRENCY_LIMIT = 5
TRACKER_CONCURRENCY_LIMIT = 3
TRACKER_TIMEOUT_SECONDS = 6
MAGNET_TIMEOUT_SECONDS = 12
RETRY_COUNT = 2


class MagnetCheckError(Exception):
    pass


class InvalidMagnetError(MagnetCheckError):
    pass


def extract_info_hash(magnet_link):
    parsed = urlparse(magnet_link or "")
    if parsed.scheme != "magnet":
        raise InvalidMagnetError("无效的磁力链接")
    xt_values = parse_qs(parsed.query).get("xt", [])
    for value in xt_values:
        prefix = "urn:btih:"
        if value.lower().startswith(prefix):
            raw = value[len(prefix):].strip()
            if len(raw) == 40:
                try:
                    return bytes.fromhex(raw)
                except ValueError as exc:
                    raise InvalidMagnetError("无效的磁力链接") from exc
            try:
                padded = raw.upper() + "=" * ((8 - len(raw) % 8) % 8)
                decoded = base64.b32decode(padded)
            except Exception as exc:
                raise InvalidMagnetError("无效的磁力链接") from exc
            if len(decoded) == 20:
                return decoded
    raise InvalidMagnetError("无效的磁力链接")


def extract_trackers_from_magnet(magnet_link):
    parsed = urlparse(magnet_link or "")
    if parsed.scheme != "magnet":
        return []
    return _dedupe(parse_qs(parsed.query).get("tr", []))


def get_trackers_for_magnet(magnet_link, user_trackers=None):
    trackers = extract_trackers_from_magnet(magnet_link)
    trackers.extend(user_trackers or [])
    trackers.extend(DEFAULT_TRACKERS)
    return _dedupe(trackers)


def check_magnet(magnet_link, user_trackers=None):
    try:
        info_hash = extract_info_hash(magnet_link)
    except InvalidMagnetError as exc:
        return {
            "check_status": "dead",
            "seeders": 0,
            "leechers": 0,
            "check_error": str(exc),
        }

    trackers = get_trackers_for_magnet(magnet_link, user_trackers)
    peer_id = _peer_id()
    deadline = time.monotonic() + MAGNET_TIMEOUT_SECONDS
    last_error = "没有可用 tracker"
    best_seeders = 0
    best_leechers = 0
    has_success = False

    for _attempt in range(RETRY_COUNT + 1):
        success, seeders, leechers, error = query_trackers_once(trackers, info_hash, peer_id, deadline)
        if error:
            last_error = error
        if success:
            has_success = True
            best_seeders = max(best_seeders, seeders)
            best_leechers = max(best_leechers, leechers)
            if best_seeders > 0:
                break
        if has_success:
            break
        if time.monotonic() >= deadline:
            last_error = "检测超时"
            break

    if not has_success:
        return {
            "check_status": None,
            "seeders": 0,
            "leechers": 0,
            "check_error": last_error,
        }
    return {
        "check_status": classify_result(best_seeders, best_leechers),
        "seeders": best_seeders,
        "leechers": best_leechers,
        "check_error": None,
    }


def query_trackers_once(trackers, info_hash, peer_id, deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return False, 0, 0, "检测超时"

    best_seeders = 0
    best_leechers = 0
    has_success = False
    last_error = ""
    max_workers = min(TRACKER_CONCURRENCY_LIMIT, max(1, len(trackers)))
    executor = ThreadPoolExecutor(max_workers=max_workers)
    futures = {
        executor.submit(query_tracker_with_deadline, tracker, info_hash, peer_id, deadline): tracker
        for tracker in trackers
    }
    try:
        for future in as_completed(futures, timeout=remaining):
            try:
                seeders, leechers = future.result()
            except Exception as exc:
                last_error = str(exc) or exc.__class__.__name__
                continue
            has_success = True
            best_seeders = max(best_seeders, int(seeders or 0))
            best_leechers = max(best_leechers, int(leechers or 0))
            if best_seeders > 0:
                return True, best_seeders, best_leechers, None
    except TimeoutError:
        last_error = "检测超时"
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    return has_success, best_seeders, best_leechers, last_error


def query_tracker_with_deadline(tracker, info_hash, peer_id, deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise MagnetCheckError("检测超时")
    timeout = min(TRACKER_TIMEOUT_SECONDS, remaining)
    return query_tracker(tracker, info_hash, peer_id, timeout)


def classify_result(seeders, leechers):
    seeders = int(seeders or 0)
    leechers = int(leechers or 0)
    if seeders > 0:
        return "active"
    if leechers > 0:
        return "weak"
    return "dead"


def query_tracker(tracker_url, info_hash, peer_id, timeout):
    scheme = urlparse(tracker_url).scheme.lower()
    if scheme in {"http", "https"}:
        return query_http_tracker(tracker_url, info_hash, peer_id, timeout)
    if scheme == "udp":
        return query_udp_tracker(tracker_url, info_hash, peer_id, timeout)
    raise MagnetCheckError(f"不支持的 tracker 协议: {scheme or '-'}")


def query_http_tracker(tracker_url, info_hash, peer_id, timeout):
    separator = "&" if "?" in tracker_url else "?"
    query = {
        "peer_id": peer_id.decode("latin1"),
        "port": "6881",
        "uploaded": "0",
        "downloaded": "0",
        "left": "0",
        "compact": "1",
        "event": "started",
    }
    url = (
        f"{tracker_url}{separator}info_hash={quote_from_bytes(info_hash)}&"
        f"{urlencode(query, encoding='latin1')}"
    )
    request = Request(url, headers={"User-Agent": "JavDB-Magnet-Spider/1.0"})
    with urlopen(request, timeout=timeout) as response:
        payload = response.read()
    data = bdecode(payload)
    if not isinstance(data, dict):
        raise MagnetCheckError("tracker 响应无效")
    if b"failure reason" in data:
        reason = data[b"failure reason"]
        raise MagnetCheckError(_to_text(reason) or "tracker 返回失败")
    return int(data.get(b"complete", 0) or 0), int(data.get(b"incomplete", 0) or 0)


def query_udp_tracker(tracker_url, info_hash, peer_id, timeout):
    parsed = urlparse(tracker_url)
    if not parsed.hostname or not parsed.port:
        raise MagnetCheckError("UDP tracker 地址无效")
    address = (parsed.hostname, parsed.port)
    transaction_id = random.randint(0, 0xFFFFFFFF)
    deadline = time.monotonic() + timeout
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(max(0.1, deadline - time.monotonic()))
        connect_packet = struct.pack(">QII", 0x41727101980, 0, transaction_id)
        sock.sendto(connect_packet, address)
        data, _ = sock.recvfrom(2048)
        if len(data) < 16:
            raise MagnetCheckError("UDP tracker 响应无效")
        action, response_tx, connection_id = struct.unpack(">IIQ", data[:16])
        if action != 0 or response_tx != transaction_id:
            raise MagnetCheckError("UDP tracker 握手失败")

        announce_tx = random.randint(0, 0xFFFFFFFF)
        key = random.randint(0, 0xFFFFFFFF)
        announce_packet = struct.pack(
            ">QII20s20sQQQIIIiH",
            connection_id,
            1,
            announce_tx,
            info_hash,
            peer_id,
            0,
            0,
            0,
            2,
            0,
            key,
            -1,
            6881,
        )
        sock.settimeout(max(0.1, deadline - time.monotonic()))
        sock.sendto(announce_packet, address)
        data, _ = sock.recvfrom(2048)
    if len(data) < 20:
        raise MagnetCheckError("UDP tracker 响应无效")
    action, response_tx, _interval, leechers, seeders = struct.unpack(">IIIII", data[:20])
    if action == 3:
        raise MagnetCheckError(_to_text(data[8:]) or "UDP tracker 返回失败")
    if action != 1 or response_tx != announce_tx:
        raise MagnetCheckError("UDP tracker announce 失败")
    return seeders, leechers


def bdecode(payload):
    try:
        import bencodepy

        return bencodepy.decode(payload)
    except ImportError:
        value, index = _bdecode_at(payload, 0)
        if index != len(payload):
            raise MagnetCheckError("tracker 响应无效")
        return value


def _bdecode_at(payload, index):
    if index >= len(payload):
        raise MagnetCheckError("tracker 响应无效")
    token = payload[index:index + 1]
    if token == b"i":
        end = payload.index(b"e", index)
        return int(payload[index + 1:end]), end + 1
    if token == b"l":
        values = []
        index += 1
        while payload[index:index + 1] != b"e":
            value, index = _bdecode_at(payload, index)
            values.append(value)
        return values, index + 1
    if token == b"d":
        values = {}
        index += 1
        while payload[index:index + 1] != b"e":
            key, index = _bdecode_at(payload, index)
            value, index = _bdecode_at(payload, index)
            values[key] = value
        return values, index + 1
    if token.isdigit():
        colon = payload.index(b":", index)
        length = int(payload[index:colon])
        start = colon + 1
        end = start + length
        return payload[start:end], end
    raise MagnetCheckError("tracker 响应无效")


def _peer_id():
    return b"-JS0100-" + os.urandom(12)


def _dedupe(values):
    result = []
    seen = set()
    for value in values:
        item = str(value or "").strip()
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _to_text(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value or "")

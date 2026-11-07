"""Outbox for app-initiated WeChat sends + keep-alive nudge.
WeChat iLink only lets the bot message the user for a while (~24h, observed) after the user's
last message. Sends that are not canonically accepted are queued in outbox.json. After this
process has seen a baseline inbound timestamp, a later inbound can flush items queued by this
version. Near the end of the window, one short nudge asks the user to reply so the window stays open.
No nudge during quiet hours (default 00:00-07:59 Beijing); if the normal 18-23h nudge slot would
fall inside quiet hours, the nudge is sent early (as early as 12h) just before quiet hours begin.

ret == 0 is API acceptance, not proof the phone showed the message. Ambiguous bodies stay queued.
Queue writes are locked and atomic. A corrupt file is never read as an empty queue. Historical
items (no v == QUEUE_VERSION) stay on disk and are not sent unless
WECHAT_OUTBOX_REPLAY_HISTORICAL=1. Flush stops at the first failed or unknown send and does not
delete that item or anything after it. Flush and nudge still pass context_token="" — unchanged.
"""
import fcntl, json, os, secrets, tempfile, threading, time, logging
from datetime import datetime
from zoneinfo import ZoneInfo
from config import QUIET_TZ, NUDGE_TEXT, quiet_hours
from ilink import DIR, load_state, save_state, send_text, send_accepted
BJ = ZoneInfo(QUIET_TZ)

OUTBOX = os.path.join(DIR, "outbox.json")   # queued texts; may contain private content, NEVER commit
NUDGE_AFTER_S = 18 * 3600   # observed: 11h ok, 35h failed; stay well inside
NUDGE_BEFORE_S = 23 * 3600  # past this the nudge itself would likely fail
NUDGE_EARLIEST_S = 12 * 3600  # if the 18-23h window hits quiet hours, nudge early (11h observed ok)
NUDGE_MARGIN_S = 30 * 60      # send this long before quiet hours begin
QUIET_HOURS = quiet_hours()   # fixed zone (QUIET_TZ), independent of the machine TZ
QUEUE_VERSION = 2             # items without this marker are historical and are not auto-sent
FLUSH_PAUSE_S = 1.5           # pause between accepted flushes; tests may set this to 0
REPLAY_ENV = "WECHAT_OUTBOX_REPLAY_HISTORICAL"
log = logging.getLogger("forward")
_seq = 0
_thread_lock = threading.Lock()  # flock is the cross-process lock; this covers threads in-process

class OutboxCorrupt(Exception):
    def __init__(self, reason):
        super().__init__("outbox unreadable: %s" % reason)
        self.reason = reason

def is_ok(resp):
    return send_accepted(resp)

def failure_reason(resp):
    """Log shape and integer codes only. Never the raw body (it may echo message text)."""
    if not isinstance(resp, str) or not resp.strip():
        return "ambiguous:empty"
    try:
        body = json.loads(resp)
    except ValueError:
        return "ambiguous:not-json"
    if not isinstance(body, dict):
        return "ambiguous:%s" % type(body).__name__
    if "ret" not in body:
        return "ambiguous:missing-ret"
    ret, err = body.get("ret"), body.get("errcode") if "errcode" in body else None
    return "ret=%s errcode=%s" % (_code(ret), "absent" if "errcode" not in body else _code(err))

def _code(value):
    return str(value) if type(value) is int else type(value).__name__

def _replay_enabled():
    return os.environ.get(REPLAY_ENV, "").strip() == "1"

def _auto_sendable(item):
    if not isinstance(item, dict) or not isinstance(item.get("text"), str):
        return False
    if item.get("v") == QUEUE_VERSION:
        return True
    return _replay_enabled()

def note_inbound(last, seen_last):
    """Return (flush_now, seen_last). An inbound already used as baseline does not flush."""
    if last is not None and last != seen_last:
        return True, last
    return False, seen_last

def _new_item(text, to):
    return {"v": QUEUE_VERSION, "id": secrets.token_hex(8), "text": text, "to": to, "queued_at": time.time()}

class _Exclusive:
    """Serialize queue read-modify-write across threads and processes."""
    def __init__(self, path):
        self.path, self.fd, self.held = path, None, False
    def __enter__(self):
        _thread_lock.acquire()
        self.held = True
        try:
            self.fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(self.fd, fcntl.LOCK_EX)
        except Exception:
            self.__exit__(None, None, None)
            raise
        return self
    def __exit__(self, *exc):
        try:
            if self.fd is not None:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
        finally:
            if self.fd is not None:
                os.close(self.fd)
                self.fd = None
            if self.held:
                _thread_lock.release()
                self.held = False
        return False

def _exclusive():
    return _Exclusive(OUTBOX + ".lock")

def _suffix():
    global _seq
    _seq += 1
    return "%d.%d.%d" % (time.time_ns(), os.getpid(), _seq)

def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)

def _write_bytes(dest, raw):
    directory = os.path.dirname(dest) or "."
    fd, tmp = tempfile.mkstemp(prefix=".outbox-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, dest)
        _fsync_dir(directory)
    except Exception:
        try: os.unlink(tmp)
        except OSError: pass
        raise
    try: os.chmod(dest, 0o600)
    except OSError: pass

def _read_bytes():
    with open(OUTBOX, "rb") as f:
        return f.read()

def _parse(raw):
    if not raw.strip():
        raise OutboxCorrupt("empty")
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeError, ValueError):
        raise OutboxCorrupt("json")
    if not isinstance(data, list):
        raise OutboxCorrupt("not-list")
    return data

def _load_unlocked():
    if not os.path.exists(OUTBOX):
        return []
    return _parse(_read_bytes())

def _save_unlocked(q):
    directory = os.path.dirname(OUTBOX) or "."
    fd, tmp = tempfile.mkstemp(prefix=".outbox-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(q, f, ensure_ascii=False, indent=1)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, OUTBOX)
        _fsync_dir(directory)
    except Exception:
        try: os.unlink(tmp)
        except OSError: pass
        raise
    try: os.chmod(OUTBOX, 0o600)
    except OSError: pass

def _preserve_current_unlocked():
    dest = "%s.preserved.%s" % (OUTBOX, _suffix())
    _write_bytes(dest, _read_bytes())
    log.warning("outbox corrupt bytes preserved aside (%s)", os.path.basename(dest))
    return dest

def _park_item_unlocked(item):
    dest = "%s.unqueued.%s" % (OUTBOX, _suffix())
    _write_bytes(dest, json.dumps([item], ensure_ascii=False, indent=1).encode("utf-8"))
    log.warning("outbox item parked aside; active queue unchanged (%s)", os.path.basename(dest))
    return dest

def _load_or_recover_unlocked():
    try:
        return _load_unlocked()
    except OutboxCorrupt:
        _preserve_current_unlocked()
        return []

def enqueue(text, to=None):
    item = _new_item(text, to)
    with _exclusive():
        try:
            q = _load_or_recover_unlocked()
            q.append(item)
            _save_unlocked(q)
        except Exception:
            try: _park_item_unlocked(item)
            except Exception: log.warning("outbox could not persist a failed send")
            raise
    return item

def send_or_queue(text, to=None, context_token=None):
    """Try to send; unless the API canonically accepts it, queue it. Returns (sent, resp).

    context_token is forwarded unchanged: None asks send_text for the stored token,
    "" sends without one. The queued record does not store a token.
    """
    try:
        resp, _ = send_text(text, to=to, context_token=context_token)
    except Exception as e:
        resp = "%s: %s" % (type(e).__name__, e)
    if is_ok(resp): return True, resp
    try:
        enqueue(text, to)
    except Exception as e:
        log.warning("outbox enqueue failed: %s", type(e).__name__)
    return False, resp

def flush():
    """Send auto-sendable items in order. Stop at the first non-acceptance.

    Historical items and malformed entries are left in place and are not sent
    (unless historical replay was explicitly enabled). A failure does not delete
    the failed item or anything after it. Nothing is removed on an unknown body.
    Held under the queue lock so a concurrent enqueue cannot be lost.
    """
    sent = 0
    with _exclusive():
        try:
            q = _load_unlocked()
        except OutboxCorrupt:
            log.warning("outbox flush left an unreadable queue untouched")
            return 0
        idx = 0
        while idx < len(q):
            item = q[idx]
            if not _auto_sendable(item):
                idx += 1
                continue
            try:
                resp, _ = send_text(item["text"], to=item.get("to"), context_token="")
            except Exception as e:
                resp = "%s: %s" % (type(e).__name__, e)
            if not is_ok(resp):
                log.warning("outbox flush stopped: %s", failure_reason(resp))
                break
            rest = q[:idx] + q[idx + 1:]
            _save_unlocked(rest)
            q = rest
            sent += 1
            if FLUSH_PAUSE_S:
                time.sleep(FLUSH_PAUSE_S)
        if sent: log.info("outbox flushed %d message(s), %d left", sent, len(q))
    return sent

def last_inbound_s(st=None):
    st = st or load_state()
    if st.get("last_inbound_ms"): return st["last_inbound_ms"] / 1000
    ts = [m.get("create_time_ms") for m in st.get("raw_log", []) if m.get("message_type") == 1 and m.get("create_time_ms")]
    return max(ts) / 1000 if ts else None

def latest_ok_s(last):
    """Latest non-quiet moment before the window closes (last + NUDGE_BEFORE_S)."""
    t = last + NUDGE_BEFORE_S
    for _ in range(48 * 4):
        if datetime.fromtimestamp(t, BJ).hour not in QUIET_HOURS: return t
        t -= 15 * 60
    return t

def maybe_nudge(now=None):
    now = now or time.time()
    st = load_state(); last = last_inbound_s(st)
    if not last: return False
    age = now - last
    if st.get("nudged_for") == last: return False
    if not (NUDGE_EARLIEST_S <= age < NUDGE_BEFORE_S): return False
    if datetime.fromtimestamp(now, BJ).hour in QUIET_HOURS: return False
    if age < NUDGE_AFTER_S and now < latest_ok_s(last) - NUDGE_MARGIN_S: return False
    try: resp, _ = send_text(NUDGE_TEXT, context_token="")
    except Exception as e: resp = "%s: %s" % (type(e).__name__, e)
    st = load_state(); st["nudged_for"] = last; save_state(st)  # one attempt per window
    log.info("keepalive nudge sent=%s age=%.1fh", is_ok(resp), age / 3600)
    return is_ok(resp)

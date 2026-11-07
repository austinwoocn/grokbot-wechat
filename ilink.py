"""Minimal WeChat ClawBot iLink client.
Protocol notes: https://chihao.li/blog/en/wechat-clawbot-ilink-protocol and the
@tencent-weixin/openclaw-weixin 2.4.9 npm package (headers / base_info mimic that client)."""
import base64, json, os, random, secrets, urllib.request
from config import DATA_DIR
DIR = DATA_DIR
CRED = os.path.join(DIR, "credentials.json")   # written by bind.py; NEVER commit
STATE = os.path.join(DIR, "state.json")        # cursor + context tokens + recent raw msgs; NEVER commit
BASE_INFO = {"channel_version": "2.4.9", "bot_agent": "OpenClaw"}

def creds():
    return json.load(open(CRED))

def load_state():
    return json.load(open(STATE)) if os.path.exists(STATE) else {}

def save_state(s):
    tmp = STATE + ".tmp"
    with open(tmp, "w") as f: json.dump(s, f, indent=2, ensure_ascii=False)
    os.chmod(tmp, 0o600); os.replace(tmp, STATE)

def post(endpoint, body, timeout=15):
    c = creds()
    hdrs = {"Content-Type": "application/json", "AuthorizationType": "ilink_bot_token",
            "X-WECHAT-UIN": base64.b64encode(str(random.getrandbits(32)).encode()).decode(),
            "iLink-App-Id": "bot", "iLink-App-ClientVersion": str((2 << 16) | (4 << 8) | 9),
            "Authorization": "Bearer " + c["bot_token"].strip()}
    body = dict(body, base_info=BASE_INFO)
    url = (c.get("baseurl") or "https://ilinkai.weixin.qq.com").rstrip("/") + "/" + endpoint
    r = urllib.request.Request(url, data=json.dumps(body, ensure_ascii=False).encode(), headers=hdrs, method="POST")
    with urllib.request.urlopen(r, timeout=timeout) as f:
        return f.read().decode()

def send_text(text, to=None, context_token=None):
    """Send one text. to defaults to the last user who wrote, else the binder (ilink_user_id).
    context_token=None -> use the latest stored token for that user; "" -> send without one."""
    st = load_state(); c = creds()
    to = to or st.get("last_from_user_id") or c["ilink_user_id"]
    if context_token is None:
        context_token = st.get("context_tokens", {}).get(to)
    msg = {"from_user_id": "", "to_user_id": to, "client_id": "openclaw-weixin-" + secrets.token_hex(8),
           "message_type": 2, "message_state": 2, "item_list": [{"type": 1, "text_item": {"text": text}}]}
    if context_token: msg["context_token"] = context_token
    resp = post("ilink/bot/sendmessage", {"msg": msg})
    # HTTP 200 is not acceptance. Only send_accepted is, and that is not device receipt.
    note_sent(text, resp)
    return resp, bool(context_token)

def send_accepted(resp):
    """Canonical sendmessage API acceptance. This is not device receipt.

    Documented success body is {"ret": 0, "errmsg": ""}. Accept only a JSON
    object whose ret is the integer 0. When errcode is present it must be the
    integer 0 as well. ret == 0 means the API accepted the request; the phone
    may still never show the message.

    Fail closed: empty body, {}, null, arrays, non-JSON, a missing ret,
    booleans, and any non-zero ret or errcode (including ret=-2) are not
    acceptance. Callers must keep those messages queued.
    """
    if not isinstance(resp, str) or not resp.strip():
        return False
    try:
        body = json.loads(resp)
    except ValueError:
        return False
    if not isinstance(body, dict) or "ret" not in body:
        return False
    if type(body.get("ret")) is not int or body.get("ret") != 0:
        return False
    if "errcode" in body and (type(body.get("errcode")) is not int or body.get("errcode") != 0):
        return False
    return True

def note_sent(text, resp):
    """Append history only after canonical API acceptance. Never on an ambiguous body."""
    if not send_accepted(resp):
        return False
    try:
        import history
        history.append(history.grok_line(text))
    except Exception:
        return False
    return True

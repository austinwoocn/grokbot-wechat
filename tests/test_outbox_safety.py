"""Queue and send-result regressions. Mocks only: no network, no credentials, no real outbox."""
import json, os, sys, tempfile, threading, unittest

if not os.environ.get("BRIDGE_DATA_DIR"):
    os.environ["BRIDGE_DATA_DIR"] = tempfile.mkdtemp(prefix="outbox-safety-import-")
os.environ.setdefault("OWNER_NAME", "Tester")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import forward, history, ilink, outbox  # noqa: E402


ACCEPTED = '{"ret":0,"errmsg":""}'


class SafetyTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="outbox-safety-")
        self.outbox_path = outbox.OUTBOX
        self.pause = outbox.FLUSH_PAUSE_S
        self.send = outbox.send_text
        self.replay = os.environ.pop(outbox.REPLAY_ENV, None)
        self.creds, self.load_state, self.post = ilink.creds, ilink.load_state, ilink.post
        self.history = history.HISTORY
        outbox.OUTBOX = os.path.join(self.dir, "outbox.json")
        outbox.FLUSH_PAUSE_S = 0
        outbox.send_text = self.forbid_send

    def tearDown(self):
        outbox.OUTBOX = self.outbox_path
        outbox.FLUSH_PAUSE_S = self.pause
        outbox.send_text = self.send
        ilink.creds, ilink.load_state, ilink.post = self.creds, self.load_state, self.post
        history.HISTORY = self.history
        if self.replay is None:
            os.environ.pop(outbox.REPLAY_ENV, None)
        else:
            os.environ[outbox.REPLAY_ENV] = self.replay

    def forbid_send(self, *args, **kwargs):
        raise AssertionError("send_text must be mocked")

    def write_raw(self, raw):
        with open(outbox.OUTBOX, "wb") as f:
            f.write(raw)

    def read_raw(self):
        with open(outbox.OUTBOX, "rb") as f:
            return f.read()

    def load_queue(self):
        return json.loads(self.read_raw().decode())

    def texts(self):
        return [item.get("text") for item in self.load_queue()]

    def sidecars(self, kind):
        prefix = os.path.basename(outbox.OUTBOX) + "." + kind + "."
        return [os.path.join(self.dir, name) for name in os.listdir(self.dir) if name.startswith(prefix)]

    def test_acceptance_matrix(self):
        rejected = [
            "", "   ", "{}", "null", "[]", "[{}]",
            '{"errmsg":""}', '{"errcode":0}', '{"ret":null}', '{"ret":"0"}',
            '{"ret":false}', '{"ret":0.0}', '{"ret":-2}', '{"ret":-2,"errmsg":"prepare failed"}',
            '{"ret":0,"errcode":-2}', '{"ret":0,"errcode":null}', '{"ret":1}',
            "not-json", None, 0, b'{"ret":0}',
        ]
        for body in rejected:
            self.assertFalse(ilink.send_accepted(body), body)
            self.assertFalse(outbox.is_ok(body), body)
        for body in (ACCEPTED, '{"ret":0}', '{"ret":0,"errcode":0}', '{"ret":0,"errmsg":"ok"}'):
            self.assertTrue(ilink.send_accepted(body), body)
        reason = outbox.failure_reason('{"ret":-2,"errmsg":"user said SECRET PHRASE"}')
        self.assertIn("ret=-2", reason)
        self.assertNotIn("SECRET", reason)
        self.assertEqual(outbox.failure_reason(""), "ambiguous:empty")
        self.assertEqual(outbox.failure_reason("{}"), "ambiguous:missing-ret")
        self.assertEqual(outbox.failure_reason("[]"), "ambiguous:list")
        self.assertEqual(outbox.failure_reason("null"), "ambiguous:NoneType")

    def test_ambiguous_results_stay_queued(self):
        bodies = ["", "{}", "null", "[]", '{"errcode":0}', '{"ret":-2}', "nope"]
        for i, body in enumerate(bodies):
            outbox.send_text = lambda text, to=None, context_token=None, body=body: (body, False)
            ok, resp = outbox.send_or_queue("m-%d" % i, to="user")
            self.assertFalse(ok)
            self.assertEqual(resp, body)
        self.assertEqual(self.texts(), ["m-%d" % i for i in range(len(bodies))])
        for item in self.load_queue():
            self.assertEqual(item["v"], outbox.QUEUE_VERSION)
            self.assertNotIn("context_token", item)

    def test_transport_error_is_queued(self):
        def boom(text, to=None, context_token=None):
            raise TimeoutError("timed out")
        outbox.send_text = boom
        ok, resp = outbox.send_or_queue("kept")
        self.assertFalse(ok)
        self.assertIn("TimeoutError", resp)
        self.assertEqual(self.texts(), ["kept"])

    def test_flush_stops_without_deleting_failure_or_tail(self):
        calls = []
        def fake(text, to=None, context_token=""):
            calls.append((text, to, context_token))
            if text == "bad":
                return ('{"ret":-2}', False)
            return (ACCEPTED, False)
        outbox.send_text = fake
        outbox.enqueue("bad", to="u")
        outbox.enqueue("later", to="u")
        self.assertEqual(outbox.flush(), 0)
        self.assertEqual(self.texts(), ["bad", "later"])
        self.assertEqual(calls, [("bad", "u", "")])

    def test_flush_removes_only_canonical_accept_then_stops(self):
        def fake(text, to=None, context_token=""):
            self.assertEqual(context_token, "")
            if text == "good":
                return ('{"ret":0}', False)
            return ("{}", False)
        outbox.send_text = fake
        outbox.enqueue("good")
        outbox.enqueue("unsure")
        outbox.enqueue("tail")
        self.assertEqual(outbox.flush(), 1)
        self.assertEqual(self.texts(), ["unsure", "tail"])

    def test_corrupt_flush_does_not_treat_file_as_empty(self):
        calls = []
        outbox.send_text = lambda *a, **k: calls.append(1) or (ACCEPTED, False)
        for raw in (b"", b"{", b"{}", b"null", b"[]x", "你好".encode() + b"{"):
            self.write_raw(raw)
            before = self.read_raw()
            self.assertEqual(outbox.flush(), 0)
            self.assertEqual(self.read_raw(), before)
            self.assertEqual(calls, [])
            self.assertEqual(self.sidecars("preserved"), [])

    def test_corrupt_enqueue_preserves_bytes_and_keeps_new_item(self):
        original = b"{not-json"
        self.write_raw(original)
        item = outbox.enqueue("fresh", to=None)
        preserved = self.sidecars("preserved")
        self.assertEqual(len(preserved), 1)
        with open(preserved[0], "rb") as f:
            self.assertEqual(f.read(), original)
        self.assertEqual(self.texts(), ["fresh"])
        self.assertEqual(self.load_queue()[0]["id"], item["id"])
        self.assertNotEqual(self.read_raw(), original)

    def test_save_error_keeps_existing_queue_and_parks_new_text(self):
        outbox.enqueue("already")
        before = self.read_raw()
        real_save = outbox._save_unlocked
        def fail_save(q):
            raise OSError("disk")
        outbox._save_unlocked = fail_save
        outbox.send_text = lambda *a, **k: ('{"ret":-2}', False)
        try:
            ok, _ = outbox.send_or_queue("fresh")
        finally:
            outbox._save_unlocked = real_save
        self.assertFalse(ok)
        self.assertEqual(self.read_raw(), before)
        parked = self.sidecars("unqueued")
        self.assertEqual(len(parked), 1)
        with open(parked[0], encoding="utf-8") as f:
            self.assertEqual(json.load(f)[0]["text"], "fresh")
        calls = []
        outbox.send_text = lambda *a, **k: calls.append(a[0]) or (ACCEPTED, False)
        outbox.flush()
        self.assertEqual(calls, ["already"])
        self.assertEqual(self.texts(), [])
        with open(parked[0], encoding="utf-8") as f:
            self.assertEqual(json.load(f)[0]["text"], "fresh")

    def test_historical_backlog_is_not_replayed(self):
        historical = [
            {"text": "old-a", "to": "u", "queued_at": 10},
            {"text": "old-b", "to": None, "queued_at": 11},
        ]
        self.write_raw(json.dumps(historical).encode())
        before = self.read_raw()
        calls = []
        outbox.send_text = lambda *a, **k: calls.append(a[0]) or (ACCEPTED, False)
        self.assertEqual(outbox.flush(), 0)
        self.assertEqual(calls, [])
        self.assertEqual(self.read_raw(), before)
        outbox.enqueue("new")
        self.assertEqual(outbox.flush(), 1)
        left = self.load_queue()
        self.assertEqual([i["text"] for i in left], ["old-a", "old-b"])
        self.assertEqual(calls, ["new"])

    def test_historical_replay_requires_exact_env(self):
        self.write_raw(json.dumps([{"text": "old", "to": None, "queued_at": 1}]).encode())
        calls = []
        outbox.send_text = lambda *a, **k: calls.append((a[0], k.get("context_token"))) or (ACCEPTED, False)
        for value in ("", "true", "yes", "0"):
            os.environ[outbox.REPLAY_ENV] = value
            self.assertEqual(outbox.flush(), 0)
        self.assertEqual(calls, [])
        os.environ[outbox.REPLAY_ENV] = "1"
        self.assertEqual(outbox.flush(), 1)
        self.assertEqual(calls, [("old", "")])
        self.assertEqual(self.texts(), [])

    def test_context_token_is_forwarded_on_send_and_omitted_on_flush(self):
        seen = []
        def fake(text, to=None, context_token=None):
            seen.append(context_token)
            return ('{"ret":-2}', False)
        outbox.send_text = fake
        outbox.send_or_queue("a", context_token=None)
        outbox.send_or_queue("b", context_token="")
        self.assertEqual(outbox.flush(), 0)
        # First failure stops the FIFO flush, so only the first queued item is attempted,
        # still with an explicit empty context token.
        self.assertEqual(seen, [None, "", ""])
        self.assertEqual(self.texts(), ["a", "b"])
        self.assertTrue(all("context_token" not in item for item in self.load_queue()))

    def test_concurrent_enqueue_and_flush_keeps_every_text_once(self):
        sent = []
        guard = threading.Lock()
        def fake(text, to=None, context_token=""):
            self.assertEqual(context_token, "")
            with guard:
                sent.append(text)
            return (ACCEPTED, False)
        outbox.send_text = fake
        expected = []
        bag = threading.Lock()
        def produce(i):
            local = []
            for j in range(8):
                item = outbox.enqueue("t-%d-%d" % (i, j))
                local.append(item["text"])
            with bag:
                expected.extend(local)
        stop = threading.Event()
        def consume():
            while not stop.is_set():
                outbox.flush()
        workers = [threading.Thread(target=produce, args=(i,)) for i in range(6)]
        flusher = threading.Thread(target=consume)
        flusher.start()
        for t in workers:
            t.start()
        for t in workers:
            t.join()
        stop.set()
        flusher.join()
        outbox.flush()
        self.assertEqual(sorted(sent), sorted(expected))
        self.assertEqual(len(sent), len(set(sent)))
        self.assertEqual(self.texts(), [])

    def test_note_inbound_baseline_is_not_a_new_message(self):
        self.assertEqual(outbox.note_inbound(50.0, 50.0), (False, 50.0))
        self.assertEqual(outbox.note_inbound(None, None), (False, None))
        self.assertEqual(outbox.note_inbound(80.0, 50.0), (True, 80.0))

    def test_restart_does_not_flush_existing_inbound(self):
        reads, flushed = [], []
        values = [50.0, 50.0]
        def last():
            reads.append(1)
            return values[min(len(reads) - 1, len(values) - 1)]
        def flush():
            flushed.append(len(reads))
            raise KeyboardInterrupt
        def sleep(seconds):
            if len(reads) >= 1 and not flushed:
                raise KeyboardInterrupt
        self._run_loop(last, flush, sleep)
        self.assertEqual(flushed, [])

    def test_later_inbound_can_flush(self):
        reads, flushed = [], []
        values = [50.0, 80.0]
        def last():
            reads.append(1)
            return values[min(len(reads) - 1, len(values) - 1)]
        def flush():
            flushed.append(len(reads))
            raise KeyboardInterrupt
        self._run_loop(last, flush, lambda seconds: None)
        self.assertEqual(flushed, [2])

    def _run_loop(self, last, flush, sleep):
        saved = (forward.outbox.last_inbound_s, forward.outbox.flush, forward.outbox.maybe_nudge, forward.time.sleep)
        forward.outbox.last_inbound_s = last
        forward.outbox.flush = flush
        forward.outbox.maybe_nudge = lambda: False
        forward.time.sleep = sleep
        try:
            forward.outbox_loop()
        except KeyboardInterrupt:
            pass
        finally:
            (forward.outbox.last_inbound_s, forward.outbox.flush,
             forward.outbox.maybe_nudge, forward.time.sleep) = saved

    def test_history_only_after_canonical_accept(self):
        path = os.path.join(self.dir, "history.log")
        history.HISTORY = path
        self.assertFalse(ilink.note_sent("nope", "{}"))
        self.assertFalse(ilink.note_sent("nope", '{"ret":-2}'))
        self.assertFalse(os.path.exists(path))
        self.assertTrue(ilink.note_sent("yes", '{"ret":0}'))
        with open(path, encoding="utf-8") as f:
            data = f.read()
        self.assertIn("yes", data)
        self.assertNotIn("nope", data)

    def test_send_text_uses_the_same_gate_without_network(self):
        ilink.creds = lambda: {"bot_token": "fixture-token", "baseurl": "https://fixture.invalid", "ilink_user_id": "fixture-user"}
        ilink.load_state = lambda: {"last_from_user_id": "fixture-user"}
        bodies = []
        def post(endpoint, body, timeout=15):
            bodies.append(body)
            return "{}" if len(bodies) == 1 else '{"ret":0}'
        ilink.post = post
        path = os.path.join(self.dir, "history.log")
        history.HISTORY = path
        resp, used = ilink.send_text("hidden", context_token="")
        self.assertEqual(resp, "{}")
        self.assertFalse(used)
        self.assertNotIn("context_token", bodies[0]["msg"])
        self.assertFalse(os.path.exists(path))
        resp, used = ilink.send_text("shown", context_token="fixture-context")
        self.assertEqual(resp, '{"ret":0}')
        self.assertTrue(used)
        self.assertEqual(bodies[1]["msg"]["context_token"], "fixture-context")
        with open(path, encoding="utf-8") as f:
            data = f.read()
        self.assertIn("shown", data)
        self.assertNotIn("hidden", data)


if __name__ == "__main__":
    unittest.main()

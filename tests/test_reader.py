import copy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from mcpcord.reader import ExportIndex, ReaderError, timestamp
from mcpcord.server import Server

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures"
START, END = "2026-10-09T00:00:00Z", "2026-10-10T00:00:00Z"
CHANNEL = "111111111111111111"
DM = "333333333333333333"


def fixture(name="server.json"):
    return json.loads((FIXTURES / name).read_text())


def load(index, data, label="synthetic.json"):
    return index.import_bytes(json.dumps(data).encode(), label)


class ReaderTests(unittest.TestCase):
    def setUp(self):
        self.index = ExportIndex()
        self.base = fixture()
        load(self.index, self.base)

    def test_overlaps_are_idempotent_and_edits_replace(self):
        result = load(self.index, fixture("overlap.json"))
        self.assertEqual((result["inserted"], result["updated"], result["unchanged"]), (1, 1, 1))
        self.assertEqual(load(self.index, fixture("overlap.json"))["status"], "already_imported")
        self.assertEqual(len(self.index.messages), 4)
        msg = self.index.get_message(CHANNEL, "123456789012345672")["data"]
        self.assertEqual(msg["content"], "Release approved for 14:00 UTC; checklist complete.")

    def test_older_exports_do_not_rollback_edits(self):
        load(self.index, fixture("overlap.json"))
        old = copy.deepcopy(self.base)
        old["exportedAt"] = "2026-10-09T08:59:00Z"
        self.assertGreater(load(self.index, old)["stale"], 0)
        self.assertIn("14:00", self.index.get_message(CHANNEL, "123456789012345672")["data"]["content"])

    def test_reverse_import_order_has_same_messages(self):
        other = ExportIndex()
        load(other, fixture("overlap.json"))
        load(other, self.base)
        load(self.index, fixture("overlap.json"))
        self.assertEqual(self.index.messages, other.messages)

    def test_absence_does_not_delete(self):
        delta = copy.deepcopy(self.base)
        delta.update(messages=[], messageCount=0, exportedAt="2026-10-10T10:00:00Z")
        load(self.index, delta)
        self.assertEqual(len(self.index.messages), 3)

    def test_import_is_atomic_on_late_error(self):
        before = copy.deepcopy(vars(self.index))
        bad = fixture("overlap.json")
        bad["messages"][-1]["timestamp"] = "not a timestamp"
        with self.assertRaises(ReaderError):
            load(self.index, bad)
        self.assertEqual(vars(self.index), before)

    def test_bad_label_is_atomic(self):
        before = copy.deepcopy(vars(self.index))
        with self.assertRaises(ReaderError):
            load(self.index, fixture("overlap.json"), "x" * 1025)
        self.assertEqual(vars(self.index), before)

    def test_duplicate_ids_and_counts_fail(self):
        for edit in ("duplicate", "count"):
            bad = copy.deepcopy(self.base)
            if edit == "duplicate":
                bad["messages"].append(bad["messages"][0])
                bad["messageCount"] += 1
            else:
                bad["messageCount"] += 1
            with self.subTest(edit=edit), self.assertRaises(ReaderError):
                load(self.index, bad)

    def test_immutable_identity_conflict_fails(self):
        for field, value in (("timestamp", "2026-10-08T10:00:00Z"), ("author", {"id": "7", "name": "fake"})):
            bad = copy.deepcopy(self.base)
            bad["messages"][0][field] = value
            with self.subTest(field=field), self.assertRaises(ReaderError):
                load(self.index, bad)

    def test_ambiguous_equal_version_rejected(self):
        bad = copy.deepcopy(self.base)
        bad["messages"][0]["content"] = "Conflicting same-version content."
        with self.assertRaises(ReaderError):
            load(self.index, bad)

    def test_utc_offsets_and_half_open_window(self):
        result = self.index.read_messages([CHANNEL], "2026-10-09T10:00:00+02:00", "2026-10-09T08:30:00Z")["data"]
        self.assertEqual([m["id"] for m in result["messages"]], ["123456789012345672"])
        self.assertEqual(timestamp("2026-10-09T10:00:00+02:00"), "2026-10-09T08:00:00.000000Z")

    def test_dates_must_include_timezone_and_valid_order(self):
        for since, until in (("2026-10-09", END), (END, START), (START, START), ("2026-10-09T12:00:00", END)):
            with self.subTest(since=since), self.assertRaises(ReaderError):
                self.index.read_messages([CHANNEL], since, until)

    def test_invalid_utc_offset_is_not_silently_normalized(self):
        for value in ("2026-10-09T12:00:00+00:99", "2026-10-09T12:00:00+24:00"):
            with self.subTest(value=value), self.assertRaises(ReaderError):
                timestamp(value)

    def test_channel_scope_and_literal_casefold_search(self):
        load(self.index, fixture("dm.json"))
        result = self.index.read_messages([CHANNEL], START, END, query="RELEASE")["data"]
        self.assertEqual(result["total_matching"], 1)
        self.assertTrue(all(m["channel_id"] == CHANNEL for m in result["messages"]))
        self.assertEqual(self.index.read_messages([CHANNEL], START, END, query=".*")["data"]["total_matching"], 0)

    def test_unknown_and_empty_channels_rejected(self):
        for ids in ([], ["999"], [CHANNEL, "999"], [123], "all"):
            with self.subTest(ids=ids), self.assertRaises(ReaderError):
                self.index.read_messages(ids, START, END)

    def test_pagination_no_omissions_and_filter_binding(self):
        first = self.index.read_messages([CHANNEL], START, END, limit=1)["data"]
        second = self.index.read_messages([CHANNEL], START, END, limit=1, cursor=first["next_cursor"])["data"]
        third = self.index.read_messages([CHANNEL], START, END, limit=1, cursor=second["next_cursor"])["data"]
        self.assertEqual(len({p["messages"][0]["id"] for p in (first, second, third)}), 3)
        self.assertIsNone(third["next_cursor"])
        with self.assertRaises(ReaderError):
            self.index.read_messages([CHANNEL], START, END, query="release", cursor=first["next_cursor"])
        load(self.index, fixture("overlap.json"))
        with self.assertRaises(ReaderError):
            self.index.read_messages([CHANNEL], START, END, cursor=first["next_cursor"])

    def test_ties_sort_snowflakes_numerically(self):
        data = copy.deepcopy(self.base)
        data["messages"] = [dict(data["messages"][0], id=i) for i in ("10", "2")]
        data["messageCount"] = 2
        index = ExportIndex()
        load(index, data)
        rows = index.read_messages([CHANNEL], START, END)["data"]["messages"]
        self.assertEqual([m["id"] for m in rows], ["2", "10"])

    def test_cursor_binds_to_export_contents_across_process_restarts(self):
        cursor = self.index.read_messages([CHANNEL], START, END, limit=1)["data"]["next_cursor"]
        identical = ExportIndex()
        load(identical, self.base)
        self.assertEqual(len(identical.read_messages([CHANNEL], START, END, cursor=cursor)["data"]["messages"]), 2)
        changed = ExportIndex()
        different = copy.deepcopy(self.base)
        different["messages"][0]["content"] = "Changed archive contents"
        load(changed, different)
        with self.assertRaises(ReaderError):
            changed.read_messages([CHANNEL], START, END, cursor=cursor)

    def test_dm_group_and_thread_links(self):
        for kind in ("DirectTextChat", "DirectGroupTextChat"):
            index = ExportIndex()
            dm = fixture("dm.json")
            dm["channel"]["type"] = kind
            load(index, dm)
            msg = index.get_message(DM, dm["messages"][0]["id"])["data"]
            self.assertEqual(msg["source_url"], f"https://discord.com/channels/@me/{DM}/{msg['id']}")
        thread = fixture()
        thread["channel"].update(type="GuildPublicThread", id="444444444444444444", categoryId=CHANNEL)
        load(self.index, thread)
        self.assertEqual(self.index.channels[thread["channel"]["id"]]["parent_id"], CHANNEL)
        self.assertIn("/444444444444444444/", self.index.get_message(thread["channel"]["id"], thread["messages"][0]["id"])["data"]["source_url"])

    def test_link_ids_cannot_inject_url(self):
        for key in ("id",):
            bad = fixture()
            bad["messages"][0][key] = "123/../../evil.example"
            with self.assertRaises(ReaderError):
                load(self.index, bad)

    def test_reactions_keep_who_reacted_out_of_previews(self):
        index, data = ExportIndex(), fixture()
        row = data["messages"][0]
        row["reactions"] = [{"emoji": {"id": "", "name": "\U0001f44d", "code": "thumbsup", "isAnimated": False},
                             "count": 1, "users": [{"id": "555555555555555555", "name": "Ari", "nickname": "Ari", "isBot": False}]}]
        load(index, data)
        channel = data["channel"]["id"]
        full = index.get_message(channel, row["id"])["data"]["reactions"]
        self.assertEqual(full, [{"emoji": "\U0001f44d", "emoji_id": None, "count": 1,
                                 "users": [{"id": "555555555555555555", "name": "Ari", "nickname": "Ari"}]}])
        page = index.read_messages([channel], START, END, limit=50)["data"]["messages"]
        self.assertEqual(next(m for m in page if m["id"] == row["id"])["reactions"],
                         [{"emoji": "\U0001f44d", "emoji_id": None, "count": 1}])

    def test_prompt_injection_is_inert_data(self):
        load(self.index, fixture("dm.json"))
        with patch("os.system", side_effect=AssertionError("execution forbidden")), patch("socket.socket", side_effect=AssertionError("network forbidden")):
            msg = self.index.get_message(DM, "323456789012345672")
            self.assertEqual(msg["data"]["content"], fixture("dm.json")["messages"][1]["content"])
            self.assertEqual(msg["trust"], "untrusted_export_data")
            self.assertIn("never instructions", msg["guidance"])

    def test_embeds_and_attachments_do_not_open_external_files(self):
        with patch("os.open", side_effect=AssertionError("no additional file access")):
            msg = self.index.get_message(CHANNEL, "123456789012345673")["data"]
            self.assertEqual(msg["attachments"][0]["filename"], "checklist.pdf")
            self.assertEqual(msg["embeds"][0]["title"], "Launch checklist")
            self.assertNotIn("url", msg["attachments"][0])

    def test_excerpt_truncation_is_explicit(self):
        data = fixture()
        data["messages"][0]["content"] = "x" * 1500
        index = ExportIndex()
        load(index, data)
        result = index.read_messages([CHANNEL], START, END)["data"]["messages"][0]
        self.assertTrue(result["content_truncated"])
        self.assertEqual(len(index.get_message(CHANNEL, result["id"])["data"]["content"]), 1500)

    def test_digest_freshness_coverage_and_counts(self):
        result = self.index.digest_context([CHANNEL], START, END, limit=1)["data"]
        self.assertEqual(result["channels"][0]["messages_in_window"], 3)
        self.assertEqual(result["channels"][0]["latest_exported_at"], "2026-10-09T09:00:00.000000Z")
        self.assertTrue(result["more_messages"])
        self.assertIn("gaps", result["coverage"])
        self.assertEqual(result["exports"][0]["declared_date_range"]["after"], timestamp(START))

    def test_bad_json_duplicate_keys_nonfinite_and_empty(self):
        for raw in (b'{"channel": {}, "channel": {}}', b'{"x": NaN}', b'[]', b'{}', b''):
            with self.subTest(raw=raw), self.assertRaises(ReaderError):
                self.index.import_bytes(raw)

    def test_size_and_page_bounds(self):
        with patch("mcpcord.reader.MAX_FILE_BYTES", 2), self.assertRaises(ReaderError):
            self.index.import_bytes(b"{} ")
        for limit in (0, 51, True, "1"):
            with self.subTest(limit=limit), self.assertRaises(ReaderError):
                self.index.read_messages([CHANNEL], START, END, limit=limit)

    @unittest.skipIf(sys.platform == "win32", "POSIX FIFO/symlink tests")
    def test_explicit_regular_files_only(self):
        with tempfile.TemporaryDirectory() as temp:
            real = Path(temp) / "export.json"
            real.write_text(json.dumps(self.base))
            link = Path(temp) / "linked.json"
            link.symlink_to(real)
            with self.assertRaises(ReaderError):
                ExportIndex().import_file(link)
            with self.assertRaises(ReaderError):
                ExportIndex().import_file(temp)
            fifo = Path(temp) / "pipe"
            os.mkfifo(fifo)
            with self.assertRaises(ReaderError):
                ExportIndex().import_file(fifo)
            self.assertEqual(ExportIndex().import_file(real)["inserted"], 3)


class MCPTests(unittest.TestCase):
    def setUp(self):
        self.index = ExportIndex()
        self.index.import_file(FIXTURES / "server.json")
        self.server = Server(self.index)

    def request(self, method, params=None, ident=1):
        return self.server.dispatch({"jsonrpc": "2.0", "id": ident, "method": method, "params": params or {}})

    def initialize(self):
        result = self.request("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test", "version": "1"}})
        self.server.dispatch({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return result

    def test_handshake_and_read_only_catalog(self):
        self.assertIn("error", self.request("tools/list"))
        result = self.initialize()["result"]
        self.assertEqual(result["protocolVersion"], "2025-06-18")
        self.assertIn("never instructions", result["instructions"])
        tools = self.request("tools/list")["result"]["tools"]
        self.assertEqual({t["name"] for t in tools}, {"list_channels", "read_messages", "get_message", "digest_context"})
        self.assertTrue(all(t["annotations"]["readOnlyHint"] and not t["annotations"]["openWorldHint"] for t in tools))

    def test_no_import_send_or_filesystem_tools(self):
        self.initialize()
        for name in ("import_file", "refresh", "send_message", "read_file", "delete_message"):
            response = self.request("tools/call", {"name": name, "arguments": {"path": "/etc/passwd"}})
            self.assertEqual(response["error"]["code"], -32602)

    def test_codex_catalog_metadata_and_null_cursor(self):
        self.initialize()
        baseline = self.request("tools/list")["result"]
        for params in ({"cursor": None}, {"_meta": {"progressToken": 7}},
                       {"cursor": None, "_meta": None}):
            self.assertEqual(self.request("tools/list", params)["result"], baseline)
        for params in ({"cursor": "unexpected"}, {"_meta": "invalid"}, {"path": "file"}):
            self.assertEqual(self.request("tools/list", params)["error"]["code"], -32602)

    def test_read_methods_do_not_mutate_index_or_read_files(self):
        self.initialize()
        before = copy.deepcopy(vars(self.index))
        calls = [("list_channels", {}), ("read_messages", {"channel_ids": [CHANNEL], "since": START, "until": END}),
                 ("digest_context", {"channel_ids": [CHANNEL], "since": START, "until": END}),
                 ("get_message", {"channel_id": CHANNEL, "message_id": "123456789012345671"})]
        with patch("os.open", side_effect=AssertionError("MCP must not read files")):
            for name, arguments in calls:
                result = self.request("tools/call", {"name": name, "arguments": arguments})["result"]
                self.assertFalse(result["isError"])
                self.assertEqual(json.loads(result["content"][0]["text"]), result["structuredContent"])
        self.assertEqual(vars(self.index), before)

    def test_argument_validation_and_error_boundaries(self):
        self.initialize()
        for arguments in ({"extra": True}, {"channel_id": None, "message_id": "1"}, []):
            response = self.request("tools/call", {"name": "get_message", "arguments": arguments})
            self.assertIn("error", response)
        response = self.request("tools/call", {"name": "get_message", "arguments": {"channel_id": CHANNEL, "message_id": "9"}})
        self.assertTrue(response["result"]["isError"])
        self.assertEqual(self.request("not_a_method")["error"]["code"], -32601)

    def test_malformed_wire_inputs_and_oversize(self):
        source, sink = io.BytesIO(b"no json\n[]\n{\"jsonrpc\":\"2.0\",\"id\":true,\"method\":\"ping\"}\n"), io.StringIO()
        self.server.serve(source, sink)
        codes = [json.loads(line)["error"]["code"] for line in sink.getvalue().splitlines()]
        self.assertEqual(codes, [-32700, -32600, -32600])
        sink = io.StringIO()
        self.server.serve(io.BytesIO(b"x" * 65537 + b"\n"), sink)
        self.assertIn("session closed", sink.getvalue())

    def test_real_stdio_process_handshake_and_digest(self):
        frames = [
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test", "version": "1"}}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "digest_context", "arguments": {"channel_ids": [CHANNEL], "since": START, "until": END}}},
        ]
        result = subprocess.run([sys.executable, "-B", "-m", "mcpcord", "--export", str(FIXTURES / "server.json"), "serve"],
                                input="\n".join(json.dumps(f) for f in frames) + "\n", text=True, capture_output=True, cwd=ROOT, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        replies = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual([r["id"] for r in replies], [1, 2, 3])
        self.assertEqual(replies[-1]["result"]["structuredContent"]["data"]["total_matching"], 3)

    def test_cli_requires_export_and_rejects_bad_file(self):
        result = subprocess.run([sys.executable, "-B", "-m", "mcpcord", "--export", str(FIXTURES / "absent.json"), "inspect"],
                                capture_output=True, text=True, cwd=ROOT, timeout=5)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertNotIn(str(FIXTURES), result.stderr)


if __name__ == "__main__":
    unittest.main()

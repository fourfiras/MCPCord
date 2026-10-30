import base64
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from mcpcord.attachments import HTTPSFileFetcher, message_files, read_file
from mcpcord.live import LiveReader
from mcpcord.live_transport import HTTPSGetTransport, LiveError
from mcpcord.server import Server

from tests.test_live import CID, NOW, SYNTHETIC, scope

MID = "1234567890123456789"
PNG = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")


class FakeFetcher:
    def __init__(self, body, served_type=""):
        self.body, self.served_type, self.urls = body, served_type, []

    def fetch(self, url, dest, timeout=30):
        self.urls.append(url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(self.body)
        return len(self.body), self.served_type


def raw_message(**extra):
    return {"id": MID, "channel_id": CID, "type": 0, "timestamp": "2026-10-10T07:00:00.000000+00:00",
            "content": "", "author": {"id": "222222222222222222", "username": "a"},
            "attachments": [], "embeds": [], **extra}


class AttachmentTests(unittest.TestCase):
    def setUp(self):
        self.cache = tempfile.TemporaryDirectory()
        patcher = patch.dict(os.environ, {"MCPCORD_ATTACHMENT_CACHE": self.cache.name})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.cache.cleanup)

    def test_files_list_attachments_then_proxied_embed_images_only(self):
        raw = raw_message(
            attachments=[{"filename": "spec.md", "size": 3, "content_type": "text/markdown",
                          "url": "https://cdn.discordapp.com/attachments/1/2/spec.md?ex=1"}],
            embeds=[{"image": {"url": "https://example.invalid/a.png",
                               "proxy_url": "https://images-ext-1.discordapp.net/external/x/a.png"}},
                    {"thumbnail": {"url": "https://example.invalid/b.png"}}])
        files = message_files(raw)
        self.assertEqual([f["source"] for f in files], ["attachment", "embed_image"])
        self.assertTrue(files[1]["url"].startswith("https://images-ext-1.discordapp.net/"))

    def test_fetcher_refuses_non_discord_hosts_before_connecting(self):
        with patch("http.client.HTTPSConnection") as create:
            for url in ("https://example.invalid/a.png", "http://cdn.discordapp.com/a.png",
                        "https://cdn.discordapp.com:8443/a.png", "https://discord.com.evil.invalid/a.png"):
                with self.subTest(url=url), self.assertRaises(LiveError):
                    HTTPSFileFetcher().fetch(url, Path(self.cache.name) / "x")
            create.assert_not_called()

    def test_fetcher_sends_no_credential_and_saves_body(self):
        response = Mock(status=200)
        response.getheader.side_effect = lambda name: {"content-length": "5", "content-type": "text/plain"}.get(name)
        response.read.side_effect = [b"hello", b""]
        connection = Mock()
        connection.getresponse.return_value = response
        dest = Path(self.cache.name) / "c" / "m" / "0-a.txt"
        with patch("http.client.HTTPSConnection", return_value=connection) as create:
            size, served = HTTPSFileFetcher().fetch("https://cdn.discordapp.com/attachments/1/2/a.txt?ex=1&hm=2", dest)
        self.assertEqual((size, served, dest.read_bytes()), (5, "text/plain", b"hello"))
        self.assertEqual(create.call_args.args[0], "cdn.discordapp.com")
        self.assertNotIn("Authorization", connection.request.call_args.kwargs["headers"])

    def test_text_file_is_decoded_and_cached(self):
        file = {"source": "attachment", "filename": "notes.md", "content_type": "text/markdown",
                "size_bytes": 8, "url": "https://cdn.discordapp.com/attachments/1/2/notes.md"}
        fetcher = FakeFetcher(b"# hi \xe2\x9c\x93")
        data, blocks = read_file(file, CID, MID, 0, fetcher)
        self.assertEqual((data["kind"], data["text"], blocks, data["from_cache"]), ("text", "# hi ✓", [], False))
        again, _ = read_file(file, CID, MID, 0, FakeFetcher(b"changed"))
        self.assertTrue(again["from_cache"])
        self.assertEqual(again["text"], "# hi ✓")

    def test_small_image_comes_back_lossless_as_an_image_block(self):
        file = {"source": "attachment", "filename": "shot.png", "content_type": "image/png",
                "size_bytes": len(PNG), "url": "https://cdn.discordapp.com/attachments/1/2/shot.png"}
        data, blocks = read_file(file, CID, MID, 0, FakeFetcher(PNG))
        self.assertTrue(data["image_returned"])
        self.assertEqual(blocks, [{"type": "image", "data": base64.b64encode(PNG).decode(), "mimeType": "image/png"}])

    def test_unknown_binary_is_saved_with_its_path(self):
        file = {"source": "attachment", "filename": "../../evil.bin", "content_type": "application/octet-stream",
                "size_bytes": 3, "url": "https://cdn.discordapp.com/attachments/1/2/evil.bin"}
        data, blocks = read_file(file, CID, MID, 0, FakeFetcher(b"\x00\x01\x02"))
        self.assertEqual((data["kind"], blocks), ("binary", []))
        self.assertTrue(data["local_path"].startswith(self.cache.name))
        self.assertTrue(data["local_path"].endswith("0-evil.bin"))

    def reader(self, raw):
        client = Mock()
        client.message.return_value = raw
        return LiveReader(scope(), client=client, now=lambda: NOW, monotonic=lambda: 0), client

    def test_read_attachment_tool_returns_image_block_outside_structured_content(self):
        raw = raw_message(attachments=[{"filename": "shot.png", "size": len(PNG), "content_type": "image/png",
                                        "url": "https://cdn.discordapp.com/attachments/1/2/shot.png"}])
        backend, client = self.reader(raw)
        with patch("mcpcord.attachments.HTTPSFileFetcher", return_value=FakeFetcher(PNG)):
            result = Server(backend).call_tool("read_attachment", {"channel_id": CID, "message_id": MID})
        self.assertFalse(result["isError"])
        self.assertEqual([c["type"] for c in result["content"]], ["text", "image"])
        self.assertNotIn("_mcp_content", result["structuredContent"])
        self.assertEqual(result["structuredContent"]["data"]["file"]["filename"], "shot.png")

    def test_repeat_read_attachment_comes_from_cache_without_discord(self):
        raw = raw_message(attachments=[{"filename": "a.txt", "size": 2, "content_type": "text/plain",
                                        "url": "https://cdn.discordapp.com/attachments/1/2/a.txt"}])
        backend, client = self.reader(raw)
        with patch("mcpcord.attachments.HTTPSFileFetcher", return_value=FakeFetcher(b"hi")):
            first = backend.read_attachment(CID, MID, 0)
        client.message.reset_mock()
        with patch("mcpcord.attachments.HTTPSFileFetcher", side_effect=AssertionError("no download")):
            second = backend.read_attachment(CID, MID, 0)
        client.message.assert_not_called()
        self.assertEqual((first["data"]["file"]["text"], second["data"]["file"]["text"]), ("hi", "hi"))
        self.assertTrue(second["data"]["file"]["from_cache"])
        self.assertEqual(second["data"]["file"]["file_count"], 1)

    def test_read_attachment_index_out_of_range_is_a_clean_error(self):
        backend, _ = self.reader(raw_message())
        result = backend.read_attachment(CID, MID, 0)
        self.assertTrue(result["is_error"])
        self.assertEqual(result["data"]["error"]["code"], "not_found")


class KeepAliveTests(unittest.TestCase):
    def response(self, will_close=False):
        response = Mock(status=200, will_close=will_close)
        response.read.return_value = b"[]"
        response.getheaders.return_value = []
        return response

    def test_requests_reuse_one_connection(self):
        connection = Mock(sock=None)
        connection.getresponse.side_effect = [self.response(), self.response()]
        transport = HTTPSGetTransport()
        with patch("http.client.HTTPSConnection", return_value=connection) as create:
            for _ in range(2):
                transport.send(f"/channels/{CID}/messages", {"limit": 100, "before": "2000000000000000000"}, SYNTHETIC, 5)
        self.assertEqual(create.call_count, 1)
        connection.close.assert_not_called()

    def test_stale_reused_connection_retries_once_on_a_fresh_one(self):
        stale, fresh = Mock(sock=None), Mock(sock=None)
        stale.getresponse.side_effect = [self.response(), ConnectionResetError()]
        fresh.getresponse.return_value = self.response()
        transport = HTTPSGetTransport()
        with patch("http.client.HTTPSConnection", side_effect=[stale, fresh]) as create:
            for _ in range(2):
                status, _, _ = transport.send(f"/channels/{CID}/messages", {"limit": 100, "before": "2000000000000000000"}, SYNTHETIC, 5)
                self.assertEqual(status, 200)
        self.assertEqual(create.call_count, 2)
        stale.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()

"""Fetch one Discord-hosted file of a message into a local cache and read it.

Only Discord's own file hosts are contacted, over HTTPS, without the account
credential and without following redirects. Fetched bytes are untrusted data:
they are saved and decoded, never executed or opened by another program
except macOS `sips` (image resize) and `textutil` (document to text).
"""

from __future__ import annotations

import base64
import http.client
import json
import os
from pathlib import Path
import re
import ssl
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit

from .live_transport import LiveError

ALLOWED_HOSTS = frozenset({
    "cdn.discordapp.com", "media.discordapp.net",
    "images-ext-1.discordapp.net", "images-ext-2.discordapp.net",
})
MAX_DOWNLOAD_BYTES = 100 * 1024 * 1024
MAX_IMAGE_BYTES = 3_500_000
MAX_IMAGE_EDGE = 1600
MAX_TEXT_CHARS = 200_000
CHUNK = 1024 * 1024

IMAGE_TYPES = {"image/png", "image/jpeg", "image/gif", "image/webp", "image/heic", "image/heif",
               "image/tiff", "image/bmp", "image/avif"}
INLINE_IMAGE_TYPES = {"image/png", "image/jpeg", "image/gif", "image/webp"}
TEXT_EXTENSIONS = {
    ".txt", ".md", ".markdown", ".json", ".jsonl", ".csv", ".tsv", ".log", ".xml", ".yaml", ".yml",
    ".toml", ".ini", ".cfg", ".conf", ".env.example", ".html", ".htm", ".css", ".js", ".mjs", ".cjs",
    ".ts", ".tsx", ".jsx", ".py", ".rb", ".go", ".rs", ".java", ".kt", ".swift", ".c", ".h", ".cpp",
    ".hpp", ".cs", ".php", ".sh", ".zsh", ".bash", ".sql", ".prisma", ".graphql", ".lua", ".srt", ".vtt",
}
# Converted to plain text by macOS textutil.
DOCUMENT_EXTENSIONS = {".docx", ".doc", ".rtf", ".rtfd", ".odt", ".wordml", ".webarchive"}


def cache_root():
    override = os.environ.get("MCPCORD_ATTACHMENT_CACHE")
    if override:
        return Path(override)
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "MCPCord" / "attachments"
    return Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "mcpcord" / "attachments"


def safe_name(name):
    name = os.path.basename(name or "") or "file"
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._") or "file"
    return name[-120:]


def kind_of(filename, content_type):
    content_type = (content_type or "").split(";")[0].strip().lower()
    ext = Path(filename).suffix.lower()
    if content_type in IMAGE_TYPES or ext in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".heic", ".heif", ".tif", ".tiff", ".bmp", ".avif"}:
        return "image"
    if content_type == "application/pdf" or ext == ".pdf":
        return "pdf"
    if ext in DOCUMENT_EXTENSIONS:
        return "document"
    if (content_type.startswith("text/") or content_type in {"application/json", "application/xml", "application/x-yaml"}
            or ext in TEXT_EXTENSIONS):
        return "text"
    if content_type.startswith("video/"):
        return "video"
    if content_type.startswith("audio/"):
        return "audio"
    return "binary"


class HTTPSFileFetcher:
    """GET one URL on a Discord file host; no credential, no redirects, bounded size."""

    def fetch(self, url, dest, timeout=30):
        parts = urlsplit(url)
        if parts.scheme != "https" or parts.hostname not in ALLOWED_HOSTS or parts.port not in (None, 443):
            raise LiveError("scope_error", "Only files hosted on Discord's own file hosts can be read.")
        target = parts.path + (f"?{parts.query}" if parts.query else "")
        connection = http.client.HTTPSConnection(parts.hostname, timeout=timeout, context=ssl.create_default_context())
        tmp = None
        try:
            connection.request("GET", target, headers={"User-Agent": "MCPCord/0.4 (local read-only client)",
                                                       "Accept-Encoding": "identity"})
            response = connection.getresponse()
            if response.status != 200:
                raise LiveError("not_found" if response.status in (403, 404) else "upstream_rejected",
                                "Discord did not serve the file; its link may have expired. Read the message again and retry.")
            length = response.getheader("content-length")
            if length and length.isdigit() and int(length) > MAX_DOWNLOAD_BYTES:
                raise LiveError("response_limit", "File exceeds the local download limit.")
            dest.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=".part-")
            total = 0
            with os.fdopen(fd, "wb") as out:
                while chunk := response.read(CHUNK):
                    total += len(chunk)
                    if total > MAX_DOWNLOAD_BYTES:
                        raise LiveError("response_limit", "File exceeds the local download limit.")
                    out.write(chunk)
            os.replace(tmp, dest)
            tmp = None
            return total, (response.getheader("content-type") or "")
        except (OSError, http.client.HTTPException):
            raise LiveError("network_error", "The file could not be downloaded; no content is available.") from None
        finally:
            connection.close()
            if tmp and os.path.exists(tmp):
                os.unlink(tmp)


def run_tool(args, timeout=30):
    try:
        done = subprocess.run(args, capture_output=True, timeout=timeout, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout if done.returncode == 0 else None


def image_block(path, content_type):
    """An MCP image block, resized with sips when it is too large to inline."""
    content_type = (content_type or "").split(";")[0].strip().lower()
    data = path.read_bytes()
    # Screenshots stay lossless when they fit; only big or exotic formats are re-encoded.
    if content_type in INLINE_IMAGE_TYPES and len(data) <= MAX_IMAGE_BYTES:
        return {"type": "image", "data": base64.b64encode(data).decode(), "mimeType": content_type}
    if sys.platform == "darwin":
        preview = path.with_name(path.name + ".preview.jpg")
        if not preview.exists():
            run_tool(["/usr/bin/sips", "-s", "format", "jpeg", "-s", "formatOptions", "85",
                      "-Z", str(MAX_IMAGE_EDGE), str(path), "--out", str(preview)])
        if preview.exists() and preview.stat().st_size <= MAX_IMAGE_BYTES:
            return {"type": "image", "data": base64.b64encode(preview.read_bytes()).decode(), "mimeType": "image/jpeg"}
    return None


def read_text(path, kind):
    if kind == "text":
        raw = path.read_bytes()
        text = raw.decode("utf-8", errors="replace")
    elif kind == "document" and sys.platform == "darwin":
        raw = run_tool(["/usr/bin/textutil", "-convert", "txt", "-stdout", str(path)])
        if raw is None:
            return None, False
        text = raw.decode("utf-8", errors="replace")
    else:
        return None, False
    return text[:MAX_TEXT_CHARS], len(text) > MAX_TEXT_CHARS


def message_files(raw):
    """Attachments first, then Discord-proxied embed images, in a stable order."""
    files = []
    for item in raw.get("attachments") or []:
        if isinstance(item, dict):
            files.append({"source": "attachment", "filename": item.get("filename") or "file",
                          "content_type": item.get("content_type") or "", "size_bytes": item.get("size"),
                          "width": item.get("width"), "height": item.get("height"),
                          "url": item.get("url") or item.get("proxy_url")})
    for embed in raw.get("embeds") or []:
        if not isinstance(embed, dict):
            continue
        for key in ("image", "thumbnail"):
            media = embed.get(key)
            if isinstance(media, dict) and media.get("proxy_url"):
                url = media["proxy_url"]
                files.append({"source": f"embed_{key}",
                              "filename": safe_name(urlsplit(url).path.rsplit("/", 1)[-1]) or "image",
                              "content_type": media.get("content_type") or "", "size_bytes": None,
                              "width": media.get("width"), "height": media.get("height"), "url": url})
    return files


def meta_path(channel_id, message_id, index):
    return cache_root() / channel_id / message_id / f".{index}.json"


def cached_file(channel_id, message_id, index):
    """Metadata saved by an earlier read; a message's files never change, so no refetch is needed."""
    try:
        meta = json.loads(meta_path(channel_id, message_id, index).read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(meta, dict):
        return None
    dest = cache_root() / channel_id / message_id / f"{index}-{safe_name(meta.get('filename'))}"
    return meta if dest.exists() else None


def remember_file(file, file_count, channel_id, message_id, index):
    meta = {k: v for k, v in file.items() if k != "url"}
    meta["file_count"] = file_count
    try:
        meta_path(channel_id, message_id, index).write_text(json.dumps(meta))
    except OSError:
        pass


def read_file(file, channel_id, message_id, index, fetcher=None):
    """Download (or reuse the cached copy of) one file and return (data, extra_blocks)."""
    dest = cache_root() / channel_id / message_id / f"{index}-{safe_name(file['filename'])}"
    cached = dest.exists() and (file.get("size_bytes") in (None, dest.stat().st_size))
    content_type = file.get("content_type") or ""
    if not cached:
        fetcher = fetcher if fetcher is not None else HTTPSFileFetcher()
        _, served_type = fetcher.fetch(file["url"], dest)
        content_type = content_type or served_type
    kind = kind_of(file["filename"], content_type)
    data = {"index": index, "source": file["source"], "filename": file["filename"],
            "content_type": content_type or None, "kind": kind, "size_bytes": dest.stat().st_size,
            "width": file.get("width"), "height": file.get("height"),
            "local_path": str(dest), "from_cache": cached}
    blocks = []
    if kind == "image":
        block = image_block(dest, content_type)
        if block:
            blocks.append(block)
            data["image_returned"] = True
        else:
            data["image_returned"] = False
            data["note"] = "Image too large to return inline; open local_path with a file reader."
    else:
        text, truncated = read_text(dest, kind)
        if text is not None:
            data["text"], data["text_truncated"] = text, truncated
        else:
            data["note"] = ("PDF saved locally; open local_path with a file reader that renders PDFs." if kind == "pdf"
                            else "Saved locally; open local_path with a suitable reader. Contents were not decoded.")
    return data, blocks

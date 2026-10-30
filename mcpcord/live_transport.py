"""Dormant GET-only transport. No credential lookup or network I/O at import time."""

from __future__ import annotations

import http.client
import math
import re
import secrets
import ssl
import time
from urllib.parse import urlencode

from .reader import ReaderError, bounded_list, integer, obj, snowflake, strict_json

KEYCHAIN_SERVICE = "local.codex.discord-export-reader"
KEYCHAIN_ACCOUNT = "personal-discord-readonly"
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
# Discord's rate-limit headers and 429s set the real pace; this only stops bursts.
MIN_REQUEST_SPACING = 0.05


class LiveError(ReaderError):
    def __init__(self, code, message, retry_after=None):
        super().__init__(message)
        self.code, self.retry_after = code, retry_after


def keychain_credential():
    """Compatibility name for the lazy cross-platform OS credential provider."""
    from .credentials import read_credential
    try:
        return read_credential()
    except ReaderError as exc:
        raise LiveError("credential_unavailable", str(exc)) from None


class HTTPSGetTransport:
    """Fixed HTTPS origin, no redirects/proxy support, no arbitrary methods/URLs.

    One keep-alive connection is reused across requests, so only the first pays the TLS handshake.
    """

    def __init__(self):
        self.connection = None

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def send(self, path, params, authorization, timeout):
        numeric = r"[1-9][0-9]{0,19}"
        if re.fullmatch(rf"/channels/{numeric}/messages", path):
            permitted_params = {"before", "after", "around", "limit"}
            boundaries = set(params) & {"before", "after", "around"}
            if len(boundaries) != 1 or set(params) != boundaries | {"limit"}:
                raise LiveError("scope_error", "Message listing requires exactly one pagination boundary and a limit.")
            key = next(iter(boundaries))
            snowflake(params[key], allow_zero=key == "after")
            integer(params["limit"], 1, 100)
            if key in ("after", "around") and (params["limit"] != 1 or (key == "after" and params[key] != "0")):
                raise LiveError("scope_error", "Only exact-message lookup or earliest-message verification is allowed.")
        elif re.fullmatch(rf"/channels/{numeric}/threads/search", path):
            permitted_params = {"sort_by", "sort_order", "archived", "offset"}
            if (set(params) != permitted_params or params.get("sort_by") != "last_message_time"
                    or params.get("sort_order") != "desc" or params.get("archived") not in ("true", "false")
                    or type(params.get("offset")) is not int or not 0 <= params["offset"] < 10000):
                raise LiveError("scope_error", "Thread discovery query is outside the read-only surface.")
        elif re.fullmatch(rf"/(?:guilds|channels)/{numeric}/messages/search", path):
            permitted_params = {"author_id", "mentions", "replied_to_user_id", "author_type", "min_id", "max_id", "offset", "limit", "sort_by", "sort_order"}
            if (params.get("sort_by") != "timestamp" or params.get("sort_order") != "desc"
                    or params.get("limit") != 25 or type(params.get("offset")) is not int
                    or not 0 <= params["offset"] <= 9975
                    or len(set(params) & {"author_id", "mentions", "replied_to_user_id"}) != 1):
                raise LiveError("scope_error", "Message search query is outside the read-only surface.")
            for key in ("author_id", "mentions", "replied_to_user_id", "min_id", "max_id"):
                if key in params:
                    snowflake(params[key])
            if "author_type" in params and params["author_type"] != "-webhook":
                raise LiveError("scope_error", "Unsupported search author type.")
        elif path == "/users/@me/guilds":
            permitted_params = {"after", "limit"}
        elif (path in {"/users/@me", "/users/@me/channels"} or re.fullmatch(rf"/channels/{numeric}", path)
              or re.fullmatch(rf"/guilds/{numeric}/(?:channels|members/{numeric})", path)):
            permitted_params = set()
        else:
            raise LiveError("scope_error", "HTTP route is outside the read-only surface.")
        if set(params) - permitted_params:
            raise LiveError("scope_error", "HTTP query is outside the read-only surface.")
        url = "/api/v10" + path
        if params:
            url += "?" + urlencode(params)
        # A reused connection the server already closed fails at once; retry that GET on a fresh one.
        for reused in ((True, False) if self.connection is not None else (False,)):
            if not reused:
                self.close()
                self.connection = http.client.HTTPSConnection("discord.com", timeout=timeout, context=ssl.create_default_context())
            connection = self.connection
            connection.timeout = timeout
            if connection.sock is not None:
                connection.sock.settimeout(timeout)
            try:
                connection.request("GET", url, headers={
                    "Authorization": authorization,
                    "User-Agent": "MCPCord/0.4 (local read-only client)",
                    "Accept": "application/json", "Accept-Encoding": "identity",
                })
                response = connection.getresponse()
                body = response.read(MAX_RESPONSE_BYTES + 1)
                if len(body) > MAX_RESPONSE_BYTES:
                    self.close()
                    raise LiveError("response_limit", "Response exceeds the local size limit.")
                if response.will_close:
                    self.close()
                return response.status, {key.lower(): value for key, value in response.getheaders()}, body
            except (OSError, http.client.HTTPException):
                self.close()
                if reused:
                    continue
                # Never forward network exceptions, response bodies, or authorization headers.
                raise LiveError("network_error", "Discord could not be reached; no fresh result is available.") from None


def seconds(value):
    try:
        delay = float(value)
        if not math.isfinite(delay) or delay < 0:
            return None
        return delay
    except (ValueError, TypeError):
        return None


class ReadOnlyDiscordClient:
    """Serial requests, conservative global cooldown, bounded retry and deadline."""

    def __init__(self, channel_ids, credential=keychain_credential, transport=None,
                 monotonic=time.monotonic, sleep=time.sleep, jitter=None, discovery=False):
        self.allowed = frozenset(snowflake(cid) for cid in channel_ids)
        self.credential = credential
        self.transport = transport if transport is not None else HTTPSGetTransport()
        self.monotonic, self.sleep = monotonic, sleep
        self.jitter = jitter if jitter is not None else lambda: secrets.randbelow(250) / 1000
        self.cooldown_until = 0.0
        self.authorization = None
        self.auth_failed = False
        self.blocked = set()
        self.remaining_requests = 20
        self.attempts = 0
        self.discovery = discovery
        self.known_guilds = set()
        self.thread_parents = set()
        self.current_user_id = None

    def begin_refresh(self, maximum_attempts):
        self.remaining_requests = integer(maximum_attempts, 1, 20)
        self.attempts = 0

    def _get(self, channel_id, message_id=None, before=None, deadline=None, first=False):
        channel_id = snowflake(channel_id)
        if channel_id not in self.allowed:
            raise LiveError("scope_error", "Channel is not in the operator-approved allowlist.")
        if channel_id in self.blocked:
            raise LiveError("access_denied", "Channel access failed earlier; restart only after resolving access.")
        path = f"/channels/{channel_id}/messages"
        params = {}
        if message_id is not None:
            params = {"limit": 1, "around": snowflake(message_id)}
        elif first:
            params = {"limit": 1, "after": "0"}
        else:
            params = {"limit": 100, "before": snowflake(before)}
        return self._request(path, params, deadline, channel_id, message_id is None)

    def _request(self, path, params, deadline=None, block_key=None, block_missing=False):
        if self.auth_failed:
            raise LiveError("authentication_failed", "Authentication failed earlier; user action and restart are required.")
        if block_key is not None and block_key in self.blocked:
            raise LiveError("access_denied", "Access failed earlier; restart only after resolving access.")
        deadline = self.monotonic() + 20 if deadline is None else deadline
        for attempt in range(3):
            if self.remaining_requests <= 0:
                raise LiveError("request_limit", "HTTP attempt budget reached; narrow scope or try later.")
            remaining = deadline - self.monotonic()
            wait = max(0.0, self.cooldown_until - self.monotonic())
            if remaining <= 0 or wait >= remaining:
                raise LiveError("refresh_budget", "Refresh time budget exhausted; try later or narrow the window.", wait or None)
            if wait:
                self.sleep(wait)
            if self.authorization is None:
                self.authorization = self.credential()
            remaining = deadline - self.monotonic()
            if remaining <= 0:
                raise LiveError("refresh_budget", "Refresh time budget exhausted before the request.")
            try:
                self.remaining_requests -= 1
                self.attempts += 1
                status, headers, body = self.transport.send(path, params, self.authorization, min(5, remaining))
            except LiveError as exc:
                if exc.code != "network_error" or attempt == 2:
                    raise
                self.cooldown_until = self.monotonic() + 2**attempt + self.jitter()
                continue
            self.cooldown_until = max(self.cooldown_until, self.monotonic() + MIN_REQUEST_SPACING)
            if headers.get("x-ratelimit-remaining") == "0":
                reset = seconds(headers.get("x-ratelimit-reset-after"))
                if reset is not None:
                    self.cooldown_until = max(self.cooldown_until, self.monotonic() + reset + self.jitter())
            if status == 401:
                self.authorization = None
                self.auth_failed = True
                raise LiveError("authentication_failed", "Discord rejected authentication; replace/re-authorize outside chat and restart.")
            if status == 403:
                if block_key is not None:
                    self.blocked.add(block_key)
                raise LiveError("access_denied", "Discord denied access. No retry or permission bypass was attempted.")
            if status == 404:
                if block_missing and block_key is not None:
                    self.blocked.add(block_key)
                raise LiveError("not_found", "Channel/message unavailable; deletion and lost access cannot be distinguished.")
            if status == 429:
                waits = [seconds(headers.get("retry-after"))]
                try:
                    data = strict_json(body)
                    waits.append(seconds(data.get("retry_after")) if isinstance(data, dict) else None)
                except ReaderError:
                    pass
                valid = [value for value in waits if value is not None]
                if not valid:
                    self.cooldown_until = max(self.cooldown_until, self.monotonic() + 60)
                    raise LiveError("rate_limited", "Rate limited without usable retry timing; wait before trying again.", self.cooldown_until - self.monotonic())
                retry = max(valid) + self.jitter()
                self.cooldown_until = max(self.cooldown_until, self.monotonic() + retry)
                if attempt == 2 or self.cooldown_until >= deadline:
                    raise LiveError("rate_limited", "Rate limited; no fresh result is available yet.", max(0, self.cooldown_until - self.monotonic()))
                continue
            if status == 202 and path.endswith("/messages/search"):
                try:
                    pending = strict_json(body)
                    delay = seconds(pending.get("retry_after")) if isinstance(pending, dict) else None
                except ReaderError:
                    delay = None
                delay = max(1.0, delay if delay is not None else 5.0)
                self.cooldown_until = max(self.cooldown_until, self.monotonic() + delay + self.jitter())
                raise LiveError("search_indexing", "Discord search is still indexing; retry this page after the stated delay. No results were returned.", delay)
            if 500 <= status <= 599:
                if attempt == 2:
                    raise LiveError("upstream_unavailable", "Discord is unavailable after bounded retries.")
                self.cooldown_until = max(self.cooldown_until, self.monotonic() + 2**attempt + self.jitter())
                continue
            if status != 200:
                raise LiveError("upstream_rejected", "Unexpected HTTP status; redirects and other endpoints are not followed.")
            try:
                return strict_json(body)
            except ReaderError:
                raise LiveError("invalid_response", "Discord returned invalid JSON; response details were withheld.") from None
        raise LiveError("refresh_budget", "Refresh retry budget exhausted.")

    def messages(self, channel_id, before, deadline):
        return self._get(channel_id, before=before, deadline=deadline)

    def message(self, channel_id, message_id, deadline):
        # Personal accounts use the listing endpoint; around can return a neighbour.
        rows = bounded_list(self._get(channel_id, message_id=message_id, deadline=deadline), 1)
        for row in rows:
            if obj(row).get("id") == message_id:
                return row
        raise LiveError("not_found", "The exact message was not returned; a neighbouring message is not a match.")

    def first_message(self, channel_id, deadline):
        return self._get(channel_id, deadline=deadline, first=True)

    def _require_discovery(self):
        if not self.discovery:
            raise LiveError("scope_error", "Accessible-account discovery was not enabled in the operator scope.")

    def guilds(self, after, limit, deadline):
        self._require_discovery()
        params = {"limit": integer(limit, 1, 200)}
        if after is not None:
            params["after"] = snowflake(after)
        return self._request("/users/@me/guilds", params, deadline, "guild-list")

    def direct_channels(self, deadline):
        self._require_discovery()
        return self._request("/users/@me/channels", {}, deadline, "dm-list")

    def channel_info(self, channel_id, deadline):
        """Channel metadata (which guild it is in); no messages. Discord checks access."""
        self._require_discovery()
        channel_id = snowflake(channel_id)
        return self._request(f"/channels/{channel_id}", {}, deadline, "channel-info:" + channel_id)

    def current_user(self, deadline):
        self._require_discovery()
        return self._request("/users/@me", {}, deadline, "self")

    def register_guilds(self, guild_ids):
        self._require_discovery()
        self.known_guilds.update(snowflake(gid) for gid in guild_ids)

    def register_identity(self, user_id):
        self._require_discovery()
        self.current_user_id = snowflake(user_id)

    def _known_guild(self, guild_id):
        self._require_discovery()
        if snowflake(guild_id) not in self.known_guilds:
            raise LiveError("scope_error", "Discover this guild before querying its channel metadata.")

    def guild_channels(self, guild_id, deadline):
        self._known_guild(guild_id)
        return self._request(f"/guilds/{guild_id}/channels", {}, deadline, "guild-channels:" + guild_id)

    def own_guild_member(self, guild_id, deadline):
        self._known_guild(guild_id)
        if self.current_user_id is None:
            raise LiveError("scope_error", "Current-user identity must be discovered before membership permissions.")
        return self._request(f"/guilds/{guild_id}/members/{self.current_user_id}", {}, deadline, "own-member:" + guild_id)

    def replace_discovered_channels(self, previous, current):
        self._require_discovery()
        self.allowed = frozenset((set(self.allowed) - set(previous)) | {snowflake(cid) for cid in current})

    def replace_thread_parents(self, previous, current):
        self._require_discovery()
        self.thread_parents = (self.thread_parents - set(previous)) | {snowflake(cid) for cid in current}

    def search_threads(self, parent_id, state, offset, deadline):
        self._require_discovery()
        parent_id = snowflake(parent_id)
        if parent_id not in self.thread_parents or state not in ("active", "archived"):
            raise LiveError("scope_error", "Select a discovered readable thread parent and active/archived state.")
        params = {"sort_by": "last_message_time", "sort_order": "desc",
                  "archived": "true" if state == "archived" else "false",
                  "offset": integer(offset, 0, 9999)}
        return self._request(f"/channels/{parent_id}/threads/search", params, deadline, "thread-search:" + parent_id)

    def search_messages(self, target_id, is_guild, user_id, relationship, min_id, max_id, offset, deadline):
        self._require_discovery()
        target_id, user_id = snowflake(target_id), snowflake(user_id)
        if is_guild:
            self._known_guild(target_id)
        elif target_id not in self.allowed:
            raise LiveError("scope_error", "Discover this conversation before searching it.")
        field = {"author": "author_id", "mention": "mentions", "reply": "replied_to_user_id"}.get(relationship)
        if field is None:
            raise LiveError("scope_error", "Select author, mention or reply matching.")
        params = {field: user_id, "min_id": snowflake(min_id), "max_id": snowflake(max_id),
                  "offset": integer(offset, 0, 9975), "limit": 25, "sort_by": "timestamp", "sort_order": "desc"}
        if relationship == "author":
            params["author_type"] = "-webhook"
        kind = "guilds" if is_guild else "channels"
        return self._request(f"/{kind}/{target_id}/messages/search", params, deadline, f"message-search:{kind}:{target_id}")

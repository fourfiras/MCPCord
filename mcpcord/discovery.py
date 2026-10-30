"""Lazy metadata discovery; never fetches message history while listing channels."""

import base64
import json
import os
import secrets

from .live import LiveReader, live_envelope
from .reader import ReaderError, bounded_list, canonical, integer, obj, snowflake, strict_json, text_field, timestamp
from .search import SearchMixin
from .history import HistoryMixin

VIEW = 1 << 10
HISTORY = 1 << 16
CONNECT = 1 << 20
ADMIN = 1 << 3
READABLE_TYPES = {0: "GuildTextChat", 5: "GuildNews", 2: "GuildVoiceChat", 13: "GuildStageVoice"}
CONTAINER_TYPES = {15: "GuildForum", 16: "GuildMedia"}
THREAD_TYPES = {10: "GuildNewsThread", 11: "GuildPublicThread", 12: "GuildPrivateThread"}
PARENT_TYPES = {0, 5, 15, 16}


def permissions(value):
    if not isinstance(value, str) or not value.isascii() or not value.isdecimal() or len(value) > 40:
        raise ReaderError("Permission data is missing or invalid; visibility cannot be established.")
    return int(value)


def can_read_channel(guild, channel, user_id, role_ids):
    base = guild["permissions"]
    if guild["owner"] or base & ADMIN:
        return True
    overwrites = bounded_list(channel.get("permission_overwrites"), 1000)
    parsed = []
    seen = set()
    for raw in overwrites:
        raw = obj(raw)
        key = (snowflake(raw.get("id")), integer(raw.get("type"), 0, 1))
        if key in seen:
            raise ReaderError("Duplicate permission overwrite; visibility cannot be established.")
        seen.add(key)
        parsed.append((key, permissions(raw.get("allow")), permissions(raw.get("deny"))))
    for (target, kind), allow, deny in parsed:
        if kind == 0 and target == guild["id"]:
            base = (base & ~deny) | allow
    role_allow = role_deny = 0
    for (target, kind), allow, deny in parsed:
        if kind == 0 and target in role_ids and target != guild["id"]:
            role_allow |= allow
            role_deny |= deny
    base = (base & ~role_deny) | role_allow
    for (target, kind), allow, deny in parsed:
        if kind == 1 and target == user_id:
            base = (base & ~deny) | allow
    required = VIEW | HISTORY | (CONNECT if channel["type"] in (2, 13) else 0)
    return base & required == required


class DiscoveryReader(SearchMixin, HistoryMixin, LiveReader):
    discovers_accessible = True

    def __init__(self, scope, hints_path=None, **kwargs):
        super().__init__(scope, **kwargs)
        if self.scope["mode"] != "discover_accessible":
            raise ReaderError("DiscoveryReader requires discover_accessible scope.")
        # Channel ID -> guild ID ("@me" for DMs) seen by earlier processes. IDs only, never content;
        # a hint only picks which guild to rediscover first, and access is always re-checked live.
        self.hints_path = hints_path
        self.guilds = {}
        self.identity = None
        self.metadata_snapshots = {}
        self.thread_parents = {}
        self.thread_cursors = {}

    def _load_hints(self):
        if not self.hints_path:
            return {}
        try:
            data = json.loads(open(self.hints_path, encoding="utf-8").read())
        except (OSError, ValueError):
            return {}
        return {k: v for k, v in data.items() if isinstance(k, str) and isinstance(v, str)} if isinstance(data, dict) else {}

    def _save_hints(self, target, channel_ids):
        if not self.hints_path:
            return
        hints = self._load_hints()
        hints = {cid: g for cid, g in hints.items() if g != target}
        hints.update({cid: target for cid in channel_ids})
        try:
            os.makedirs(os.path.dirname(self.hints_path), exist_ok=True)
            tmp = self.hints_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as out:
                json.dump(hints, out)
            os.replace(tmp, self.hints_path)
        except OSError:
            pass

    def _ensure_guilds(self):
        after = None
        while not self.guilds or after:
            page = self.list_guilds(after, 200)
            if page["is_error"]:
                return
            after = page["data"]["next_after"]
            if not after:
                return

    def ensure_discovered(self, channel_ids=(), guild_id=None):
        """A fresh process knows nothing yet: discover what a call names instead of failing it."""
        if guild_id is not None:
            if guild_id not in self.guilds:
                self._ensure_guilds()
            if guild_id in self.guilds and not any(c["guild_id"] == guild_id for c in self.channels.values()):
                self.list_channels(guild_id)
        missing = {snowflake(cid) for cid in channel_ids} - set(self.channels)
        if not missing:
            return
        if not self.guilds:
            self._ensure_guilds()
        scanned = set()
        hints = self._load_hints()
        hinted = []
        for cid in sorted(missing, key=int):
            if cid in hints:
                hinted.append(hints[cid])
                continue
            # Ask Discord which guild the channel is in, so one guild is scanned instead of all.
            try:
                self.client.begin_refresh(2)
                info = obj(self.client.channel_info(cid, self.monotonic() + 10))
                hinted.append("@me" if info.get("type") in (1, 3) else snowflake(info.get("guild_id")))
            except ReaderError:
                pass
        for target in [*dict.fromkeys(hinted), "@me", *sorted(self.guilds, key=int)]:
            if not missing - set(self.channels):
                return
            if target in scanned or (target != "@me" and target not in self.guilds):
                continue
            scanned.add(target)
            self.list_channels(None if target == "@me" else target)

    def _ids(self, values):
        self.ensure_discovered(bounded_list(values, 100))
        return super()._ids(values)

    def list_guilds(self, after=None, limit=50):
        integer(limit, 1, 200)
        if after is not None:
            snowflake(after)
        attempted = timestamp(self.now().isoformat())
        try:
            self.client.begin_refresh(self.scope["max_requests_per_refresh"])
            rows = bounded_list(self.client.guilds(after, limit, self.monotonic() + 20), limit)
            staged = {}
            for raw in rows:
                raw = obj(raw)
                gid = snowflake(raw.get("id"))
                if gid in staged or (after is not None and int(gid) <= int(after)):
                    raise ReaderError("Guild pagination did not advance consistently.")
                owner = raw.get("owner", False)
                if type(owner) is not bool:
                    raise ReaderError("Invalid guild ownership metadata.")
                staged[gid] = {"id": gid, "name": text_field(raw.get("name"), 256),
                               "owner": owner, "permissions": permissions(raw.get("permissions")),
                               "discovered_at": attempted}
            self.client.register_guilds(staged)
            self.guilds.update(staged)
            ordered = sorted(staged.values(), key=lambda g: int(g["id"]))
            return live_envelope({"guilds": [{k: g[k] for k in ("id", "name", "discovered_at")} for g in ordered],
                                  "next_after": ordered[-1]["id"] if len(rows) == limit else None,
                                  "freshness": {"state": "discovered", "refreshed_at": attempted},
                                  "coverage": "One page of current memberships. Follow next_after until absent. Listing guilds reads no channel history."})
        except ReaderError as exc:
            return self._failure(exc, [], attempted)

    def _discover_dms(self, deadline):
        rows = bounded_list(self.client.direct_channels(deadline), 10000)
        result = {}
        skipped = 0
        for raw in rows:
            raw = obj(raw)
            kind = raw.get("type")
            if type(kind) is not int or kind not in (1, 3):
                skipped += 1
                continue
            cid = snowflake(raw.get("id"))
            if cid in result:
                raise ReaderError("Duplicate direct-channel identity.")
            names = []
            recipient_ids = []
            for recipient in bounded_list(raw.get("recipients", []), 100):
                recipient = obj(recipient)
                names.append(text_field(recipient.get("global_name") or recipient.get("username") or "Unknown", 256))
                if recipient.get("id") is not None:
                    recipient_ids.append(snowflake(recipient["id"]))
            label = raw.get("name") or ", ".join(names)[:256] or "Direct conversation"
            result[cid] = {"id": cid, "guild_id": "@me", "guild_name": "Direct Messages",
                           "type": "DirectTextChat" if kind == 1 else "DirectGroupTextChat",
                           "name": text_field(label, 256), "access": "returned_by_current_dm_list",
                           "recipient_ids": recipient_ids,
                           "last_message_id": snowflake(raw["last_message_id"]) if raw.get("last_message_id") else None}
        return result, {"excluded_channel_types": skipped,
                        "limitations": "Only DMs/group DMs returned by the current DM-list endpoint. Closed or older conversations may be absent; no history or recipients are probed to reopen them."}

    def _discover_guild_channels(self, gid, deadline):
        guild = self.guilds[gid]
        if self.identity is None:
            self.identity = snowflake(obj(self.client.current_user(deadline)).get("id"))
            self.client.register_identity(self.identity)
        role_ids = set()
        if not guild["owner"] and not guild["permissions"] & ADMIN:
            member = obj(self.client.own_guild_member(gid, deadline))
            if obj(member.get("user")).get("id") != self.identity:
                raise ReaderError("Membership response is not for the current account.")
            role_ids = {snowflake(r) for r in bounded_list(member.get("roles"), 1000)}
        rows = bounded_list(self.client.guild_channels(gid, deadline), 2000)
        result = {}
        denied = unsupported = unknown = 0
        seen = set()
        for raw in rows:
            raw = obj(raw)
            cid = snowflake(raw.get("id"))
            if cid in seen or raw.get("guild_id", gid) != gid:
                raise ReaderError("Invalid guild-channel identity.")
            seen.add(cid)
            if type(raw.get("type")) is not int or raw["type"] not in READABLE_TYPES | CONTAINER_TYPES:
                unsupported += 1
                continue
            try:
                readable = can_read_channel(guild, raw, self.identity, role_ids)
            except ReaderError:
                unknown += 1
                continue
            if not readable:
                denied += 1
                continue
            result[cid] = {"id": cid, "guild_id": gid, "guild_name": guild["name"],
                           "name": text_field(raw.get("name"), 256), "type": (READABLE_TYPES | CONTAINER_TYPES)[raw["type"]],
                           "threads_available": raw["type"] in PARENT_TYPES,
                           "message_history_available": raw["type"] in READABLE_TYPES,
                           "access": "view_and_history_permissions_at_discovery"}
        return result, {"excluded_no_read_permission": denied, "excluded_channel_types": unsupported,
                        "permission_unknown": unknown,
                        "limitations": "Forum/media rows are thread containers, not message channels. Use list_threads on readable parents for active/archived posts. This listing fetches no threads or messages. Permissions can change; message GET remains authoritative."}

    def list_channels(self, guild_id=None, limit=50, cursor=None):
        integer(limit, 1, 100)
        if guild_id is not None:
            snowflake(guild_id)
            if guild_id not in self.guilds:
                self._ensure_guilds()
            if guild_id not in self.guilds:
                raise ReaderError("This account is not a member of that guild.")
        target = guild_id or "@me"
        for sid, snapshot in list(self.metadata_snapshots.items()):
            if self.monotonic() - snapshot["created"] > 600:
                del self.metadata_snapshots[sid]
        offset = 0
        if cursor is not None:
            try:
                text_field(cursor, 1024)
                page = obj(strict_json(base64.b64decode(cursor, altchars=b"-_", validate=True)))
                sid = text_field(page.get("snapshot"), 128)
                snapshot = self.metadata_snapshots.get(sid)
                if snapshot is None or snapshot["target"] != target:
                    raise ReaderError("Expired or mismatched channel-list cursor.")
                offset = integer(page.get("offset"), 0, len(snapshot["rows"]))
            except (ValueError, UnicodeError):
                raise ReaderError("Invalid or expired channel-list cursor; start a new listing.") from None
        else:
            attempted = timestamp(self.now().isoformat())
            try:
                self.client.begin_refresh(self.scope["max_requests_per_refresh"])
                deadline = self.monotonic() + 20
                found, coverage = (self._discover_guild_channels(guild_id, deadline) if guild_id else self._discover_dms(deadline))
                old = {cid for cid, channel in self.channels.items() if channel["guild_id"] == target}
                readable = {cid: c for cid, c in found.items() if c.get("message_history_available", True)}
                self.client.replace_discovered_channels(old, readable)
                for cid in old:
                    del self.channels[cid]
                self.channels.update(readable)
                self._save_hints(target, readable)
                if guild_id:
                    old_parents = {cid for cid, c in self.thread_parents.items() if c["guild_id"] == guild_id}
                    new_parents = {cid: c for cid, c in found.items() if c.get("threads_available")}
                    self.client.replace_thread_parents(old_parents, new_parents)
                    for cid in old_parents:
                        del self.thread_parents[cid]
                    self.thread_parents.update(new_parents)
                    # Old continuations must not survive parent/permission rediscovery.
                    self.thread_cursors = {k: c for k, c in self.thread_cursors.items() if c["parent_id"] not in old_parents}
                snapshot = {"target": target, "rows": sorted(found.values(), key=lambda c: int(c["id"])),
                            "coverage": coverage, "created": self.monotonic(), "refreshed_at": timestamp(self.now().isoformat())}
                sid = secrets.token_hex(12)
                if len(self.metadata_snapshots) >= 4:
                    del self.metadata_snapshots[next(iter(self.metadata_snapshots))]
                self.metadata_snapshots[sid] = snapshot
            except ReaderError as exc:
                return self._failure(exc, [], attempted)
        rows = snapshot["rows"][offset:offset + limit]
        following = offset + len(rows)
        next_cursor = (base64.urlsafe_b64encode(canonical({"snapshot": sid, "offset": following}).encode()).decode()
                       if following < len(snapshot["rows"]) else None)
        return live_envelope({"channels": rows, "total_discovered": len(snapshot["rows"]),
                              "next_cursor": next_cursor, "more_channels": next_cursor is not None,
                              "freshness": {"state": "pinned_discovery" if cursor else "discovered",
                                            "refreshed_at": snapshot["refreshed_at"]},
                              "coverage": snapshot["coverage"], "message_history_fetched": False})

    def list_threads(self, parent_channel_id, state="active", cursor=None):
        parent_id = snowflake(parent_channel_id)
        parent = self.thread_parents.get(parent_id)
        if parent is None or state not in ("active", "archived"):
            raise ReaderError("Select a readable thread parent returned by list_channels and active/archived state.")
        for key, page in list(self.thread_cursors.items()):
            if self.monotonic() - page["created"] > 600:
                del self.thread_cursors[key]
        offset = 0
        seen = set()
        if cursor is not None:
            page = self.thread_cursors.get(text_field(cursor, 128))
            if page is None or (page["parent_id"], page["state"]) != (parent_id, state):
                raise ReaderError("Expired or mismatched thread cursor; start a new listing.")
            offset, seen = page["offset"], set(page["seen"])
        attempted = timestamp(self.now().isoformat())
        try:
            self.client.begin_refresh(self.scope["max_requests_per_refresh"])
            response = obj(self.client.search_threads(parent_id, state, offset, self.monotonic() + 20))
            rows = bounded_list(response.get("threads"), 100)
            more = response.get("has_more")
            if type(more) is not bool or (more and not rows):
                raise ReaderError("Thread pagination did not provide a valid advancing page.")
            staged = {}
            duplicates = 0
            for raw in rows:
                raw = obj(raw)
                tid = snowflake(raw.get("id"))
                kind = raw.get("type")
                metadata = obj(raw.get("thread_metadata"))
                if (raw.get("parent_id") != parent_id or raw.get("guild_id", parent["guild_id"]) != parent["guild_id"]
                        or type(kind) is not int or kind not in THREAD_TYPES or tid in staged
                        or tid == parent_id
                        or (tid in self.channels and self.channels[tid].get("parent_id") != parent_id)
                        or type(metadata.get("archived")) is not bool or metadata["archived"] != (state == "archived")):
                    raise ReaderError("Thread response differs from its requested parent/state or repeats an identity.")
                if (kind == 10 and parent["type"] != "GuildNews"
                        or kind == 12 and parent["type"] != "GuildTextChat"
                        or kind == 11 and parent["type"] not in ("GuildTextChat", "GuildForum", "GuildMedia")):
                    raise ReaderError("Thread type differs from its parent channel type.")
                staged[tid] = {"id": tid, "guild_id": parent["guild_id"], "guild_name": parent["guild_name"],
                               "name": text_field(raw.get("name"), 256), "type": THREAD_TYPES[kind],
                               "parent_id": parent_id, "archived": metadata["archived"],
                               "archive_timestamp": timestamp(metadata.get("archive_timestamp")),
                               "last_message_id": snowflake(raw["last_message_id"]) if raw.get("last_message_id") else None,
                               "access": "returned_by_accessible_thread_search", "message_history_available": True}
                duplicates += tid in seen
            following = offset + len(rows)
            if following > 10000 or sum("parent_id" in c for c in self.channels.values()) + len(staged) > 10000:
                raise ReaderError("Thread metadata limit reached; restart and narrow the discovery scope.")
            old = {cid for cid, c in self.channels.items() if c.get("parent_id") == parent_id
                   and c.get("archived") == (state == "archived")} if cursor is None else set()
            self.client.replace_discovered_channels(old, staged)
            for cid in old:
                del self.channels[cid]
            self.channels.update(staged)
            next_cursor = None
            if more and following < 10000:
                next_cursor = secrets.token_hex(24)
                if len(self.thread_cursors) >= 32:
                    del self.thread_cursors[next(iter(self.thread_cursors))]
                self.thread_cursors[next_cursor] = {"parent_id": parent_id, "state": state,
                    "offset": following, "seen": seen | set(staged), "created": self.monotonic()}
            return live_envelope({"threads": [c for cid, c in staged.items() if cid not in seen],
                "next_cursor": next_cursor, "more_threads": more, "page_offset": offset,
                "duplicate_rows_omitted": duplicates, "local_limit_reached": more and next_cursor is None,
                "freshness": {"state": "refreshed", "refreshed_at": timestamp(self.now().isoformat())},
                "message_history_fetched": False,
                "coverage": "One live metadata page for this parent and state. Active and archived require separate calls. Private threads are limited to what Discord returns for this account. Offset pagination is not atomic; activity can move rows and cause omissions/duplicates. No joins, unarchives or history reads occur. This personal-account endpoint is undocumented and can change."})
        except ReaderError as exc:
            return self._failure(exc, [], attempted)

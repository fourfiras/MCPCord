"""Bounded Discord-index search; result counts never imply exhaustive history."""
import secrets

from .analysis import activity, matches_user
from .live import LiveReader, api_to_export, as_time, boundary_id, live_envelope
from .reader import ReaderError, bounded_list, canonical, integer, normalize_message, obj, snowflake, text_field, timestamp


class SearchMixin(LiveReader):
    guilds: dict[str, dict]

    identity: str | None

    def _discover_dms(self, deadline):
        raise NotImplementedError("DiscoveryReader supplies DM discovery.")

    def search_user_messages(self, user_id, since, until, relationship="author", guild_id=None, channel_id=None, cursor=None):
        user_id = snowflake(user_id)
        if (guild_id is None) == (channel_id is None):
            raise ReaderError("Select exactly one discovered server or conversation; page other scopes explicitly.")
        target = snowflake(guild_id or channel_id)
        if guild_id and guild_id not in self.guilds:
            self._ensure_guilds()
        if channel_id:
            self.ensure_discovered([channel_id])
        if guild_id and guild_id not in self.guilds:
            raise ReaderError("Discover the requested guild before searching it.")
        if channel_id and channel_id not in self.channels:
            raise ReaderError("Discover the requested conversation before searching it.")
        if relationship not in ("author", "mention", "reply"):
            raise ReaderError("Select author, mention or reply matching.")
        since, until = timestamp(since), timestamp(until)
        if as_time(until) > self.now():
            until = timestamp(self.now().isoformat())
        if since >= until or as_time(since).year < 2015:
            raise ReaderError("Choose a Discord-era interval with since < until.")
        signature = canonical([user_id, since, until, relationship, guild_id, channel_id])
        if not hasattr(self, "search_cursors"):
            self.search_cursors: dict[str, dict] = {}
        self.search_cursors = {k: v for k, v in self.search_cursors.items() if self.monotonic() - v["created"] <= 600}
        offset = 0
        if cursor is not None:
            page = self.search_cursors.get(text_field(cursor, 128))
            if page is None or page["signature"] != signature:
                raise ReaderError("Expired or mismatched search cursor; repeat the original filters.")
            offset = page["offset"]
        attempted = timestamp(self.now().isoformat())
        try:
            self.client.begin_refresh(self.scope["max_requests_per_refresh"])
            lower = max(1, int(boundary_id(since)) - 1)
            response = obj(self.client.search_messages(target, guild_id is not None, user_id, relationship,
                          str(lower), boundary_id(until), offset, self.monotonic() + 20))
            total = integer(response.get("total_results"), 0, 2**63 - 1)
            groups = bounded_list(response.get("messages"), 25)
            result, seen = [], set()
            omitted = 0
            for group in groups:
                for raw in bounded_list(group, 10):
                    raw = obj(raw)
                    cid = snowflake(raw.get("channel_id"))
                    if channel_id and cid != channel_id:
                        raise ReaderError("Search response has a different conversation identity.")
                    if guild_id and raw.get("guild_id", guild_id) != guild_id:
                        raise ReaderError("Search response has a different server identity.")
                    channel = self.channels.get(cid) or {"id": cid, "guild_id": guild_id,
                              "guild_name": self.guilds[guild_id]["name"], "name": "Search result channel", "type": "GuildTextChat"}
                    if guild_id and channel["guild_id"] != guild_id:
                        raise ReaderError("Search result channel belongs to a different server.")
                    row = normalize_message(api_to_export(raw, channel), channel, attempted, "discord-search")
                    key = (cid, row["id"])
                    if key in seen:
                        continue
                    seen.add(key)
                    if not since <= row["timestamp"] < until or not matches_user(row, user_id, relationship):
                        omitted += 1
                        continue
                    row.pop("source_export_sha256")
                    row["content_truncated"] = len(row["content"]) > 1000
                    row["content"] = row["content"][:1000]
                    result.append(row)
            following = offset + 25  # A short result page is explicitly not an end-of-results signal.
            next_cursor = None
            if following < total and following <= 9975:
                next_cursor = secrets.token_hex(24)
                if len(self.search_cursors) >= 32:
                    del self.search_cursors[next(iter(self.search_cursors))]
                self.search_cursors[next_cursor] = {"signature": signature, "offset": following, "created": self.monotonic()}
            return live_envelope({"messages": result, "reported_total_results": total, "returned_matching": len(result),
                "offset": offset, "next_cursor": next_cursor, "search_offset_limit_reached": following > 9975 and following < total,
                "excluded_unverified_or_outside_window": omitted,
                "doing_deep_historical_index": bool(response.get("doing_deep_historical_index", False)),
                "freshness": {"state": "search_index", "refreshed_at": timestamp(self.now().isoformat())},
                "filters": {"user_id": user_id, "relationship": relationship, "since": since, "until": until,
                            "guild_id": guild_id, "channel_id": channel_id},
                "exhaustive": False,
                "coverage": "Discord search index only. Totals may drift and indexes may omit messages. Short pages do not imply completion. Offset is capped at 9975; narrow the date window or explicitly collect history for deeper analysis. Authors, mentions and replies are verified separately by exact IDs; unverifiable reply targets are excluded. DM search is an undocumented personal-account endpoint."})
        except ReaderError as exc:
            return self._failure(exc, [], attempted)

    def find_user_conversations(self, user_id):
        user_id = snowflake(user_id)
        # Fresh metadata only; membership in a DM is not a claim of message activity.
        attempted = timestamp(self.now().isoformat())
        try:
            self.client.begin_refresh(self.scope["max_requests_per_refresh"])
            if self.identity is None:
                self.identity = snowflake(obj(self.client.current_user(self.monotonic() + 20)).get("id"))
                self.client.register_identity(self.identity)
            found, coverage = self._discover_dms(self.monotonic() + 20)
            selected = [c for c in found.values() if user_id in c.get("recipient_ids", [])]
            if self.identity == user_id:
                selected = list(found.values())
            self.client.replace_discovered_channels([], {c["id"]: c for c in selected})
            self.channels.update({c["id"]: c for c in selected})
            return live_envelope({"user_id": user_id, "conversations": selected, "match_basis": "exact current DM recipient membership, not authorship",
                                  "coverage": coverage, "message_history_fetched": False})
        except ReaderError as exc:
            return self._failure(exc, [], attempted)

    def analyze_messages(self, channel_ids, since, until, user_id=None, relationship="author", timezone="UTC", bucket="day"):
        ids, since, until = self._window(channel_ids, since, until)
        # Validate options before any network access.
        activity([], timezone, user_id, relationship, bucket)
        attempted = timestamp(self.now().isoformat())
        try:
            index, freshness = self._refresh(ids, since, until)
            rows, _ = index._select(ids, since, until)
            for row in rows:
                if row.get("is_reply") and not row.get("reply_to_user_id") and row.get("reference"):
                    ref = row["reference"]
                    original = index.messages.get((ref.get("channelId") or row["channel_id"], ref.get("messageId")))
                    if original and original.get("author_kind") != "webhook":
                        row["reply_to_user_id"] = original["author"]["id"]
            data = activity(rows, timezone, user_id, relationship, bucket)
            data.update({"freshness": freshness, "since": since, "until": until, "coverage": freshness["coverage"]})
            return live_envelope(data)
        except ReaderError as exc:
            return self._failure(exc, ids, attempted)

# Tool map

| Tool | Purpose |
| --- | --- |
| `list_guilds` | One membership page; follow `next_after`. |
| `list_channels` | Current DMs, or readable channels/containers in a discovered guild. |
| `list_threads` | One active/archived metadata page in a readable parent. |
| `read_messages` | Fresh date/channel window with reaction counts; optional literal text search. |
| `get_message` | One exact fresh message of any age, using the personal-account-compatible listing endpoint, with who reacted. |
| `digest_context` | Fresh, cited evidence for an assistant-written digest. |
| `search_user_messages` | Discord-index search by exact author, mention or reply target ID. |
| `find_user_conversations` | Current DM membership by exact recipient ID. |
| `analyze_messages` | Fresh-window activity and conversation counts. |
| `start_history_import` | Create a local job; does not fetch messages. |
| `continue_history_import` | Fetch a bounded job slice, then stop. |
| `cancel_history_import` | Stop future slices; retain collected observations. |
| `history_import_status` | Check per-channel progress, errors and coverage. |
| `query_history` | Search already collected observations. |
| `analyze_history` | Analyze collected observations with date and timezone filters. |

All Discord access is GET-only. The three job-control tools declare local mutations in MCP annotations because they change the local job/index, not because they write to Discord.

For a server history job: discover the guild, channels, and the active/archived thread pages needed for your scope; then call `start_history_import` with `guild_id` and either `since` or explicit `all_history: true`. Inspect the returned channel snapshot. Continue in bounded slices. Check the final manifest before interpreting results. No tool promises every historical message.

For a full DM history, pass its discovered ID in `channel_ids` and set `all_history: true`. Continue the same job while `needs_continuation` is true, respecting `retry_at` and inspecting errors. `max_pages` is per call; `total_page_limit` is null. `page_evidence` records the request cursor, response count, oldest returned identity, next cursor and terminal reason. The final empty page is independently checked with `after=0&limit=1`; an older uncollected message causes `history_gap`, not completion. This check cannot recover deleted or inaccessible messages. A cancelled job needs an explicit resume; storage quotas require addressing the reported limit.

Use `user_id` as a string. `relationship` is exactly `author`, `mention` or `reply`. Default authorship matching excludes known webhook authors. A name or digits in plain text is not an ID match.

Dates use full ISO timestamps with `Z` or an offset and `[since, until)` semantics. Use `timezone` for presentation in activity analysis, not to make an offset-free input timestamp valid.

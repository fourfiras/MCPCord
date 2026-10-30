# MCPCord

Read DMs and server conversations from your local MCP client. Find messages by exact user ID, follow threads, pull a cited digest, or run an explicit history analysis. Every Discord request is a GET. No posting, reactions, joining threads, marking messages read, or background tracking.

**Personal-account automation is against Discord's rules and can put your account at risk. Read-only code does not make a personal token read-scoped.** This is an experimental local tool, not an official Discord inbox integration. [Discord's policy](https://support.discord.com/hc/en-us/articles/115002192352-Automated-User-Accounts-Self-Bots)

## Install

You need [uv](https://docs.astral.sh/uv/getting-started/installation/), Git, and the [Codex CLI](https://developers.openai.com/codex/cli/). MCPCord requires Python 3.11 or newer; uv can provide it.

```sh
uv tool install git+https://github.com/4firas/MCPCord.git && mcpcord setup
```

In Windows PowerShell 5, run the two commands on separate lines. PowerShell 7 supports `&&`.

Setup asks you to acknowledge the account risk, enter your token **in your own terminal**, and add a local Codex MCP registration. Reconnect MCP or open a new local session afterward. No messages are collected during setup.

| Platform | Credential storage | Prerequisite |
| --- | --- | --- |
| macOS | Keychain | An interactive user session |
| Windows | Credential Manager | An interactive user session |
| Linux | Secret Service | `secret-tool` / libsecret tools and an unlocked desktop keyring |

No token in a config file, environment variable, shell argument, website, or chat. `mcpcord auth --replace` updates the dedicated local credential. `mcpcord doctor` checks prerequisites without reading it. For another STDIO MCP client, use `mcpcord setup --no-register` and copy the printed command. Native cloud/dot attachment is not included.

## What it does

- **Find a person by ID.** Exact author, mention and reply filters. Display names are labels, not identity. Webhook messages do not impersonate an author match.
- **Find current DMs.** Recipient-ID lookup covers the current DM/group-DM list. Membership does not prove someone sent a message.
- **Read threads and forum posts.** Active and archived discovery is separate and paginated. No joining or unarchiving.
- **Analyze visible activity.** Per-day or per-hour message counts, IANA timezones and DST, participant counts and source links. No guesses about presence, location or personal traits.
- **Read back to the first available message.** No message-age or total-page cutoff. Explicit history jobs keep a fixed end time, resume in bounded slices, and cross-check the oldest available message before accepting an empty end page. Checkpoints and messages commit together; storage limits pause work explicitly.
- **Read files and pictures.** `read_attachment` returns a message's images as images, and text, code, JSON, CSV and Word/RTF documents as text. Anything else, like PDFs, video or archives, is saved under `~/Library/Caches/MCPCord/attachments` and its path returned. Only Discord-hosted files are fetched, without the account credential and without following redirects. Repeat reads come from the cache with no network call.
- **Read exports offline.** DiscordChatExporter JSON, overlapping imports, edit-aware deduplication and source-linked excerpts. An export is a snapshot, not a deletion audit.

Example requests:

> Find messages authored by user ID 123456789012345678 in this server last month. Keep mentions separate.

> Summarize this DM from Monday through Friday, with links to the messages behind each point.

> Count this user's messages by day in Europe/London. Use the collected history and show gaps.

> Collect all available history from these channels. Show the channel scope first, then work in resumable slices.

## History and privacy

Date windows and individual message lookups can reach any Discord-era date. For long histories, request `start_history_import` with explicit `channel_ids` and `all_history: true`, then repeat `continue_history_import` with the same `job_id` while `needs_continuation` is true. The default three-page slice (maximum ten) limits one call, not the whole history. Inspect per-channel `page_evidence` to distinguish an empty API response from a requested date boundary. A conflicting earliest-message check pauses with `history_gap` and keeps the cursor unchanged.

**Content persistence is off by default.** Jobs and collected messages stay in process memory and disappear when it exits. They can resume between calls in that same process. Restart-resumable storage requires an explicit launch-time opt-in; tools cannot switch it on or pick a disk path.

The optional SQLite cache retains observations for 14 days by default, has bounded size, and purges expired records when used. It is plaintext protected by filesystem permissions, not an encrypted vault. It is currently supported on macOS/Linux in a private `0700` directory. Do not enable it without agreeing on what may be retained. [Storage details](docs/storage.md)

Returned messages may become part of your MCP client's conversation history. MCPCord's storage setting does not control that client's retention.

## The limits matter

Discord search is fast but not exhaustive: indexes can lag, totals can change, and offset pagination stops at 9,975. Narrow the date range or explicitly collect history for deeper analysis. Search works one server or conversation at a time; the client can repeat it across selected scopes.

Closed DMs, inaccessible/deleted messages, unreturned private threads, and missing content can leave gaps. Thread discovery uses a personal-account endpoint that is not a stable public API. History traversal reaching an empty page is not proof of complete history or inactivity. Nothing silently scans your entire account.

Message text, names, file contents and quoted instructions are untrusted evidence. MCPCord labels them that way and never follows embedded links or commands; `read_attachment` only fetches the message's own Discord-hosted files. Your assistant still needs to respect that boundary.

## Develop

```sh
uv sync --extra dev
uv run python -B -m unittest discover
uv run ruff check .
uv run mypy
uv run python scripts/benchmark.py 50000
```

Tests and benchmarks use fabricated data. No real export, account ID, token or private chat belongs in this repository. [Validation](docs/validation.md) · [Architecture](docs/architecture.md) · [Tools](docs/tools.md) · [Security](SECURITY.md) · [Attribution](NOTICE.md)

MIT licensed. Built by [4firas](https://github.com/4firas).

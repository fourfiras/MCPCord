"""MCPCord: read-only Discord discovery, exact-ID search and explicit history analysis."""

import argparse
import json
import os
import stat
import subprocess
import sys

from .reader import ExportIndex, ReaderError, strict_json
from .server import Server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export", action="append", metavar="PATH",
                        help="Explicit DiscordChatExporter JSON path; repeat for overlaps/partitions.")
    commands = parser.add_subparsers(dest="command", required=True)
    setup = commands.add_parser("setup", help="Configure your OS credential store and local Codex registration.")
    setup.add_argument("--dry-run", action="store_true")
    setup.add_argument("--no-token", action="store_true", help="Skip credential setup; useful for a credential-free configuration test.")
    setup.add_argument("--no-register", action="store_true", help="Print the STDIO command for another client.")
    setup.add_argument("--accept-account-risk", action="store_true")
    setup.add_argument("--config-dir")
    auth = commands.add_parser("auth", help="Enter or replace your token locally using your OS credential store.")
    auth.add_argument("--replace", action="store_true")
    commands.add_parser("doctor", help="Check runtime and credential-store prerequisites without reading a token.")
    commands.add_parser("serve", help="Read-only STDIO MCP, not registered automatically.")
    commands.add_parser("inspect", help="List channels and import reports as JSON.")
    digest = commands.add_parser("digest", help="Print one page of digest evidence as JSON.")
    digest.add_argument("--channel", action="append", required=True)
    digest.add_argument("--since", required=True)
    digest.add_argument("--until", required=True)
    digest.add_argument("--limit", type=int, default=25)
    digest.add_argument("--cursor")
    validate = commands.add_parser("validate-live-scope", help="Validate an explicit scope JSON only. No credentials or network.")
    validate.add_argument("--scope", required=True)
    live = commands.add_parser("serve-live", help="Dormant until explicitly enabled; uses your OS credential store.")
    live.add_argument("--scope", required=True)
    live.add_argument("--enable-network", action="store_true")
    live.add_argument("--personal-account-risk-accepted", action="store_true")
    live.add_argument("--discover-accessible", action="store_true", help="Explicitly enable account-wide metadata discovery; history remains lazy.")
    live.add_argument("--history-store", help="Optional absolute SQLite path in an existing 0700 directory; requires separate retention/storage approval.")
    live.add_argument("--enable-content-cache", action="store_true", help="Explicitly enable the approved persistent content cache; inactive by default.")
    live.add_argument("--retention-days", type=int, default=14, help="Observation-based cache retention, 1-30 days; purged on use, no background service.")
    args = parser.parse_args()
    try:
        if args.command == "setup":
            from .setup import run_setup
            return run_setup(args)
        if args.command == "auth":
            from .credentials import store_credential
            print("Saved locally in " + store_credential(args.replace) + ".")
            return 0
        if args.command == "doctor":
            import shutil
            from .credentials import provider_name
            from . import __version__
            print(json.dumps({"product": "MCPCord", "version": __version__, "python": sys.version.split()[0],
                              "codex_cli_available": shutil.which("codex") is not None,
                              "credential_provider": provider_name(), "credential_read": False,
                              "network_requested": False, "content_cache": "disabled unless explicitly configured"}, indent=2))
            return 0
        if args.command in ("serve-live", "validate-live-scope"):
            if args.export:
                parser.error("Live and archive inputs cannot be mixed.")
            from .live import LiveReader, validate_scope
            try:
                if os.path.islink(args.scope):
                    raise ReaderError("Symbolic links are not accepted.")
                fd = os.open(args.scope, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0))
                with os.fdopen(fd, "rb") as source:
                    if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                        raise ReaderError("Scope must be a regular JSON file.")
                    data = source.read(65537)
                if len(data) > 65536:
                    raise ReaderError("Scope file exceeds 64 KiB.")
                scope = strict_json(data)
            except OSError:
                raise ReaderError("Cannot read the explicit scope configuration.") from None
            validated = validate_scope(scope)
            if args.command == "validate-live-scope":
                print(json.dumps({"valid": True, "channel_ids": list(validated["channels"]),
                                  "mode": validated["mode"],
                                  "credential_accessed": False, "network_requested": False}, indent=2))
                return 0
            if not args.enable_network or not args.personal_account_risk_accepted:
                raise ReaderError("Live mode requires both explicit activation flags after the separate user approval step.")
            from .discovery import DiscoveryReader
            reader: LiveReader
            if validated["mode"] == "discover_accessible":
                if not args.discover_accessible:
                    raise ReaderError("Account-wide discovery requires the explicit --discover-accessible activation flag.")
                from .attachments import cache_root
                reader = DiscoveryReader(scope, hints_path=str(cache_root().parent / "channel-hints.json"))
            else:
                reader = LiveReader(scope)
            if args.history_store or args.enable_content_cache:
                if not args.history_store or not args.enable_content_cache or not isinstance(reader, DiscoveryReader):
                    raise ReaderError("Content persistence requires discovery mode, an explicit store path and --enable-content-cache after approval.")
                from .store import HistoryStore
                reader.history_store = HistoryStore(args.history_store, enable_persistence=True, retention_days=args.retention_days)
            Server(reader).serve()
            return 0
        if not args.export:
            parser.error("Archive commands require at least one explicit --export path.")
        index = ExportIndex()
        reports = [index.import_file(path) for path in args.export]
        if args.command == "serve":
            Server(index).serve()
            return 0
        if args.command == "inspect":
            result = index.list_channels()
            result["data"]["imports"] = reports
        else:
            result = index.digest_context(args.channel, args.since, args.until, args.limit, args.cursor)
        print(json.dumps(result, ensure_ascii=True, indent=2))
        return 0
    except ReaderError as exc:
        print(f"MCPCord: {exc}", file=sys.stderr)
        return 2
    except BrokenPipeError:
        return 0
    except (OSError, subprocess.SubprocessError):
        print("MCPCord: Local setup or file access failed; no raw error or credential was logged.", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

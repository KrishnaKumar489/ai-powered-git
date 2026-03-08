"""
run.py  —  Unified AI Git Repo Query Pipeline
==============================================

Combines all three stages into one entry point:

    Stage 1  metadata.py   → repo_structure, git_actions, code_metadata, ukg
    Stage 2  build_index.py → index_<ts>.json
    Stage 3  query.py       → AI answers via Bedrock Nova Pro

Usage
-----
First time (or after new commits):
    python run.py --repo /path/to/repo -q "what does login do"

Re-use existing index (skip re-collection):
    python run.py --repo /path/to/repo --skip-collect -q "compare main and develop"

Interactive REPL:
    python run.py --repo /path/to/repo

All options:
    python run.py --help
"""

import argparse
import sys
import os
from pathlib import Path
from datetime import datetime


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _divider(char="=", width=66):
    print(char * width)

def _header(title):
    _divider()
    print(f"  {title}")
    _divider()

def _step(n, total, title):
    _divider("-")
    print(f"  STEP {n}/{total}  {title}")
    _divider("-")


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — Metadata collection
# ─────────────────────────────────────────────────────────────────────────────

def run_collect(repo_path: str, output_dir: str,
                base_branch: str, local_only: bool,
                skip_code: bool) -> tuple:
    """
    Calls metadata.run() and returns
    (structure_path, actions_path, code_path, ukg_path).
    """
    try:
        import metadata_collector
    except ImportError:
        print("  ERROR: metadata.py not found. It must be in the same directory as run.py.",
              file=sys.stderr)
        sys.exit(1)

    return metadata_collector.run(
        repo_path      = repo_path,
        output_dir     = output_dir,
        include_remote = not local_only,
        base_branch    = base_branch,
        skip_code      = skip_code,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — Index build
# ─────────────────────────────────────────────────────────────────────────────

def run_build_index(structure_path, actions_path,
                    code_path, ukg_path,
                    output_dir: str) -> Path:
    """
    Calls build_index.build_index() and writes index_<ts>.json.
    Returns the path to the written index file.
    """
    try:
        import build_index as bi_mod
    except ImportError:
        print("  ERROR: build_index.py not found. It must be in the same directory as run.py.",
              file=sys.stderr)
        sys.exit(1)

    index = bi_mod.build_index(
        structure_path = Path(structure_path),
        actions_path   = Path(actions_path),
        code_path      = Path(code_path)   if code_path  else None,
        ukg_path       = Path(ukg_path)    if ukg_path   else None,
    )

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
    index_path  = out_dir / f"index_{ts}.json"

    import json
    index_path.write_text(
        json.dumps(index, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    # print summary
    total_files   = sum(len(b["files"]) for b in index["branches"].values())
    total_commits = sum(len(b["git"]["commits"]) for b in index["branches"].values())
    total_fns     = sum(
        len(f["code"]["functions"])
        for b in index["branches"].values()
        for f in b["files"].values()
    )
    lk = index["lookup"]
    print(f"\n  Index summary:")
    print(f"    Branches  : {index['total_branches']}")
    print(f"    Files     : {total_files:,}")
    print(f"    Commits   : {total_commits:,}")
    print(f"    Functions : {total_fns:,}")
    print(f"    Lookup entries:")
    print(f"      file_to_branches     : {len(lk['file_to_branches']):,}")
    print(f"      function_to_branches : {len(lk['function_to_branches']):,}")
    print(f"      author_to_commits    : {len(lk['author_to_commits']):,}")
    print(f"\n  INDEX -> {index_path.resolve()}")

    return index_path


# ─────────────────────────────────────────────────────────────────────────────
# Stage 3 — Query
# ─────────────────────────────────────────────────────────────────────────────

def run_query(index_path: Path, query_str, branch,
              aws_region: str, dry_run: bool):
    """
    Loads query.py's engine and runs either a single query or the REPL.
    """
    try:
        import query as q_mod
    except ImportError:
        print("  ERROR: query.py not found. It must be in the same directory as run.py.",
              file=sys.stderr)
        sys.exit(1)

    index = q_mod.load_index(index_path)

    if query_str:
        ans = q_mod.query(
            query_str, index,
            preferred_branch = branch,
            aws_region       = aws_region,
            dry_run          = dry_run,
        )
        print()
        q_mod._print_md(ans)
    else:
        q_mod.repl(index, branch, aws_region, dry_run)


# ─────────────────────────────────────────────────────────────────────────────
# Index resolution — find or build
# ─────────────────────────────────────────────────────────────────────────────

def _find_latest_index(directory: Path):
    matches = sorted(
        directory.glob("index_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    return matches[0] if matches else None


def _index_is_fresh(index_path: Path, repo_path: str,
                    max_age_hours: int = 24) -> bool:
    """Returns True if the index was built within max_age_hours."""
    if not index_path or not index_path.exists():
        return False
    age_seconds = datetime.now().timestamp() - index_path.stat().st_mtime
    return age_seconds < max_age_hours * 3600


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="AI-powered git repo query — single entry point.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
━━━  QUICK START  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  1. Clone your repo:
       git clone https://github.com/you/your-repo.git

  2. Run a query (collects metadata + builds index automatically):
       python run.py --repo ./your-repo -q "what does login do"

  3. Ask more questions (reuses the index, no re-collection):
       python run.py --repo ./your-repo --skip-collect -q "compare main and develop"

  4. Interactive REPL (no -q flag):
       python run.py --repo ./your-repo --skip-collect

━━━  ALL OPTIONS  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  --repo           Path to the git repository (required)
  --reports-dir    Where to store JSON reports and index (default: ./reports)
  -q / --query     Question to ask (omit for interactive REPL)
  --skip-collect   Skip Stage 1+2, reuse the latest existing index
  --force-collect  Always re-collect even if a fresh index exists
  --local-only     Only scan local branches (skip remote tracking branches)
  --skip-code      Skip tree-sitter code analysis (faster, no function data)
  --base           Base branch for ahead/behind stats (default: main)
  --branch         Pin all queries to a specific branch
  --region         AWS Bedrock region (default: us-east-1)
  --dry-run        Print the prompt without calling Bedrock

━━━  EXAMPLE QUERIES  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  python run.py --repo ./repo -q "what does the login function do"
  python run.py --repo ./repo -q "compare main and develop"
  python run.py --repo ./repo -q "which files change the most"
  python run.py --repo ./repo -q "what breaks if I change auth.py"
  python run.py --repo ./repo -q "find files that use JWT"
  python run.py --repo ./repo -q "top contributors"
  python run.py --repo ./repo -q "what changed last week"
  python run.py --repo ./repo -q "unused functions"
        """,
    )

    # ── repo + output ─────────────────────────────────────────────────────────
    parser.add_argument(
        "--repo", default=".",
        help="Path to the git repository (default: current directory)"
    )
    parser.add_argument(
        "--reports-dir", default="./reports",
        help="Directory to store JSON reports and index (default: ./reports)"
    )

    # ── collection control ────────────────────────────────────────────────────
    collect_grp = parser.add_mutually_exclusive_group()
    collect_grp.add_argument(
        "--skip-collect", action="store_true",
        help="Skip metadata collection and index build — reuse latest index"
    )
    collect_grp.add_argument(
        "--force-collect", action="store_true",
        help="Force re-collection even if a fresh index already exists"
    )
    parser.add_argument(
        "--max-index-age", type=int, default=24, metavar="HOURS",
        help="Auto re-collect if index is older than N hours (default: 24)"
    )

    # ── collection options ────────────────────────────────────────────────────
    parser.add_argument("--local-only",  action="store_true",
                        help="Only scan local branches")
    parser.add_argument("--skip-code",   action="store_true",
                        help="Skip tree-sitter code analysis")
    parser.add_argument("--base",        default="main",
                        help="Base branch for ahead/behind stats (default: main)")

    # ── query options ─────────────────────────────────────────────────────────
    parser.add_argument("-q", "--query",  default=None,
                        help="Question to ask (omit for interactive REPL)")
    parser.add_argument("--branch",       default=None,
                        help="Pin queries to a specific branch")
    parser.add_argument("--region",       default="us-east-1",
                        help="AWS Bedrock region (default: us-east-1)")
    parser.add_argument("--dry-run",      action="store_true",
                        help="Print prompt without calling Bedrock")

    args = parser.parse_args()

    # ── resolve paths ─────────────────────────────────────────────────────────
    repo_path   = str(Path(args.repo).resolve())
    reports_dir = Path(args.reports_dir)

    # validate repo
    if not Path(repo_path).exists():
        print(f"  ERROR: repo path not found: {repo_path}", file=sys.stderr)
        sys.exit(1)
    if not (Path(repo_path) / ".git").exists():
        print(f"  ERROR: {repo_path} is not a git repository (no .git folder).",
              file=sys.stderr)
        sys.exit(1)

    _header("AI Git Repo Query  —  run.py")
    print(f"  Repo        : {repo_path}")
    print(f"  Reports dir : {reports_dir.resolve()}")
    print(f"  Query       : {args.query or '(interactive REPL)'}")
    print(f"  Region      : {args.region}")
    _divider()

    # ── decide whether to collect ─────────────────────────────────────────────
    existing_index = _find_latest_index(reports_dir)
    index_fresh    = _index_is_fresh(existing_index, repo_path,
                                     max_age_hours=args.max_index_age)

    need_collect = (
        args.force_collect
        or (not args.skip_collect and not index_fresh)
    )

    if args.skip_collect and not existing_index:
        print("  WARNING: --skip-collect requested but no index found.",
              file=sys.stderr)
        print("  Will collect now.", file=sys.stderr)
        need_collect = True

    # ── STAGE 1 + 2  (collection + index build) ───────────────────────────────
    if need_collect:
        if existing_index and not args.force_collect:
            age_h = (datetime.now().timestamp() - existing_index.stat().st_mtime) / 3600
            print(f"  Existing index is {age_h:.1f}h old (limit: {args.max_index_age}h) — re-collecting.")
        elif args.force_collect:
            print("  --force-collect: re-collecting regardless of index age.")
        else:
            print("  No existing index found — collecting now.")

        _step(1, 3, "Metadata Collection  (metadata.py)")
        struct_p, actions_p, code_p, ukg_p = run_collect(
            repo_path   = repo_path,
            output_dir  = str(reports_dir),
            base_branch = args.base,
            local_only  = args.local_only,
            skip_code   = args.skip_code,
        )

        _step(2, 3, "Index Build  (build_index.py)")
        index_path = run_build_index(
            structure_path = struct_p,
            actions_path   = actions_p,
            code_path      = code_p,
            ukg_path       = ukg_p,
            output_dir     = str(reports_dir),
        )

    else:
        age_h = (datetime.now().timestamp() - existing_index.stat().st_mtime) / 3600
        print(f"  Using existing index: {existing_index.name}  ({age_h:.1f}h old)")
        print(f"  (Use --force-collect to re-collect, or --max-index-age to change threshold)")
        index_path = existing_index

    # ── STAGE 3  (query) ──────────────────────────────────────────────────────
    total_steps = 3 if need_collect else 1
    _step(total_steps, total_steps, "Query  (query.py)")
    run_query(
        index_path = index_path,
        query_str  = args.query,
        branch     = args.branch,
        aws_region = args.region,
        dry_run    = args.dry_run,
    )


if __name__ == "__main__":
    main()

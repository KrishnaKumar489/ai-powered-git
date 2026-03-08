"""
main.py
-------
Entry point for the Git Repository Metadata Exporter.

Produces THREE branch-aware JSON output files:

    repo_structure_<ts>.json
        File system snapshot per branch:
        files, directories, folder tree, sizes, blob hashes, extensions.

    git_actions_<ts>.json
        All git activity per branch:
        commits (typed), merges, file events, per-file history,
        tags, branch stats, stashes, hooks, gitignore rules.

    code_metadata_<ts>.json
        Code-level symbols per branch (requires tree-sitter):
        functions (name, lines, docstring, decorators),
        classes (name, bases, methods, lines),
        imports, call graph, file dependency graph.

Usage:
    python main.py [repo_path] [-o OUTPUT_DIR] [--base BRANCH] [--local-only]
                   [--skip-code]

Examples:
    python main.py
    python main.py /path/to/repo
    python main.py /path/to/repo -o ./reports
    python main.py /path/to/repo --base develop --local-only
    python main.py /path/to/repo --skip-code        # skip tree-sitter extraction
"""

import subprocess
import json
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

# ── Windows UTF-8 fix ────────────────────────────────────────────────────────
# Prevents UnicodeEncodeError when git output contains non-ASCII characters
# (author names, commit messages, file paths with special characters).
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from file_data       import structure_for_branch
from git_action_data import actions_for_branch, get_stashes, get_hooks
from code_data       import code_metadata_for_branch


# ─────────────────────────── git helpers ────────────────────────────────────

def _git(args, cwd, check=True):
    r = subprocess.run(
        ["git"] + args, cwd=cwd,
        capture_output=True, text=True, check=check
    )
    return r.stdout.strip()


def _git_lines(args, cwd):
    out = _git(args, cwd, check=False)
    return [l for l in out.splitlines() if l.strip()]


# ─────────────────────────── branch discovery ────────────────────────────────

def _fetch_all(repo_path: str) -> None:
    try:
        _git(["fetch", "--all", "--quiet"], repo_path)
    except subprocess.CalledProcessError:
        pass


def _all_branches(repo_path: str, include_remote: bool = True) -> list[dict]:
    fmt = (
        "%(refname:short)|%(objectname)|%(objectname:short)"
        "|%(subject)|%(creatordate:iso-strict)|%(authorname)|%(authoremail)"
    )
    local  = _git_lines(["for-each-ref", "--format", fmt, "refs/heads/"], repo_path)
    remote = (
        _git_lines(["for-each-ref", "--format", fmt, "refs/remotes/"], repo_path)
        if include_remote else []
    )

    branches = []
    for line in local + remote:
        parts = line.split("|", 6)
        while len(parts) < 7:
            parts.append("")
        name, full_hash, short_hash, subject, created, author, email = parts

        if name.endswith("/HEAD"):
            continue

        branches.append({
            "name":             name,
            "tip_commit":       full_hash,
            "tip_short":        short_hash,
            "tip_subject":      subject,
            "created_date":     created,
            "created_by":       author,
            "created_by_email": email,
            "is_remote":        "/" in name,
        })

    return branches


def _current_branch(repo_path: str) -> str:
    try:
        return _git(["rev-parse", "--abbrev-ref", "HEAD"], repo_path)
    except subprocess.CalledProcessError:
        return "HEAD"


def _remote_url(repo_path: str) -> str:
    try:
        return _git(["remote", "get-url", "origin"], repo_path)
    except subprocess.CalledProcessError:
        return "N/A"


# ─────────────────────────── main runner ─────────────────────────────────────

def run(
    repo_path:      str,
    output_dir:     str,
    include_remote: bool = True,
    base_branch:    str  = "main",
    skip_code:      bool = False,
) -> tuple[str, str, str | None]:
    """
    Collect all metadata from every branch and write three JSON files.

    Returns:
        (structure_path, actions_path, code_path, ukg_path)
        code_path and ukg_path are None when skip_code=True.
    """
    repo_path  = str(Path(repo_path).resolve())
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    _header("Git Repo Metadata Exporter")
    print(f"  Repo      : {repo_path}")
    print(f"  Output    : {output_dir.resolve()}")
    print(f"  Code scan : {'DISABLED (--skip-code)' if skip_code else 'ENABLED'}")

    print("  Fetching remote refs ...", end="\r")
    _fetch_all(repo_path)

    branches   = _all_branches(repo_path, include_remote)
    cur        = _current_branch(repo_path)
    remote_url = _remote_url(repo_path)

    print(f"  Remote    : {remote_url}          ")
    print(f"  Branches  : {len(branches)} found")
    _divider()

    repo_meta = {
        "repo_path":      repo_path,
        "remote_url":     remote_url,
        "current_branch": cur,
        "collected_at":   datetime.now(timezone.utc).isoformat(),
        "total_branches": len(branches),
        "stashes":        get_stashes(repo_path),
        "hooks":          get_hooks(repo_path),
    }

    all_structure: list[dict] = []
    all_actions:   list[dict] = []
    all_code:      list[dict] = []

    for idx, bi in enumerate(branches, 1):
        name = bi["name"]
        kind = "(remote)" if bi["is_remote"] else "(local) "
        curr = " <- current" if name == cur else ""
        print(f"\n  [{idx:>3}/{len(branches)}] {kind} {name}{curr}")

        # ── 1. File structure ────────────────────────────────────────────────
        print(f"    [structure] ...", end="\r")
        struct = structure_for_branch(repo_path, bi)
        all_structure.append(struct)
        print(f"    [structure] {struct['total_files']:>5} files  "
              f"{struct['total_size_bytes']:>12,} bytes")

        # ── 2. Git actions ───────────────────────────────────────────────────
        print(f"    [actions]   ...", end="\r")
        actions = actions_for_branch(repo_path, bi, base_branch)
        all_actions.append(actions)
        print(f"    [actions]   {len(actions['commits']):>5} commits  "
              f"{len(actions['file_actions']):>5} file events  "
              f"{len(actions['tags']):>3} tags")

        # ── 3. Code symbols ──────────────────────────────────────────────────
        if not skip_code:
            print(f"    [code]      ...", end="\r")
            code_rec = code_metadata_for_branch(repo_path, bi)
            all_code.append(code_rec)
            ukg_s = code_rec.get("ukg_stats", {})
            print(f"    [code]      {code_rec['total_functions']:>5} functions  "
                  f"{code_rec['total_classes']:>4} classes  "
                  f"{len(code_rec['call_graph']):>4} call edges  "
                  f"ukg: {ukg_s.get('total_nodes',0)} nodes / {ukg_s.get('total_edges',0)} edges")

    _divider()

    # ── Write OUTPUT 1: repo_structure ───────────────────────────────────────
    struct_path = output_dir / f"repo_structure_{ts}.json"
    struct_path.write_text(json.dumps({
        "description": (
            "File system snapshot per branch: files, paths, folders, "
            "sizes, blob hashes, extension breakdown."
        ),
        "repository":     repo_meta,
        "total_branches": len(all_structure),
        "branches":       all_structure,
    }, indent=2, ensure_ascii=False), encoding="utf-8")

    # ── Write OUTPUT 2: git_actions ──────────────────────────────────────────
    actions_path = output_dir / f"git_actions_{ts}.json"
    actions_path.write_text(json.dumps({
        "description": (
            "All git actions per branch: commits (conventional-commit typed), "
            "merges, file add/delete/rename/copy/modify events, per-file commit "
            "history (follows renames), tags, branch stats (ahead/behind base, "
            "contributors, date range), merged branches, gitignore patterns, "
            "stashes, hooks."
        ),
        "repository":     repo_meta,
        "total_branches": len(all_actions),
        "branches":       all_actions,
    }, indent=2, ensure_ascii=False), encoding="utf-8")

    # ── Write OUTPUT 3: code_metadata (symbols, imports, calls — NO ukg blob) ──
    # ── Write OUTPUT 4: ukg  (Unified Knowledge Graph — one graph per branch) ──
    code_path = None
    ukg_path  = None
    if not skip_code:
        # Strip the ukg blob out of code records before writing code_metadata
        # so that file stays readable. UKG gets its own dedicated file.
        code_records_slim = []
        ukg_branches      = []
        for rec in all_code:
            ukg_branches.append({
                "branch":    rec["branch"],
                "is_remote": rec["is_remote"],
                "tip_commit": rec["tip_commit"],
                "ukg_stats": rec.get("ukg_stats", {}),
                "ukg":       rec.get("ukg", {}),
            })
            slim = {k: v for k, v in rec.items() if k != "ukg"}
            code_records_slim.append(slim)

        code_path = output_dir / f"code_metadata_{ts}.json"
        code_path.write_text(json.dumps({
            "description": (
                "Code-level symbol metadata per branch (tree-sitter powered): "
                "functions (name, line range, docstring, decorators), "
                "classes (name, base classes, methods, line range), "
                "imports, outbound calls, "
                "call_graph (function-level directed edges), "
                "dependency_graph (file-level import edges). "
                "UKG is stored separately in ukg_<ts>.json. "
                "Supports: Python, JavaScript, Java, HTML, CSS."
            ),
            "repository":     repo_meta,
            "total_branches": len(code_records_slim),
            "branches":       code_records_slim,
        }, indent=2, ensure_ascii=False), encoding="utf-8")

        ukg_path = output_dir / f"ukg_{ts}.json"
        ukg_path.write_text(json.dumps({
            "description": (
                "Unified Knowledge Graph (UKG) per branch. "
                "Node types: file:<path>, func:<path>:<name>, class:<path>:<name>. "
                "Edge types: contains (file->func, file->class, class->func), "
                "imports (file->file), calls (func->func), inherits (class->class). "
                "Load with: UnifiedKnowledgeGraph.from_dict(branch['ukg']) from code_metadata.py. "
                "Use k_hop_subgraph(start_node, k=2) to fetch the AI-relevant slice."
            ),
            "repository":     repo_meta,
            "total_branches": len(ukg_branches),
            "branches":       ukg_branches,
        }, indent=2, ensure_ascii=False), encoding="utf-8")

    # ── Summary ───────────────────────────────────────────────────────────────
    total_files   = sum(b["total_files"]      for b in all_structure)
    total_commits = sum(len(b["commits"])     for b in all_actions)
    total_events  = sum(len(b["file_actions"])for b in all_actions)
    total_fns     = sum(b.get("total_functions", 0) for b in all_code)
    total_cls     = sum(b.get("total_classes", 0)   for b in all_code)

    print(f"  Done!")
    print(f"  Branches  : {len(branches)}")
    print(f"  Files     : {total_files:,}")
    print(f"  Commits   : {total_commits:,}")
    print(f"  Events    : {total_events:,}")
    if not skip_code:
        print(f"  Functions : {total_fns:,}")
        print(f"  Classes   : {total_cls:,}")
    print()
    print(f"  OUTPUT 1 repo_structure -> {struct_path.resolve()}")
    print(f"  OUTPUT 2 git_actions    -> {actions_path.resolve()}")
    if code_path:
        print(f"  OUTPUT 3 code_metadata  -> {code_path.resolve()}")
    if ukg_path:
        ukg_total_nodes = sum(b["ukg_stats"].get("total_nodes", 0) for b in ukg_branches if not skip_code)
        ukg_total_edges = sum(b["ukg_stats"].get("total_edges", 0) for b in ukg_branches if not skip_code)
        print(f"  OUTPUT 4 ukg            -> {ukg_path.resolve()}")
        print(f"           ({ukg_total_nodes} nodes, {ukg_total_edges} edges across all branches)")
    _divider()

    return (
        str(struct_path),
        str(actions_path),
        str(code_path) if code_path else None,
        str(ukg_path)  if ukg_path  else None,
    )


# ─────────────────────────── utils ───────────────────────────────────────────

def _divider(): print("=" * 62)
def _header(title): _divider(); print(f"  {title}"); _divider()


# ─────────────────────────── CLI ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Export git repo metadata to three branch-aware JSON files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Outputs (all timestamped, all branch-aware):
  repo_structure_<ts>.json  — files, folders, sizes, blob hashes
  git_actions_<ts>.json     — commits, merges, file events, history
  code_metadata_<ts>.json   — functions, classes, imports, call graph
  ukg_<ts>.json             — Unified Knowledge Graph (nodes + edges)

Examples:
  python main.py
  python main.py /path/to/repo
  python main.py /path/to/repo -o ./reports
  python main.py /path/to/repo --base develop --local-only
  python main.py /path/to/repo --skip-code
        """,
    )
    parser.add_argument(
        "repo", nargs="?", default=".",
        help="Path to the git repository (default: current directory)",
    )
    parser.add_argument(
        "-o", "--output-dir", default=".",
        help="Directory for output JSON files (default: current directory)",
    )
    parser.add_argument(
        "--base", default="main",
        help="Base branch for ahead/behind stats (default: main)",
    )
    parser.add_argument(
        "--local-only", action="store_true",
        help="Only traverse local branches, skip remote tracking branches",
    )
    parser.add_argument(
        "--skip-code", action="store_true",
        help="Skip tree-sitter code symbol extraction (faster, no OUTPUT 3)",
    )

    args = parser.parse_args()
    run(
        repo_path      = args.repo,
        output_dir     = args.output_dir,
        include_remote = not args.local_only,
        base_branch    = args.base,
        skip_code      = args.skip_code,
    )


if __name__ == "__main__":
    main()

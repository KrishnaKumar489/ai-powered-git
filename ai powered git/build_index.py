
import json
import argparse
import sys
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict


# ─────────────────────────── file auto-detection ─────────────────────────────

def _find_latest(directory: Path, prefix: str) -> Path | None:
    """Return the most-recently modified JSON file matching prefix in directory."""
    matches = sorted(
        directory.glob(f"{prefix}*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return matches[0] if matches else None


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


# ─────────────────────────── index builder ───────────────────────────────────

def build_index(
    structure_path: Path,
    actions_path:   Path,
    code_path:      Path | None,
    ukg_path:       Path | None,
) -> dict:
    """
    Merge four JSON files into one unified branch-centric index.

    Parameters
    ----------
    structure_path : repo_structure_<ts>.json
    actions_path   : git_actions_<ts>.json
    code_path      : code_metadata_<ts>.json  (None if --skip-code was used)
    ukg_path       : ukg_<ts>.json            (None if --skip-code was used)

    Returns
    -------
    Fully merged index dict.
    """

    print("  Loading JSON files ...")
    struct_data  = _load(structure_path)
    actions_data = _load(actions_path)
    code_data    = _load(code_path)   if code_path  else {"branches": []}
    ukg_data     = _load(ukg_path)    if ukg_path   else {"branches": []}

    # ── Keyed lookups from each source ───────────────────────────────────────

    # structure:  branch_name → branch record
    struct_by_branch: dict = {
        b["branch"]: b for b in struct_data.get("branches", [])
    }

    # actions:    branch_name → branch record
    actions_by_branch: dict = {
        b["branch"]: b for b in actions_data.get("branches", [])
    }

    # code:       branch_name → branch record
    code_by_branch: dict = {
        b["branch"]: b for b in code_data.get("branches", [])
    }

    # ukg:        branch_name → branch record
    ukg_by_branch: dict = {
        b["branch"]: b for b in ukg_data.get("branches", [])
    }

    # file_commit_history keyed by (branch, filepath) for fast join
    history_index: dict = {}   # (branch, filepath) → history record
    for b in actions_data.get("branches", []):
        branch_name = b["branch"]
        for fh in b.get("file_commit_history", []):
            history_index[(branch_name, fh["filepath"])] = fh

    # ── Cross-branch lookup tables ────────────────────────────────────────────
    file_to_branches:     dict = defaultdict(list)   # filepath     → [branch]
    function_to_branches: dict = defaultdict(list)   # func_id      → [branch]
    class_to_branches:    dict = defaultdict(list)   # class_name   → [branch]
    author_to_commits:    dict = defaultdict(list)   # email        → [{...}]
    commit_to_branch:     dict = {}                  # commit_hash  → branch

    # ── Build per-branch entries ──────────────────────────────────────────────
    all_branch_names = sorted(
        set(struct_by_branch) | set(actions_by_branch) | set(code_by_branch)
    )

    branches_index: dict = {}

    for branch_name in all_branch_names:

        sb  = struct_by_branch.get(branch_name,  {})
        ab  = actions_by_branch.get(branch_name, {})
        cb  = code_by_branch.get(branch_name,    {})
        ub  = ukg_by_branch.get(branch_name,     {})

        # ── 1. META ──────────────────────────────────────────────────────────
        meta = {
            "tip_commit":           sb.get("tip_commit")   or ab.get("tip_commit", ""),
            "is_remote":            sb.get("is_remote")    or ab.get("is_remote", False),
            "created_date":         sb.get("created_date") or ab.get("created_date", ""),
            "created_by":           sb.get("created_by")   or ab.get("created_by", ""),
            "total_files":          sb.get("total_files", 0),
            "total_size_bytes":     sb.get("total_size_bytes", 0),
            "extension_summary":    sb.get("extension_summary", []),
            "stats":                ab.get("stats", {}),
            "commit_type_breakdown":ab.get("commit_type_breakdown", {}),
            "merged_branches":      ab.get("merged_branches", []),
            "tags":                 ab.get("tags", []),
            "gitignore_patterns":   ab.get("gitignore_patterns", []),
            "ukg_stats":            ub.get("ukg_stats", {}),
        }

        # ── 2. FILES  (structure + code + history joined on filepath) ─────────
        files_index: dict = {}

        # Seed from structure (every tracked file)
        for f in sb.get("files", []):
            fp = f["filepath"]
            files_index[fp] = {
                "info": {
                    "filename":  f["filename"],
                    "directory": f["directory"],
                    "extension": f["extension"],
                    "size_bytes":f["size_bytes"],
                    "blob_hash": f["blob_hash"],
                    "mode":      f["mode"],
                },
                "code":    {
                    "functions": [],
                    "classes":   [],
                    "imports":   [],
                    "calls":     [],
                },
                "history": {},
            }
            # Cross-branch lookup
            file_to_branches[fp].append(branch_name)

        # Overlay code symbols
# Overlay code symbols
        files_section = cb.get("files", {})

        if isinstance(files_section, dict):
            iterable = files_section.items()

        elif isinstance(files_section, list):
            iterable = ((f["filepath"], f) for f in files_section)

        else:
            iterable = []

        for fp, sym in iterable:
            if fp not in files_index:
                files_index[fp] = {"info": {}, "code": {}, "history": {}}

            files_index[fp]["code"] = {
                "functions": sym.get("functions", []),
                "classes":   sym.get("classes",   []),
                "imports":   sym.get("imports",   []),
                "calls":     sym.get("calls",     []),
            }
            
            # Function / class cross-branch lookups
            for fn in sym.get("functions", []):
                if isinstance(fn, dict):
                    fn_name = fn.get("name")
                else:
                    fn_name = fn

                if fn_name:
                    func_id = f"{fp}:{fn_name}"
                    function_to_branches[func_id].append(branch_name)

            # Classes
            for cls in sym.get("classes", []):
                if isinstance(cls, dict):
                    cls_name = cls.get("name")
                else:
                    cls_name = cls

                if cls_name:
                    class_to_branches[cls_name].append(branch_name)

        # Overlay per-file history
        for fp in list(files_index.keys()):
            hist = history_index.get((branch_name, fp))
            if hist:
                files_index[fp]["history"] = {
                    "total_commits":  hist["total_commits"],
                    "first_seen":     hist["first_seen"],
                    "last_modified":  hist["last_modified"],
                    "unique_authors": hist["unique_authors"],
                    "commits":        hist["commits"],
                }

        # ── 3. GIT  (commits / merges / file_actions) ─────────────────────────
        commits = ab.get("commits", [])
        for c in commits:
            commit_to_branch[c["hash"]] = branch_name
            author_to_commits[c["author_email"]].append({
                "branch":  branch_name,
                "hash":    c["hash"],
                "date":    c["author_date"],
                "subject": c["subject"],
                "type":    c["type"],
            })

        git_section = {
            "commits":      commits,
            "merges":       ab.get("merges",       []),
            "file_actions": ab.get("file_actions", []),
        }

        # ── 4. GRAPH  (call_graph + dependency_graph + ukg) ───────────────────
        graph_section = {
            "call_graph":       cb.get("call_graph",       {}),
            "dependency_graph": cb.get("dependency_graph", {}),
            "ukg":              ub.get("ukg",              {}),
        }

        # ── Assemble branch entry ─────────────────────────────────────────────
        branches_index[branch_name] = {
            "meta":  meta,
            "files": files_index,
            "git":   git_section,
            "graph": graph_section,
        }

        _print_branch_summary(branch_name, meta, files_index)

    # ── Finalise lookup tables ────────────────────────────────────────────────
    lookup = {
        "file_to_branches":     dict(file_to_branches),
        "function_to_branches": dict(function_to_branches),
        "class_to_branches":    dict(class_to_branches),
        "author_to_commits":    dict(author_to_commits),
        "commit_to_branch":     commit_to_branch,
    }

    return {
        "description": (
            "Unified branch-centric index. "
            "Each branch entry contains: meta (stats, tags, ukg_stats), "
            "files (info + code symbols + commit history per file), "
            "git (commits, merges, file_actions), "
            "graph (call_graph, dependency_graph, ukg). "
            "Top-level lookup tables enable cross-branch querying by "
            "file, function, class, author, or commit hash."
        ),
        "indexed_at":  datetime.now(timezone.utc).isoformat(),
        "repository":  struct_data.get("repository", actions_data.get("repository", {})),
        "total_branches": len(branches_index),
        "sources": {
            "repo_structure":  str(structure_path),
            "git_actions":     str(actions_path),
            "code_metadata":   str(code_path)  if code_path  else None,
            "ukg":             str(ukg_path)   if ukg_path   else None,
        },
        "branches": branches_index,
        "lookup":   lookup,
    }


# ─────────────────────────── print helpers ───────────────────────────────────

def _print_branch_summary(branch_name: str, meta: dict, files: dict) -> None:
    files_with_code    = sum(1 for f in files.values() if f["code"]["functions"] or f["code"]["classes"])
    files_with_history = sum(1 for f in files.values() if f["history"])
    print(
        f"    {branch_name:<40}"
        f"  files:{len(files):>5}"
        f"  w/code:{files_with_code:>5}"
        f"  w/history:{files_with_history:>5}"
        f"  commits:{meta['stats'].get('total_commits', 0):>5}"
    )


def _divider(): print("=" * 72)
def _header(t): _divider(); print(f"  {t}"); _divider()


# ─────────────────────────── CLI ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Build unified index from main.py JSON outputs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Auto-detection (use -i to point at the output directory of main.py):
  python build_index.py -i ./reports

Explicit file paths:
  python build_index.py \\
      --structure repo_structure_20250601_120000.json \\
      --actions   git_actions_20250601_120000.json \\
      --code      code_metadata_20250601_120000.json \\
      --ukg       ukg_20250601_120000.json \\
      -o ./reports
        """,
    )
    parser.add_argument(
        "-i", "--input-dir", default=None,
        help="Directory containing the four JSON files (auto-picks latest of each).",
    )
    parser.add_argument("--structure", default=None, help="Explicit repo_structure_*.json path.")
    parser.add_argument("--actions",   default=None, help="Explicit git_actions_*.json path.")
    parser.add_argument("--code",      default=None, help="Explicit code_metadata_*.json path.")
    parser.add_argument("--ukg",       default=None, help="Explicit ukg_*.json path.")
    parser.add_argument(
        "-o", "--output-dir", default=None,
        help="Where to write index_<ts>.json (default: same as input-dir or cwd).",
    )

    args = parser.parse_args()

    # ── Resolve file paths ────────────────────────────────────────────────────
    input_dir = Path(args.input_dir) if args.input_dir else Path(".")

    def _resolve(explicit: str | None, prefix: str) -> Path | None:
        if explicit:
            p = Path(explicit)
            if not p.exists():
                print(f"  ERROR: {p} not found.", file=sys.stderr)
                sys.exit(1)
            return p
        found = _find_latest(input_dir, prefix)
        if found:
            print(f"  Auto-detected: {found.name}")
        return found

    _header("build_index.py — Unified Index Builder")

    structure_path = _resolve(args.structure, "repo_structure_")
    actions_path   = _resolve(args.actions,   "git_actions_")
    code_path      = _resolve(args.code,      "code_metadata_")
    ukg_path       = _resolve(args.ukg,       "ukg_")

    if not structure_path or not actions_path:
        print(
            "  ERROR: At minimum repo_structure and git_actions files are required.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not code_path:
        print("  WARNING: code_metadata not found — code symbols will be empty.")
    if not ukg_path:
        print("  WARNING: ukg not found — graph section will be empty.")

    _divider()

    # ── Build ─────────────────────────────────────────────────────────────────
    print("  Building index ...\n")
    index = build_index(structure_path, actions_path, code_path, ukg_path)

    # ── Write output ──────────────────────────────────────────────────────────
    output_dir = Path(args.output_dir) if args.output_dir else input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"index_{ts}.json"
    output_path.write_text(
        json.dumps(index, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # ── Summary ───────────────────────────────────────────────────────────────
    _divider()
    total_files   = sum(len(b["files"]) for b in index["branches"].values())
    total_commits = sum(
        len(b["git"]["commits"]) for b in index["branches"].values()
    )
    total_fns = sum(
        len(f["code"]["functions"])
        for b in index["branches"].values()
        for f in b["files"].values()
    )
    total_cls = sum(
        len(f["code"]["classes"])
        for b in index["branches"].values()
        for f in b["files"].values()
    )
    lk = index["lookup"]
    print(f"  Branches  : {index['total_branches']}")
    print(f"  Files     : {total_files:,}  (unique paths across branches)")
    print(f"  Commits   : {total_commits:,}")
    print(f"  Functions : {total_fns:,}")
    print(f"  Classes   : {total_cls:,}")
    print(f"  Lookup")
    print(f"    file_to_branches     : {len(lk['file_to_branches']):,} entries")
    print(f"    function_to_branches : {len(lk['function_to_branches']):,} entries")
    print(f"    class_to_branches    : {len(lk['class_to_branches']):,} entries")
    print(f"    author_to_commits    : {len(lk['author_to_commits']):,} entries")
    print(f"    commit_to_branch     : {len(lk['commit_to_branch']):,} entries")
    print()
    print(f"  OUTPUT -> {output_path.resolve()}")
    _divider()


if __name__ == "__main__":

    main()

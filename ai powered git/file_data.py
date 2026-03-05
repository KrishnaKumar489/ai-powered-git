"""
file_metadata.py
----------------
Collects file system snapshot metadata from a git repository — branch-aware.
Reads git objects directly. No checkout needed.

Exported:
    structure_for_branch(repo_path, branch_info) -> dict
"""

import subprocess
from pathlib import Path
from collections import defaultdict


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


# ─────────────────────────── file listing ────────────────────────────────────

def files_on_branch(repo_path: str, branch: str) -> list[dict]:
    """
    List all files tracked on a given branch with blob-level metadata.

    Returns a list of dicts with keys:
        filepath, filename, directory, extension,
        size_bytes, blob_hash, mode
    """
    try:
        out = _git(["ls-tree", "-r", "-l", "--full-tree", branch], repo_path)
    except subprocess.CalledProcessError:
        return []

    files = []
    for line in out.splitlines():
        # git ls-tree -l format: <mode> <type> <hash> <size>\t<path>
        try:
            meta, path = line.split("\t", 1)
            parts      = meta.split()
            mode       = parts[0]
            ftype      = parts[1]
            blob_hash  = parts[2]
            size       = int(parts[3]) if parts[3].isdigit() else 0
        except (ValueError, IndexError):
            continue

        if ftype != "blob":
            continue

        p = Path(path)
        files.append({
            "filepath":  path,
            "filename":  p.name,
            "directory": str(p.parent) if str(p.parent) != "." else "",
            "extension": p.suffix.lower() if p.suffix else "(none)",
            "size_bytes": size,
            "blob_hash": blob_hash,
            "mode":      mode,
        })

    return files


# ─────────────────────────── folder tree ─────────────────────────────────────

def build_folder_tree(files: list[dict]) -> list[dict]:
    """
    Derive full folder hierarchy from a flat file list with aggregated stats.

    Returns a sorted list of dicts with keys:
        folder_path, file_count, total_size_bytes, extensions[]
    """
    folders: dict = defaultdict(
        lambda: {"file_count": 0, "total_size_bytes": 0, "extensions": set()}
    )

    for f in files:
        parts = Path(f["filepath"]).parts
        for depth in range(1, len(parts)):          # every ancestor folder
            folder = str(Path(*parts[:depth]))
            folders[folder]["file_count"]       += 1
            folders[folder]["total_size_bytes"] += f["size_bytes"]
            folders[folder]["extensions"].add(f["extension"])

    return [
        {
            "folder_path":      fp,
            "file_count":       stats["file_count"],
            "total_size_bytes": stats["total_size_bytes"],
            "extensions":       sorted(stats["extensions"]),
        }
        for fp, stats in sorted(folders.items())
    ]


# ─────────────────────────── extension summary ───────────────────────────────

def build_extension_summary(files: list[dict]) -> list[dict]:
    """Aggregate file counts and sizes by extension, sorted by count desc."""
    summary: dict = defaultdict(lambda: {"count": 0, "total_bytes": 0})
    for f in files:
        summary[f["extension"]]["count"]       += 1
        summary[f["extension"]]["total_bytes"] += f["size_bytes"]

    return [
        {"extension": ext, **stats}
        for ext, stats in sorted(summary.items(), key=lambda x: -x[1]["count"])
    ]


# ─────────────────────────── branch structure ────────────────────────────────

def structure_for_branch(repo_path: str, branch_info: dict) -> dict:
    """
    Collect complete file-system snapshot for one branch.

    Args:
        repo_path:   Absolute path to the git repository.
        branch_info: Branch dict as returned by git_utils.all_branches().

    Returns a dict with:
        branch, is_remote, tip_commit,
        total_files, total_size_bytes,
        extension_summary[], folders[], files[]
    """
    branch  = branch_info["name"]
    files   = files_on_branch(repo_path, branch)
    folders = build_folder_tree(files)
    ext_sum = build_extension_summary(files)

    return {
        "branch":            branch,
        "is_remote":         branch_info["is_remote"],
        "tip_commit":        branch_info["tip_commit"],
        "created_date":      branch_info.get("created_date", ""),
        "created_by":        branch_info.get("created_by", ""),
        "total_files":       len(files),
        "total_size_bytes":  sum(f["size_bytes"] for f in files),
        "extension_summary": ext_sum,
        "folders":           folders,
        "files":             files,
    }

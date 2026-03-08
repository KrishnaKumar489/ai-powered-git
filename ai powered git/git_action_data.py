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


# ─────────────────────────── commit classification ───────────────────────────

# Conventional-commit prefixes → semantic type labels
_COMMIT_PREFIXES = [
    (("feat", "feature"),  "feature"),
    (("fix",),             "bugfix"),
    (("refactor",),        "refactor"),
    (("docs",),            "docs"),
    (("test",),            "test"),
    (("chore",),           "chore"),
    (("ci",),              "ci"),
    (("perf",),            "performance"),
    (("revert",),          "revert"),
    (("merge",),           "merge"),
    (("bump",),            "version-bump"),
    (("wip",),             "wip"),
    (("style",),           "style"),
    (("build",),           "build"),
    (("hotfix",),          "hotfix"),
    (("release",),         "release"),
    (("security",),        "security"),
    (("deps",),            "dependency-update"),
]

def classify_commit(subject: str) -> str:
    """Map a commit subject line to a semantic type string."""
    s = subject.lower().strip()
    for prefixes, label in _COMMIT_PREFIXES:
        if any(s.startswith(p) for p in prefixes):
            return label
    return "other"


# ─────────────────────────── commits ─────────────────────────────────────────

def get_commits(repo_path: str, branch: str) -> list[dict]:
    """
    Return all non-merge commits on a branch.

    Each dict contains:
        hash, short_hash, author, author_email,
        author_date, commit_date, parent_hashes[],
        subject, body, type (classified)
    """
    fmt   = "%H|%h|%an|%ae|%ai|%ci|%P|%s|%b"
    lines = _git_lines(["log", branch, f"--format={fmt}", "--no-merges"], repo_path)

    commits = []
    for line in lines:
        parts = line.split("|", 8)
        while len(parts) < 9:
            parts.append("")
        full_hash, short_hash, author, email, \
            author_date, commit_date, parents, subject, body = parts

        commits.append({
            "hash":         full_hash,
            "short_hash":   short_hash,
            "author":       author,
            "author_email": email,
            "author_date":  author_date,
            "commit_date":  commit_date,
            "parent_hashes": [p for p in parents.split() if p],
            "subject":      subject,
            "body":         body.strip(),
            "type":         classify_commit(subject),
        })

    return commits


# ─────────────────────────── merges ──────────────────────────────────────────

def get_merges(repo_path: str, branch: str) -> list[dict]:
    """
    Return all merge commits on a branch.

    Each dict contains:
        hash, short_hash, author, author_email, date,
        merged_from (parent commit), merged_into (base commit), subject
    """
    fmt   = "%H|%h|%an|%ae|%ai|%P|%s"
    lines = _git_lines(["log", branch, f"--format={fmt}", "--merges"], repo_path)

    merges = []
    for line in lines:
        parts = line.split("|", 6)
        while len(parts) < 7:
            parts.append("")
        full_hash, short_hash, author, email, date, parents, subject = parts
        parent_list = parents.split()

        merges.append({
            "hash":         full_hash,
            "short_hash":   short_hash,
            "author":       author,
            "author_email": email,
            "date":         date,
            "merged_from":  parent_list[1] if len(parent_list) > 1 else "",
            "merged_into":  parent_list[0] if parent_list else "",
            "subject":      subject,
        })

    return merges


# ─────────────────────────── file-level actions ──────────────────────────────

def get_file_actions(repo_path: str, branch: str) -> list[dict]:
    """
    Parse git log with --diff-filter to capture file-level events.

    Action types: added, deleted, modified, renamed, copied

    Each dict contains:
        action, commit_hash, author, author_email, date, commit_subject,
        filepath  (for added/deleted/modified)
        from_path / to_path  (for renamed/copied)
    """
    lines = _git_lines(
        ["log", branch,
         "--diff-filter=ADRMC",
         "--name-status",
         "--format=COMMIT:%H|%an|%ae|%ai|%s"],
        repo_path
    )

    action_map = {
        "A": "added",
        "D": "deleted",
        "M": "modified",
        "R": "renamed",
        "C": "copied",
    }

    actions        = []
    current_commit = {}

    for line in lines:
        if line.startswith("COMMIT:"):
            parts = line[7:].split("|", 4)
            while len(parts) < 5:
                parts.append("")
            current_commit = {
                "hash":    parts[0],
                "author":  parts[1],
                "email":   parts[2],
                "date":    parts[3],
                "subject": parts[4],
            }
        elif line and current_commit:
            cols        = line.split("\t")
            status_raw  = cols[0].rstrip("0123456789")   # strip similarity score
            action_type = action_map.get(status_raw[0], "unknown")

            entry = {
                "action":         action_type,
                "commit_hash":    current_commit["hash"],
                "author":         current_commit["author"],
                "author_email":   current_commit["email"],
                "date":           current_commit["date"],
                "commit_subject": current_commit["subject"],
            }

            if action_type in ("renamed", "copied") and len(cols) >= 3:
                entry["from_path"] = cols[1]
                entry["to_path"]   = cols[2]
            elif len(cols) >= 2:
                entry["filepath"]  = cols[1]
            else:
                entry["filepath"]  = cols[0]

            actions.append(entry)

    return actions


# ─────────────────────────── per-file commit history ─────────────────────────

def get_file_commit_history(repo_path: str, branch: str) -> list[dict]:
    """
    For every file tracked on the branch, fetch its full commit history
    (following renames via --follow).

    Returns a list of dicts, one per file:
        filepath, total_commits, first_seen, last_modified,
        unique_authors[], commits[]
    """
    files   = _git_lines(["ls-tree", "-r", "--name-only", branch], repo_path)
    history = []

    for filepath in files:
        fmt   = "%H|%h|%an|%ae|%ai|%s"
        lines = _git_lines(
            ["log", f"--format={fmt}", branch, "--follow", "--", filepath],
            repo_path
        )
        commits = []
        for line in lines:
            parts = line.split("|", 5)
            while len(parts) < 6:
                parts.append("")
            commits.append({
                "hash":         parts[0],
                "short_hash":   parts[1],
                "author":       parts[2],
                "author_email": parts[3],
                "date":         parts[4],
                "subject":      parts[5],
                "type":         classify_commit(parts[5]),
            })

        if commits:
            history.append({
                "filepath":       filepath,
                "total_commits":  len(commits),
                "first_seen":     commits[-1]["date"],
                "last_modified":  commits[0]["date"],
                "unique_authors": list({c["author"] for c in commits}),
                "commits":        commits,
            })

    return history


# ─────────────────────────── tags ────────────────────────────────────────────

def get_tags(repo_path: str, branch: str) -> list[dict]:
    """
    Return all tags reachable from the given branch, sorted newest first.

    Each dict: tag, commit_hash, date, tagger, tagger_email, message
    """
    tag_names = _git_lines(
        ["tag", "--merged", branch, "--sort=-version:refname"], repo_path
    )
    tags = []
    for tag in tag_names:
        info = _git(
            ["show", "--quiet", "--format=%H|%ai|%an|%ae|%s", tag], repo_path
        ).splitlines()
        if not info:
            continue
        parts = info[0].split("|", 4)
        while len(parts) < 5:
            parts.append("")
        tags.append({
            "tag":          tag,
            "commit_hash":  parts[0],
            "date":         parts[1],
            "tagger":       parts[2],
            "tagger_email": parts[3],
            "message":      parts[4],
        })
    return tags


# ─────────────────────────── branch stats ────────────────────────────────────

def get_branch_stats(repo_path: str, branch: str, base_branch: str = "main") -> dict:
    """
    Compute summary statistics for a branch.

    Returns:
        total_commits, commits_ahead_of_base, commits_behind_base,
        base_branch_compared, unique_contributors, contributor_emails[],
        first_commit_date, last_commit_date
    """
    stats: dict = {}

    try:
        stats["total_commits"] = int(_git(["rev-list", "--count", branch], repo_path))
    except (subprocess.CalledProcessError, ValueError):
        stats["total_commits"] = 0

    try:
        ab     = _git(["rev-list", "--left-right", "--count",
                        f"{base_branch}...{branch}"], repo_path)
        behind, ahead = ab.split()
        stats["commits_ahead_of_base"] = int(ahead)
        stats["commits_behind_base"]   = int(behind)
        stats["base_branch_compared"]  = base_branch
    except (subprocess.CalledProcessError, ValueError):
        stats["commits_ahead_of_base"] = None
        stats["commits_behind_base"]   = None
        stats["base_branch_compared"]  = base_branch

    try:
        authors = _git_lines(["log", branch, "--format=%ae", "--no-merges"], repo_path)
        stats["unique_contributors"] = len(set(authors))
        stats["contributor_emails"]  = sorted(set(authors))
    except subprocess.CalledProcessError:
        stats["unique_contributors"] = 0
        stats["contributor_emails"]  = []

    try:
        stats["first_commit_date"] = _git(
            ["log", branch, "--reverse", "--format=%ai", "--max-count=1"], repo_path
        )
        stats["last_commit_date"]  = _git(
            ["log", branch, "--format=%ai", "--max-count=1"], repo_path
        )
    except subprocess.CalledProcessError:
        stats["first_commit_date"] = ""
        stats["last_commit_date"]  = ""

    return stats


# ─────────────────────────── merged branches ─────────────────────────────────

def get_merged_branches(repo_path: str, branch: str) -> list[str]:
    """Return names of all branches already merged into the given branch."""
    lines = _git_lines(
        ["branch", "--merged", branch, "--format=%(refname:short)"], repo_path
    )
    return [b for b in lines if b != branch]


# ─────────────────────────── gitignore ───────────────────────────────────────

def get_gitignore_patterns(repo_path: str, branch: str) -> list[str]:
    """
    Extract active .gitignore patterns from the branch tree (no disk read).
    Returns only non-empty, non-comment lines.
    """
    try:
        content = _git(["show", f"{branch}:.gitignore"], repo_path)
        return [
            l.strip() for l in content.splitlines()
            if l.strip() and not l.strip().startswith("#")
        ]
    except subprocess.CalledProcessError:
        return []


# ─────────────────────────── stashes ─────────────────────────────────────────

def get_stashes(repo_path: str) -> list[dict]:
    """
    Return all stash entries in the repository (global, not branch-scoped).

    Each dict: ref, date, message
    """
    lines = _git_lines(["stash", "list", "--format=%gd|%ai|%s"], repo_path)
    stashes = []
    for line in lines:
        parts = line.split("|", 2)
        while len(parts) < 3:
            parts.append("")
        stashes.append({"ref": parts[0], "date": parts[1], "message": parts[2]})
    return stashes


# ─────────────────────────── hooks ───────────────────────────────────────────

def get_hooks(repo_path: str) -> list[dict]:
    """
    Return git hooks present in .git/hooks (excluding .sample files).

    Each dict: hook (name), executable (bool)
    """
    hooks_dir = Path(repo_path) / ".git" / "hooks"
    if not hooks_dir.exists():
        return []
    return [
        {
            "hook":       f.name,
            "executable": bool(f.stat().st_mode & 0o111),
        }
        for f in sorted(hooks_dir.iterdir())
        if not f.name.endswith(".sample")
    ]


# ─────────────────────────── branch rollup ───────────────────────────────────

def actions_for_branch(
    repo_path: str,
    branch_info: dict,
    base_branch: str = "main",
) -> dict:
    """
    Collect all git actions for a single branch.

    Args:
        repo_path:    Absolute path to the git repository.
        branch_info:  Branch dict from git_utils.all_branches().
        base_branch:  Branch used for ahead/behind comparison.

    Returns a dict with:
        branch, is_remote, tip_commit, tip_subject, created_date, created_by,
        stats{}, commit_type_breakdown{},
        merged_branches[], gitignore_patterns[],
        commits[], merges[], file_actions[], tags[], file_commit_history[]
    """
    branch = branch_info["name"]

    print(f"    commits      ...", end="\r")
    commits = get_commits(repo_path, branch)

    print(f"    merges       ...", end="\r")
    merges = get_merges(repo_path, branch)

    print(f"    file actions ...", end="\r")
    file_actions = get_file_actions(repo_path, branch)

    print(f"    tags         ...", end="\r")
    tags = get_tags(repo_path, branch)

    print(f"    file history ...", end="\r")
    file_history = get_file_commit_history(repo_path, branch)

    print(f"    stats        ...", end="\r")
    stats           = get_branch_stats(repo_path, branch, base_branch)
    merged_branches = get_merged_branches(repo_path, branch)
    gitignore       = get_gitignore_patterns(repo_path, branch)
    print(" " * 60, end="\r")

    type_breakdown: dict = defaultdict(int)
    for c in commits:
        type_breakdown[c["type"]] += 1

    return {
        "branch":               branch,
        "is_remote":            branch_info["is_remote"],
        "tip_commit":           branch_info["tip_commit"],
        "tip_subject":          branch_info.get("tip_subject", ""),
        "created_date":         branch_info.get("created_date", ""),
        "created_by":           branch_info.get("created_by", ""),
        "stats":                stats,
        "commit_type_breakdown": dict(type_breakdown),
        "merged_branches":      merged_branches,
        "gitignore_patterns":   gitignore,
        "commits":              commits,
        "merges":               merges,
        "file_actions":         file_actions,
        "tags":                 tags,
        "file_commit_history":  file_history,
    }


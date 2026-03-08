"""
query_agent.py  —  Agentic Query Engine (replaces query.py)
============================================================

Instead of hardcoded intent patterns, gives Bedrock a set of tools
and lets it decide what to fetch. Bedrock calls tools in a loop
until it has enough information to answer.

Usage:
    python query_agent.py -i ./reports -q "what does login do"
    python query_agent.py -i ./reports  # REPL
"""

import json
import re
import subprocess
import argparse
import sys
from pathlib import Path

try:
    from rich.console import Console
    from rich.markdown import Markdown
    from rich.panel import Panel
    console = Console()
    def _print_md(text):     console.print(Markdown(text))
    def _panel(title, body): console.print(Panel(body, title=title))
except ImportError:
    def _print_md(text):     print(text)
    def _panel(title, body): print(f"\n[{title}]\n{body}")


# ─────────────────────────────────────────────────────────────────────────────
# TOOL DEFINITIONS  (sent to Bedrock so it knows what it can call)
# ─────────────────────────────────────────────────────────────────────────────

TOOLS = [
    {
        "toolSpec": {
            "name": "list_branches",
            "description": "List all branches in the repo with file counts and commit counts.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "get_branch_info",
            "description": (
                "Get full info about a branch: file list with function names, "
                "recent commits, contributors, stats. Use this for branch summaries "
                "or when you need to know what files exist on a branch."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "branch": {
                            "type": "string",
                            "description": "Branch name (e.g. 'main', 'api-additions', 'origin/develop'). Partial names like 'api-additions' will be matched."
                        }
                    },
                    "required": ["branch"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "get_file",
            "description": (
                "Fetch the actual source code of a file from the repo. "
                "Use this when you need to read what a function does, how something is implemented, "
                "or explain code. You MUST call this to get real code — never guess at implementations."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "filepath": {
                            "type": "string",
                            "description": "File path as it appears in the repo (e.g. 'fastapi/main.py')"
                        },
                        "branch": {
                            "type": "string",
                            "description": "Branch to fetch from"
                        },
                        "function_name": {
                            "type": "string",
                            "description": "Optional: extract only this function/class from the file"
                        }
                    },
                    "required": ["filepath", "branch"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "search_files",
            "description": (
                "Search for files or functions by keyword. Returns ranked list of matches. "
                "Use this when you don't know the exact filename — e.g. 'find files related to authentication' "
                "or 'which file handles post retrieval'. Returns file paths, function names, import lists."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "keywords": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Keywords to search for (e.g. ['login', 'auth', 'jwt'])"
                        },
                        "branch": {
                            "type": "string",
                            "description": "Branch to search (optional, defaults to main)"
                        }
                    },
                    "required": ["keywords"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "get_file_history",
            "description": "Get commit history for a specific file: who changed it, when, and what they did.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "filepath": {"type": "string"},
                        "branch":   {"type": "string"}
                    },
                    "required": ["filepath", "branch"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "get_call_graph",
            "description": "Find what functions call a given function, and what that function calls. Use for tracing execution flow.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "function_name": {"type": "string", "description": "Function name to trace"},
                        "branch":        {"type": "string"}
                    },
                    "required": ["function_name"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "get_contributors",
            "description": "Get contributor stats: who committed the most, to which branches, recent activity.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "branch": {"type": "string", "description": "Optional: filter to a specific branch"}
                    },
                    "required": []
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "get_recent_commits",
            "description": "Get the most recent commits across all branches or a specific branch.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "branch": {"type": "string", "description": "Optional branch filter"},
                        "limit":  {"type": "integer", "description": "Max commits to return (default 15)"}
                    },
                    "required": []
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "compare_branches",
            "description": "Compare two branches: file diffs, function diffs, contributor diffs, commit counts.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "branch1": {"type": "string"},
                        "branch2": {"type": "string"}
                    },
                    "required": ["branch1", "branch2"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "get_hotspots",
            "description": "Find the most frequently changed files (highest commit churn). Useful for identifying risky areas.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "branch": {"type": "string"},
                        "limit":  {"type": "integer", "description": "Number of files to return (default 15)"}
                    },
                    "required": []
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "find_dead_code",
            "description": "Find functions that are defined but never called anywhere in the codebase.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "branch": {"type": "string"}
                    },
                    "required": []
                }
            }
        }
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# TOOL EXECUTOR  (runs the actual tool calls Bedrock requests)
# ─────────────────────────────────────────────────────────────────────────────

def _str(val) -> str:
    if isinstance(val, str): return val
    if isinstance(val, dict):
        for k in ("name","func_name","class_name","symbol"):
            if k in val: return str(val[k])
        return " ".join(str(v) for v in val.values() if isinstance(v,str))
    return str(val)


def _resolve_branch(index: dict, name: str) -> str | None:
    """Match a short branch name like 'api-additions' to 'origin/api-additions'."""
    branches = index["branches"]
    # exact match first
    if name in branches:
        return name
    # short name match
    for b in sorted(branches, key=len, reverse=True):
        short = re.sub(r"^(origin|remotes/[^/]+)/", "", b.lower())
        if name.lower() == short or name.lower() == b.lower():
            return b
    # partial match
    for b in branches:
        if name.lower() in b.lower():
            return b
    return None


def _pick_default_branch(index: dict) -> str:
    branches = list(index["branches"].keys())
    for d in ("main","master","develop","origin/main","origin/master"):
        if d in branches: return d
    non_head = [b for b in branches if "HEAD" not in b]
    return non_head[0] if non_head else branches[0]


def _fetch_code(repo_path: str, blob_hash: str | None,
                branch: str, filepath: str,
                function_name: str | None = None,
                max_lines: int = 300) -> str:
    raw = ""
    if blob_hash:
        try:
            r = subprocess.run(["git","cat-file","blob",blob_hash],
                               cwd=repo_path, capture_output=True, check=True)
            raw = r.stdout.decode("utf-8", errors="replace")
        except subprocess.CalledProcessError:
            pass
    if not raw:
        try:
            r = subprocess.run(["git","show",f"{branch}:{filepath}"],
                               cwd=repo_path, capture_output=True, check=True)
            raw = r.stdout.decode("utf-8", errors="replace")
        except subprocess.CalledProcessError:
            return f"[Could not fetch {filepath} from {branch}]"

    if function_name:
        # extract just the named function/class
        lines = raw.splitlines()
        pat = re.compile(
            rf"^(async\s+)?def\s+{re.escape(function_name)}\b"
            rf"|^class\s+{re.escape(function_name)}\b"
        )
        start = next((i for i,l in enumerate(lines) if pat.search(l)), None)
        if start is not None:
            bi   = len(lines[start]) - len(lines[start].lstrip())
            body = [lines[start]]
            for line in lines[start+1:]:
                s = line.lstrip()
                if not s: body.append(line); continue
                if len(line)-len(s) <= bi: break
                body.append(line)
            return "\n".join(body)

    lines = raw.splitlines()
    if len(lines) > max_lines:
        return "\n".join(lines[:max_lines]) + f"\n... [{len(lines)-max_lines} more lines]"
    return raw


def execute_tool(tool_name: str, tool_input: dict,
                 index: dict, repo_path: str) -> str:
    """Execute a tool call requested by Bedrock and return result as string."""

    branches  = index["branches"]
    lookup    = index["lookup"]
    default_b = _pick_default_branch(index)

    # ── list_branches ─────────────────────────────────────────────────────────
    if tool_name == "list_branches":
        lines = ["Branches in this repository:\n"]
        for b, bd in sorted(branches.items()):
            m     = bd["meta"]
            stats = m.get("stats", {})
            total_commits = stats.get("total_commits") or m.get("total_commits") or len(bd["git"]["commits"])
            short = re.sub(r"^(origin|remotes/[^/]+)/", "", b)
            lines.append(
                f"  {short:<40} "
                f"files:{m.get('total_files',0) or len(bd['files']):>5}  "
                f"commits:{total_commits:>5}"
            )
        return "\n".join(lines)

    # ── get_branch_info ───────────────────────────────────────────────────────
    if tool_name == "get_branch_info":
        br_name = tool_input.get("branch", default_b)
        br = _resolve_branch(index, br_name)
        if not br:
            return f"Branch '{br_name}' not found. Available: {list(branches.keys())}"
        bd   = branches[br]
        meta = bd["meta"]
        stats = meta.get("stats", {})
        def _s(k, fb=0): return stats.get(k) or meta.get(k) or fb

        file_rows = []
        for fp, fe in sorted(bd["files"].items()):
            code = fe.get("code", {})
            fns  = [_str(f) for f in code.get("functions",[])][:5]
            cls  = [_str(c) for c in code.get("classes",[])][:3]
            hist = fe.get("history", {})
            row  = f"  {fp}"
            if fns: row += f"  fns:[{', '.join(fns)}{'...' if len(fns)==5 else ''}]"
            if cls: row += f"  cls:[{', '.join(cls)}]"
            row += f"  ({fe.get('info',{}).get('size_bytes',0)} bytes, {hist.get('total_commits',0)} commits)"
            file_rows.append(row)

        recent = "\n".join(
            f"  {c.get('short_hash',c.get('hash','')[:7])} "
            f"{str(c.get('author_date',c.get('date','')))[:10]} "
            f"{c.get('author','')} — {c.get('subject','')}"
            for c in bd["git"]["commits"][:10]
        )

        return (
            f"Branch: {br}\n"
            f"Total files   : {meta.get('total_files',0) or len(bd['files'])}\n"
            f"Total commits : {_s('total_commits') or len(bd['git']['commits'])}\n"
            f"Contributors  : {_s('unique_contributors')} — {_s('contributor_emails',[])}\n"
            f"Ahead of base : {_s('commits_ahead_of_base')}\n"
            f"Last commit   : {_s('last_commit_date')}\n"
            f"Commit types  : {meta.get('commit_type_breakdown',{})}\n"
            f"\nFiles ({len(file_rows)} total):\n" + "\n".join(file_rows) +
            f"\n\nRecent commits:\n{recent}"
        )

    # ── get_file ──────────────────────────────────────────────────────────────
    if tool_name == "get_file":
        filepath = tool_input.get("filepath","")
        br_name  = tool_input.get("branch", default_b)
        fn_name  = tool_input.get("function_name")
        br       = _resolve_branch(index, br_name) or br_name

        # find blob_hash from index
        fe = branches.get(br, {}).get("files", {}).get(filepath)
        if not fe:
            # fuzzy match filepath
            for b, bd in branches.items():
                for fp in bd["files"]:
                    if filepath.lower() in fp.lower().replace("\\","/"):
                        fe = bd["files"][fp]
                        filepath = fp
                        br = b
                        break
                if fe: break

        blob_hash = fe.get("info",{}).get("blob_hash") if fe else None
        code = _fetch_code(repo_path, blob_hash, br, filepath, fn_name)
        lines = code.splitlines()
        print(f"    [tool] get_file: {filepath} ({len(lines)} lines)")
        return f"File: {filepath} @ {br}\n\n```\n{code}\n```"

    # ── search_files ──────────────────────────────────────────────────────────
    if tool_name == "search_files":
        keywords = tool_input.get("keywords", [])
        br_name  = tool_input.get("branch", default_b)
        br       = _resolve_branch(index, br_name) or default_b

        _SYNONYMS = {
            "retrieve":["get","fetch","find","query","lookup"],
            "create":  ["post","add","insert","new","make"],
            "update":  ["put","patch","edit","modify","save"],
            "delete":  ["remove","destroy","drop"],
            "login":   ["auth","signin","authenticate","token"],
            "auth":    ["login","signin","authenticate","token"],
        }

        def _expand(tok):
            t = tok.lower()
            return list(dict.fromkeys([t] + _SYNONYMS.get(t,[]) + ([t[:4]] if len(t)>4 else [])))

        results = []
        for fp, fe in branches[br]["files"].items():
            code  = fe.get("code",{})
            fns   = [_str(f) for f in code.get("functions",[])]
            cls   = [_str(c) for c in code.get("classes",[])]
            imps  = [_str(i) for i in code.get("imports",[])]
            calls = [_str(c) for c in code.get("calls",[])]
            fp_l  = fp.lower().replace("\\","/")
            hits  = []
            for kw in keywords:
                for c in _expand(kw):
                    if any(c in f.lower() for f in fns):   hits.append(f"fn:{kw}"); break
                    if any(c in f.lower() for f in cls):   hits.append(f"cls:{kw}"); break
                    if any(c in f.lower() for f in imps):  hits.append(f"imp:{kw}"); break
                    if any(c in f.lower() for f in calls): hits.append(f"call:{kw}"); break
                if kw.lower() in fp_l: hits.append(f"path:{kw}")
            if hits:
                results.append({
                    "filepath":fp, "hits":hits,
                    "functions":fns[:8], "classes":cls[:4],
                    "score": len(set(h.split(":")[0] for h in hits))
                })

        results.sort(key=lambda x: (-x["score"], -len(x["hits"])))
        if not results:
            return f"No files found matching keywords: {keywords}"

        lines = [f"Search results for {keywords} on branch '{br}':\n"]
        for r in results[:10]:
            lines.append(f"  {r['filepath']}  matches:{r['hits']}")
            if r["functions"]: lines.append(f"    functions: {r['functions']}")
            if r["classes"]:   lines.append(f"    classes: {r['classes']}")
        return "\n".join(lines)

    # ── get_file_history ──────────────────────────────────────────────────────
    if tool_name == "get_file_history":
        filepath = tool_input.get("filepath","")
        br_name  = tool_input.get("branch", default_b)
        br       = _resolve_branch(index, br_name) or default_b
        fe       = branches[br]["files"].get(filepath,{})
        h        = fe.get("history",{})
        commits  = "\n".join(
            f"  {c.get('short_hash',c.get('hash','')[:7])} "
            f"{str(c.get('author_date',c.get('date','')))[:10]} "
            f"{c.get('author','')} — {c.get('subject','')}"
            for c in h.get("commits",[])[:20]
        )
        return (
            f"History for {filepath} @ {br}\n"
            f"Total commits  : {h.get('total_commits',0)}\n"
            f"First seen     : {h.get('first_seen','')}\n"
            f"Last modified  : {h.get('last_modified','')}\n"
            f"Unique authors : {h.get('unique_authors',[])}\n\n"
            f"Commits:\n{commits or '  (none)'}"
        )

    # ── get_call_graph ────────────────────────────────────────────────────────
    if tool_name == "get_call_graph":
        fn_name = tool_input.get("function_name","")
        br_name = tool_input.get("branch", default_b)
        br      = _resolve_branch(index, br_name) or default_b
        cg      = branches[br]["graph"].get("call_graph",{})
        # find matching func_id
        fid = next((k for k in cg if fn_name.lower() in k.lower()), None)
        if not fid:
            return f"Function '{fn_name}' not found in call graph on {br}."
        callees = cg.get(fid,[])
        callers = [c for c,ts in cg.items() if fid in ts]
        return (
            f"Call graph for '{fn_name}' ({fid}) @ {br}\n"
            f"Calls (outgoing)   : {callees[:20]}\n"
            f"Called by (incoming): {callers[:20]}"
        )

    # ── get_contributors ──────────────────────────────────────────────────────
    if tool_name == "get_contributors":
        br_name = tool_input.get("branch")
        am      = lookup["author_to_commits"]
        name_map = {}
        for bn, bd in branches.items():
            for c in bd["git"]["commits"]:
                em = c.get("author_email","")
                if em and em not in name_map:
                    name_map[em] = c.get("author","")

        ranked = sorted([
            {"email":em, "name":name_map.get(em,""),
             "commits":len([c for c in cs if not br_name or c.get("branch","").endswith(br_name)]),
             "branches":list({c["branch"] for c in cs})}
            for em,cs in am.items()
        ], key=lambda x: -x["commits"])

        lines = ["Contributor rankings:\n"]
        for r in ranked[:15]:
            if r["commits"] == 0: continue
            lines.append(f"  {r['commits']:>4}  {r.get('name') or r['email']}  branches:{r['branches']}")
        return "\n".join(lines)

    # ── get_recent_commits ────────────────────────────────────────────────────
    if tool_name == "get_recent_commits":
        br_name = tool_input.get("branch")
        limit   = min(tool_input.get("limit", 15), 30)
        all_c   = []
        for bn, bd in branches.items():
            if br_name and not (br_name.lower() in bn.lower()):
                continue
            for c in bd["git"]["commits"][:50]:
                all_c.append({**c,"branch":bn})
        all_c.sort(key=lambda c: c.get("author_date",""), reverse=True)
        rows = "\n".join(
            f"  [{c['branch']}] {c.get('hash','')[:7]} "
            f"{str(c.get('author_date',''))[:10]} "
            f"{c.get('author','')} — {c.get('subject','')}"
            for c in all_c[:limit]
        )
        return f"Recent commits:\n{rows}"

    # ── compare_branches ──────────────────────────────────────────────────────
    if tool_name == "compare_branches":
        b1 = _resolve_branch(index, tool_input.get("branch1","")) or default_b
        b2 = _resolve_branch(index, tool_input.get("branch2","")) or default_b
        d1, d2 = branches[b1], branches[b2]
        m1, m2 = d1["meta"], d2["meta"]
        f1, f2 = set(d1["files"]), set(d2["files"])
        fn1, fn2 = [], []
        for fp in list(f1 & f2)[:50]:
            s1 = {_str(f) for f in d1["files"][fp].get("code",{}).get("functions",[])}
            s2 = {_str(f) for f in d2["files"][fp].get("code",{}).get("functions",[])}
            fn1 += [f"{fp}:{f}" for f in s1-s2]
            fn2 += [f"{fp}:{f}" for f in s2-s1]
        def _mc(m):
            s = m.get("stats",{})
            return s.get("total_commits") or m.get("total_commits") or 0
        return (
            f"Branch comparison: {b1} vs {b2}\n\n"
            f"{b1}:\n"
            f"  Files: {m1.get('total_files',0) or len(d1['files'])}  "
            f"Commits: {_mc(m1)}  "
            f"Contributors: {m1.get('stats',{}).get('unique_contributors',0)}\n"
            f"  Commit types: {m1.get('commit_type_breakdown',{})}\n\n"
            f"{b2}:\n"
            f"  Files: {m2.get('total_files',0) or len(d2['files'])}  "
            f"Commits: {_mc(m2)}  "
            f"Contributors: {m2.get('stats',{}).get('unique_contributors',0)}\n"
            f"  Commit types: {m2.get('commit_type_breakdown',{})}\n\n"
            f"Files only in {b1} ({len(f1-f2)}): {sorted(f1-f2)[:15]}\n"
            f"Files only in {b2} ({len(f2-f1)}): {sorted(f2-f1)[:15]}\n"
            f"Common files: {len(f1&f2)}\n"
            f"Functions only in {b1}: {fn1[:10]}\n"
            f"Functions only in {b2}: {fn2[:10]}\n"
        )

    # ── get_hotspots ──────────────────────────────────────────────────────────
    if tool_name == "get_hotspots":
        br_name = tool_input.get("branch", default_b)
        limit   = tool_input.get("limit", 15)
        br      = _resolve_branch(index, br_name) or default_b
        scored  = sorted([
            {"filepath":fp,
             "commits":fe.get("history",{}).get("total_commits",0),
             "authors":fe.get("history",{}).get("unique_authors",[])}
            for fp,fe in branches[br]["files"].items()
        ], key=lambda x: -x["commits"])
        rows = "\n".join(
            f"  {r['commits']:>4} commits  {r['filepath']}  authors:{r['authors']}"
            for r in scored[:limit]
        )
        return f"Hotspots on {br}:\n{rows}"

    # ── find_dead_code ────────────────────────────────────────────────────────
    if tool_name == "find_dead_code":
        br_name = tool_input.get("branch", default_b)
        br      = _resolve_branch(index, br_name) or default_b
        cg      = branches[br]["graph"].get("call_graph",{})
        all_callees: set = set()
        for ts in cg.values(): all_callees.update(ts)
        dead = sorted(set(cg) - all_callees)[:30]
        rows = "\n".join(f"  {d}" for d in dead)
        return f"Dead functions on {br} ({len(dead)} found):\n{rows or '  (none found)'}"

    return f"Unknown tool: {tool_name}"


# ─────────────────────────────────────────────────────────────────────────────
# AGENTIC QUERY LOOP
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an AI assistant with access to a git repository's metadata and source code.

You have tools to:
- List branches and their contents
- Fetch actual source code files
- Search for files by keyword
- Get commit history, contributors, call graphs

IMPORTANT RULES:
1. Always call get_file() to read actual code before explaining what functions do.
   NEVER guess at implementations — fetch the real code first.
2. For branch questions, call get_branch_info() to get the file list.
3. For "how does X work" questions, use search_files() first to find the right file,
   then get_file() to read it.
4. Be specific — cite actual function names, file paths, commit hashes, and author names.
5. You can call multiple tools in sequence to build a complete answer.
"""

def query(user_query: str, index: dict,
          preferred_branch: str | None = None,
          aws_region: str = "us-east-1",
          dry_run: bool = False,
          max_rounds: int = 6) -> str:

    repo_path = index["repository"]["repo_path"]

    if dry_run:
        _panel("DRY RUN", f"Query: {user_query}\nSystem: {SYSTEM_PROMPT[:200]}...")
        return "[dry-run]"

    try:
        import boto3
    except ImportError:
        return "[ERROR] pip install boto3"

    client   = boto3.client("bedrock-runtime", region_name=aws_region)
    messages = [{"role": "user", "content": [{"text": user_query}]}]

    print(f"\n  Query    : {user_query}")
    print(f"  Mode     : agentic (tool calling)")

    for round_n in range(max_rounds):
        response = client.converse(
            modelId  = "amazon.nova-pro-v1:0",
            system   = [{"text": SYSTEM_PROMPT}],
            messages = messages,
            toolConfig = {"tools": TOOLS},
            inferenceConfig = {"maxTokens": 4096, "temperature": 0.1},
        )

        stop_reason = response["stopReason"]
        msg         = response["output"]["message"]
        messages.append(msg)   # add assistant turn to history

        # ── model finished — return text answer ───────────────────────────────
        if stop_reason == "end_turn":
            text = " ".join(
                b["text"] for b in msg["content"]
                if isinstance(b, dict) and "text" in b
            )
            print(f"  Rounds   : {round_n + 1}")
            return text

        # ── model wants to call tools ─────────────────────────────────────────
        if stop_reason == "tool_use":
            tool_results = []
            for block in msg["content"]:
                # Bedrock returns tool calls as {"toolUse": {"name":..., "input":..., "toolUseId":...}}
                tool_use = block.get("toolUse") if isinstance(block, dict) else None
                if not tool_use:
                    continue
                tool_name  = tool_use["name"]
                tool_input = tool_use["input"]
                tool_id    = tool_use["toolUseId"]

                print(f"  Tool     : {tool_name}({json.dumps(tool_input)[:80]})")
                result = execute_tool(tool_name, tool_input, index, repo_path)
                print(f"  Result   : {len(result)} chars")

                tool_results.append({
                    "toolResult": {
                        "toolUseId": tool_id,
                        "content": [{"text": result}],
                    }
                })

            # send tool results back to model
            messages.append({
                "role": "user",
                "content": tool_results,
            })
            continue

        # unexpected stop reason
        break

    return "[Max tool call rounds reached — partial answer may be incomplete]"


# ─────────────────────────────────────────────────────────────────────────────
# INDEX LOADER
# ─────────────────────────────────────────────────────────────────────────────

def load_index(index_path: Path) -> dict:
    print(f"  Loading index : {index_path.name} ...")
    data = json.loads(index_path.read_text(encoding="utf-8"))
    branches    = data.get("branches", {})
    total_files = sum(len(b["files"]) for b in branches.values())
    print(f"  Branches      : {len(branches)}")
    print(f"  Files         : {total_files:,}")
    return data


def find_latest_index(directory: Path) -> Path:
    matches = sorted(directory.glob("index_*.json"),
                     key=lambda p: p.stat().st_mtime, reverse=True)
    if not matches:
        print("ERROR: No index_*.json found.", file=sys.stderr)
        sys.exit(1)
    return matches[0]


# ─────────────────────────────────────────────────────────────────────────────
# REPL + CLI
# ─────────────────────────────────────────────────────────────────────────────

def repl(index: dict, preferred_branch: str | None, aws_region: str, dry_run: bool):
    print("=" * 66)
    print("  AI Git Repo Query — Agentic Mode")
    print(f"  Repo  : {index['repository'].get('repo_path','?')}")
    print(f"  Model : amazon.nova-pro-v1 @ {aws_region}")
    print("  Commands: :branches  :quit")
    print("=" * 66)

    while True:
        try:
            raw = input("\n  Query > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Exiting."); break
        if not raw: continue
        if raw == ":quit": break
        if raw == ":branches":
            for b in sorted(index["branches"]):
                print(f"    {b}")
            continue
        answer = query(raw, index, preferred_branch=preferred_branch,
                       aws_region=aws_region, dry_run=dry_run)
        print()
        _print_md(answer)


def main():
    parser = argparse.ArgumentParser(description="Agentic AI git query engine.")
    parser.add_argument("-i","--input-dir", default=".")
    parser.add_argument("--index",    default=None)
    parser.add_argument("-q","--query", default=None)
    parser.add_argument("--branch",   default=None)
    parser.add_argument("--region",   default="us-east-1")
    parser.add_argument("--dry-run",  action="store_true")
    parser.add_argument("--max-rounds", type=int, default=6,
                        help="Max tool-call rounds per query (default: 6)")
    args  = parser.parse_args()
    ipath = Path(args.index) if args.index else find_latest_index(Path(args.input_dir))
    index = load_index(ipath)

    if args.query:
        ans = query(args.query, index, preferred_branch=args.branch,
                    aws_region=args.region, dry_run=args.dry_run,
                    max_rounds=args.max_rounds)
        print(); _print_md(ans)
    else:
        repl(index, args.branch, args.region, args.dry_run)


# if __name__ == "__main__":
#     main()

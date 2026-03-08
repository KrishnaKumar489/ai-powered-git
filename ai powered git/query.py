"""
query_engine.py
---------------
AI-powered query engine over the unified index produced by build_index.py.

Design principle:
    The index is a navigation map — not a search corpus.
    No vectors. No RAG. No code is stored.

    Flow for every query:
        1. INTENT    — classify what the user is asking (19 intents)
        2. TRAVERSE  — walk the index to find exact branch + file + symbol
        3. FETCH     — read ONLY that code slice from the local git repo
        4. ANSWER    — send slice + index context to Bedrock Nova Pro

Supported intents:
    file_compare     — "how does home.html differ from home_dev.html"
    branch_compare   — "compare main and develop"
    symbol_compare   — "how does login() differ between main and feature/auth"
    symbol_lookup    — "what does the login function do"
    call_chain       — "who calls validate_token"
    file_lookup      — "show me auth.py"
    file_history     — "who modified auth.py"
    dependency       — "what does auth.py import"
    impact           — "what breaks if I change auth.py"
    dead_code        — "which functions are never called"
    hotspot          — "which files change the most"
    contributor_rank — "who contributed the most"
    tag_lookup       — "what is in release v1.0"
    recent_activity  — "what changed in the last week"
    search_content   — "find files that use JWT"
    branch_summary   — "summarize the feature/auth branch"
    author_lookup    — "what did alice write"
    commit_lookup    — "what changed in commit abc123"
    general          — fallback

Usage:
    python query_engine.py -i ./reports
    python query_engine.py -i ./reports -q "compare main and develop"
    python query_engine.py -i ./reports --branch feature/auth
    python query_engine.py -i ./reports -q "what does login do" --dry-run
"""

import json
import re
import subprocess
import argparse
import sys
from pathlib import Path


# ── optional: rich terminal output ───────────────────────────────────────────
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
# 1. INDEX LOADER
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
        print("ERROR: No index_*.json found. Run build_index.py first.",
              file=sys.stderr)
        sys.exit(1)
    return matches[0]


# ─────────────────────────────────────────────────────────────────────────────
# 2. INTENT CLASSIFIER
# ─────────────────────────────────────────────────────────────────────────────

_FILE_EXT = r"(py|js|java|html|css|ts)"
_FILE_PAT = rf"\b[\w_-]+\.{_FILE_EXT}\b"
_BRANCH_KW = r"(branch|main|master|develop|feature|release|hotfix)"

_INTENT_PATTERNS = [
    # ── multi-entity comparisons (checked before single-entity) ──────────────
    ("file_compare",     rf"{_FILE_PAT}.{{0,80}}{_FILE_PAT}"),
    ("branch_compare",   rf"\b(compar|diff(er)?|vs\.?|versus|between)\b.{{0,60}}{_BRANCH_KW}"
                         rf"|{_BRANCH_KW}.{{0,40}}(and|vs\.?|versus).{{0,40}}{_BRANCH_KW}"),
    ("symbol_compare",   rf"\b(compar|diff(er)?).{{0,60}}(function|method|class).{{0,60}}{_BRANCH_KW}"
                         rf"|(function|method|class).{{0,40}}(between|across).{{0,40}}branch"),

    # ── code intelligence ─────────────────────────────────────────────────────
    ("call_chain",       r"\b(call\s*chain|call\s*graph|who\s*calls?|what\s*calls?|callers?\s*of|called\s*by|trace\s*call)\b"),
    ("impact",           r"\b(what\s*(breaks?|depends?|affected|impact)|impact\s*of\s*chang|if\s*I\s*chang|what\s*uses?)\b"),
    ("dead_code",        r"\b(dead\s*code|never\s*called|unused\s*(function|method|class)|unreachable)\b"),
    ("dependency",       r"\b(import|depend|dependency|depends\s*on|what\s*does.{0,40}import|require)\b"),

    # ── file-level ────────────────────────────────────────────────────────────
    ("file_lookup",      _FILE_PAT),
    ("file_history",     r"\b(who\s*(wrote|created|changed|modified|touched)|history\s*of|commit\s*history|blame|last\s*changed)\b"),

    # ── symbol-level ─────────────────────────────────────────────────────────
    ("symbol_lookup",    r"\b(what\s*does|explain|how\s*does|show\s*me|describe)\b.{0,60}\b(function|method|class|def)\b"),
    ("symbol_lookup",    r"\b(function|method|class)\b.{0,60}\b(do|does|work|impl|implement)\b"),

    # ── analytics ─────────────────────────────────────────────────────────────
    ("hotspot",          r"\b(hotspot|most\s*(changed|modified|active|touched)|churn|frequently\s*changed)\b"),
    ("contributor_rank", r"\b(top\s*contributor|most\s*(commit|contribut|active)|who\s*(contribut|commit).{0,20}most|leaderboard|ranking)\b"),
    ("recent_activity",  r"\b(recent(ly)?|last\s*(week|month|day|\d+\s*(day|week|commit))|latest\s*change|newest\s*commit)\b"),
    ("tag_lookup",       r"\b(tag|release|version|v\d+[\.\d]*|changelog)\b"),

    # ── search ───────────────────────────────────────────────────────────────
    ("search_content",   r"\b(find\s*(files?|code|where)|search|which\s*files?\s*(use|have|contain|implement)|grep|look\s*for)\b"),

    # ── git / people ──────────────────────────────────────────────────────────
    ("branch_summary",   r"\b(branch|summarize|overview|status\s*of)\b"),
    ("author_lookup",    r"\b(who|author|contributor)\b.{0,60}\b(wrote|built|made|worked|contribut|did)\b"),
    ("commit_lookup",    r"\b(commit|change(s)?|what\s*changed|recent\s*change|last\s*change|patch)\b"),

    # ── fallback ──────────────────────────────────────────────────────────────
    ("general",          r".*"),
]


def classify_intent(query: str) -> str:
    q = query.lower()
    for intent, pattern in _INTENT_PATTERNS:
        if re.search(pattern, q):
            return intent
    return "general"


# ─────────────────────────────────────────────────────────────────────────────
# 3. INDEX TRAVERSAL
# ─────────────────────────────────────────────────────────────────────────────

def _str(val) -> str:
    """Safely convert index values to searchable string.
    Functions/classes can be stored as str or dict {'name':..., 'args':...}.
    """
    if isinstance(val, str):
        return val
    if isinstance(val, dict):
        # try common name keys
        for k in ("name", "func_name", "class_name", "symbol"):
            if k in val:
                return str(val[k])
        # fallback: join all string values
        return " ".join(str(v) for v in val.values() if isinstance(v, str))
    return str(val)


class IndexTraverser:

    def __init__(self, index: dict, preferred_branch: str | None = None):
        self.index    = index
        self.repo     = index["repository"]["repo_path"]
        self.branches = index["branches"]
        self.lookup   = index["lookup"]
        self.preferred_branch = preferred_branch

    # ── dispatch ──────────────────────────────────────────────────────────────

    def traverse(self, query: str, intent: str) -> dict:
        return {
            "file_compare":     self._file_compare,
            "branch_compare":   self._branch_compare,
            "symbol_compare":   self._symbol_compare,
            "symbol_lookup":    self._symbol_lookup,
            "call_chain":       self._call_chain,
            "file_lookup":      self._file_lookup,
            "file_history":     self._file_history,
            "dependency":       self._dependency,
            "impact":           self._impact,
            "dead_code":        self._dead_code,
            "hotspot":          self._hotspot,
            "contributor_rank": self._contributor_rank,
            "recent_activity":  self._recent_activity,
            "tag_lookup":       self._tag_lookup,
            "search_content":   self._search_content,
            "branch_summary":   self._branch_summary,
            "author_lookup":    self._author_lookup,
            "commit_lookup":    self._commit_lookup,
            "general":          self._general,
        }.get(intent, self._general)(query)

    # ── shared helpers ────────────────────────────────────────────────────────

    _STOPWORDS = {
        "what","does","the","how","is","in","of","a","an","show","me","tell",
        "about","explain","file","function","class","method","do","did","who",
        "wrote","and","or","to","for","with","that","this","it","its","are",
        "was","were","has","have","had","be","been","being","find","get",
        "give","between","across","compare","diff","on","at","by","from"
    }

    def _tokens(self, query: str) -> list[str]:
        raw = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", query)
        return [t for t in raw if len(t) >= 2
                and t.lower() not in self._STOPWORDS]

    def _pick_branch(self, candidates: list[str]) -> str:
        if self.preferred_branch and self.preferred_branch in candidates:
            return self.preferred_branch
        # prefer local branches over remote tracking branches
        for d in ("main", "master", "develop"):
            if d in candidates:
                return d
        # try origin/main, origin/master
        for d in ("origin/main", "origin/master", "origin/develop"):
            if d in candidates:
                return d
        # prefer non-HEAD remote branches over HEAD
        non_head = [c for c in candidates if "HEAD" not in c]
        return non_head[0] if non_head else (candidates[0] if candidates else next(iter(self.branches)))

    def _detect_branches(self, query: str) -> list[str]:
        q = query.lower()
        found = []
        # sort longest first to avoid partial matches
        for b in sorted(self.branches, key=len, reverse=True):
            bl = b.lower()
            # try full name first: "origin/api-additions"
            # then short name: "api-additions" (strips origin/ remotes/ prefix)
            short = re.sub(r"^(origin|remotes/[^/]+)/", "", bl)
            if (bl in q or short in q) and b not in found:
                found.append(b)
        return found

    def _find_files(self, tokens: list[str],
                    query: str = "") -> list[tuple[str, str, dict]]:
        """
        Score every file against ALL query tokens and return ranked results.

        Scoring per token per file:
            3 — exact filename match        (observe.py  vs  "observe.py")
            2 — filename stem exact match   (observe.py  vs  "observe")
            1 — token appears in full path  (routes/observe_endpoint.py vs "observe")

        Total score = sum across all tokens, so a file matching 2 tokens
        ("observe" + "endpoint") outranks one matching only 1 token ("observe").
        Ties broken by extension preference (.py first).
        """
        ext_m   = re.search(rf"\b[\w_-]+\.({_FILE_EXT[1:-1]})\b", query.lower())
        pref    = ("." + ext_m.group(1)) if ext_m else ".py"
        rank    = {".py":0,".js":1,".ts":1,".java":2,".html":3,".css":4}

        # accumulate scores per (branch, filepath) across ALL tokens
        file_scores: dict = {}   # key=(br,fp) -> {"score":int,"er":int,"fe":dict}
        lk = self.lookup["file_to_branches"]

        for tok in tokens:
            tl   = tok.lower()
            stem = tl.rsplit(".", 1)[0] if "." in tl else tl
            tex  = ("." + tl.rsplit(".", 1)[1]) if "." in tl else None

            for fp, brs in lk.items():
                fpl  = fp.lower().replace("\\", "/")
                fn   = fpl.split("/")[-1]
                fs   = fn.rsplit(".", 1)[0] if "." in fn else fn
                fe_  = ("." + fn.rsplit(".", 1)[1]) if "." in fn else ""

                if tl == fn:       sc = 3
                elif stem == fs:   sc = 2
                elif stem in fpl:  sc = 1
                else:              continue

                if tex and fe_ != tex:
                    continue

                br  = self._pick_branch(brs)
                key = (br, fp)
                er  = -1 if fe_ == pref else rank.get(fe_, 5)

                if key not in file_scores:
                    fe = self.branches[br]["files"].get(fp, {})
                    file_scores[key] = {"score": 0, "er": er, "br": br, "fp": fp, "fe": fe}
                # accumulate — more tokens matched = higher total score
                file_scores[key]["score"] += sc

        scored = sorted(
            file_scores.values(),
            key=lambda x: (-x["score"], x["er"])
        )
        return [(x["br"], x["fp"], x["fe"]) for x in scored]

    def _find_symbols(self, tokens: list[str]) -> list[tuple[str,str,str,str]]:
        results, seen = [], set()
        for tok in tokens:
            tl = tok.lower()
            for key, brs in self.lookup["function_to_branches"].items():
                fp, fn = key.rsplit(":", 1)
                if tl == fn.lower() or tl in fn.lower():
                    br  = self._pick_branch(brs)
                    uid = (br, fp, fn)
                    if uid not in seen:
                        seen.add(uid)
                        results.append((br, fp, "function", fn))
            for cls, brs in self.lookup["class_to_branches"].items():
                if tl == cls.lower() or tl in cls.lower():
                    br  = self._pick_branch(brs)
                    uid = (br, cls)
                    if uid not in seen:
                        seen.add(uid)
                        fp = self._cls_file(br, cls)
                        results.append((br, fp or "", "class", cls))
        return results

    def _cls_file(self, branch: str, cls: str) -> str | None:
        for fp, fe in self.branches[branch]["files"].items():
            if cls in fe.get("code", {}).get("classes", []):
                return fp
        return None

    def _fe(self, branch: str, fp: str) -> dict:
        return self.branches[branch]["files"].get(fp, {})

    # ── strategy implementations ──────────────────────────────────────────────

    def _symbol_lookup(self, query: str) -> dict:
        syms = self._find_symbols(self._tokens(query))
        if not syms:
            return self._file_lookup(query)
        br, fp, st, sn = syms[0]
        fe = self._fe(br, fp)
        return {
            "strategy": "symbol_lookup", "branch": br, "filepath": fp,
            "symbol": sn, "sym_type": st,
            "blob_hash": fe.get("info",{}).get("blob_hash"),
            "extension": fe.get("info",{}).get("extension",""),
            "imports":   fe.get("code",{}).get("imports",[]),
            "calls":     fe.get("code",{}).get("calls",[]),
            "history_summary": _hist_summary(fe.get("history",{})),
            "other_matches": syms[1:4],
        }

    def _call_chain(self, query: str) -> dict:
        syms = self._find_symbols(self._tokens(query))
        if not syms:
            return {"strategy":"call_chain","error":"Symbol not found"}
        br, fp, _, sn = syms[0]
        fid   = f"{fp}:{sn}"
        cg    = self.branches[br]["graph"].get("call_graph", {})
        callees = cg.get(fid, [])
        callers = [c for c, ts in cg.items() if fid in ts]
        c2     = list({cc for c in callers[:5] for cc,ts in cg.items() if c in ts})[:10]
        fe = self._fe(br, fp)
        return {
            "strategy":"call_chain","branch":br,"filepath":fp,"symbol":sn,
            "func_id":fid,
            "blob_hash": fe.get("info",{}).get("blob_hash"),
            "extension": fe.get("info",{}).get("extension",""),
            "callees":callees[:20],"callers":callers[:20],
            "callers_of_callers":c2,
        }

    def _file_lookup(self, query: str) -> dict:
        files = self._find_files(self._tokens(query), query)
        if not files:
            return {"strategy":"file_lookup","error":"File not found"}
        br, fp, fe = files[0]

        # If the 2nd result exists, include it as a secondary fetch target.
        # This handles queries like "observe endpoint methods" where the answer
        # spans two files (utils/observe.py + routes/observe_endpoint.py).
        secondary = None
        if len(files) > 1:
            br2, fp2, fe2 = files[1]
            secondary = {
                "branch":    br2,
                "filepath":  fp2,
                "blob_hash": fe2.get("info",{}).get("blob_hash"),
                "extension": fe2.get("info",{}).get("extension",""),
                "functions": [_str(f) for f in fe2.get("code",{}).get("functions",[])],
                "classes":   [_str(c) for c in fe2.get("code",{}).get("classes",[])],
            }

        return {
            "strategy":"file_lookup","branch":br,"filepath":fp,
            "blob_hash": fe.get("info",{}).get("blob_hash"),
            "extension": fe.get("info",{}).get("extension",""),
            "functions": [_str(f) for f in fe.get("code",{}).get("functions",[])],
            "classes":   [_str(c) for c in fe.get("code",{}).get("classes",[])],
            "imports":   fe.get("code",{}).get("imports",[]),
            "history_summary": _hist_summary(fe.get("history",{})),
            "other_files": [(b,p) for b,p,_ in files[1:4]],
            "secondary": secondary,   # fetched alongside primary if present
        }

    def _file_history(self, query: str) -> dict:
        files = self._find_files(self._tokens(query), query)
        if not files:
            return self._author_lookup(query)
        br, fp, fe = files[0]
        h = fe.get("history", {})
        return {
            "strategy":"file_history","branch":br,"filepath":fp,
            "total_commits": h.get("total_commits",0),
            "first_seen":    h.get("first_seen",""),
            "last_modified": h.get("last_modified",""),
            "unique_authors":h.get("unique_authors",[]),
            "commits":       h.get("commits",[])[:20],
            "blob_hash": fe.get("info",{}).get("blob_hash"),
            "extension": fe.get("info",{}).get("extension",""),
        }

    def _dependency(self, query: str) -> dict:
        toks  = self._tokens(query)
        files = self._find_files(toks, query)
        syms  = self._find_symbols(toks)
        if not files and not syms:
            return {"strategy":"dependency","error":"Target not found"}
        if files:
            br, fp, fe = files[0]
        else:
            br, fp, _, _ = syms[0]; fe = self._fe(br, fp)
        dg = self.branches[br]["graph"].get("dependency_graph", {})
        return {
            "strategy":"dependency","branch":br,"filepath":fp,
            "blob_hash":  fe.get("info",{}).get("blob_hash"),
            "extension":  fe.get("info",{}).get("extension",""),
            "imports":    fe.get("code",{}).get("imports",[]),
            "direct_deps":  dg.get(fp, []),
            "reverse_deps": [s for s,ds in dg.items() if fp in ds],
        }

    def _impact(self, query: str) -> dict:
        toks  = self._tokens(query)
        files = self._find_files(toks, query)
        syms  = self._find_symbols(toks)
        br    = self._pick_branch(list(self.branches))
        if files:
            _, fp, fe = files[0]
        elif syms:
            _, fp, _, _ = syms[0]; fe = self._fe(br, fp)
        else:
            return {"strategy":"impact","error":"Target not found"}
        dg    = self.branches[br]["graph"].get("dependency_graph", {})
        cg    = self.branches[br]["graph"].get("call_graph", {})
        fns   = {f"{fp}:{fn}" for fn in fe.get("code",{}).get("functions",[])}
        fdeps = [s for s, ds in dg.items() if fp in ds]
        fcall = {c for c, ts in cg.items() if any(t in fns for t in ts)}
        return {
            "strategy":"impact","branch":br,"filepath":fp,
            "blob_hash":  fe.get("info",{}).get("blob_hash"),
            "extension":  fe.get("info",{}).get("extension",""),
            "functions":  fe.get("code",{}).get("functions",[]),
            "file_dependents": fdeps,
            "func_callers":    list(fcall),
            "total_impact":    len(fdeps) + len(fcall),
        }

    def _dead_code(self, query: str) -> dict:
        br  = self._pick_branch(list(self.branches))
        cg  = self.branches[br]["graph"].get("call_graph", {})
        all_callees: set = set()
        for ts in cg.values():
            all_callees.update(ts)
        dead = sorted(set(cg) - all_callees)
        enriched = []
        for fid in dead[:50]:
            if ":" in fid:
                fp, fn = fid.rsplit(":", 1)
                fe = self._fe(br, fp)
                enriched.append({
                    "func_id": fid, "filepath": fp, "function": fn,
                    "extension": fe.get("info",{}).get("extension",""),
                    "last_modified": fe.get("history",{}).get("last_modified",""),
                })
        return {
            "strategy":"dead_code","branch":br,
            "total_dead":len(dead),"dead_functions":enriched,
        }

    def _hotspot(self, query: str) -> dict:
        br    = self._pick_branch(list(self.branches))
        files = self.branches[br]["files"]
        scored = sorted(
            [{"filepath":fp,
              "total_commits": fe.get("history",{}).get("total_commits",0),
              "last_modified": fe.get("history",{}).get("last_modified",""),
              "unique_authors":fe.get("history",{}).get("unique_authors",[]),
              "extension":     fe.get("info",{}).get("extension",""),
              "size_bytes":    fe.get("info",{}).get("size_bytes",0)}
             for fp, fe in files.items()],
            key=lambda x: -x["total_commits"]
        )
        return {
            "strategy":"hotspot","branch":br,
            "total_files":len(scored),"top_files":scored[:20],
        }

    def _contributor_rank(self, query: str) -> dict:
        am     = self.lookup["author_to_commits"]
        # build name map
        name_map: dict = {}
        for bn, bd in self.branches.items():
            for c in bd["git"]["commits"]:
                em = c.get("author_email","")
                if em and em not in name_map:
                    name_map[em] = c.get("author","")
        ranked = sorted([
            {"email": em,
             "name":  name_map.get(em,""),
             "total_commits": len(cs),
             "branches_active": list({c["branch"] for c in cs}),
             "commit_types": _type_breakdown(cs),
             "recent_commit": cs[0] if cs else {}}
            for em, cs in am.items()
        ], key=lambda x: -x["total_commits"])
        return {
            "strategy":"contributor_rank",
            "total_contributors":len(ranked),"ranked":ranked[:20],
        }

    def _recent_activity(self, query: str) -> dict:
        nm = re.search(r"\b(\d+)\s*(commit|day|week)", query.lower())
        limit = int(nm.group(1)) if nm else 15
        all_c = []
        for bn, bd in self.branches.items():
            for c in bd["git"]["commits"][:50]:
                all_c.append({**c, "branch": bn})
        all_c.sort(key=lambda c: c.get("author_date",""), reverse=True)
        return {
            "strategy":"recent_activity",
            "recent_commits": all_c[:min(limit, 30)],
            "total_scanned":  len(all_c),
        }

    def _tag_lookup(self, query: str) -> dict:
        all_tags = []
        for bn, bd in self.branches.items():
            for t in bd["meta"].get("tags",[]):
                all_tags.append({**t,"branch":bn})
        vp = re.search(r"\bv?\d+[\.\d]*\b", query)
        if vp:
            ver = vp.group(0).lower()
            filtered = [t for t in all_tags if ver in t.get("tag","").lower()]
            if filtered:
                all_tags = filtered
        all_tags.sort(key=lambda t: t.get("date",""), reverse=True)
        return {"strategy":"tag_lookup","total_tags":len(all_tags),"tags":all_tags[:20]}

    # semantic synonym map — expands query tokens to related code terms
    _SYNONYMS: dict = {
        "retrieve":["get","fetch","find","query","lookup","load"],
        "retriev": ["get","fetch","find","query","lookup","load"],
        "fetch":   ["get","retrieve","query","find","load"],
        "create":  ["post","add","insert","new","make"],
        "update":  ["put","patch","edit","modify","save"],
        "delete":  ["remove","destroy","drop","del"],
        "login":   ["auth","signin","authenticate","token","jwt"],
        "auth":    ["login","signin","authenticate","token","jwt"],
        "user":    ["account","profile","member","person"],
        "post":    ["article","entry","item","record","content"],
        "list":    ["all","index","many","collection","array"],
        "specific":["id","param","path","single"],
    }

    def _expand_tok(self, tok: str) -> list:
        t     = tok.lower()
        extra = self._SYNONYMS.get(t, [])
        stem  = [t[:4]] if len(t) > 4 else []
        return list(dict.fromkeys([t] + extra + stem))

    def _search_content(self, query: str) -> dict:
        toks = self._tokens(query)
        br   = self._pick_branch(list(self.branches))
        res  = []

        for fp, fe in self.branches[br]["files"].items():
            code  = fe.get("code", {})
            fns   = code.get("functions", [])
            cls   = code.get("classes",   [])
            imps  = code.get("imports",   [])
            calls = code.get("calls",     [])
            fp_l  = fp.lower().replace("\\", "/")

            hits = []
            for tok in toks:
                candidates = self._expand_tok(tok)
                for c in candidates:
                    if any(c in _str(f).lower() for f in fns):
                        hits.append(f"fn:{tok}"); break
                for c in candidates:
                    if any(c in _str(cl).lower() for cl in cls):
                        hits.append(f"cls:{tok}"); break
                for c in candidates:
                    if any(c in _str(i).lower() for i in imps):
                        hits.append(f"imp:{tok}"); break
                for c in candidates:
                    if any(c in _str(ca).lower() for ca in calls):
                        hits.append(f"call:{tok}"); break
                if tok.lower() in fp_l:
                    hits.append(f"path:{tok}")

            if hits:
                res.append({
                    "filepath":fp,"branch":br,"matched_in":hits,
                    "functions":[_str(f) for f in fns],"classes":[_str(c) for c in cls],
                    "blob_hash":fe.get("info",{}).get("blob_hash"),
                    "extension":fe.get("info",{}).get("extension",""),
                    "score":   len(set(h.split(":")[0] for h in hits)),
                })

        res.sort(key=lambda x: (-x["score"], -len(x["matched_in"])))
        return {
            "strategy":"search_content","branch":br,"tokens":toks,
            "total_matches":len(res),"results":res[:15],
            "filepath":  res[0]["filepath"]  if res else None,
            "blob_hash": res[0]["blob_hash"] if res else None,
            "extension": res[0]["extension"] if res else "",
        }

    def _branch_summary(self, query: str) -> dict:
        det  = self._detect_branches(query)
        br   = det[0] if det else self._pick_branch(list(self.branches))
        import sys
        print(f"  Branch   : {br}", file=sys.stderr)
        bd   = self.branches[br]
        meta = bd["meta"]
        stats = meta.get("stats", {})

        # resilient stat reading — handles both meta.stats.total_commits
        # and meta.total_commits depending on build_index version
        def _stat(key, fallback=0):
            return stats.get(key) or meta.get(key) or fallback

        # build file list with per-file summary from index (no code read needed)
        file_list = []
        for fp, fe in bd["files"].items():
            code = fe.get("code", {})
            hist = fe.get("history", {})
            info = fe.get("info", {})
            file_list.append({
                "path":      fp,
                "extension": info.get("extension", ""),
                "size_bytes":info.get("size_bytes", 0),
                "functions": [_str(f) for f in code.get("functions", [])],
                "classes":   [_str(c) for c in code.get("classes", [])],
                "imports":   code.get("imports", [])[:5],
                "last_modified": hist.get("last_modified", ""),
                "total_commits": hist.get("total_commits", 0),
                "authors":   hist.get("unique_authors", []),
            })
        # sort by path for readability
        file_list.sort(key=lambda x: x["path"])

        return {
            "strategy":"branch_summary","branch":br,
            "tip_commit":            meta.get("tip_commit",""),
            "total_files":           meta.get("total_files",0) or len(bd["files"]),
            "total_size_bytes":      meta.get("total_size_bytes",0),
            "total_commits":         _stat("total_commits") or len(bd["git"]["commits"]),
            "commits_ahead":         _stat("commits_ahead_of_base"),
            "unique_contributors":   _stat("unique_contributors"),
            "contributor_emails":    _stat("contributor_emails", []),
            "last_commit_date":      _stat("last_commit_date"),
            "commit_type_breakdown": meta.get("commit_type_breakdown",{}),
            "extension_summary":     meta.get("extension_summary",[])[:10],
            "tags":                  meta.get("tags",[])[:5],
            "merged_branches":       meta.get("merged_branches",[]),
            "recent_commits":        bd["git"]["commits"][:10],
            "files":                 file_list,
            "stats":                 stats,
        }

    def _branch_compare(self, query: str) -> dict:
        det  = self._detect_branches(query)
        allb = list(self.branches)
        if len(det) >= 2:
            b1, b2 = det[0], det[1]
        elif len(det) == 1:
            b1 = det[0]
            others = [b for b in allb if b != b1]
            b2 = self._pick_branch(others) if others else b1
        else:
            # no branch names in query — pick the two most active branches
            b1 = self._pick_branch(allb)
            others = [b for b in allb if b != b1]
            b2 = self._pick_branch(others) if others else b1

        # tell the user which two branches are being compared
        import sys
        print(f"  Comparing: {b1!r}  vs  {b2!r}", file=sys.stderr)
        d1, d2   = self.branches[b1], self.branches[b2]
        m1, m2   = d1["meta"], d2["meta"]
        f1, f2   = set(d1["files"]), set(d2["files"])
        common   = f1 & f2

        fn1, fn2 = [], []
        for fp in list(common)[:50]:
            s1 = set(_str(f) for f in d1["files"][fp].get("code",{}).get("functions",[]))
            s2 = set(_str(f) for f in d2["files"][fp].get("code",{}).get("functions",[]))
            fn1 += [f"{fp}:{f}" for f in s1-s2]
            fn2 += [f"{fp}:{f}" for f in s2-s1]

        em1 = set(m1.get("stats",{}).get("contributor_emails",[]))
        em2 = set(m2.get("stats",{}).get("contributor_emails",[]))

        def _bmeta(m, bd):
            s = m.get("stats", {})
            def _s(k, fb=0):
                return s.get(k) or m.get(k) or fb
            return {
                "tip_commit":            m.get("tip_commit",""),
                "total_files":           m.get("total_files",0) or len(bd["files"]),
                "total_commits":         _s("total_commits") or len(bd["git"]["commits"]),
                "commits_ahead":         _s("commits_ahead_of_base"),
                "unique_contributors":   _s("unique_contributors"),
                "contributor_emails":    _s("contributor_emails", []),
                "commit_type_breakdown": m.get("commit_type_breakdown",{}),
                "last_commit":           _s("last_commit_date",""),
            }

        return {
            "strategy":"branch_compare",
            "branch_1":b1,"branch_2":b2,
            "meta_1":_bmeta(m1,d1),"meta_2":_bmeta(m2,d2),
            "files_only_in_b1":  sorted(f1-f2)[:30],
            "files_only_in_b2":  sorted(f2-f1)[:30],
            "common_files_count":len(common),
            "fns_only_in_b1":    fn1[:20],
            "fns_only_in_b2":    fn2[:20],
            "contributors_only_in_b1": sorted(em1-em2),
            "contributors_only_in_b2": sorted(em2-em1),
            "recent_commits_b1": d1["git"]["commits"][:5],
            "recent_commits_b2": d2["git"]["commits"][:5],
        }

    def _symbol_compare(self, query: str) -> dict:
        toks = self._tokens(query)
        det  = self._detect_branches(query)
        syms = self._find_symbols(toks)
        if not syms:
            return {"strategy":"symbol_compare","error":"Symbol not found"}
        sn, st = syms[0][3], syms[0][2]
        target_branches = det[:2] if len(det) >= 2 else list(self.branches)[:2]
        found = []
        for br in target_branches:
            for fp, fe in self.branches[br]["files"].items():
                code = fe.get("code",{})
                if sn in [_str(f) for f in code.get("functions",[])] or                    sn in [_str(c) for c in code.get("classes",[])]:
                    found.append({
                        "branch":br,"filepath":fp,"symbol":sn,"sym_type":st,
                        "blob_hash":fe.get("info",{}).get("blob_hash"),
                        "extension":fe.get("info",{}).get("extension",""),
                    })
                    break
        return {
            "strategy":"symbol_compare","symbol":sn,"sym_type":st,"files":found,
            "branch":  found[0]["branch"]   if found else "",
            "filepath":found[0]["filepath"] if found else "",
        }

    def _file_compare(self, query: str) -> dict:
        names = list(dict.fromkeys(
            f.lower() for f in
            re.findall(rf"\b[\w_-]+\.(?:{_FILE_EXT[1:-1]})\b", query, re.I)
        ))
        found = []
        for fn in names:
            res = self._find_files([fn], fn)
            if res:
                br, fp, fe = res[0]
                found.append({
                    "branch":br,"filepath":fp,
                    "blob_hash":fe.get("info",{}).get("blob_hash"),
                    "extension":fe.get("info",{}).get("extension",""),
                    "functions":fe.get("code",{}).get("functions",[]),
                    "classes":  fe.get("code",{}).get("classes",[]),
                    "imports":  fe.get("code",{}).get("imports",[]),
                })
        if not found:
            return {"strategy":"file_compare","error":"No files found","filenames":names}
        return {
            "strategy":"file_compare","files":found,
            "branch":found[0]["branch"],"filepath":found[0]["filepath"],
        }

    def _author_lookup(self, query: str) -> dict:
        toks = self._tokens(query)
        am   = self.lookup["author_to_commits"]
        name_map: dict = {}
        for bn, bd in self.branches.items():
            for c in bd["git"]["commits"]:
                em = c.get("author_email","")
                if em and em not in name_map:
                    name_map[em] = c.get("author","")

        matches: dict = {}
        for tok in toks:
            t = tok.lower()
            for em, cs in am.items():
                if t in em.lower() or t in name_map.get(em,"").lower():
                    matches[em] = cs

        if not matches:
            return {"strategy":"author_lookup","error":"Author not found"}
        best = max(matches, key=lambda e: len(matches[e]))
        cs   = matches[best]
        return {
            "strategy":"author_lookup","author_email":best,
            "author_name": name_map.get(best,""),
            "total_commits":  len(cs),
            "branches_active":list({c["branch"] for c in cs}),
            "recent_commits": cs[:20],
            "commit_types":   _type_breakdown(cs),
        }

    def _commit_lookup(self, query: str) -> dict:
        toks = self._tokens(query)
        br   = self._pick_branch(list(self.branches))
        for tok in toks:
            if len(tok) >= 6:
                for bn, bd in self.branches.items():
                    for c in bd["git"]["commits"]:
                        if c["hash"].startswith(tok) or \
                           c.get("short_hash","").startswith(tok):
                            return {"strategy":"commit_lookup","branch":bn,"commit":c}
        all_c = []
        for bn, bd in self.branches.items():
            for c in bd["git"]["commits"][:5]:
                all_c.append({**c,"branch":bn})
        all_c.sort(key=lambda c: c.get("author_date",""), reverse=True)
        return {"strategy":"commit_lookup","branch":br,"recent_commits":all_c[:15]}

    def _general(self, query: str) -> dict:
        """
        Best-effort fallback — cascades through every strategy.
        Order matters: exact matches first, then semantic search,
        so vague queries like "how is a post retrieved by id"
        find the real file instead of hallucinating.
        """
        toks  = self._tokens(query)

        # 1. exact filename match (most reliable)
        files = self._find_files(toks, query)
        if files:
            return self._file_lookup(query)

        # 2. semantic/keyword search — catches natural language queries
        #    Run BEFORE symbol lookup so "post retrieved" finds main.py
        #    rather than a partial symbol match that returns 1 line
        sc = self._search_content(query)
        if sc.get("filepath"):
            # promote to file_lookup — no symbol set, so full file is fetched
            fp  = sc["filepath"]
            br  = sc["branch"]
            fe  = self._fe(br, fp)
            return {
                "strategy":        "file_lookup",
                "branch":          br,
                "filepath":        fp,
                "blob_hash":       fe.get("info",{}).get("blob_hash"),
                "extension":       fe.get("info",{}).get("extension",""),
                "functions":       [_str(f) for f in fe.get("code",{}).get("functions",[])],
                "classes":         [_str(c) for c in fe.get("code",{}).get("classes",[])],
                "imports":         fe.get("code",{}).get("imports",[]),
                "history_summary": _hist_summary(fe.get("history",{})),
                # no "symbol" key here — ensures fetch_code returns the FULL file
                "search_hits":     sc["results"][0]["matched_in"] if sc["results"] else [],
                "search_tokens":   sc["tokens"],
            }

        # 3. exact symbol match (last before branch — avoids 1-line slice on vague queries)
        syms = self._find_symbols(toks)
        if syms:
            return self._symbol_lookup(query)

        # 4. branch name detected
        det = self._detect_branches(query)
        if det:
            return self._branch_summary(query)

        # 5. last resort — recent activity
        return self._recent_activity(query)


# ─────────────────────────────────────────────────────────────────────────────
# 4. CODE FETCHER
# ─────────────────────────────────────────────────────────────────────────────

def fetch_code(repo_path: str, blob_hash: str | None, branch: str,
               filepath: str, symbol: str | None = None,
               sym_type: str | None = None, extension: str = "",
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

    if not symbol:
        lines = raw.splitlines()
        if len(lines) > max_lines:
            return "\n".join(lines[:max_lines]) + \
                   f"\n... [{len(lines)-max_lines} more lines]"
        return raw
    return _extract_symbol(raw, symbol, sym_type or "function", extension)


def _extract_symbol(source: str, name: str, sym_type: str, ext: str) -> str:
    lines = source.splitlines()
    e     = ext.lower().lstrip(".")

    if e == "py":
        pat = re.compile(rf"^class\s+{re.escape(name)}\b") \
              if sym_type == "class" \
              else re.compile(rf"^(async\s+)?def\s+{re.escape(name)}\b")
        indent = True
    elif e in ("js","ts"):
        pat    = re.compile(
            rf"^(export\s+)?(async\s+)?function\s+{re.escape(name)}\b"
            rf"|^(const|let|var)\s+{re.escape(name)}\s*=")
        indent = False
    elif e == "java":
        pat    = re.compile(
            rf"(public|private|protected|static|\s)+\s+\w+\s+{re.escape(name)}\s*\(")
        indent = False
    else:
        pat    = re.compile(rf"\b{re.escape(name)}\b")
        indent = False

    start = next((i for i,l in enumerate(lines) if pat.search(l)), None)
    if start is None:
        # symbol not found — return full file so Bedrock has real context
        return "\n".join(lines)

    if indent:
        bi   = len(lines[start]) - len(lines[start].lstrip())
        body = [lines[start]]
        for line in lines[start+1:]:
            s = line.lstrip()
            if not s:         body.append(line); continue
            if len(line)-len(s) <= bi: break
            body.append(line)
    else:
        body, depth, opened = [], 0, False
        for line in lines[start:]:
            body.append(line)
            depth += line.count("{") - line.count("}")
            if "{" in line: opened = True
            if opened and depth <= 0: break
            if len(body) > 300: body.append("... [truncated]"); break

    return "\n".join(body)


# ─────────────────────────────────────────────────────────────────────────────
# 5. PROMPT BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_prompt(query: str, intent: str, ctx: dict,
                 code_slice: str, extra_slices: list | None = None) -> str:
    parts = [
        "You are an AI assistant with direct access to a git repository's "
        "metadata index and the exact source code.\n",
        f"## User Query\n{query}\n",
        f"## Intent\n{intent}\n",
    ]

    fmts = {
        "symbol_lookup":    _fmt_symbol,
        "call_chain":       _fmt_call_chain,
        "file_lookup":      _fmt_file,
        "file_history":     _fmt_history,
        "dependency":       _fmt_dependency,
        "impact":           _fmt_impact,
        "dead_code":        _fmt_dead_code,
        "hotspot":          _fmt_hotspot,
        "contributor_rank": _fmt_contributor_rank,
        "recent_activity":  _fmt_recent_activity,
        "tag_lookup":       _fmt_tags,
        "search_content":   _fmt_search,
        "branch_summary":   _fmt_branch,
        "branch_compare":   _fmt_branch_compare,
        "symbol_compare":   _fmt_compare,
        "file_compare":     _fmt_compare,
        "author_lookup":    _fmt_author,
        "commit_lookup":    _fmt_commit,
    }
    fn = fmts.get(intent)
    parts.append(fn(ctx) if fn else
                 f"## Context\n```json\n{json.dumps(ctx,indent=2)[:2000]}\n```\n")

    if code_slice and not code_slice.startswith("[Could not"):
        ext = ctx.get("extension","").lstrip(".")
        parts.append(
            f"## Source Code — File 1\n"
            f"File: `{ctx.get('filepath','')}` on branch `{ctx.get('branch','')}`\n"
            f"```{ext}\n{code_slice}\n```\n"
        )

    for i, sl in enumerate(extra_slices or [], start=2):
        if sl.get("code") and not sl["code"].startswith("[Could not"):
            ext = sl.get("extension","").lstrip(".")
            parts.append(
                f"## Source Code — File {i}\n"
                f"File: `{sl['filepath']}` on branch `{sl['branch']}`\n"
                f"```{ext}\n{sl['code']}\n```\n"
            )

    # for metadata-only intents (branch_compare, hotspot, etc.) there is no
    # code slice — but the index context is complete. Tell Bedrock explicitly.
    has_code = bool(code_slice or extra_slices)
    if has_code:
        instruction = (
            "Answer using ONLY the source code and index context above. "
            "Never invent or assume code not shown. "
            "Be specific — reference actual function names, line content, and commit data. "
            "If something is unclear from the data shown, say so explicitly."
        )
    else:
        instruction = (
            "Answer using the index context above — it contains the complete data. "
            "Do NOT say you need more information; everything required is provided. "
            "Be specific — cite actual file names, commit counts, contributor emails, "
            "commit types, and recent commit subjects from the data above."
        )
    parts.append(f"## Instructions\n{instruction}")
    return "\n".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# 6. CONTEXT FORMATTERS
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_symbol(ctx):
    hs = ctx.get("history_summary",{})
    lines = [
        "## Symbol",
        f"- `{ctx.get('sym_type','?')} {ctx.get('symbol','?')}` in `{ctx.get('filepath','?')}`",
        f"- Branch  : `{ctx.get('branch','?')}`",
        f"- Imports : {ctx.get('imports',[])}",
        f"- Calls   : {ctx.get('calls',[])[:10]}",
    ]
    if hs:
        lines += [f"- Last modified : {hs.get('last_modified','')}",
                  f"- Authors       : {hs.get('unique_authors',[])}",
                  f"- Total commits : {hs.get('total_commits',0)}"]
    return "\n".join(lines)+"\n"

def _fmt_call_chain(ctx):
    return (f"## Call Chain\n"
            f"- `{ctx.get('symbol','?')}` in `{ctx.get('filepath','?')}`\n"
            f"- Branch          : `{ctx.get('branch','?')}`\n"
            f"- Calls           : {ctx.get('callees',[])}\n"
            f"- Called by       : {ctx.get('callers',[])}\n"
            f"- Callers of callers: {ctx.get('callers_of_callers',[])}\n")

def _fmt_file(ctx):
    hits = ctx.get("search_hits",[])
    toks = ctx.get("search_tokens",[])
    extra = ""
    if hits:
        extra = (f"- Found via   : semantic search (tokens:{toks}, matched:{hits})\n"
                 f"  The code below is the REAL file — answer from it only.\n")
    return (f"## File\n"
            f"- `{ctx.get('filepath','?')}` on `{ctx.get('branch','?')}`\n"
            + extra +
            f"- Functions : {ctx.get('functions',[])}\n"
            f"- Classes   : {ctx.get('classes',[])}\n"
            f"- Imports   : {ctx.get('imports',[])[:10]}\n"
            f"- History   : {ctx.get('history_summary',{})}\n")

def _fmt_history(ctx):
    rows = "\n".join(
        f"  {c.get('short_hash','')} {c.get('author','')} "
        f"{str(c.get('date',''))[:10]} — {c.get('subject','')}"
        for c in ctx.get("commits",[])[:15])
    return (f"## File History\n"
            f"- `{ctx.get('filepath','?')}` on `{ctx.get('branch','?')}`\n"
            f"- Total commits : {ctx.get('total_commits',0)}\n"
            f"- First seen    : {ctx.get('first_seen','')}\n"
            f"- Last modified : {ctx.get('last_modified','')}\n"
            f"- Authors       : {ctx.get('unique_authors',[])}\n"
            f"- Commits:\n{rows}\n")

def _fmt_dependency(ctx):
    return (f"## Dependencies\n"
            f"- `{ctx.get('filepath','?')}` on `{ctx.get('branch','?')}`\n"
            f"- Imports      : {ctx.get('imports',[])}\n"
            f"- Depends on   : {ctx.get('direct_deps',[])}\n"
            f"- Imported by  : {ctx.get('reverse_deps',[])}\n")

def _fmt_impact(ctx):
    return (f"## Impact Analysis\n"
            f"- `{ctx.get('filepath','?')}` on `{ctx.get('branch','?')}`\n"
            f"- Functions         : {ctx.get('functions',[])}\n"
            f"- Files that import : {ctx.get('file_dependents',[])}\n"
            f"- Callers into file : {ctx.get('func_callers',[])}\n"
            f"- Total impact      : {ctx.get('total_impact',0)}\n")

def _fmt_dead_code(ctx):
    rows = "\n".join(
        f"  {d['filepath']} → {d['function']}  (last modified: {d.get('last_modified','')})"
        for d in ctx.get("dead_functions",[])[:20])
    return (f"## Dead Code — `{ctx.get('branch','?')}`\n"
            f"- Total dead : {ctx.get('total_dead',0)}\n{rows}\n")

def _fmt_hotspot(ctx):
    rows = "\n".join(
        f"  {f['total_commits']:>4} commits  {f['filepath']}"
        for f in ctx.get("top_files",[])[:15])
    return (f"## Hotspots — `{ctx.get('branch','?')}`\n"
            f"- Total files : {ctx.get('total_files',0)}\n{rows}\n")

def _fmt_contributor_rank(ctx):
    rows = "\n".join(
        f"  {r['total_commits']:>4}  {r.get('name') or r['email']}"
        f"  branches:{r['branches_active']}"
        for r in ctx.get("ranked",[])[:15])
    return (f"## Contributor Rankings\n"
            f"- Total : {ctx.get('total_contributors',0)}\n{rows}\n")

def _fmt_recent_activity(ctx):
    rows = "\n".join(
        f"  [{c.get('branch','')}] {c.get('hash','')[:7]}"
        f" {str(c.get('author_date',''))[:10]}"
        f" {c.get('author','')} — {c.get('subject','')}"
        for c in ctx.get("recent_commits",[]))
    return f"## Recent Activity\n{rows}\n"

def _fmt_tags(ctx):
    rows = "\n".join(
        f"  {t.get('tag','')}  {str(t.get('date',''))[:10]}"
        f"  {t.get('tagger','')}  [{t.get('branch','')}] — {t.get('message','')}"
        for t in ctx.get("tags",[]))
    return f"## Tags / Releases  (total: {ctx.get('total_tags',0)})\n{rows}\n"

def _fmt_search(ctx):
    rows = "\n".join(
        f"  {r['filepath']}  hits:{r['matched_in']}"
        for r in ctx.get("results",[])[:10])
    return (f"## Search Results — `{ctx.get('branch','?')}`\n"
            f"- Tokens  : {ctx.get('tokens',[])}\n"
            f"- Matches : {ctx.get('total_matches',0)}\n{rows}\n")

def _fmt_branch(ctx):
    rows = "\n".join(
        f"  {c.get('short_hash', c.get('hash','')[:7])}"
        f" {str(c.get('author_date', c.get('date','')))[:10]}"
        f" {c.get('author','')} — {c.get('subject','')}"
        for c in ctx.get("recent_commits",[])[:10])

    # read from flat fields (new) or nested stats (old) — whichever has data
    stats          = ctx.get("stats", {})
    total_commits  = ctx.get("total_commits") or stats.get("total_commits") or len(ctx.get("recent_commits",[]))
    ahead          = ctx.get("commits_ahead") or stats.get("commits_ahead_of_base","?")
    behind         = stats.get("commits_behind_base","?")
    contributors   = ctx.get("unique_contributors") or stats.get("unique_contributors",0)
    last_date      = ctx.get("last_commit_date") or stats.get("last_commit_date","")
    emails         = ctx.get("contributor_emails") or stats.get("contributor_emails",[])

    # build file listing
    file_list = ctx.get("files", [])
    file_rows = ""
    if file_list:
        file_rows = "\n".join(
            f"  {f['path']}"
            + (f"  [{', '.join(f['functions'][:3])}{'...' if len(f['functions'])>3 else ''}]"
               if f.get('functions') else "")
            + (f"  classes:[{', '.join(f['classes'][:2])}]"
               if f.get('classes') else "")
            + (f"  ({f['size_bytes']} bytes, {f['total_commits']} commits)"
               if f.get('total_commits') else f"  ({f['size_bytes']} bytes)")
            for f in file_list
        )

    return (
        f"## Branch — `{ctx.get('branch','?')}`\n"
        f"IMPORTANT: All data below is real index data. Answer from it directly.\n"
        f"- Tip commit    : {ctx.get('tip_commit','')}\n"
        f"- Total files   : {ctx.get('total_files',0)}\n"
        f"- Total commits : {total_commits}\n"
        f"- Ahead of base : {ahead}\n"
        f"- Contributors  : {contributors} — {emails}\n"
        f"- Last commit   : {last_date}\n"
        f"- Commit types  : {ctx.get('commit_type_breakdown',{})}\n"
        f"- Ext summary   : {ctx.get('extension_summary',[])}\n"
        f"- Tags          : {[t.get('tag') for t in ctx.get('tags',[])]}\n"
        f"- Merged from   : {ctx.get('merged_branches',[])}\n"
        f"\n### Files ({len(file_list)} total)\n"
        f"{file_rows or '  (no files found)'}\n"
        f"\n### Recent commits\n{rows or '  (no commits found)'}\n"
    )

def _fmt_branch_compare(ctx):
    m1, m2 = ctx.get("meta_1",{}), ctx.get("meta_2",{})
    b1, b2 = ctx.get("branch_1","?"), ctx.get("branch_2","?")

    def _commits(clist):
        if not clist:
            return "  (no commits)"
        return "\n".join(
            f"  {c.get('short_hash',c.get('hash','')[:7])} "
            f"{str(c.get('author_date', c.get('date','')))[:10]} "
            f"{c.get('author','')} — {c.get('subject','')}"
            for c in clist
        )

    def _files(flist, limit=15):
        if not flist: return "  (none)"
        return "\n".join(f"  {f}" for f in flist[:limit])

    only1 = ctx.get("files_only_in_b1",[])
    only2 = ctx.get("files_only_in_b2",[])
    fn1   = ctx.get("fns_only_in_b1",[])
    fn2   = ctx.get("fns_only_in_b2",[])
    c1    = ctx.get("contributors_only_in_b1",[])
    c2    = ctx.get("contributors_only_in_b2",[])

    # build a plain-text summary Bedrock can directly answer from
    lines = [
        f"## Branch Comparison: `{b1}` vs `{b2}`",
        f"",
        f"IMPORTANT: Both branches are fully described below. Answer the comparison",
        f"using ONLY this data. Do not say you need more information.",
        f"",
        f"### {b1} — Stats",
        f"- Total files        : {m1.get('total_files',0)}",
        f"- Total commits      : {m1.get('total_commits',0)}",
        f"- Unique contributors: {m1.get('unique_contributors',0)}",
        f"- Commits ahead of base: {m1.get('commits_ahead',0)}",
        f"- Commit types       : {m1.get('commit_type_breakdown',{})}",
        f"- Tip commit         : {m1.get('tip_commit','')}",
        f"- Last commit date   : {m1.get('last_commit','')}",
        f"",
        f"### {b2} — Stats",
        f"- Total files        : {m2.get('total_files',0)}",
        f"- Total commits      : {m2.get('total_commits',0)}",
        f"- Unique contributors: {m2.get('unique_contributors',0)}",
        f"- Commits ahead of base: {m2.get('commits_ahead',0)}",
        f"- Commit types       : {m2.get('commit_type_breakdown',{})}",
        f"- Tip commit         : {m2.get('tip_commit','')}",
        f"- Last commit date   : {m2.get('last_commit','')}",
        f"",
        f"### Files only in `{b1}` ({len(only1)} files)",
        _files(only1),
        f"",
        f"### Files only in `{b2}` ({len(only2)} files)",
        _files(only2),
        f"",
        f"### Common files: {ctx.get('common_files_count',0)}",
        f"",
        f"### Functions only in `{b1}`",
        "\n".join(f"  {f}" for f in fn1[:15]) or "  (none — same functions in both)",
        f"",
        f"### Functions only in `{b2}`",
        "\n".join(f"  {f}" for f in fn2[:15]) or "  (none — same functions in both)",
        f"",
        f"### Contributors only in `{b1}`: {c1 or '(same contributors)'}",
        f"### Contributors only in `{b2}`: {c2 or '(same contributors)'}",
        f"",
        f"### Recent commits — `{b1}`",
        _commits(ctx.get("recent_commits_b1",[])),
        f"",
        f"### Recent commits — `{b2}`",
        _commits(ctx.get("recent_commits_b2",[])),
    ]
    return "\n".join(lines) + "\n"

def _fmt_compare(ctx):
    files = ctx.get("files", [])
    sym   = ctx.get("symbol","")
    lines = [f"## {'Symbol' if sym else 'File'} Comparison"]
    if sym:
        lines.append(f"- Symbol: `{ctx.get('sym_type','?')} {sym}`")
    for i, f in enumerate(files, 1):
        lines.append(f"- File {i}: `{f.get('filepath','?')}` on `{f.get('branch','?')}`")
        if f.get("functions"): lines.append(f"  functions: {f['functions']}")
        if f.get("classes"):   lines.append(f"  classes  : {f['classes']}")
    return "\n".join(lines)+"\n"

def _fmt_author(ctx):
    rows = "\n".join(
        f"  [{c.get('branch','')}] {c.get('hash','')[:7]}"
        f" {str(c.get('date',''))[:10]} — {c.get('subject','')}"
        for c in ctx.get("recent_commits",[])[:15])
    return (f"## Author: {ctx.get('author_name','') or ctx.get('author_email','?')}\n"
            f"- Email          : {ctx.get('author_email','')}\n"
            f"- Total commits  : {ctx.get('total_commits',0)}\n"
            f"- Active branches: {ctx.get('branches_active',[])}\n"
            f"- Commit types   : {ctx.get('commit_types',{})}\n"
            f"- Recent:\n{rows}\n")

def _fmt_commit(ctx):
    c = ctx.get("commit")
    if c:
        return (f"## Commit\n"
                f"- Hash   : {c.get('hash','')}\n"
                f"- Author : {c.get('author','')} <{c.get('author_email','')}>\n"
                f"- Date   : {c.get('author_date','')}\n"
                f"- Type   : {c.get('type','')}\n"
                f"- Subject: {c.get('subject','')}\n"
                f"- Body   : {c.get('body','')}\n")
    rows = "\n".join(
        f"  [{c.get('branch','')}] {c.get('hash','')[:7]}"
        f" {str(c.get('author_date',''))[:10]} — {c.get('subject','')}"
        for c in ctx.get("recent_commits",[]))
    return f"## Recent Commits\n{rows}\n"


# ─────────────────────────────────────────────────────────────────────────────
# 7. BEDROCK CALLER
# ─────────────────────────────────────────────────────────────────────────────

def call_bedrock(prompt: str, region: str = "us-east-1") -> str:
    try:
        import boto3
    except ImportError:
        return "[ERROR] pip install boto3"
    try:
        client   = boto3.client("bedrock-runtime", region_name=region)
        response = client.converse(
            modelId="amazon.nova-pro-v1:0",
            messages=[{"role":"user","content":[{"text":prompt}]}],
            inferenceConfig={"maxTokens":4096,"temperature":0.1},
        )
        return response["output"]["message"]["content"][0]["text"]
    except Exception as e:
        return f"[Bedrock ERROR] {e}"


# ─────────────────────────────────────────────────────────────────────────────
# 8. QUERY PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

_MULTI_FETCH   = {"file_compare","symbol_compare"}
_NEEDS_CODE    = {"symbol_lookup","call_chain","file_lookup","dependency",
                  "impact","search_content","general"} | _MULTI_FETCH


def query(user_query: str, index: dict,
          preferred_branch: str | None = None,
          aws_region: str = "us-east-1",
          dry_run: bool = False) -> str:

    repo_path = index["repository"]["repo_path"]

    # 1 — intent
    intent = classify_intent(user_query)
    print(f"\n  Intent   : {intent}")

    # 2 — traverse
    traverser = IndexTraverser(index, preferred_branch=preferred_branch)
    ctx       = traverser.traverse(user_query, intent)
    print(f"  Strategy : {ctx.get('strategy','?')}")
    if "error" in ctx:
        print(f"  Warning  : {ctx['error']}")

    # 3 — fetch code
    code_slice:   str  = ""
    extra_slices: list = []

    if intent in _NEEDS_CODE and not ctx.get("error"):
        if intent in _MULTI_FETCH:
            for i, fe in enumerate(ctx.get("files",[])):
                print(f"  Fetching : {fe['filepath']} @ {fe['branch']}")
                sl = fetch_code(
                    repo_path=repo_path, blob_hash=fe.get("blob_hash"),
                    branch=fe["branch"], filepath=fe["filepath"],
                    symbol=fe.get("symbol"), sym_type=fe.get("sym_type"),
                    extension=fe.get("extension",""),
                )
                print(f"  Slice    : {len(sl.splitlines())} lines")
                if i == 0:
                    code_slice = sl
                else:
                    extra_slices.append({
                        "filepath":fe["filepath"],"branch":fe["branch"],
                        "extension":fe.get("extension",""),"code":sl,
                    })
        elif ctx.get("filepath"):
            print(f"  Fetching : {ctx['filepath']} @ {ctx.get('branch','?')}")
            code_slice = fetch_code(
                repo_path=repo_path, blob_hash=ctx.get("blob_hash"),
                branch=ctx.get("branch",""), filepath=ctx.get("filepath",""),
                symbol=ctx.get("symbol"), sym_type=ctx.get("sym_type"),
                extension=ctx.get("extension",""),
            )
            print(f"  Slice    : {len(code_slice.splitlines())} lines")

            # fetch secondary file if traversal found a close second match
            sec = ctx.get("secondary")
            if sec and sec.get("filepath"):
                print(f"  Fetching : {sec['filepath']} @ {sec['branch']} (secondary)")
                sec_code = fetch_code(
                    repo_path=repo_path, blob_hash=sec.get("blob_hash"),
                    branch=sec["branch"], filepath=sec["filepath"],
                    extension=sec.get("extension",""),
                )
                print(f"  Slice    : {len(sec_code.splitlines())} lines")
                extra_slices.append({
                    "filepath":  sec["filepath"],
                    "branch":    sec["branch"],
                    "extension": sec.get("extension",""),
                    "code":      sec_code,
                })

    # 4 — prompt
    prompt = build_prompt(user_query, intent, ctx, code_slice, extra_slices)

    if dry_run:
        _panel("PROMPT (dry-run)", prompt)
        return "[dry-run — no Bedrock call]"

    # 5 — Bedrock
    print("  Calling  : Bedrock nova-pro ...")
    return call_bedrock(prompt, region=aws_region)


# ─────────────────────────────────────────────────────────────────────────────
# 9. UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _short_branch(b: str) -> str:
    """Strip origin/ prefix for display."""
    return re.sub(r"^(origin|remotes/[^/]+)/", "", b)


def _hist_summary(h: dict) -> dict:
    if not h: return {}
    return {"total_commits":h.get("total_commits",0),
            "first_seen":   h.get("first_seen",""),
            "last_modified":h.get("last_modified",""),
            "unique_authors":h.get("unique_authors",[])}

def _type_breakdown(commits: list) -> dict:
    bd: dict = {}
    for c in commits:
        t = c.get("type","other")
        bd[t] = bd.get(t,0)+1
    return bd

def _divider(): print("="*66)
def _header(t): _divider(); print(f"  {t}"); _divider()


# ─────────────────────────────────────────────────────────────────────────────
# 10. REPL
# ─────────────────────────────────────────────────────────────────────────────

def repl(index: dict, preferred_branch: str | None,
         aws_region: str, dry_run: bool):
    _header("AI Git Repo Query Engine")
    print(f"  Repo   : {index['repository'].get('repo_path','?')}")
    print(f"  Remote : {index['repository'].get('remote_url','?')}")
    print(f"  Model  : amazon.nova-pro-v1 @ {aws_region}")
    if preferred_branch:
        print(f"  Branch : {preferred_branch} (pinned)")
    print("\n  Commands: :branches  :branch <n>  :intents  :quit")
    _divider()

    while True:
        try:
            raw = input("\n  Query > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Exiting."); break

        if not raw: continue
        if raw == ":quit": break

        if raw == ":branches":
            for b in sorted(index["branches"]):
                m = index["branches"][b]["meta"]
                print(f"    {b:<50}"
                      f" commits:{m.get('stats',{}).get('total_commits',0):>5}"
                      f"  files:{m.get('total_files',0):>5}")
            continue

        if raw.startswith(":branch "):
            preferred_branch = raw[8:].strip()
            print(f"  Pinned to: {preferred_branch}"); continue

        if raw == ":intents":
            for i in ["file_compare","branch_compare","symbol_compare",
                      "symbol_lookup","call_chain","file_lookup","file_history",
                      "dependency","impact","dead_code","hotspot",
                      "contributor_rank","recent_activity","tag_lookup",
                      "search_content","branch_summary","author_lookup",
                      "commit_lookup","general"]:
                print(f"    {i}")
            continue

        answer = query(raw, index, preferred_branch=preferred_branch,
                       aws_region=aws_region, dry_run=dry_run)
        print()
        _print_md(answer)


# ─────────────────────────────────────────────────────────────────────────────
# 11. CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="AI query engine over the unified git repo index.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python query_engine.py -i ./reports
  python query_engine.py -i ./reports -q "compare main and develop"
  python query_engine.py -i ./reports -q "how does login differ between branches"
  python query_engine.py -i ./reports -q "what breaks if I change auth.py"
  python query_engine.py -i ./reports -q "which files change the most"
  python query_engine.py -i ./reports -q "find files that use JWT"
  python query_engine.py -i ./reports -q "top contributors"
  python query_engine.py -i ./reports -q "what changed last week"
  python query_engine.py -i ./reports -q "show release tags"
  python query_engine.py -i ./reports -q "unused functions"
  python query_engine.py -i ./reports -q "who calls validate_token"
  python query_engine.py -i ./reports --dry-run -q "explain home.html"
        """
    )
    parser.add_argument("-i","--input-dir", default=".",
                        help="Directory containing index_*.json")
    parser.add_argument("--index", default=None,
                        help="Explicit index file path")
    parser.add_argument("-q","--query", default=None,
                        help="Single query (skips REPL)")
    parser.add_argument("--branch", default=None,
                        help="Pin traversal to a specific branch")
    parser.add_argument("--region", default="us-east-1",
                        help="AWS Bedrock region")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print prompt without calling Bedrock")

    args  = parser.parse_args()
    ipath = Path(args.index) if args.index \
            else find_latest_index(Path(args.input_dir))
    index = load_index(ipath)

    if args.query:
        ans = query(args.query, index, preferred_branch=args.branch,
                    aws_region=args.region, dry_run=args.dry_run)
        print(); _print_md(ans)
    else:
        repl(index, args.branch, args.region, args.dry_run)


#if __name__ == "__main__":
#    main()
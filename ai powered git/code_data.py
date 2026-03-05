"""
code_metadata.py
----------------
Extracts code-level symbols from a git branch using tree-sitter.
Branch-aware: reads blobs directly from git objects — no checkout needed.

Supports: .py  .js  .java  .html  .css

Extracted per file:
    functions   — name, line range, decorators, docstring (Python)
    classes     — name, line range, methods, base classes (Python/Java)
    imports     — raw import statements
    calls       — outbound function call names
    call_graph  — directed edges  caller_id → callee_id
    dependency_graph — file-level import edges

Exported:
    code_metadata_for_branch(repo_path, branch_info) -> dict
"""

import os
import re
import subprocess
from collections import defaultdict
from pathlib import Path

# ── tree-sitter (optional — gracefully degrades if not installed) ─────────────
try:
    from tree_sitter import Parser, Language
    from tree_sitter_python     import language as _py_lang
    from tree_sitter_javascript import language as _js_lang
    from tree_sitter_java       import language as _java_lang
    from tree_sitter_html       import language as _html_lang
    from tree_sitter_css        import language as _css_lang
    _TS_AVAILABLE = True
except ImportError:
    _TS_AVAILABLE = False
    print("[code_metadata] tree-sitter not installed — symbol extraction disabled.")
    print("  Install with: pip install tree-sitter tree-sitter-python "
          "tree-sitter-javascript tree-sitter-java tree-sitter-html tree-sitter-css")


SUPPORTED_EXTENSIONS = (".py", ".js", ".java", ".html", ".css")


# ─────────────────────────── git helpers ────────────────────────────────────

def _git(args, cwd, check=True):
    r = subprocess.run(
        ["git"] + args, cwd=cwd,
        capture_output=True, text=True, check=check
    )
    return r.stdout

def _git_lines(args, cwd):
    return [l for l in _git(args, cwd, check=False).splitlines() if l.strip()]


# ─────────────────────────── parser factory ──────────────────────────────────

def _build_parsers() -> dict:
    if not _TS_AVAILABLE:
        return {}

    def make(lang_func):
        p = Parser()
        p.language = Language(lang_func())
        return p

    return {
        ".py":   make(_py_lang),
        ".js":   make(_js_lang),
        ".java": make(_java_lang),
        ".html": make(_html_lang),
        ".css":  make(_css_lang),
    }


# ─────────────────────────── file content fetcher ────────────────────────────

def _fetch_blob(repo_path: str, branch: str, filepath: str) -> str:
    """Read file content from git at the given branch tip — no checkout."""
    result = subprocess.run(
        ["git", "show", f"{branch}:{filepath}"],
        cwd=repo_path, capture_output=True
    )
    if result.returncode != 0:
        return ""
    return result.stdout.decode("utf-8", errors="ignore")


def _files_on_branch(repo_path: str, branch: str) -> list[str]:
    """List all tracked files on a branch filtered to supported extensions."""
    lines = _git_lines(["ls-tree", "-r", "--name-only", branch], repo_path)
    return [p for p in lines if p.endswith(SUPPORTED_EXTENSIONS)]


# ══════════════════════════════════════════════════════════════════════════════
#  SYMBOL EXTRACTORS  (one per language)
# ══════════════════════════════════════════════════════════════════════════════

class _SymbolExtractor:
    """Shared state across all files in a branch for cross-file resolution."""

    def __init__(self, parsers: dict):
        self.parsers = parsers
        # global name → list of "filepath:name" ids
        self.function_index: dict[str, list[str]] = defaultdict(list)
        # filepath → {func_name: [full_ids]}
        self.file_func_index: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
        # filepath → {class_name: {method: [ids]}}
        self.class_method_index: dict[str, dict] = defaultdict(dict)
        # filepath → {alias: module}
        self.import_aliases: dict[str, dict[str, str]] = defaultdict(dict)
        # directed call graph edges: caller_id → [callee_id]
        self.call_edges: dict[str, list[str]] = defaultdict(list)
        # directed import graph: filepath → [filepath]
        self.import_edges: dict[str, list[str]] = defaultdict(list)
        # Java helpers
        self.java_class_map:      dict[str, str] = {}   # fqcn → filepath
        self.java_package_map:    dict[str, str] = {}   # filepath → package
        self.java_pkg_classes:    dict[str, set] = defaultdict(set)

    # ── dispatch ──────────────────────────────────────────────────────────────

    def extract(self, filepath: str, code: str) -> dict:
        ext = Path(filepath).suffix.lower()
        record = {"functions": [], "classes": [], "imports": [], "calls": []}

        if not self.parsers:
            return record   # tree-sitter not available

        if ext == ".py":
            self._python(filepath, code, record)
        elif ext == ".js":
            self._js(filepath, code, record)
        elif ext == ".java":
            self._java(filepath, code, record)
        elif ext == ".html":
            self._html(filepath, code, record)
        elif ext == ".css":
            self._css(filepath, code, record)

        return record

    def resolve_import_edges(self, filepath: str, code: str,
                              module_map: dict[str, str]) -> None:
        """Second pass: build file-level import dependency edges."""
        ext = Path(filepath).suffix.lower()
        if ext == ".py":
            self._resolve_py_imports(filepath, code, module_map)
        elif ext == ".js":
            self._resolve_js_imports(filepath, code, module_map)
        elif ext == ".java":
            self._resolve_java_imports(filepath)

    # ── PYTHON ────────────────────────────────────────────────────────────────

    def _python(self, filepath: str, code: str, record: dict) -> None:
        parser = self.parsers[".py"]
        tree   = parser.parse(code.encode("utf-8"))

        def index(node, class_stack: list):
            if node.type == "class_definition":
                name_node = node.child_by_field_name("name")
                if name_node:
                    cls_name  = code[name_node.start_byte:name_node.end_byte]
                    bases     = self._py_bases(node, code)
                    start_ln  = node.start_point[0] + 1
                    end_ln    = node.end_point[0] + 1
                    record["classes"].append({
                        "name":    cls_name,
                        "bases":   bases,
                        "lines":   [start_ln, end_ln],
                        "methods": [],
                    })
                    class_stack = class_stack + [cls_name]

            if node.type == "function_definition":
                name_node = node.child_by_field_name("name")
                if name_node:
                    fn_name  = code[name_node.start_byte:name_node.end_byte]
                    full_id  = f"{filepath}:{fn_name}"
                    start_ln = node.start_point[0] + 1
                    end_ln   = node.end_point[0] + 1
                    doc      = self._py_docstring(node, code)
                    decs     = self._py_decorators(node, code)

                    fn_rec = {
                        "name":       fn_name,
                        "lines":      [start_ln, end_ln],
                        "docstring":  doc,
                        "decorators": decs,
                    }
                    record["functions"].append(fn_rec)
                    self.function_index[fn_name].append(full_id)
                    self.file_func_index[filepath][fn_name].append(full_id)

                    if class_stack:
                        cls = class_stack[-1]
                        # attach to class record
                        for c in record["classes"]:
                            if c["name"] == cls:
                                c["methods"].append(fn_name)
                        self.class_method_index[filepath].setdefault(cls, {})
                        self.class_method_index[filepath][cls].setdefault(fn_name, []).append(full_id)

            if node.type in ("import_statement", "import_from_statement"):
                text = code[node.start_byte:node.end_byte].strip()
                record["imports"].append(text)
                # build alias map for call resolution
                for m in re.findall(r'import\s+([\w\.]+)', text):
                    alias = m.split(".")[-1]
                    self.import_aliases[filepath][alias] = m

            for child in node.children:
                index(child, class_stack)

        index(tree.root_node, [])

        # second pass — resolve calls
        def resolve_calls(node, func_stack: list, class_stack: list):
            if node.type == "class_definition":
                n = node.child_by_field_name("name")
                if n:
                    class_stack = class_stack + [code[n.start_byte:n.end_byte]]

            if node.type == "function_definition":
                n = node.child_by_field_name("name")
                if n:
                    func_stack = func_stack + [f"{filepath}:{code[n.start_byte:n.end_byte]}"]

            if node.type == "call" and func_stack:
                caller   = func_stack[-1]
                call_txt = code[node.start_byte:node.end_byte]
                record["calls"].append(call_txt.split("(")[0].strip())

                fn_node = node.child_by_field_name("function")
                if fn_node:
                    callee_name, is_self, is_import = None, False, False
                    if fn_node.type == "identifier":
                        callee_name = code[fn_node.start_byte:fn_node.end_byte]
                        is_import   = callee_name in self.import_aliases[filepath]
                    elif fn_node.type == "attribute":
                        attr = fn_node.child_by_field_name("attribute")
                        obj  = fn_node.child_by_field_name("object")
                        if attr:
                            callee_name = code[attr.start_byte:attr.end_byte]
                        if obj:
                            obj_t = code[obj.start_byte:obj.end_byte]
                            is_self   = obj_t == "self"
                            is_import = obj_t in self.import_aliases[filepath]

                    if callee_name:
                        self._resolve_py_call(
                            caller, callee_name, filepath,
                            class_stack[-1] if class_stack else None,
                            is_self, is_import
                        )

            for child in node.children:
                resolve_calls(child, func_stack, class_stack)

        resolve_calls(tree.root_node, [], [])

    def _resolve_py_call(self, caller, callee, filepath, current_class, is_self, is_import):
        if is_self and current_class:
            for t in self.class_method_index[filepath].get(current_class, {}).get(callee, []):
                if t != caller:
                    self.call_edges[caller].append(t)
            return
        if callee in self.file_func_index[filepath]:
            for t in self.file_func_index[filepath][callee]:
                if t != caller:
                    self.call_edges[caller].append(t)
            return
        for t in self.function_index.get(callee, []):
            if t != caller:
                self.call_edges[caller].append(t)

    @staticmethod
    def _py_bases(class_node, code: str) -> list[str]:
        for child in class_node.children:
            if child.type == "argument_list":
                return [
                    code[a.start_byte:a.end_byte]
                    for a in child.children
                    if a.type == "identifier"
                ]
        return []

    @staticmethod
    def _py_docstring(func_node, code: str) -> str:
        body = func_node.child_by_field_name("body")
        if not body:
            return ""
        for child in body.children:
            if child.type == "expression_statement":
                for gc in child.children:
                    if gc.type == "string":
                        raw = code[gc.start_byte:gc.end_byte]
                        return raw.strip('"\' \t\n').split("\n")[0][:120]
        return ""

    @staticmethod
    def _py_decorators(func_node, code: str) -> list[str]:
        decs = []
        for child in func_node.children:
            if child.type == "decorator":
                decs.append(code[child.start_byte:child.end_byte].strip())
        return decs

    def _resolve_py_imports(self, filepath: str, code: str,
                             module_map: dict[str, str]) -> None:
        for line in code.splitlines():
            line = line.strip()
            if not line.startswith(("import ", "from ")):
                continue
            resolved = self._py_resolve_stmt(filepath, line, module_map)
            for r in resolved:
                if r and r != filepath and r not in self.import_edges[filepath]:
                    self.import_edges[filepath].append(r)

    def _py_resolve_stmt(self, current: str, stmt: str,
                          module_map: dict[str, str]) -> list[str]:
        if stmt.startswith("from "):
            parts  = stmt.split()
            module = parts[1] if len(parts) > 1 else ""
            return [self._py_resolve_from(current, module, module_map)]
        if stmt.startswith("import "):
            mods = stmt.replace("import", "").split(",")
            out  = []
            for m in mods:
                mod = m.strip().split(" as ")[0].strip()
                r   = self._py_resolve_abs(mod, module_map)
                if r:
                    out.append(r)
            return out
        return []

    @staticmethod
    def _py_resolve_abs(module: str, module_map: dict) -> str | None:
        mp = module.replace(".", "/") + ".py"
        for p in module_map:
            if p.endswith(mp):
                return p
        init = module.replace(".", "/") + "/__init__.py"
        for p in module_map:
            if p.endswith(init):
                return p
        return None

    @staticmethod
    def _py_resolve_from(current: str, module: str,
                          module_map: dict) -> str | None:
        if module.startswith("."):
            dots   = len(module) - len(module.lstrip("."))
            clean  = module.lstrip(".")
            base   = os.path.dirname(current)
            for _ in range(dots - 1):
                base = os.path.dirname(base)
            candidate = (
                os.path.normpath(os.path.join(base, clean.replace(".", "/") + ".py"))
                if clean else
                os.path.normpath(os.path.join(base, "__init__.py"))
            )
            return candidate if candidate in module_map else None
        return _SymbolExtractor._py_resolve_abs(module, module_map)

    # ── JAVASCRIPT ────────────────────────────────────────────────────────────

    def _js(self, filepath: str, code: str, record: dict) -> None:
        parser = self.parsers[".js"]
        tree   = parser.parse(code.encode("utf-8"))

        def walk(node, current_fn=None):
            if node.type == "function_declaration":
                n = node.child_by_field_name("name")
                if n:
                    name    = code[n.start_byte:n.end_byte]
                    full_id = f"{filepath}:{name}"
                    record["functions"].append({
                        "name":  name,
                        "lines": [node.start_point[0]+1, node.end_point[0]+1],
                    })
                    self.function_index[name].append(full_id)
                    self.file_func_index[filepath][name].append(full_id)
                    current_fn = full_id

            if node.type == "import_statement":
                record["imports"].append(code[node.start_byte:node.end_byte].strip())

            if node.type == "call_expression" and current_fn:
                fn = node.child_by_field_name("function")
                if fn and fn.type == "identifier":
                    callee = code[fn.start_byte:fn.end_byte]
                    record["calls"].append(callee)
                    for t in self.function_index.get(callee, []):
                        if t != current_fn:
                            self.call_edges[current_fn].append(t)

            for child in node.children:
                walk(child, current_fn)

        walk(tree.root_node)

    def _resolve_js_imports(self, filepath: str, code: str,
                             module_map: dict) -> None:
        for m in re.findall(r"""['"](\.{1,2}/[^'"]+)['"]""", code):
            base      = os.path.dirname(filepath)
            candidate = os.path.normpath(os.path.join(base, m))
            for suffix in ("", ".js", "/index.js"):
                full = candidate + suffix
                if full in module_map and full not in self.import_edges[filepath]:
                    self.import_edges[filepath].append(full)
                    break

    # ── JAVA ──────────────────────────────────────────────────────────────────

    def _java(self, filepath: str, code: str, record: dict) -> None:
        parser = self.parsers[".java"]
        tree   = parser.parse(code.encode("utf-8"))

        pkg_m = re.search(r'package\s+([\w\.]+);', code)
        if pkg_m:
            pkg        = pkg_m.group(1)
            class_name = Path(filepath).stem
            fqcn       = f"{pkg}.{class_name}"
            self.java_class_map[fqcn]   = filepath
            self.java_package_map[filepath] = pkg
            self.java_pkg_classes[pkg].add(class_name)

        file_methods: set[str] = set()

        def walk(node, current_fn=None):
            nonlocal file_methods
            if node.type == "class_declaration":
                n = node.child_by_field_name("name")
                if n:
                    cls_name = code[n.start_byte:n.end_byte]
                    record["classes"].append({
                        "name":  cls_name,
                        "lines": [node.start_point[0]+1, node.end_point[0]+1],
                    })

            if node.type == "import_declaration":
                record["imports"].append(code[node.start_byte:node.end_byte].strip())

            if node.type == "method_declaration":
                n = node.child_by_field_name("name")
                if n:
                    name    = code[n.start_byte:n.end_byte]
                    full_id = f"{filepath}:{name}"
                    record["functions"].append({
                        "name":  name,
                        "lines": [node.start_point[0]+1, node.end_point[0]+1],
                    })
                    self.function_index[name].append(full_id)
                    self.file_func_index[filepath][name].append(full_id)
                    file_methods.add(name)
                    current_fn = full_id

            if node.type == "method_invocation" and current_fn:
                n = node.child_by_field_name("name")
                if n:
                    callee = code[n.start_byte:n.end_byte]
                    record["calls"].append(callee)
                    targets = [
                        t for t in self.function_index.get(callee, [])
                        if t.startswith(filepath)
                    ] or self.function_index.get(callee, [])
                    for t in targets:
                        if t != current_fn:
                            self.call_edges[current_fn].append(t)

            for child in node.children:
                walk(child, current_fn)

        walk(tree.root_node)

    def _resolve_java_imports(self, filepath: str) -> None:
        # same-package implicit usage
        pkg = self.java_package_map.get(filepath)
        if not pkg:
            return
        current_cls = Path(filepath).stem
        for cls in self.java_pkg_classes.get(pkg, set()):
            if cls == current_cls:
                continue
            fqcn   = f"{pkg}.{cls}"
            target = self.java_class_map.get(fqcn)
            if target and target not in self.import_edges[filepath]:
                self.import_edges[filepath].append(target)

    # ── HTML / CSS ────────────────────────────────────────────────────────────

    def _html(self, filepath: str, code: str, record: dict) -> None:
        record["imports"] += re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', code)
        record["imports"] += re.findall(r'<link[^>]+href=["\']([^"\']+)["\']', code)
        record["imports"] += re.findall(r'\{%\s*(?:include|extends)\s+[\'"]([^\'"]+)[\'"]', code)
        record["functions"] = [
            {"name": b, "lines": []} for b in re.findall(r'\{%\s*block\s+(\w+)', code)
        ]

    def _css(self, filepath: str, code: str, record: dict) -> None:
        for url in re.findall(r'@import\s+[\'"]([^\'"]+)[\'"]', code):
            record["imports"].append(url)
        selectors = re.findall(r'([.#]?[\w-]+)\s*\{', code)
        record["functions"] = [{"name": s, "lines": []} for s in selectors]




# ══════════════════════════════════════════════════════════════════════════════
#  UNIFIED KNOWLEDGE GRAPH  (UKG)
# ══════════════════════════════════════════════════════════════════════════════

class UnifiedKnowledgeGraph:
    """
    A directed graph that unifies file structure, import dependencies,
    class containment, and function call relationships into one traversable
    graph the AI can walk to answer questions without loading the full repo.

    Node types:
        file:<filepath>              — a source file
        class:<filepath>:<ClassName> — a class definition
        func:<filepath>:<func_name>  — a function / method

    Edge types:
        contains   file  → class     (file contains class)
        contains   file  → func      (file contains top-level function)
        contains   class → func      (class contains method)
        imports    file  → file      (file imports another file)
        calls      func  → func      (function calls another function)
        inherits   class → class     (class extends another class)

    Serialised as an adjacency list so it can be stored in JSON and
    re-loaded as a networkx DiGraph by the query_engine without re-running
    the full analysis.
    """

    def __init__(self):
        try:
            import networkx as nx
            self.graph = nx.DiGraph()
            self._nx = nx
            self._available = True
        except ImportError:
            self.graph = None
            self._available = False
            print("[UKG] networkx not installed — graph disabled. "
                  "Install with: pip install networkx")

    # ── build ─────────────────────────────────────────────────────────────────

    def build(
        self,
        file_records:   list[dict],
        call_graph:     dict[str, list[str]],
        dep_graph:      dict[str, list[str]],
    ) -> None:
        """
        Populate the graph from already-extracted symbol data.

        Args:
            file_records:  list of per-file dicts from _SymbolExtractor
            call_graph:    {caller_id: [callee_id, ...]}
            dep_graph:     {filepath: [imported_filepath, ...]}
        """
        if not self._available:
            return

        # ── 1. File nodes ─────────────────────────────────────────────────────
        for rec in file_records:
            fp        = rec["filepath"]
            file_node = f"file:{fp}"
            self.graph.add_node(file_node, type="file", filepath=fp)

            # ── 2. Class nodes + containment edges ───────────────────────────
            for cls in rec.get("classes", []):
                cls_node = f"class:{fp}:{cls['name']}"
                self.graph.add_node(cls_node,
                    type="class",
                    name=cls["name"],
                    filepath=fp,
                    lines=cls.get("lines", []),
                    bases=cls.get("bases", []),
                    methods=cls.get("methods", []),
                )
                self.graph.add_edge(file_node, cls_node, type="contains")

                # methods → func nodes under the class
                for method_name in cls.get("methods", []):
                    func_node = f"func:{fp}:{method_name}"
                    if not self.graph.has_node(func_node):
                        self.graph.add_node(func_node,
                            type="function",
                            name=method_name,
                            filepath=fp,
                        )
                    self.graph.add_edge(cls_node, func_node, type="contains")

            # ── 3. Top-level function nodes + containment edges ───────────────
            method_names = {
                m for cls in rec.get("classes", [])
                for m in cls.get("methods", [])
            }
            for fn in rec.get("functions", []):
                func_node = f"func:{fp}:{fn['name']}"
                if not self.graph.has_node(func_node):
                    self.graph.add_node(func_node,
                        type="function",
                        name=fn["name"],
                        filepath=fp,
                        lines=fn.get("lines", []),
                        docstring=fn.get("docstring", ""),
                        decorators=fn.get("decorators", []),
                    )
                # only add file→func edge for top-level (not methods)
                if fn["name"] not in method_names:
                    self.graph.add_edge(file_node, func_node, type="contains")

        # ── 4. Import (dependency) edges ──────────────────────────────────────
        for src_fp, targets in dep_graph.items():
            src_node = f"file:{src_fp}"
            if not self.graph.has_node(src_node):
                self.graph.add_node(src_node, type="file", filepath=src_fp)
            for tgt_fp in targets:
                tgt_node = f"file:{tgt_fp}"
                if not self.graph.has_node(tgt_node):
                    self.graph.add_node(tgt_node, type="file", filepath=tgt_fp)
                self.graph.add_edge(src_node, tgt_node, type="imports")

        # ── 5. Call edges ─────────────────────────────────────────────────────
        for caller_id, callees in call_graph.items():
            caller_node = f"func:{caller_id}"
            if not self.graph.has_node(caller_node):
                self.graph.add_node(caller_node, type="function")
            for callee_id in callees:
                callee_node = f"func:{callee_id}"
                if not self.graph.has_node(callee_node):
                    self.graph.add_node(callee_node, type="function")
                self.graph.add_edge(caller_node, callee_node, type="calls")

        # ── 6. Inheritance edges ──────────────────────────────────────────────
        # build a name → class_node index first
        class_name_index: dict[str, str] = {}
        for node, data in self.graph.nodes(data=True):
            if data.get("type") == "class":
                class_name_index[data["name"]] = node

        for node, data in list(self.graph.nodes(data=True)):
            if data.get("type") == "class":
                for base in data.get("bases", []):
                    parent_node = class_name_index.get(base)
                    if parent_node and parent_node != node:
                        self.graph.add_edge(node, parent_node, type="inherits")

    # ── stats ─────────────────────────────────────────────────────────────────

    def stats(self) -> dict:
        if not self._available:
            return {}
        node_types: dict[str, int] = {}
        edge_types: dict[str, int] = {}
        for _, d in self.graph.nodes(data=True):
            t = d.get("type", "unknown")
            node_types[t] = node_types.get(t, 0) + 1
        for _, _, d in self.graph.edges(data=True):
            t = d.get("type", "unknown")
            edge_types[t] = edge_types.get(t, 0) + 1
        try:
            is_dag = self._nx.is_directed_acyclic_graph(self.graph)
            sccs   = list(self._nx.strongly_connected_components(self.graph))
            largest_scc = max((len(c) for c in sccs), default=0)
        except Exception:
            is_dag, largest_scc = None, None
        return {
            "total_nodes":  self.graph.number_of_nodes(),
            "total_edges":  self.graph.number_of_edges(),
            "node_types":   node_types,
            "edge_types":   edge_types,
            "is_dag":       is_dag,
            "largest_scc":  largest_scc,
        }

    # ── k-hop subgraph (used by query_engine) ────────────────────────────────

    def k_hop_subgraph(self, start_node: str, k: int = 2) -> dict:
        """
        Extract a k-hop neighbourhood around a node.
        Returns serialised adjacency list (same format as to_dict()).
        Used by the AI to fetch only the relevant slice of the graph.
        """
        if not self._available or start_node not in self.graph:
            return {}

        visited  = {start_node}
        frontier = {start_node}
        for _ in range(k):
            next_f = set()
            for node in frontier:
                neighbours = (
                    set(self.graph.successors(node)) |
                    set(self.graph.predecessors(node))
                )
                for n in neighbours:
                    if n not in visited:
                        visited.add(n)
                        next_f.add(n)
            frontier = next_f

        sub = self.graph.subgraph(visited).copy()
        return _serialise_graph(sub)

    # ── serialisation ─────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Serialise the full graph to a JSON-safe adjacency dict."""
        if not self._available:
            return {}
        return _serialise_graph(self.graph)

    @classmethod
    def from_dict(cls, data: dict) -> "UnifiedKnowledgeGraph":
        """
        Re-hydrate a UKG from a serialised adjacency dict.
        Used by query_engine to load without re-running analysis.
        """
        ukg = cls()
        if not ukg._available:
            return ukg
        for node, info in data.items():
            attrs = {k: v for k, v in info.items() if k != "edges"}
            ukg.graph.add_node(node, **attrs)
            for edge in info.get("edges", []):
                ukg.graph.add_edge(node, edge["target"], type=edge.get("type", ""))
        return ukg

    # ── node finders (helpers for query_engine) ───────────────────────────────

    def find_function(self, name: str) -> list[str]:
        """Return all func: node ids whose name matches (case-insensitive)."""
        q = name.lower()
        return [
            n for n, d in self.graph.nodes(data=True)
            if d.get("type") == "function" and q in d.get("name", "").lower()
        ]

    def find_file(self, name: str) -> list[str]:
        """Return all file: node ids whose path contains name."""
        q = name.lower()
        return [
            n for n, d in self.graph.nodes(data=True)
            if d.get("type") == "file" and q in d.get("filepath", "").lower()
        ]

    def find_class(self, name: str) -> list[str]:
        """Return all class: node ids whose name matches."""
        q = name.lower()
        return [
            n for n, d in self.graph.nodes(data=True)
            if d.get("type") == "class" and q in d.get("name", "").lower()
        ]


def _serialise_graph(g) -> dict:
    """Convert a networkx DiGraph to a JSON-safe adjacency dict."""
    out = {}
    for node in g.nodes:
        attrs = dict(g.nodes[node])
        out[node] = {
            **attrs,
            "edges": [
                {"target": tgt, "type": g.edges[node, tgt].get("type", "")}
                for tgt in g.successors(node)
            ],
        }
    return out

# ══════════════════════════════════════════════════════════════════════════════
#  BRANCH-LEVEL RUNNER
# ══════════════════════════════════════════════════════════════════════════════

def code_metadata_for_branch(repo_path: str, branch_info: dict) -> dict:
    """
    Extract code metadata for every supported file on a branch,
    and build a Unified Knowledge Graph (UKG) over all symbols.

    Args:
        repo_path:   Absolute path to the git repository.
        branch_info: Branch dict from main.py _all_branches().

    Returns a dict with:
        branch, is_remote, tip_commit,
        total_files, total_functions, total_classes,
        dependency_graph{}   — file-level import edges
        call_graph{}         — function-level call edges
        ukg{}                — full Unified Knowledge Graph (adjacency list)
        ukg_stats{}          — node/edge counts, DAG check, largest SCC
        files[]              — per-file: functions, classes, imports, calls
    """
    branch    = branch_info["name"]
    parsers   = _build_parsers()
    extractor = _SymbolExtractor(parsers)

    filepaths  = _files_on_branch(repo_path, branch)
    module_map = {fp: fp for fp in filepaths}

    file_records: list[dict] = []
    code_cache:   dict[str, str] = {}

    for i, filepath in enumerate(filepaths, 1):
        print(f"      [{i:>4}/{len(filepaths)}] {filepath[:65]:<65}", end="\r")
        code = _fetch_blob(repo_path, branch, filepath)
        code_cache[filepath] = code

        record = extractor.extract(filepath, code)
        record["filepath"] = filepath
        record["imports"]  = sorted(set(record["imports"]))
        record["calls"]    = sorted(set(record["calls"]))
        file_records.append(record)

    # second pass — resolve import edges now that all files are indexed
    for filepath, code in code_cache.items():
        extractor.resolve_import_edges(filepath, code, module_map)

    print(" " * 80, end="\r")

    call_graph = {k: sorted(set(v)) for k, v in extractor.call_edges.items()}
    dep_graph  = {k: sorted(set(v)) for k, v in extractor.import_edges.items()}

    # ── Build Unified Knowledge Graph ──────────────────────────────────────
    print(f"      [ukg] building graph ...", end="\r")
    ukg = UnifiedKnowledgeGraph()
    ukg.build(file_records, call_graph, dep_graph)
    ukg_dict  = ukg.to_dict()
    ukg_stats = ukg.stats()
    print(" " * 80, end="\r")

    total_fns = sum(len(f["functions"]) for f in file_records)
    total_cls = sum(len(f["classes"])   for f in file_records)

    return {
        "branch":           branch,
        "is_remote":        branch_info["is_remote"],
        "tip_commit":       branch_info["tip_commit"],
        "total_files":      len(file_records),
        "total_functions":  total_fns,
        "total_classes":    total_cls,
        "dependency_graph": dep_graph,
        "call_graph":       call_graph,
        "ukg_stats":        ukg_stats,
        "ukg":              ukg_dict,
        "files":            file_records,
    }

#!/usr/bin/env python3
"""Execute chapter 3 notebooks (ch1/ch2 have none) against in-memory Qdrant.

Uses qdrant-client local mode so verification works without Docker.
"""
from __future__ import annotations

import json
import os
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("AIPS_QDRANT_HOST", "localhost")
os.environ.setdefault("AIPS_QDRANT_PORT", "6333")

# Stub aips package before engine imports (avoids pyspark-heavy aips/__init__.py).
import types
_aips = types.ModuleType("aips")
_env = types.ModuleType("aips.environment")
_store = {}
_env.get = lambda k, default=None: _store.get(k, os.environ.get(k, default))
_env.set = lambda k, v: _store.__setitem__(k, v)
sys.modules["aips"] = _aips
sys.modules["aips.environment"] = _env
_aips.environment = _env

import nbformat
from qdrant_client import QdrantClient


def patch_qdrant_local():
    """Force QdrantEngine / QdrantCollection onto an in-memory client."""
    from engines.qdrant import QdrantEngine as qe_mod
    from engines.qdrant import QdrantCollection as qc_mod
    import engines.qdrant.QdrantEngine as eng
    import engines.qdrant.QdrantCollection as col

    shared = QdrantClient(":memory:")

    def engine_init(self, host=None):
        from engines.Engine import Engine
        from engines.qdrant.config import QDRANT_HOST, QDRANT_PORT
        Engine.__init__(self, "qdrant")
        self.host = host or QDRANT_HOST
        self.port = QDRANT_PORT
        self.base_url = f"http://{self.host}:{self.port}"
        self.client = shared

    def health_check(self, log=False, retries=0):
        try:
            _ = self.client.get_collections()
            if log:
                print("Qdrant is online (local :memory: mode)")
            return True
        except Exception as ex:
            if log:
                print(f"Qdrant failed the health check: {ex}")
            return False

    def collection_init(self, name, client=None):
        from engines.Collection import Collection
        from engines.qdrant.config import get_id_field, get_schema, get_vector_field_names
        Collection.__init__(self, name.lower())
        self.client = client or shared
        self.schema = get_schema(self.name)
        self.id_field = get_id_field(self.name)
        self.vector_fields = get_vector_field_names(self.name)

    eng.QdrantEngine.__init__ = engine_init
    eng.QdrantEngine.health_check = health_check
    col.QdrantCollection.__init__ = collection_init
    return shared


def run_notebook(path: Path, globals_dict: dict) -> dict:
    nb = nbformat.read(path, as_version=4)
    results = {"path": str(path), "cells": [], "ok": True}
    print(f"\n======== RUNNING {path.name} ({len(nb.cells)} cells) ========")
    for i, cell in enumerate(nb.cells):
        if cell.cell_type != "code":
            continue
        src = "".join(cell.source) if isinstance(cell.source, list) else cell.source
        if not src.strip():
            continue
        # Skip display-only IPython helpers if missing
        if "display(" in src or "HTML(" in src:
            # provide no-op fallbacks
            globals_dict.setdefault("display", lambda *a, **k: None)
            try:
                from IPython.display import HTML  # noqa: F401
            except Exception:
                globals_dict.setdefault("HTML", lambda x: x)
        print(f"\n--- cell {i} ---")
        print(src[:200].replace("\n", " | ") + ("..." if len(src) > 200 else ""))
        try:
            # Make get_engine prefer qdrant
            if "from aips import" in src or "get_engine" in src:
                globals_dict.setdefault("__name__", "__main__")
            exec(compile(src, f"{path.name}:cell{i}", "exec"), globals_dict)
            results["cells"].append({"i": i, "status": "ok"})
            print("OK")
        except Exception as ex:
            results["cells"].append({"i": i, "status": "error", "error": str(ex)})
            results["ok"] = False
            print("ERROR:", ex)
            traceback.print_exc()
            # Continue other cells for ch03.1 math notebook; stop hard on ch03.2 engine failures
            if "controlling-relevance" in path.name:
                break
    return results


def main():
    # Lightweight aips surface without importing full aips (avoids pyspark).
    import importlib.util

    # Provide minimal aips helpers used by ch03.1
    def num2str(number):
        return str(round(number, 4))

    def vec2str(vector):
        return "[" + ", ".join(map(num2str, vector)) + "]"

    def tokenize(text):
        return text.replace(".", "").replace(",", "").lower().split()

    def display_search(query, documents):
        print("Query:", query)
        for d in documents:
            print(" ", d.get("title"), d.get("score"), d.get("id"))

    shared = patch_qdrant_local()

    # Register get_engine / set_engine without full aips package import chain
    from engines.qdrant.QdrantEngine import QdrantEngine

    def get_engine(engine_override=None, host_override=None, log=False):
        return QdrantEngine(host_override)

    def set_engine(name):
        assert name.lower() == "qdrant"

    g = {
        "num2str": num2str,
        "vec2str": vec2str,
        "tokenize": tokenize,
        "display_search": display_search,
        "get_engine": get_engine,
        "set_engine": set_engine,
        "QdrantEngine": QdrantEngine,
    }

    # Extend existing aips stub for `from aips import *`
    aips_mod = sys.modules["aips"]
    aips_mod.num2str = num2str
    aips_mod.vec2str = vec2str
    aips_mod.tokenize = tokenize
    aips_mod.display_search = display_search
    aips_mod.get_engine = get_engine
    aips_mod.set_engine = set_engine
    aips_mod.healthcheck = lambda: print("healthcheck ok") if get_engine().health_check(log=True) else print("unhealthy")
    aips_mod.__all__ = [
        "num2str", "vec2str", "tokenize", "display_search",
        "get_engine", "set_engine", "healthcheck",
    ]

    ch03 = ROOT / "chapters" / "ch03"
    notebooks = [
        ch03 / "1.vectors-and-text-similarity.ipynb",
        ch03 / "2.controlling-relevance.ipynb",
    ]

    print("NOTE: chapters 1 and 2 have no notebooks in this repo.")
    print("Using Qdrant local :memory: mode (Docker daemon unavailable).")

    all_ok = True
    summaries = []
    for nb_path in notebooks:
        # fresh-ish globals per notebook but keep engine patches
        nb_g = dict(g)
        nb_g.update({
            "num2str": num2str,
            "vec2str": vec2str,
            "tokenize": tokenize,
            "display_search": display_search,
            "get_engine": get_engine,
        })
        # For controlling-relevance: `from aips import *`
        result = run_notebook(nb_path, nb_g)
        summaries.append(result)
        all_ok = all_ok and result["ok"]

    print("\n======== SUMMARY ========")
    for r in summaries:
        status = "PASS" if r["ok"] else "FAIL"
        errs = [c for c in r["cells"] if c.get("status") == "error"]
        print(f"{status}: {r['path']}  cells_ok={sum(1 for c in r['cells'] if c['status']=='ok')} errors={len(errs)}")
        for e in errs:
            print(f"   cell {e['i']}: {e.get('error')}")

    if not all_ok:
        sys.exit(1)
    print("ALL_CH03_NOTEBOOKS_OK")


if __name__ == "__main__":
    main()

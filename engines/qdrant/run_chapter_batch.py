#!/usr/bin/env python3
"""Execute a batch of chapter notebooks against the active AIPS engine (Qdrant).

Usage inside notebooks container:
  python /tmp/notebooks/engines/qdrant/run_chapter_batch.py 3 4 5
"""
from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path

# Prefer bind-mounted repo
for p in ("/tmp/notebooks", str(Path(__file__).resolve().parents[2])):
    if p not in sys.path and Path(p).exists():
        sys.path.insert(0, p)

import nbformat

ROOT = Path("/tmp/notebooks") if Path("/tmp/notebooks/chapters").exists() else Path(__file__).resolve().parents[2]


def reset_spark():
    """Drop any stale Spark session so heavy notebooks start clean."""
    try:
        from pyspark.sql import SparkSession
        existing = SparkSession.getActiveSession()
        if existing is not None:
            existing.stop()
            print("Stopped existing SparkSession")
    except Exception as ex:
        print(f"Spark reset note: {ex}")


def setup_engines():
    import aips
    aips.set_engine("qdrant")
    eng = aips.get_engine()
    ok = eng.health_check(log=True, retries=5)
    if not ok:
        raise RuntimeError("Qdrant healthcheck failed")
    print("engine:", eng.name, "url:", getattr(eng, "base_url", "?"))
    # SKG / entity extraction still require Solr; probe but don't hard-fail yet.
    try:
        from engines.solr.SolrEngine import SolrEngine
        solr_ok = SolrEngine().health_check(log=True, retries=2)
        print("solr_available:", solr_ok)
    except Exception as ex:
        print("solr_available: False", ex)
    return aips


def run_notebook(path: Path, g: dict) -> dict:
    nb = nbformat.read(path.open(), as_version=4)
    result = {"path": str(path.relative_to(ROOT)), "ok": True, "cells_ok": 0, "errors": []}
    print(f"\n======== {result['path']} ========")
    for i, cell in enumerate(nb.cells):
        if cell.cell_type != "code":
            continue
        src = cell.source if isinstance(cell.source, str) else "".join(cell.source)
        if not src.strip():
            continue
        # Skip IPython magics / shell cells (e.g. `! head file.csv`) and interactive prompts
        first = src.lstrip().splitlines()[0] if src.lstrip() else ""
        if first.startswith(("!", "%", "%%")) or src.lstrip().startswith(("!", "%")):
            print(f"  skip cell {i}: ipython magic/shell")
            continue
        if "input(" in src:
            print(f"  skip cell {i}: interactive input()")
            continue
        print(f"  cell {i}...", end="", flush=True)
        try:
            # Provide display fallbacks
            g.setdefault("display", lambda *a, **k: None)
            exec(compile(src, f"{path.name}:{i}", "exec"), g)
            result["cells_ok"] += 1
            print(" ok")
        except Exception as ex:
            result["ok"] = False
            msg = f"{type(ex).__name__}: {ex}"
            result["errors"].append({"cell": i, "error": msg})
            print(f" FAIL {msg}")
            # continue through notebook to collect more signal, except hard env errors
            if isinstance(ex, (MemoryError, SystemExit)):
                break
    return result


def notebooks_for_chapters(chapters):
    paths = []
    for ch in chapters:
        if ch in (1, 2):
            print(f"ch{ch:02d}: no notebooks")
            continue
        folder = ROOT / "chapters" / f"ch{ch:02d}"
        if not folder.exists():
            print(f"missing {folder}")
            continue
        for nb in sorted(folder.glob("*.ipynb")):
            # skip defunct
            if "defunct" in nb.name.lower():
                continue
            paths.append(nb)
    return paths


def ensure_retrotech_collections(aips):
    """Make sure products/signals exist before ch4.2 if a prior run populated them."""
    eng = aips.get_engine()
    for name in ("products", "signals"):
        try:
            count = eng.get_collection(name).get_document_count()
            print(f"collection {name}: {count} docs")
        except Exception as ex:
            print(f"collection {name}: unavailable ({ex})")


def main(argv):
    chapters = [int(x) for x in argv] if argv else [3, 4, 5]
    print("Batch chapters:", chapters)
    print("ROOT", ROOT)
    aips = setup_engines()
    ensure_retrotech_collections(aips)

    # Shared globals for sequential chapter notebooks (state can carry datasets)
    g = {
        "__name__": "__main__",
        "display": lambda *a, **k: None,
    }
    # Seed common imports used after `from aips import *`
    g.update({k: getattr(aips, k) for k in dir(aips) if not k.startswith("_")})

    results = []
    for path in notebooks_for_chapters(chapters):
        # ch5.1 requires the dedicated spaCy experimental env
        if path.name.startswith("1.open-information-extraction"):
            print(f"\nNOTE: {path.name} should use ch5-spacy kernel; running with import hook")
            try:
                import runpy
                # Ensure spacy_experimental importable from ch5 env site-packages
                import site
                spacy_site = "/opt/conda/envs/ch5-spacy/lib/python3.10/site-packages"
                if spacy_site not in sys.path:
                    sys.path.insert(0, spacy_site)
            except Exception as ex:
                print("ch5-spacy path hook failed:", ex)

        reset_spark()
        g["get_engine"] = aips.get_engine
        g["set_engine"] = aips.set_engine
        try:
            aips.set_engine("qdrant")
        except Exception:
            pass
        results.append(run_notebook(path, g))

    print("\n======== BATCH SUMMARY ========")
    all_ok = True
    for r in results:
        status = "PASS" if r["ok"] else "FAIL"
        all_ok = all_ok and r["ok"]
        print(f"{status}  cells_ok={r['cells_ok']:3d}  errors={len(r['errors'])}  {r['path']}")
        for e in r["errors"][:5]:
            print(f"         cell {e['cell']}: {e['error'][:160]}")
    if not results:
        print("No notebooks executed")
        return 1
    print("BATCH_OK" if all_ok else "BATCH_HAS_FAILURES")
    return 0 if all_ok else 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

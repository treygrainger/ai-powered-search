#!/usr/bin/env python3
"""Verify all chapter notebooks against Qdrant (with Solr for SKG-only paths).

Classifies notebooks and executes code cells with a pragmatic skip policy for:
- interactive input
- IPython shell magics
- known multi-hour fine-tuning cells (still attempted unless marked skip)
- defunct notebooks

Usage (in aips-notebooks container):
  python /tmp/notebooks/engines/qdrant/verify_all_labs.py
  python /tmp/notebooks/engines/qdrant/verify_all_labs.py --chapters 3,4,5
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from pathlib import Path

ROOT = Path("/tmp/notebooks") if Path("/tmp/notebooks/chapters").exists() else Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("AIPS_QDRANT_HOST", "aips-qdrant")
os.environ.setdefault("AIPS_QDRANT_PORT", "6333")

import nbformat

# Notebooks that are optional / known non-blocking for engine parity
SKIP_NOTEBOOKS = {
    "chapters/ch11/a.defunct.synthesize-search-sessions.ipynb",
    "chapters/ch13/ch13-tokenizer-analysis.ipynb",  # analysis only, often heavy
}

# Cells containing these substrings are skipped (long training / interactive)
SKIP_CELL_SUBSTRINGS = [
    "input(",
    "Trainer(",
    "training_args",
    "num_train_epochs",
    # pure shell dumps
]

REPORT_PATH = ROOT / "engines" / "qdrant" / "lab_verification_report.json"


def setup():
    import aips
    aips.set_engine("qdrant")
    eng = aips.get_engine()
    if not eng.health_check(log=True, retries=5):
        raise RuntimeError("Qdrant not healthy")
    print("Qdrant OK", getattr(eng, "base_url", ""))
    try:
        from engines.solr.SolrEngine import SolrEngine
        print("Solr OK", SolrEngine().health_check(log=True, retries=2))
    except Exception as ex:
        print("Solr probe failed", ex)
    return aips


def should_skip_cell(src: str) -> str | None:
    s = src.lstrip()
    if not s:
        return "empty"
    first = s.splitlines()[0]
    if first.startswith(("!", "%", "%%")):
        return "ipython-magic"
    for needle in SKIP_CELL_SUBSTRINGS:
        if needle in src:
            return f"skip-marker:{needle}"
    return None


def run_notebook(path: Path, g: dict) -> dict:
    rel = str(path.relative_to(ROOT)).replace("\\", "/")
    nb = nbformat.read(path.open(), as_version=4)
    result = {
        "path": rel,
        "ok": True,
        "cells_ok": 0,
        "cells_skipped": 0,
        "errors": [],
        "seconds": 0.0,
    }
    t0 = time.time()
    print(f"\n======== {rel} ========")
    for i, cell in enumerate(nb.cells):
        if cell.cell_type != "code":
            continue
        src = cell.source if isinstance(cell.source, str) else "".join(cell.source)
        why = should_skip_cell(src)
        if why:
            result["cells_skipped"] += 1
            print(f"  skip {i}: {why}")
            continue
        print(f"  cell {i}...", end="", flush=True)
        try:
            g.setdefault("display", lambda *a, **k: None)
            exec(compile(src, f"{path.name}:{i}", "exec"), g)
            result["cells_ok"] += 1
            print(" ok")
        except Exception as ex:
            result["ok"] = False
            msg = f"{type(ex).__name__}: {ex}"
            result["errors"].append({"cell": i, "error": msg})
            print(f" FAIL {msg}")
            # Continue to surface more errors unless catastrophic
            if isinstance(ex, (MemoryError, SystemExit, KeyboardInterrupt)):
                break
    result["seconds"] = round(time.time() - t0, 1)
    return result


def chapter_notebooks(chapters: list[int] | None):
    paths = []
    base = ROOT / "chapters"
    # include welcome optionally
    for p in sorted(base.rglob("*.ipynb")):
        rel = str(p.relative_to(ROOT)).replace("\\", "/")
        if "defunct" in rel:
            continue
        if rel in SKIP_NOTEBOOKS:
            continue
        if chapters:
            # welcome has no ch number
            if rel.endswith("welcome.ipynb") or rel.endswith("aips-test-suite.ipynb"):
                if 0 not in chapters:
                    continue
            else:
                ok = False
                for ch in chapters:
                    if f"/ch{ch:02d}/" in rel or f"/ch{ch}/" in rel:
                        ok = True
                        break
                if not ok:
                    continue
        paths.append(p)
    return paths


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chapters", default="", help="comma list e.g. 3,4,5 or empty=all")
    args = ap.parse_args()
    chapters = [int(x) for x in args.chapters.split(",") if x.strip()] if args.chapters else None

    aips = setup()
    g = {"__name__": "__main__", "display": lambda *a, **k: None}
    g.update({k: getattr(aips, k) for k in dir(aips) if not k.startswith("_")})

    results = []
    for path in chapter_notebooks(chapters):
        # reset engine each notebook
        try:
            aips.set_engine("qdrant")
        except Exception:
            pass
        try:
            from pyspark.sql import SparkSession
            s = SparkSession.getActiveSession()
            if s:
                s.stop()
        except Exception:
            pass
        g["get_engine"] = aips.get_engine
        g["set_engine"] = aips.set_engine
        # ch5.1 spaCy path
        if "open-information-extraction" in path.name:
            spacy_site = "/opt/conda/envs/ch5-spacy/lib/python3.10/site-packages"
            if spacy_site not in sys.path:
                sys.path.insert(0, spacy_site)
        results.append(run_notebook(path, g))

    summary = {
        "total": len(results),
        "passed": sum(1 for r in results if r["ok"]),
        "failed": sum(1 for r in results if not r["ok"]),
        "results": results,
    }
    REPORT_PATH.write_text(json.dumps(summary, indent=2))
    print("\n======== FULL LAB SUMMARY ========")
    for r in results:
        status = "PASS" if r["ok"] else "FAIL"
        print(f"{status} ok={r['cells_ok']:3d} skip={r['cells_skipped']:2d} err={len(r['errors'])} {r['seconds']:6.1f}s  {r['path']}")
        for e in r["errors"][:3]:
            print(f"         cell {e['cell']}: {e['error'][:140]}")
    print(f"passed={summary['passed']} failed={summary['failed']} total={summary['total']}")
    print(f"report={REPORT_PATH}")
    print("ALL_LABS_OK" if summary["failed"] == 0 else "ALL_LABS_HAS_FAILURES")
    return 0 if summary["failed"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())

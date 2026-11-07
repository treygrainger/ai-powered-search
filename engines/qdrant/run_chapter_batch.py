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


def setup_qdrant():
    import aips
    aips.set_engine("qdrant")
    eng = aips.get_engine()
    ok = eng.health_check(log=True, retries=5)
    if not ok:
        raise RuntimeError("Qdrant healthcheck failed")
    print("engine:", eng.name, "url:", getattr(eng, "base_url", "?"))
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


def main(argv):
    chapters = [int(x) for x in argv] if argv else [3, 4, 5]
    print("Batch chapters:", chapters)
    print("ROOT", ROOT)
    aips = setup_qdrant()

    # Shared globals for sequential chapter notebooks (state can carry datasets)
    g = {
        "__name__": "__main__",
        "display": lambda *a, **k: None,
    }
    # Seed common imports used after `from aips import *`
    g.update({k: getattr(aips, k) for k in dir(aips) if not k.startswith("_")})

    results = []
    for path in notebooks_for_chapters(chapters):
        # Fresh-ish but keep engine selection
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

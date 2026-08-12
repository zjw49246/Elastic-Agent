#!/usr/bin/env python3
"""Build a deployable static bundle for UI v2.

No Node toolchain: assets are plain ES modules.  This script only

* validates that every relative import in ``js/`` resolves to a real file,
* content-hashes JS/CSS into immutable filenames,
* rewrites ``index.html`` (and inter-module imports) to those names,
* emits ``asset-manifest.json`` (version, git commit, sha256 per file).

Usage::

    python scripts/build_ui_v2.py --out dist/ui-v2

The output directory is deploy-ready for Cloudflare Workers Static Assets and
is not committed to the repository.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
UI_ROOT = REPO_ROOT / "src" / "elastic_agent" / "api" / "ui_v2"

_IMPORT_RE = re.compile(
    r"""(?m)^\s*(?:import|export)[^'"]*from\s+['"](?P<path>[./][^'"]+)['"]"""
    r"""|import\(\s*['"](?P<dyn>[./][^'"]+)['"]\s*\)"""
)


def fail(message: str) -> None:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(1)


def iter_modules() -> list[Path]:
    return sorted((UI_ROOT / "js").rglob("*.js"))


def validate_imports() -> dict[Path, list[tuple[str, Path]]]:
    """Return {module: [(specifier, resolved_target)]}; fail on broken links."""
    graph: dict[Path, list[tuple[str, Path]]] = {}
    for module in iter_modules():
        source = module.read_text(encoding="utf-8")
        edges: list[tuple[str, Path]] = []
        for match in _IMPORT_RE.finditer(source):
            spec = match.group("path") or match.group("dyn")
            if spec is None:
                continue
            target = (module.parent / spec).resolve()
            if not target.is_file():
                fail(f"{module.relative_to(UI_ROOT)} imports missing module: {spec}")
            try:
                target.relative_to(UI_ROOT.resolve())
            except ValueError:
                fail(f"{module.relative_to(UI_ROOT)} imports outside ui_v2: {spec}")
            edges.append((spec, target))
        graph[module] = edges
    return graph


def content_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:12]


def git_commit() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT, capture_output=True, text=True, check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def build(out_dir: Path) -> dict:
    if not UI_ROOT.is_dir():
        fail(f"ui_v2 source directory not found: {UI_ROOT}")
    graph = validate_imports()

    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)

    # Hash leaf-first so an importer's hash reflects its dependencies' names.
    hashed: dict[Path, str] = {}  # source path -> hashed relative name

    def rewrite_and_hash(module: Path) -> str:
        if module in hashed:
            return hashed[module]
        source = module.read_text(encoding="utf-8")
        for spec, target in graph.get(module, []):
            hashed_target = rewrite_and_hash(target)
            target_rel = Path(hashed_target)
            new_spec = _relative_specifier(module, target, target_rel)
            source = source.replace(f"'{spec}'", f"'{new_spec}'")
            source = source.replace(f'"{spec}"', f'"{new_spec}"')
        data = source.encode("utf-8")
        rel = module.relative_to(UI_ROOT)
        digest = content_hash(data)
        hashed_rel = str(rel.with_name(f"{rel.stem}.{digest}{rel.suffix}"))
        (out_dir / hashed_rel).parent.mkdir(parents=True, exist_ok=True)
        (out_dir / hashed_rel).write_bytes(data)
        hashed[module] = hashed_rel
        return hashed_rel

    def _relative_specifier(importer: Path, target: Path, target_hashed_rel: Path) -> str:
        importer_out_dir = importer.relative_to(UI_ROOT).parent
        rel = Path(*([".."] * len(importer_out_dir.parts))) / target_hashed_rel \
            if importer_out_dir.parts else target_hashed_rel
        # Normalise to a ./-prefixed POSIX specifier.
        spec = Path(*rel.parts)
        text = spec.as_posix()
        if not text.startswith("."):
            text = f"./{text}"
        return text

    for module in iter_modules():
        rewrite_and_hash(module)

    # CSS + other assets.
    css_map: dict[str, str] = {}
    for asset in sorted((UI_ROOT / "assets").glob("*")):
        data = asset.read_bytes()
        digest = content_hash(data)
        rel = Path("assets") / f"{asset.stem}.{digest}{asset.suffix}"
        (out_dir / rel).parent.mkdir(parents=True, exist_ok=True)
        (out_dir / rel).write_bytes(data)
        css_map[f"assets/{asset.name}"] = str(rel)

    # index.html entry rewrite.
    index = (UI_ROOT / "index.html").read_text(encoding="utf-8")
    # The Manager serves source modules below a revisioned namespace so an
    # intermediary cannot reuse an older un-hashed module graph.  A CDN build
    # already content-hashes every file, so flatten that source-only prefix.
    index = re.sub(r"/ui-v2/rev/[A-Za-z0-9._-]+/", "/ui-v2/", index)
    entry = UI_ROOT / "js" / "app.js"
    index = index.replace("js/app.js", hashed[entry].replace("\\", "/"))
    for original, hashed_name in css_map.items():
        index = index.replace(original, hashed_name.replace("\\", "/"))
    (out_dir / "index.html").write_text(index, encoding="utf-8")

    manifest = {
        "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "git_commit": git_commit(),
        "entry": hashed[entry].replace("\\", "/"),
        "files": {
            str(src.relative_to(UI_ROOT)).replace("\\", "/"): {
                "output": out.replace("\\", "/"),
                "sha256": hashlib.sha256((out_dir / out).read_bytes()).hexdigest(),
            }
            for src, out in sorted(hashed.items())
        },
        "assets": css_map,
    }
    (out_dir / "asset-manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8",
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="dist/ui-v2", help="output directory")
    parser.add_argument("--check", action="store_true",
                        help="only validate imports, build nothing")
    args = parser.parse_args()

    if args.check:
        validate_imports()
        print("ui_v2 imports OK")
        return

    manifest = build(Path(args.out).resolve())
    print(f"built ui_v2 → {args.out} (entry {manifest['entry']}, "
          f"{len(manifest['files'])} modules, commit {manifest['git_commit'][:12]})")


if __name__ == "__main__":
    main()

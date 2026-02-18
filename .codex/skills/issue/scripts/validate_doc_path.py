#!/usr/bin/env python3
import argparse
import json
import re
import sys
from pathlib import Path

TASK_RE = re.compile(r"^docs/tasks/(?P<id>\d{6})-[^/]+\.md$")
RFC_RE = re.compile(r"^docs/rfcs/(?P<id>\d{4})-[^/]+\.md$")


def title_hint(path: Path) -> str | None:
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith("# "):
                return line[2:].strip() or None
    except Exception:
        return None
    return None


def normalize(path: Path) -> str:
    return path.as_posix().lstrip("./")


def validate(path: Path) -> dict:
    rel = normalize(path)
    if not path.exists():
        return {"valid": False, "error": f"path not found: {rel}"}
    if not path.is_file():
        return {"valid": False, "error": f"path is not a file: {rel}"}

    task_match = TASK_RE.match(rel)
    if task_match:
        return {
            "valid": True,
            "path": rel,
            "doc_type": "task",
            "doc_id": task_match.group("id"),
            "title_hint": title_hint(path),
        }
    rfc_match = RFC_RE.match(rel)
    if rfc_match:
        return {
            "valid": True,
            "path": rel,
            "doc_type": "rfc",
            "doc_id": rfc_match.group("id"),
            "title_hint": title_hint(path),
        }

    return {
        "valid": False,
        "path": rel,
        "error": "invalid path pattern; expected docs/tasks/<6 digits>-<slug>.md or docs/rfcs/<4 digits>-<slug>.md",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate doc-first path for issue creation.")
    parser.add_argument("--path", required=True, help="Path to task/rfc markdown doc.")
    args = parser.parse_args()

    res = validate(Path(args.path))
    print(json.dumps(res, ensure_ascii=True))
    return 0 if res.get("valid") else 1


if __name__ == "__main__":
    sys.exit(main())

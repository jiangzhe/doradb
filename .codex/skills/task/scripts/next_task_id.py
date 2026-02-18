#!/usr/bin/env python3
import argparse
import re
import sys
from pathlib import Path

TASK_FILE_RE = re.compile(r"^(?P<id>\d+)-[^/]+\.md$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print the next task id based on docs/tasks/<id>-<slug>.md files."
    )
    parser.add_argument(
        "--dir",
        default="docs/tasks",
        help="Task directory to scan. Default: docs/tasks",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=6,
        help="ID width for zero padding. Default: 6",
    )
    return parser.parse_args()


def next_task_id(task_dir: Path, width: int) -> str:
    if width <= 0:
        raise ValueError("width must be a positive integer")
    if not task_dir.exists():
        raise FileNotFoundError(f"task directory not found: {task_dir.as_posix()}")
    if not task_dir.is_dir():
        raise NotADirectoryError(f"not a directory: {task_dir.as_posix()}")

    max_id = 0
    for entry in task_dir.iterdir():
        if not entry.is_file():
            continue
        match = TASK_FILE_RE.match(entry.name)
        if not match:
            continue
        value = int(match.group("id"))
        if value > max_id:
            max_id = value

    next_id = max_id + 1
    text = str(next_id)
    if len(text) > width:
        raise ValueError(f"next id {next_id} does not fit width {width}")
    return text.zfill(width)


def main() -> int:
    args = parse_args()
    try:
        result = next_task_id(Path(args.dir), args.width)
    except Exception as exc:  # pragma: no cover - terminal-facing error path
        print(str(exc), file=sys.stderr)
        return 1
    print(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())

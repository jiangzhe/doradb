#!/usr/bin/env python3
import argparse
import re
import sys
from pathlib import Path

TASK_NAME_RE = re.compile(r"^(?P<id>\d{6})-(?P<slug>[a-z0-9]+(?:-[a-z0-9]+)*)\.md$")
TITLE_LINE_RE = re.compile(r"^# Task:.*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a docs/tasks task document from template with validated id and slug."
    )
    parser.add_argument("--title", required=True, help="Task title without '# Task:' prefix.")
    parser.add_argument(
        "--slug",
        required=True,
        help="Kebab-case slug used in output filename (e.g. add-task-skill).",
    )
    id_group = parser.add_mutually_exclusive_group(required=True)
    id_group.add_argument("--id", dest="task_id", help="Six-digit id (e.g. 000023).")
    id_group.add_argument(
        "--auto-id",
        action="store_true",
        help="Compute the next id from --output-dir.",
    )
    parser.add_argument(
        "--template",
        default="docs/tasks/000000-template.md",
        help="Template file path. Default: docs/tasks/000000-template.md",
    )
    parser.add_argument(
        "--output-dir",
        default="docs/tasks",
        help="Output directory for task docs. Default: docs/tasks",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing file if present.",
    )
    return parser.parse_args()


def validate_slug(slug: str) -> str:
    if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", slug):
        raise ValueError("slug must be kebab-case: [a-z0-9]+(?:-[a-z0-9]+)*")
    return slug


def validate_id(task_id: str) -> str:
    if not re.fullmatch(r"\d{6}", task_id):
        raise ValueError("id must be exactly 6 digits")
    return task_id


def detect_next_id(output_dir: Path) -> str:
    max_value = 0
    if not output_dir.exists():
        raise FileNotFoundError(f"output directory not found: {output_dir.as_posix()}")
    if not output_dir.is_dir():
        raise NotADirectoryError(f"not a directory: {output_dir.as_posix()}")
    for entry in output_dir.iterdir():
        if not entry.is_file():
            continue
        match = TASK_NAME_RE.match(entry.name)
        if not match:
            continue
        value = int(match.group("id"))
        if value > max_value:
            max_value = value
    return str(max_value + 1).zfill(6)


def load_template(template_path: Path) -> str:
    if not template_path.exists():
        raise FileNotFoundError(f"template not found: {template_path.as_posix()}")
    if not template_path.is_file():
        raise FileNotFoundError(f"template is not a file: {template_path.as_posix()}")
    return template_path.read_text(encoding="utf-8")


def apply_title(template_text: str, title: str) -> str:
    clean_title = title.strip()
    if not clean_title:
        raise ValueError("title must not be empty")
    heading = f"# Task: {clean_title}"
    lines = template_text.splitlines()
    if lines and TITLE_LINE_RE.match(lines[0]):
        lines[0] = heading
        return "\n".join(lines) + ("\n" if template_text.endswith("\n") else "")
    return f"{heading}\n\n{template_text}"


def main() -> int:
    args = parse_args()
    try:
        output_dir = Path(args.output_dir)
        template_path = Path(args.template)
        slug = validate_slug(args.slug)
        task_id = detect_next_id(output_dir) if args.auto_id else validate_id(args.task_id)
        content = apply_title(load_template(template_path), args.title)

        out_path = output_dir / f"{task_id}-{slug}.md"
        if out_path.exists() and not args.force:
            raise FileExistsError(
                f"output file already exists: {out_path.as_posix()} (use --force to overwrite)"
            )
        out_path.write_text(content, encoding="utf-8")
        print(out_path.as_posix())
        return 0
    except Exception as exc:  # pragma: no cover - terminal-facing error path
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

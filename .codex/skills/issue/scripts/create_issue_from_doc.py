#!/usr/bin/env python3
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

from validate_doc_path import validate


def split_labels(csv: str) -> list[str]:
    return [x.strip() for x in csv.split(",") if x.strip()]


def normalize_labels(labels: list[str]) -> tuple[list[str], bool, str | None]:
    has_type = any(label.startswith("type:") for label in labels)
    has_priority = any(label.startswith("priority:") for label in labels)
    if not has_type:
        return labels, False, "labels must include at least one type:* label"
    if not has_priority:
        labels = [*labels, "priority:medium"]
    return labels, True, None


def build_body(doc_path: str, doc_type: str, doc_id: str, doc_title: str | None, parent: int | None) -> str:
    lines = [
        "Planning document:",
        f"- `{doc_path}`",
        f"- Type: {doc_type}",
        f"- ID: {doc_id}",
    ]
    if doc_title:
        lines.append(f"- Title: {doc_title}")
    lines.append("")
    lines.append("Generated from document-first issue workflow.")
    if parent is not None:
        lines.append(f"Part of #{parent}")
    return "\n".join(lines).strip() + "\n"


def parse_issue_number(issue_url_or_text: str) -> int | None:
    m = re.search(r"/issues/(\d+)\s*$", issue_url_or_text.strip())
    if m:
        return int(m.group(1))
    m = re.search(r"#(\d+)\s*$", issue_url_or_text.strip())
    if m:
        return int(m.group(1))
    return None


def run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False, text=True, capture_output=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create GitHub issue from docs/tasks or docs/rfcs.")
    parser.add_argument("--doc", required=True, help="Path to planning doc.")
    parser.add_argument("--title", help="Issue title override.")
    parser.add_argument("--labels", required=True, help="Comma-separated labels.")
    parser.add_argument("--assignee", default="@me", help="Assignee to add after creation.")
    parser.add_argument("--parent", type=int, help="Parent epic issue number.")
    args = parser.parse_args()

    doc = Path(args.doc)
    validated = validate(doc)
    if not validated.get("valid"):
        print(json.dumps(validated, ensure_ascii=True))
        return 1

    labels = split_labels(args.labels)
    labels, ok, err = normalize_labels(labels)
    if not ok:
        print(json.dumps({"created": False, "error": err}, ensure_ascii=True))
        return 1

    title = args.title or validated.get("title_hint") or f"{validated['doc_type'].upper()} {validated['doc_id']}"
    body = build_body(
        validated["path"],
        validated["doc_type"],
        validated["doc_id"],
        validated.get("title_hint"),
        args.parent,
    )

    temp_path = None
    try:
        fd, temp_path = tempfile.mkstemp(prefix="issue-body-", suffix=".md")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(body)

        create_cmd = [
            "gh",
            "issue",
            "create",
            "--title",
            title,
            "--body-file",
            temp_path,
            "--label",
            ",".join(labels),
        ]
        create_res = run(create_cmd)
        if create_res.returncode != 0:
            print(
                json.dumps(
                    {
                        "created": False,
                        "command": create_cmd,
                        "stderr": create_res.stderr.strip(),
                        "stdout": create_res.stdout.strip(),
                    },
                    ensure_ascii=True,
                )
            )
            return create_res.returncode

        issue_url = create_res.stdout.strip().splitlines()[-1] if create_res.stdout.strip() else ""
        issue_no = parse_issue_number(issue_url)
        if issue_no is None:
            print(
                json.dumps(
                    {
                        "created": False,
                        "error": "failed to parse issue number from gh output",
                        "stdout": create_res.stdout.strip(),
                    },
                    ensure_ascii=True,
                )
            )
            return 1

        if args.assignee:
            edit_cmd = [
                "gh",
                "issue",
                "edit",
                str(issue_no),
                "--add-assignee",
                args.assignee,
            ]
            edit_res = run(edit_cmd)
            if edit_res.returncode != 0:
                print(
                    json.dumps(
                        {
                            "created": True,
                            "issue_number": issue_no,
                            "issue_url": issue_url,
                            "assigned": False,
                            "assign_stderr": edit_res.stderr.strip(),
                            "assign_stdout": edit_res.stdout.strip(),
                        },
                        ensure_ascii=True,
                    )
                )
                return edit_res.returncode

        print(
            json.dumps(
                {
                    "created": True,
                    "issue_number": issue_no,
                    "issue_url": issue_url,
                    "doc": validated["path"],
                    "labels": labels,
                    "assignee": args.assignee,
                    "parent": args.parent,
                },
                ensure_ascii=True,
            )
        )
        return 0
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)


if __name__ == "__main__":
    sys.exit(main())

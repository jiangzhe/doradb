#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False, text=True, capture_output=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Update GitHub issue metadata and add comments.")
    parser.add_argument("--issue", type=int, required=True, help="Issue number.")
    parser.add_argument("--add-label", help="Comma-separated labels to add.")
    parser.add_argument("--remove-label", help="Comma-separated labels to remove.")
    parser.add_argument("--add-assignee", help='Assignee to add, e.g. "@me".')
    parser.add_argument("--remove-assignee", help="Assignee to remove.")
    parser.add_argument("--body", help="Body replacement text.")
    parser.add_argument("--body-file", help="Path to file with body replacement text.")
    parser.add_argument("--comment", help="Comment text to append.")
    args = parser.parse_args()

    if args.body and args.body_file:
        print(json.dumps({"ok": False, "error": "use either --body or --body-file, not both"}, ensure_ascii=True))
        return 1

    commands: list[list[str]] = []
    edit_cmd = ["gh", "issue", "edit", str(args.issue)]
    has_edit = False
    if args.add_label:
        edit_cmd.extend(["--add-label", args.add_label])
        has_edit = True
    if args.remove_label:
        edit_cmd.extend(["--remove-label", args.remove_label])
        has_edit = True
    if args.add_assignee:
        edit_cmd.extend(["--add-assignee", args.add_assignee])
        has_edit = True
    if args.remove_assignee:
        edit_cmd.extend(["--remove-assignee", args.remove_assignee])
        has_edit = True
    if args.body:
        edit_cmd.extend(["--body", args.body])
        has_edit = True
    if args.body_file:
        body_path = Path(args.body_file)
        if not body_path.exists() or not body_path.is_file():
            print(json.dumps({"ok": False, "error": f"body file not found: {args.body_file}"}, ensure_ascii=True))
            return 1
        edit_cmd.extend(["--body-file", str(body_path)])
        has_edit = True
    if has_edit:
        commands.append(edit_cmd)

    if args.comment:
        commands.append(["gh", "issue", "comment", str(args.issue), "--body", args.comment])

    if not commands:
        print(json.dumps({"ok": False, "error": "no operation requested"}, ensure_ascii=True))
        return 1

    executed = []
    for cmd in commands:
        res = run(cmd)
        executed.append(
            {
                "command": cmd,
                "returncode": res.returncode,
                "stdout": res.stdout.strip(),
                "stderr": res.stderr.strip(),
            }
        )
        if res.returncode != 0:
            print(json.dumps({"ok": False, "results": executed}, ensure_ascii=True))
            return res.returncode

    print(json.dumps({"ok": True, "issue_number": args.issue, "results": executed}, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

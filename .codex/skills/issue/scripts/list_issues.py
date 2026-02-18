#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys


def run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False, text=True, capture_output=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="List GitHub issues with JSON output.")
    parser.add_argument("--state", default="open", choices=["open", "closed", "all"])
    parser.add_argument("--assignee", help='Assignee filter, e.g. "@me".')
    parser.add_argument("--search", help="Search query for -S.")
    parser.add_argument("--label", action="append", default=[], help="Repeatable label filter.")
    parser.add_argument("--limit", type=int, default=100, help="Max number of issues.")
    args = parser.parse_args()

    cmd = [
        "gh",
        "issue",
        "list",
        "--state",
        args.state,
        "--limit",
        str(args.limit),
        "--json",
        "number,title,state,labels,assignees,url",
    ]
    if args.assignee:
        cmd.extend(["--assignee", args.assignee])
    if args.search:
        cmd.extend(["-S", args.search])
    for label in args.label:
        cmd.extend(["--label", label])

    res = run(cmd)
    if res.returncode != 0:
        print(
            json.dumps(
                {
                    "ok": False,
                    "command": cmd,
                    "stderr": res.stderr.strip(),
                    "stdout": res.stdout.strip(),
                },
                ensure_ascii=True,
            )
        )
        return res.returncode

    parsed = json.loads(res.stdout or "[]")
    print(json.dumps({"ok": True, "count": len(parsed), "issues": parsed}, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

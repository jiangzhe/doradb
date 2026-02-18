#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Close a GitHub issue with a comment.")
    parser.add_argument("--issue", type=int, required=True, help="Issue number.")
    parser.add_argument("--comment", required=True, help="Close comment.")
    args = parser.parse_args()

    cmd = ["gh", "issue", "close", str(args.issue), "--comment", args.comment]
    res = subprocess.run(cmd, check=False, text=True, capture_output=True)
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

    print(
        json.dumps(
            {
                "ok": True,
                "issue_number": args.issue,
                "stdout": res.stdout.strip(),
            },
            ensure_ascii=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

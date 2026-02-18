#!/usr/bin/env python3
import argparse
import json
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Print PR-body guidance to close an issue.")
    parser.add_argument("--issue", type=int, required=True, help="Issue number.")
    args = parser.parse_args()

    issue = args.issue
    fix = f"Fixes #{issue}"
    close = f"Closes #{issue}"
    print(
        json.dumps(
            {
                "issue_number": issue,
                "recommended": fix,
                "alternatives": [close],
                "examples": {
                    "pr_body_line": fix,
                    "completion_comment": f"Completed via PR #<number>. {close}",
                },
            },
            ensure_ascii=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

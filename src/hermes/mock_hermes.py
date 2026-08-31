from __future__ import annotations

import argparse
import sys
import time


def main() -> None:
    parser = argparse.ArgumentParser(description="Mock hermes chat command for tests")
    parser.add_argument("-q", "--query", required=True)
    parser.add_argument("--delay", type=float, default=0.0)
    parser.add_argument("--fail", action="store_true")
    arguments = parser.parse_args()
    if arguments.delay:
        time.sleep(arguments.delay)
    if arguments.fail:
        print("mock hermes failure", file=sys.stderr)
        raise SystemExit(1)
    print(f"mock-hermes: {arguments.query}")


if __name__ == "__main__":
    main()

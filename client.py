import argparse
import json
import sys
from typing import Optional

import requests


def stream_runs(prompt: str, url: str) -> int:
    try:
        response = requests.post(url, json={"prompt": prompt}, stream=True)
    except requests.RequestException as exc:
        print(f"Request failed: {exc}", file=sys.stderr)
        return 1

    if response.status_code != 200:
        try:
            payload = response.json()
        except ValueError:
            payload = response.text
        print(f"Server responded with status {response.status_code}: {payload}", file=sys.stderr)
        return 1

    print("Streaming events (Ctrl+C to stop):")

    for raw_line in response.iter_lines(decode_unicode=True):
        if raw_line is None:
            continue

        if raw_line == "":
            continue

        if raw_line.startswith(":"):
            print("[heartbeat]")
            continue

        if not raw_line.startswith("data:"):
            print(f"[unknown] {raw_line}")
            continue

        data = raw_line[len("data:") :].strip()

        if not data:
            continue

        try:
            parsed = json.loads(data)
            print(json.dumps(parsed, indent=2))
        except json.JSONDecodeError:
            # Print raw payload if it isn't JSON.
            print(data)

    return 0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send a prompt to the /runs endpoint and stream SSE responses.")
    parser.add_argument(
        "prompt",
        help="Prompt text to send to the Claude backend.",
    )
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8000/runs",
        help="Target endpoint URL (default: %(default)s).",
    )
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()
    return stream_runs(args.prompt, args.url)


if __name__ == "__main__":
    raise SystemExit(main())

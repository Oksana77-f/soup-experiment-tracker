import argparse
import json
import math
import os
import random
import time
from uuid import uuid4

import httpx


RETRYABLE = {408, 429, 500, 502, 503, 504}
MAX_ATTEMPTS = 10


def retry_delay(attempt):
    return min(0.2 * 2**attempt, 8) + random.uniform(0, 0.1)


def post_batch(client, run_id, body, attempts=MAX_ATTEMPTS, pause=time.sleep):
    for attempt in range(attempts):
        try:
            response = client.post(f"/runs/{run_id}/metrics", content=body)
        except httpx.TransportError:
            if attempt + 1 == attempts:
                raise
        else:
            if response.status_code not in RETRYABLE or attempt + 1 == attempts:
                response.raise_for_status()
                return response.json()
        pause(retry_delay(attempt))


def main():
    parser = argparse.ArgumentParser(description="Send synthetic metric batches to a tracker run.")
    parser.add_argument("run_id")
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--steps", type=int, default=30)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=3, choices=range(1, 257), metavar="1..256")
    parser.add_argument("--interval", type=float, default=0.7)
    args = parser.parse_args()
    if args.steps < 1 or args.start < 0 or args.interval < 0:
        parser.error("steps must be positive, start and interval must be nonnegative")
    token = os.environ.get("SOUP_WRITE_KEY")
    if not token:
        parser.error("Set SOUP_WRITE_KEY in your environment.")
    with httpx.Client(base_url=args.url, headers={"Authorization": f"Bearer {token}",
                                                 "Content-Type": "application/json"}, timeout=5) as client:
        end = args.start + args.steps
        for first in range(args.start, end, args.batch_size):
            points = [{"name": "loss", "step": step, "value": round(math.exp(-step / 50), 6)}
                      for step in range(first, min(first + args.batch_size, end))]
            body = json.dumps({"batch_id": str(uuid4()), "points": points}).encode()
            try:
                print(json.dumps(post_batch(client, args.run_id, body)), flush=True)
            except (httpx.HTTPError, ValueError) as exc:
                raise SystemExit(str(exc)) from None
            if first + args.batch_size < end:
                time.sleep(args.interval)


if __name__ == "__main__":
    main()

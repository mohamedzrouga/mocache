"""Fill the MinIO bucket with random objects for the lab.

    docker compose run --rm seed
    OBJECT_COUNT=20000 FORCE=1 docker compose run --rm seed

Writes `obj/000000.bin` … and a `_manifest.json` the API serves from /keys, so
the load generator never has to page a bucket listing. Sizes are drawn from a
fixed seed: two runs with the same SEED produce byte-identical objects, which
keeps benchmark numbers comparable across runs.
"""

from __future__ import annotations

import json
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:9000")
BUCKET = os.environ.get("S3_BUCKET", "lab")
COUNT = int(os.environ.get("OBJECT_COUNT", "2000"))
MIN_BYTES = int(os.environ.get("OBJECT_MIN_BYTES", str(4 * 1024)))
MAX_BYTES = int(os.environ.get("OBJECT_MAX_BYTES", str(64 * 1024)))
SEED = int(os.environ.get("SEED", "1"))
FORCE = os.environ.get("FORCE", "0") not in ("0", "", "false", "no")
WORKERS = int(os.environ.get("SEED_WORKERS", "16"))


def main() -> int:
    if MIN_BYTES > MAX_BYTES:
        print(f"OBJECT_MIN_BYTES ({MIN_BYTES}) > OBJECT_MAX_BYTES ({MAX_BYTES})", file=sys.stderr)
        return 2

    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 5, "mode": "standard"},
            max_pool_connections=WORKERS * 2,
        ),
    )
    wait_for_s3(s3)

    if not bucket_exists(s3, BUCKET):
        s3.create_bucket(Bucket=BUCKET)
        print(f"created bucket {BUCKET}")

    if not FORCE and manifest_matches(s3):
        print(f"bucket {BUCKET} already holds {COUNT} objects for seed={SEED}; FORCE=1 to rewrite")
        return 0

    rnd = random.Random(SEED)
    plan = [(f"obj/{i:06d}.bin", rnd.randint(MIN_BYTES, MAX_BYTES)) for i in range(COUNT)]

    start = time.perf_counter()
    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for _ in pool.map(lambda kv: put(s3, *kv), plan):
            done += 1
            if done % 500 == 0 or done == COUNT:
                print(f"  uploaded {done}/{COUNT}", flush=True)
    elapsed = time.perf_counter() - start

    total = sum(size for _, size in plan)
    manifest = {
        "seed": SEED,
        "count": COUNT,
        "min_bytes": MIN_BYTES,
        "max_bytes": MAX_BYTES,
        "total_bytes": total,
        "objects": [{"key": k, "size": n} for k, n in plan],
    }
    s3.put_object(
        Bucket=BUCKET,
        Key="_manifest.json",
        Body=json.dumps(manifest).encode(),
        ContentType="application/json",
    )
    print(
        f"seeded {COUNT} objects, {total / 1048576:.1f} MiB total, "
        f"avg {total / COUNT / 1024:.1f} KiB, in {elapsed:.1f}s"
    )
    return 0


def put(s3, key: str, size: int) -> None:
    # Per-key seed: the payload depends only on (SEED, key), not on upload order.
    body = random.Random(f"{SEED}:{key}").randbytes(size)
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/octet-stream")


def bucket_exists(s3, bucket: str) -> bool:
    try:
        s3.head_bucket(Bucket=bucket)
        return True
    except ClientError:
        return False


def manifest_matches(s3) -> bool:
    try:
        m = json.loads(s3.get_object(Bucket=BUCKET, Key="_manifest.json")["Body"].read())
    except (ClientError, ValueError):
        return False
    return m.get("count") == COUNT and m.get("seed") == SEED


def wait_for_s3(s3, attempts: int = 60) -> None:
    for i in range(attempts):
        try:
            s3.list_buckets()
            return
        except (ClientError, BotoCoreError):
            if i == 0:
                print(f"waiting for {ENDPOINT} ...", flush=True)
            time.sleep(min(1.0 + i * 0.1, 3.0))
    raise SystemExit(f"S3 endpoint {ENDPOINT} never became reachable")


if __name__ == "__main__":
    raise SystemExit(main())

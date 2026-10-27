"""Small, real S3/PG demo; it does NOT implement Spring Batch or real exposure rules."""
import csv
import hashlib
import io
import json
import os
import sys
from decimal import Decimal

import boto3
from botocore.config import Config
import psycopg2


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def checksum(raw):
    return hashlib.sha256(raw).hexdigest()


def s3_client():
    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))


def get(s3, bucket, key):
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def put(s3, bucket, key, raw):
    s3.put_object(Bucket=bucket, Key=key, Body=raw)


def seed(s3, prefix):
    existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    for bucket in ("eod-input", "eod-output"):
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)
    raw = b"trade_id,tree_id,amount\nT001,TREE-A,100.00\nT002,TREE-A,250.00\nT003,TREE-B,50.00\n"
    key = f"{prefix}/trades.csv"
    put(s3, "eod-input", key, raw)
    put(s3, "eod-input", f"{prefix}/manifest.json", encode({
        "business_date": os.environ["BUSINESS_DATE"],
        "input_version": os.environ["INPUT_VERSION"],
        "files": [{"key": key, "sha256": checksum(raw), "rows": 3}],
    }))
    print(json.dumps({"event": "manifest_published", "prefix": prefix}), flush=True)


def process(stage, s3, prefix, config):
    out = f"{prefix}/{config['version']}"
    if stage == "ingest":
        manifest = json.loads(get(s3, "eod-input", f"{prefix}/manifest.json"))
        if (manifest["business_date"] != os.environ["BUSINESS_DATE"] or
                manifest["input_version"] != os.environ["INPUT_VERSION"]):
            raise ValueError("Manifest identity mismatch")
        rows = []
        for file in manifest["files"]:
            raw = get(s3, "eod-input", file["key"])
            if checksum(raw) != file["sha256"]:
                raise ValueError("Input checksum mismatch")
            items = list(csv.DictReader(io.StringIO(raw.decode())))
            if len(items) != file["rows"]:
                raise ValueError("Input row count mismatch")
            rows.extend(items)
        if len({r["trade_id"] for r in rows}) != len(rows):
            raise ValueError("Duplicate trade identity")
        return f"{out}/ingest.json", {"rows": rows, "count": len(rows)}
    if stage == "tree":
        source = json.loads(get(s3, "eod-output", f"{out}/ingest.json"))
        totals = {}
        for row in source["rows"]:
            totals[row["tree_id"]] = totals.get(row["tree_id"], Decimal(0)) + Decimal(row["amount"])
        return f"{out}/tree.json", {"count": source["count"], "totals": {k: str(v) for k, v in totals.items()}}
    if stage == "exposure":
        source = json.loads(get(s3, "eod-output", f"{out}/tree.json"))
        multiplier = Decimal(config["exposure_multiplier"])
        values = {k: str((Decimal(v) * multiplier).quantize(Decimal("0.01"))) for k, v in source["totals"].items()}
        return f"{out}/exposure.json", {"count": source["count"], "exposures": values}
    if stage == "reconcile":
        source = json.loads(get(s3, "eod-output", f"{out}/exposure.json"))
        # Expected values belong to this deterministic demo fixture only.
        expected = {"TREE-A": "385.00", "TREE-B": "55.00"}
        if source["count"] != 3 or source["exposures"] != expected:
            raise ValueError(f"Demo reconciliation failed: {source}")
        return f"{out}/published.json", {"status": "PUBLISHED", "config_version": config["version"], "result": source}
    raise ValueError(f"Unknown stage {stage}")


def main():
    stage = sys.argv[1]
    prefix = f"{os.environ['BUSINESS_DATE']}/{os.environ['INPUT_VERSION']}"
    s3 = s3_client()
    if stage == "seed":
        seed(s3, prefix)
        return
    with open("/opt/eod-config/config.json", encoding="utf-8") as stream:
        config = json.load(stream)
    if config["version"] != os.environ["CONFIG_VERSION"]:
        raise ValueError("Config version mismatch")
    # One local PG instance for convenience; business rows use a separate schema.
    # Production must use separate users/permissions and actual Batch repositories.
    with psycopg2.connect("") as connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS eod_demo")
            cursor.execute("""CREATE TABLE IF NOT EXISTS eod_demo.stage_results (
                business_date date NOT NULL, input_version text NOT NULL,
                config_version text NOT NULL, stage text NOT NULL,
                output_key text NOT NULL, output_sha256 text NOT NULL,
                airflow_run_id text NOT NULL, completed_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (business_date, input_version, config_version, stage))""")
            # Serialise the same business identity; retries reuse the same output keys.
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{prefix}/{config['version']}/{stage}",))
            key, result = process(stage, s3, prefix, config)
            raw = encode(result)
            digest = checksum(raw)
            cursor.execute("""SELECT output_sha256 FROM eod_demo.stage_results
                WHERE business_date=%s AND input_version=%s AND config_version=%s AND stage=%s""",
                (os.environ["BUSINESS_DATE"], os.environ["INPUT_VERSION"], config["version"], stage))
            previous = cursor.fetchone()
            if previous and previous[0] != digest:
                raise ValueError("Immutable input/config identity produced different output")
            put(s3, "eod-output", key, raw)
            cursor.execute("""INSERT INTO eod_demo.stage_results
                (business_date,input_version,config_version,stage,output_key,output_sha256,airflow_run_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (business_date,input_version,config_version,stage) DO NOTHING""",
                (os.environ["BUSINESS_DATE"], os.environ["INPUT_VERSION"], config["version"], stage,
                 key, digest, os.environ.get("AIRFLOW_RUN_ID", "manual")))
    print(json.dumps({"event": "stage_completed", "stage": stage, "prefix": prefix,
                      "output": key, "sha256": digest, "result": result}), flush=True)


if __name__ == "__main__":
    main()

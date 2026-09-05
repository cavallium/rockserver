#!/usr/bin/env python3
"""Small-only, same-level compaction example. Defaults to a server-validated dry run.

Requires grpcurl; never uses a shell, retries uncertain RPCs, or changes DB options.
The byte budget is preflight-only: native compaction can expand its input set.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys

SERVICE = "it.cavallium.rockserver.core.common.api.proto.RocksDBService"


def choose(files, small_bytes, input_bytes, max_files):
    """Choose the best bounded contiguous run; never skip an intervening file."""
    files = sorted(files, key=lambda f: (f.get("smallestKeyHex", ""), f["name"]))
    best = None
    for start, first in enumerate(files):
        if start and files[start-1].get("largestKeyHex", "") >= first.get("smallestKeyHex", ""):
            continue
        batch, size = [], 0
        for index in range(start, min(len(files), start + max_files)):
            f = files[index]
            n = int(f.get("sizeBytes", 0))
            if f.get("beingCompacted", False) or n >= small_bytes or size + n > input_bytes:
                break
            batch.append(f)
            size += n
            clean_end = index+1 == len(files) or f.get("largestKeyHex", "") < files[index+1].get("smallestKeyHex", "")
            if len(batch) >= 2 and clean_end:
                score = ((len(batch)-1)/max(1,size), len(batch))
                if best is None or score > best[0]:
                    best = (score, start, index+1)
    return files[best[1]:best[2]] if best else []


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("address", help="Rockserver gRPC host:port")
    p.add_argument("column_id", type=int)
    p.add_argument("--level", type=int, choices=(5, 6), required=True)
    p.add_argument("--output-path-id", type=int, required=True, help="Use the ID from live metadata; capacity is often 1 on tiered v2")
    p.add_argument("--small-bytes", type=int, default=100_000_000)
    p.add_argument("--input-bytes", type=int, default=1 << 30)
    p.add_argument("--output-bytes", type=int, default=1 << 30)
    p.add_argument("--max-files", type=int, default=64)
    p.add_argument("--max-jobs", type=int, default=1)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--plaintext", action="store_true", help="Only for an endpoint configured without TLS")
    p.add_argument("--proto-dir", type=Path, default=Path(__file__).resolve().parents[1] / "src/main/proto")
    a = p.parse_args()
    if not 2 <= a.max_files <= 256 or min(a.small_bytes, a.input_bytes, a.output_bytes, a.max_jobs) <= 0:
        p.error("positive sizes/job count and 2..256 max-files are required")

    def rpc(method, request):
        command = ["grpcurl", "-max-msg-sz", str(64 << 20), "-import-path", str(a.proto_dir), "-proto", "rocksdb.proto"]
        if a.plaintext:
            command.append("-plaintext")
        command += ["-d", "@", a.address, SERVICE + "/" + method]
        # Do not automatically retry: a failed response can hide completed native work.
        result = subprocess.run(command, input=json.dumps(request), text=True, capture_output=True)
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or "RPC failed; outcome may be unknown")
        return json.loads(result.stdout)

    for _ in range(a.max_jobs if a.execute else 1):
        m = rpc("getSstMetadata", {"columnId": str(a.column_id), "level": a.level,
                                  "context": {"profile": "BATCH", "workloadContractVersion": 3, "timeoutNanos": "30000000000"}})
        paths = m.get("paths", [])
        if not 0 <= a.output_path_id < len(paths):
            p.error(f"output path is invalid; current mapping: {paths}")
        batch = choose(m.get("files", []), a.small_bytes, a.input_bytes, a.max_files)
        if not batch:
            print(json.dumps({"message": "No eligible adjacent small-file group", "level": a.level}))
            return
        request = {"workloadContractVersion": 3, "columnId": str(a.column_id), "session": m["session"],
                   "files": [f["name"] for f in batch], "level": a.level, "outputPathId": a.output_path_id,
                   "outputFileSizeLimit": str(a.output_bytes), "maxInputBytes": str(a.input_bytes),
                   "maxSubcompactions": 1, "execute": False}
        plan = rpc("compactFiles", request)
        print(json.dumps({"plan": plan, "outputPath": paths[a.output_path_id],
                          "limitScope": "observed inputs only; native expansion remains possible"}), flush=True)
        if a.execute:
            request["execute"] = True
            print(json.dumps({"result": rpc("compactFiles", request)}), flush=True)
            # Refresh on the next iteration: never reuse the previous SST inventory.


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, ValueError) as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

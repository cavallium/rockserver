#!/usr/bin/env python3
"""Run the Java/native cache fixture in fresh processes and retain its evidence."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("library", type=Path)
    parser.add_argument("output", type=Path, help="New directory for isolated DB, snapshots and reports")
    parser.add_argument("--max-bytes", type=int, default=64 * 1024 * 1024)
    args = parser.parse_args()
    library = args.library.resolve(strict=True)
    output = args.output.resolve()
    if args.max_bytes <= 0:
        parser.error("--max-bytes must be positive")
    if args.max_bytes > 64 * 1024 * 1024:
        parser.error("This corruption fixture is limited to 64 MiB snapshots")
    output.mkdir(parents=True, exist_ok=False)
    classes = output / "classes"
    classes.mkdir()
    source = Path(__file__).parent / "java" / "HotCacheExperiment.java"
    subprocess.run(["javac", "--release", "25", "-d", str(classes), str(source)], check=True, timeout=60)
    db = output / "db"
    snapshot = output / "snapshot.bin"
    reports = []

    def run(label, mode, path=snapshot):
        command = ["java", "--enable-native-access=ALL-UNNAMED", "-cp", str(classes),
                   "HotCacheExperiment", str(library), str(db), str(path), mode, str(args.max_bytes)]
        completed = subprocess.run(command, capture_output=True, text=True, timeout=60)
        (output / f"{label}.stdout").write_text(completed.stdout)
        (output / f"{label}.stderr").write_text(completed.stderr)
        (output / f"{label}.exit").write_text(str(completed.returncode) + "\n")
        if completed.returncode:
            raise RuntimeError(f"{label} failed; see {output / (label + '.stderr')}")
        # The exact native library can print initialization diagnostics to
        # stdout at process exit. Preserve them, and require one JSON report.
        candidates = [line for line in completed.stdout.splitlines() if line.startswith("{")]
        if len(candidates) != 1:
            raise RuntimeError(f"{label} emitted {len(candidates)} JSON reports")
        report = json.loads(candidates[0])
        (output / f"{label}.json").write_text(json.dumps(report, indent=2) + "\n")
        reports.append({"label": label, "report": report})
        return report

    def require(condition, message):
        if not condition:
            raise AssertionError(message)

    def require_hits(report):
        require(report["snapshot_rejected"] is False and report["snapshot_entries"] > 0,
                "Snapshot was not activated")
        require(report["data_hits"] > 0, "No restored DATA hits")
        require(report["metadata_hits"] > 0, "No restored METADATA hits")
        require(report["reads"] > 0, "No verified DB reads")

    capture = run("01-capture", "capture")
    require(capture["captured_data"] > 0, "No DATA captured")
    require(capture["captured_metadata"] > 0, "No METADATA captured")
    require(0 < snapshot.stat().st_size <= args.max_bytes, "Snapshot exceeded byte budget")
    require_hits(run("02-restart", "read"))
    recapture = run("03-recapture", "recapture")
    require_hits(recapture)
    require(recapture["captured_data"] > 0 and recapture["captured_metadata"] > 0,
            "Restored entries were not recaptured")
    require_hits(run("04-second-restart", "read"))
    require(snapshot.stat().st_size <= args.max_bytes, "Second generation exceeded byte budget")

    original = snapshot.read_bytes()
    corrupt = output / "corrupt.bin"
    damaged = bytearray(original)
    damaged[len(damaged) // 2] ^= 0x80
    corrupt.write_bytes(damaged)
    truncated = output / "truncated.bin"
    truncated.write_bytes(original[:len(original) // 2])
    for label, path in [("05-corrupt", corrupt), ("06-truncated", truncated),
                        ("07-missing", output / "missing.bin")]:
        report = run(label, "read", path)
        require(report["snapshot_entries"] == 0, f"{label} unexpectedly activated snapshot")
        if label != "07-missing":
            require(report["snapshot_rejected"] is True, f"{label} did not reject malformed snapshot")
        require(report["data_hits"] == 0 and report["metadata_hits"] == 0,
                f"{label} used rejected snapshot entries")
        require(report["reads"] > 0, f"{label} did not fall back to SST reads")
    require(snapshot.read_bytes() == original, "Read-only runs changed valid snapshot")
    shutil.rmtree(classes)
    (output / "result.json").write_text(json.dumps({"status": "PASS", "scope":
        "Local fixture restart reuse and corruption fallback; no HDD performance claim",
        "max_snapshot_bytes": args.max_bytes, "runs": reports}, indent=2) + "\n")
    print(output / "result.json")


if __name__ == "__main__":
    main()

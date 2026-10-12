#!/usr/bin/env python3
"""Qualify foreground block reuse across restarts, failures and SST replacement."""
import argparse
import os
from experiment_support import Experiment, integer, require


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("library")
    parser.add_argument("output", help="New isolated fixture and evidence directory")
    parser.add_argument("--max-bytes", type=int, default=8 * 1024 * 1024)
    args = parser.parse_args()
    if not 84 <= args.max_bytes <= 64 * 1024 * 1024:
        parser.error("--max-bytes must be between 84 and 67108864")
    experiment = Experiment(args.library, args.output, "HotCacheExperiment")
    output = experiment.output
    db, snapshot = output / "db", output / "snapshot.bin"
    environment = {key: value for key, value in os.environ.items() if not key.startswith("RSHC_TEST_")}

    def run(label, mode, path=snapshot, fault=None, cap=args.max_bytes, expected_exit=0):
        env = dict(environment)
        if fault:
            env[fault] = "1"
        result = experiment.java(label, [experiment.library, db, path, mode, cap],
                                 expected_exit=expected_exit, env=env, report=expected_exit == 0)
        if expected_exit:
            return result
        require(integer(result, "abi", 1) == 1, "Wrong report ABI")
        require(integer(result, "reads", 1) == 2048, "Fixture did not verify every requested value")
        require(integer(result, "snapshot_header_bytes", 1) == 84, "Wrong snapshot format")
        require(result["capture_observer_attached"] is (mode in ("capture", "recapture")),
                "Read-only modes unexpectedly attached capture instrumentation")
        return result

    def captured(report):
        require(report["publication_state"] == "durable", "Capture publication was not durable")
        for cf in range(2):
            for role in ("data", "index", "filter"):
                integer(report, f"cf{cf}_captured_{role}", 1)
        require(integer(report, "written_bytes", 84) == snapshot.stat().st_size <= args.max_bytes,
                "Snapshot byte count/budget mismatch")

    def reused(report):
        require(report["snapshot_rejected"] is False and report["secondary_attached"] is True,
                "Valid snapshot was not attached")
        integer(report, "snapshot_entries", 1)
        for cf in range(2):
            for role in ("data", "index", "filter"):
                integer(report, f"cf{cf}_{role}_hits", 1)
        require(report["secondary_hits"] == report["data_hits"] + report["metadata_hits"],
                "Foreground hit counters disagree")

    captured(run("01-capture", "capture"))
    reused(run("02-restart", "read"))
    recap = run("03-recapture", "recapture")
    reused(recap)
    captured(recap)
    reused(run("04-second-restart", "read"))
    original = snapshot.read_bytes()
    damaged = bytearray(original)
    damaged[len(damaged) // 2] ^= 0x80
    corrupt, truncated = output / "corrupt.bin", output / "truncated.bin"
    corrupt.write_bytes(damaged)
    truncated.write_bytes(original[:len(original) // 2])
    for label, path in [("05-corrupt", corrupt), ("06-truncated", truncated),
                        ("07-missing", output / "missing.bin")]:
        report = run(label, "read", path)
        require(report["snapshot_entries"] == 0 and report["secondary_attached"] is False,
                f"{label} attached invalid entries")
        if label != "07-missing":
            require(report["snapshot_rejected"] is True, f"{label} did not reject malformed snapshot")
        require(report["secondary_hits"] == 0, f"{label} used invalid entries")
    require(snapshot.read_bytes() == original, "Read-only runs changed published snapshot")

    _, stderr = run("08-failed-publication", "recapture", fault="RSHC_TEST_FAIL_BEFORE_RENAME", expected_exit=1)
    require("native experiment: injected publication failure" in stderr, "Unexpected publication error")
    require(snapshot.read_bytes() == original, "Pre-publication failure replaced prior snapshot")
    uncertain = run("09-post-rename", "recapture", fault="RSHC_TEST_FAIL_AFTER_RENAME")
    require(uncertain["publication_state"] == "published_durability_uncertain",
            "Post-rename failure hid installed generation")
    reused(run("10-read-published", "read"))
    captured(run("11-recover-publication", "recapture"))

    # Replace live SST identities and values. The old valid snapshot must miss.
    old_generation = snapshot.read_bytes()
    for label, mode in [("12-replace-ssts", "replace"), ("13-restart-replaced", "read")]:
        report = run(label, mode)
        require(report["fixture_generation"] == 2 and report["secondary_hits"] == 0,
                "Replaced SSTs reused stale snapshot entries")
        require(snapshot.read_bytes() == old_generation, "Replacement test changed old snapshot")
    captured(run("14-capture-replacement", "recapture"))
    reused(run("15-restart-new-generation", "read"))

    empty = output / "empty.bin"
    minimal = run("16-header-only", "recapture", empty, cap=84)
    require(minimal["written_bytes"] == empty.stat().st_size == 84,
            "Minimum cap wrote payload bytes")
    require(minimal["captured_data"] == minimal["captured_metadata"] == 0,
            "Minimum cap captured entries")
    fallback = run("17-empty-fallback", "read", empty, cap=84)
    require(fallback["snapshot_rejected"] is False and fallback["secondary_attached"] is False,
            "Empty snapshot should bypass the secondary adapter")
    _, stderr = experiment.java("18-db-destination", [experiment.library, db, db / "IDENTITY", "recapture", args.max_bytes],
                                expected_exit=1, env=environment, report=False)
    require("Snapshot must be outside" in stderr, "DB destination was not rejected by Java")
    _, stderr = experiment.java("19-invalid-cap", [experiment.library, db, snapshot, "read", 83],
                                expected_exit=1, env=environment, report=False)
    require("max-bytes must be between" in stderr, "Invalid cap was not rejected by Java")
    experiment.finish("Local Get-only DATA/INDEX/FILTER restart/replacement/failure qualification; no HDD performance claim",
                      max_snapshot_bytes=args.max_bytes, metadata_pinning=False,
                      cold_primary_boundary="EraseUnRefEntries before scoped Gets",
                      snapshot_format=2, snapshot_header_bytes=84)


if __name__ == "__main__":
    main()

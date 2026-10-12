#!/usr/bin/env python3
"""Qualify opt-in opening admission and feedback, without a HDD speedup claim."""
import argparse
import shutil
from experiment_support import Experiment, integer, require


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("library")
    parser.add_argument("output", help="New isolated fixture and evidence directory")
    args = parser.parse_args()
    experiment = Experiment(args.library, args.output, "OpeningAdmissionExperiment")
    output = experiment.output
    experiment.java("histogram", ["--self-test"], report=False)
    source_db = output / "seed-db"
    experiment.java("seed", [experiment.library, source_db, "seed", 0])
    # Both runs receive independent copies of the same completed fixture.
    # This controls dataset mutations; it does not remove OS-cache/JIT bias.
    baseline_db, adaptive_db = output / "baseline-db", output / "adaptive-db"
    shutil.copytree(source_db, baseline_db)
    shutil.copytree(source_db, adaptive_db)
    baseline = experiment.java("baseline", [experiment.library, baseline_db, "baseline", 0])
    reference = integer(baseline, "p95_upper_nanos", 1)
    candidate = experiment.java("adaptive", [experiment.library, adaptive_db, "adaptive", reference])
    for report in (baseline, candidate):
        reads = integer(report, "reads", 1)
        require(integer(report, "latency_samples", 1) == reads, "Latency samples dropped reads")
        integer(report, "windows", 1)
        integer(report, "last_window_samples", 1)
        integer(report, "last_window_nanos", 1)
        require(report["active_after_close"] == report["waiters_after_close"] == 0,
                "Admission work survived close")
    admitted = integer(candidate, "admitted", 1)
    require(integer(candidate, "released", 1) == admitted, "Not all permits returned")
    require(1 <= integer(candidate, "peak_active", 1) <= 4, "Concurrency bound violated")
    require(integer(candidate, "table_load_calls", 1) == admitted == 12,
            "The complete twelve-SST startup inventory did not finish")
    require(integer(candidate, "cancelled") == 0, "Startup inventory was abandoned at close")
    require(integer(candidate, "failed_table_load_calls") == 0, "A table load failed")
    integer(candidate, "logical_open_read_bytes", 1)
    integer(candidate, "pressured_admissions", 1)
    integer(candidate, "overlap_feedback_steps", 1)
    require(candidate["healthy_steps"] + candidate["pressure_steps"] > 0,
            "No measured feedback was applied")
    # Invalid paths must fail before opening or creating an unrelated DB.
    absent = output / "not-a-fixture"
    _, error = experiment.java("missing-marker", [experiment.library, absent, "baseline", 0],
                               expected_exit=1, report=False)
    require("Missing or incompatible opening fixture marker" in error, "Wrong fixture rejection")
    require(not absent.exists(), "Rejected fixture path was created")
    _, error = experiment.java("existing-seed", [experiment.library, source_db, "seed", 0],
                               expected_exit=1, report=False)
    require("Seed DB must be a new path" in error, "Existing seed path was not protected")
    experiment.finish("Local admission/attributed-filesystem-read/feedback fixture; no physical HDD performance result",
                      reference_p95_upper_nanos=reference, progress_interval_ms=25,
                      foreground_ssts=6, seeded_ssts=12,
                      percentile="histogram upper bound; eight buckets per power of two")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Paired local fresh-JVM qualification of the real startup JNI/Java classes."""
import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import tempfile

from experiment_support import sha256


MODES = ("none", "cache", "admission", "both")


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--classpath", required=True, help="Built production classes and candidate JNI/runtime dependencies")
    parser.add_argument("--output", required=True, type=Path, help="New retained evidence directory")
    parser.add_argument("--disk", action="append", required=True, type=Path, help="Task-owned mkdtemp parent, repeat for HDD/SSD")
    parser.add_argument("--dataset-mib", type=int, default=512)
    parser.add_argument("--operations", type=int, default=1024, help="Fixed operations per reader; two readers, every fourth operation is eight-key MultiGet")
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--buffered-reads", action="store_true", help="Explicit uncontrolled OS data-cache ablation")
    parser.add_argument("--data-sample-every", type=int, default=64, help="Production DATA sampling; 1 explicitly stresses capture")
    parser.add_argument("--metadata-sample-every", type=int, default=16, help="Production METADATA sampling; 1 explicitly stresses capture")
    parser.add_argument("--prepare-only", action="store_true", help="Compile/record contract; creates no disk fixtures or measured runs")
    args = parser.parse_args()
    if not 1 <= args.dataset_mib <= 1024 or not 1 <= args.operations <= 1_000_000 or not 1 <= args.pairs <= 10 or not 10 <= args.timeout <= 3600:
        parser.error("Bounded dataset 1..1024 MiB, operations 1..1000000, pairs 1..10, timeout 10..3600 seconds required")
    for sampling in (args.data_sample_every, args.metadata_sample_every):
        if sampling < 1 or sampling > 65536 or sampling & (sampling - 1):
            parser.error("Sampling must be a power of two in 1..65536")
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).parent / "java" / "StartupProductionQualification.java"
    classes = out / "classes"
    classes.mkdir()
    cp_entries = [Path(p).resolve(strict=True) for p in args.classpath.split(os.pathsep)]
    artifacts = []
    for path in cp_entries:
        if path.is_file():
            artifacts.append({"path": str(path), "sha256": sha256(path)})
        else:
            for name in ("StartupIoPolicy", "StartupReadCompletions"):
                for file in sorted((path / "it/cavallium/rockserver/core/impl").glob(name + "*.class")):
                    artifacts.append({"path": str(file), "sha256": sha256(file)})
            for pattern in ("NativeStartupBridge*.class", "SnapshotCache*.class", "RocksDBLoader*.class"):
                for file in sorted((path / "it/cavallium/rockserver/core/impl/rocksdb").glob(pattern)):
                    artifacts.append({"path": str(file), "sha256": sha256(file)})
    manifest = {"scope": "Local production JNI/Java bridge component qualification; no service, DDR4, dual-NUMA, or live acceptance",
                "source_sha256": sha256(source), "runner_sha256": sha256(Path(__file__)), "support_sha256": sha256(Path(__file__).parent / "experiment_support.py"),
                "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                "cache_geometry": "HyperClockCache DATA28MiB METADATA4MiB, baseline/candidate matched",
                "feedback": "Real StartupIoPolicy and StartupReadCompletions, 1second coherent native-call cohorts, 3tracked foreground tasks including writes; 100ms sampled admission-overlap classification",
                "service_acceptance": "Requires separate paired EmbeddedDB service gate",
                "sampling_scope": "Production DATA 64/METADATA 16 defaults; explicit sample-every 1 is capture stress only",
                "runtime_artifacts": artifacts, "machine": platform.uname()._asdict(),
                "parameters": {k: str(v) if isinstance(v, Path) else [str(p) for p in v] if isinstance(v, list) else v
                               for k, v in vars(args).items()},
                "cold_state": "Fresh JVM and independent copied DB/cache per run. " + ("Direct data reads bypass OS data page cache; " if not args.buffered_reads else "Buffered data reads have uncontrolled OS cache state; ") + "metadata, devices and copied fixtures may be warm. No global cache drop.",
                "resource_bounds": {"jvm_max_heap_bytes": 256 * 1024**2, "primary_cache_bytes": 32 * 1024**2,
                                    "snapshot_budget_bytes": 16 * 1024**2, "snapshot_disk_budget_bytes": 48 * 1024**2},
                "acceptance_predefined": {"zero_mismatches_crashes_timeouts": True, "all_writes_survive_fresh_jvm": True,
                                          "admission_peak_active_max": 4, "material_gain_ratio": 1.10,
                                          "protected_p99_max_ratio": 1.20, "minimum_pairs": 3,
                                          "note": "Thresholds classify this component workload only; spread crossing thresholds is inconclusive. Histograms report eight buckets per power of two, upper bounds."},
                "retained_fixture_roots": []}
    write_json(out / "manifest.json", manifest)
    command = ["javac", "--release", "25", "-Xlint:all,-restricted", "-Werror", "-cp", args.classpath,
               "-d", str(classes), str(source)]
    subprocess.run(command, check=True, stdout=(out / "compile.stdout").open("w"), stderr=(out / "compile.stderr").open("w"))
    write_json(out / "compile.command.json", command)
    (out / "java-version.txt").write_text(subprocess.run(["java", "-version"], capture_output=True, text=True, check=True).stderr)
    for name, command in (("cpu.txt", ["lscpu"]), ("memory.txt", ["free", "-b"])):
        (out / name).write_text(subprocess.run(command, capture_output=True, text=True, check=True).stdout)
    if args.prepare_only:
        write_json(out / "result.json", {"status": "PREPARED", "measured": False})
        print(out)
        return
    runs = []
    environment = {k: v for k, v in os.environ.items() if not k.startswith(("RSHC_TEST_", "ROCKSDB_TEST_"))}

    def java(label, db, cache, mode):
        command = ["/usr/bin/time", "-v", "-o", str(out / (label + ".resources.txt")),
                   "java", "-Xms32m", "-Xmx256m", "--enable-native-access=ALL-UNNAMED",
                   "-cp", str(classes) + os.pathsep + args.classpath,
                   "it.cavallium.rockserver.core.impl.rocksdb.StartupProductionQualification",
                   str(db), str(cache), mode, str(args.dataset_mib), str(args.operations), str(not args.buffered_reads).lower(), str(args.data_sample_every), str(args.metadata_sample_every)]
        write_json(out / (label + ".command.json"), command)
        with (out / (label + ".stdout")).open("w") as stdout, (out / (label + ".stderr")).open("w") as stderr:
            completed = subprocess.run(command, stdout=stdout, stderr=stderr, timeout=args.timeout, env=environment)
        (out / (label + ".exit")).write_text(str(completed.returncode) + "\n")
        if completed.returncode:
            raise RuntimeError(f"{label} failed: retained diagnostics at {out}")
        reports = [json.loads(line) for line in (out / (label + ".stdout")).read_text().splitlines() if line.startswith("{")]
        if len(reports) != 1 or reports[0].get("data_mismatches") != 0:
            raise RuntimeError(f"{label}: missing report or data mismatch")
        report = reports[0]
        for item in artifacts:
            if sha256(item["path"]) != item["sha256"]:
                raise RuntimeError("Production artifact changed during qualification")
        if "cache_stats" in report:
            counters = {role: json.loads(raw) for role, raw in report["cache_stats"].items()}
            report["cache_counters"] = counters
            for role, stats in counters.items():
                snapshot_budget = (12 if role == "data" else 4) * 1024**2
                if stats["errors"] or stats["rejected"] or stats["snapshot_bytes"] > snapshot_budget or stats["lookup_reserved_bytes"] > 2 * 1024**2:
                    raise RuntimeError(f"{label}: {role}cache correctness/resource counters violated")
            report["durable_final_capture"] = report.get("capture_status") == 0
            report["actual_secondary_hits"] = sum(stats[k] for stats in counters.values() for k in ("data_hits", "index_hits", "filter_hits", "dictionary_hits"))
            before_capture = {role: json.loads(raw) for role, raw in report.get("cache_stats_before_final_capture", {}).items()}
            report["periodic_published_snapshots_during_foreground"] = {role: stats.get("snapshots", 0) for role, stats in before_capture.items()}
        if mode in MODES and (report.get("foreground_accepted") != 3 or report.get("foreground_terminal") != 3):
            raise RuntimeError(f"{label}: foreground task accounting mismatch")
        report["cache_directory_bytes"] = sum(p.stat().st_size for p in cache.rglob("*") if p.is_file()) if cache.exists() else 0
        if report["cache_directory_bytes"] > 48 * 1024**2:
            raise RuntimeError(f"{label}: actual snapshot disk budget exceeded")
        write_json(out / (label + ".json"), report)
        return report

    try:
        run_disks(java, args, out, manifest, runs)
    except (Exception, KeyboardInterrupt) as failure:
        write_json(out / "result.json", {"correctness_status": "FAIL_OR_INCOMPLETE", "error": str(failure), "runs": runs})
        raise
    comparisons = []
    for disk_index in range(len(args.disk)):
        by_mode = {mode: [r["report"] for r in runs if r["disk"] == disk_index and r["mode"] == mode] for mode in MODES}
        for mode in MODES[1:]:
            baseline, candidate = by_mode["none"], by_mode[mode]
            gains = [a["foreground_elapsed_ns"] / b["foreground_elapsed_ns"] for a, b in zip(baseline, candidate)]
            tails = [max(b[k]["p99_upper_ns"] / max(1, a[k]["p99_upper_ns"]) for k in ("point", "multi_get", "writes")) for a, b in zip(baseline, candidate)]
            performance = "PASS" if args.pairs >= 3 and min(gains) >= 1.10 and max(tails) <= 1.20 else "INCONCLUSIVE_OR_REGRESSION"
            mechanisms = []
            for report in candidate:
                opening = report.get("admission_before_close", {})
                mechanisms.append({"actual_secondary_hits": report.get("actual_secondary_hits"),
                                   "durable_final_capture": report.get("durable_final_capture"),
                                   "periodic_published_snapshots_during_foreground": report.get("periodic_published_snapshots_during_foreground"),
                                   "overlap_classification": report["overlap_classification"],
                                   "opening_overlap_operations": report["opening_overlap"]["operations"],
                                   "completed_find_table_calls_including_hits": opening.get("completed_find_table"),
                                   "actual_physical_opened_ssts": "not exposed by API"})
            comparisons.append({"disk": disk_index, "mode": mode, "mechanism_evidence": mechanisms, "paired_throughput_ratios": gains,
                                "median_throughput_ratio": statistics.median(gains), "protected_p99_ratios": tails,
                                "performance_acceptance": performance})
    write_json(out / "result.json", {"correctness_status": "PASS", "performance_comparisons": comparisons,
                                      "scope": manifest["scope"], "runs": runs})
    print(out)


def run_disks(java, args, out, manifest, runs):
    for disk_index, disk in enumerate(args.disk):
        disk = disk.resolve(strict=True)
        if shutil.disk_usage(disk).free < 5 * args.dataset_mib * 1024**2 + 1024**3:
            raise RuntimeError(f"Insufficient reserved free space on {disk}")
        root = Path(tempfile.mkdtemp(prefix="rockserver-startup-qualification-", dir=disk))
        (root / ".task-owned").write_text("startup-production-qualification-v1\n")
        manifest["retained_fixture_roots"].append(str(root))
        write_json(out / "manifest.json", manifest)
        (out / f"disk-{disk_index}.txt").write_text(subprocess.run(["findmnt", "-T", str(root)], capture_output=True, text=True, check=True).stdout)
        seed_db, seed_cache = root / "seed-db", root / "seed-cache"
        java(f"disk-{disk_index}-seed", seed_db, seed_cache, "seed")
        fixture = []
        for file in sorted(seed_db.iterdir()):
            if file.is_file():
                fixture.append({"name": file.name, "bytes": file.stat().st_size, "sha256": sha256(file)})
        write_json(out / f"disk-{disk_index}-fixture.json", fixture)
        for pair in range(args.pairs):
            order = MODES if pair % 2 == 0 else tuple(reversed(MODES))
            for mode in order:
                label = f"disk-{disk_index}-pair-{pair}-{mode}"
                run_root = root / label
                run_root.mkdir()
                db, cache = run_root / "db", run_root / "cache"
                shutil.copytree(seed_db, db)
                if mode in ("cache", "both"):
                    shutil.copytree(seed_cache, cache)
                report = java(label, db, cache, mode)
                verification = java(label + "-restart", db, cache, "verify")
                if verification.get("persisted_writes_verified") != 128:
                    raise RuntimeError("Fresh JVM persistence verification incomplete")
                runs.append({"disk": disk_index, "pair": pair, "order": list(order), "mode": mode, "label": label, "report": report})
                write_json(out / "runs.json", runs)
                # Retain the current pair, seed and all reports. Earlier copied fixtures are task owned.
                if pair > 0:
                    previous = root / f"disk-{disk_index}-pair-{pair-1}-{mode}"
                    if previous.parent != root or not (root / ".task-owned").is_file():
                        raise RuntimeError("Task fixture ownership lost")
                    shutil.rmtree(previous)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Serialized paired fresh-JVM qualification through EmbeddedConnection's production service path."""
import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import statistics
import subprocess
import tempfile
import time
from experiment_support import sha256

MODES = ("none", "cache", "admission", "both")


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def tree_manifest(root):
    return [{"name": str(p.relative_to(root)), "bytes": p.stat().st_size, "sha256": sha256(p)}
            for p in sorted(root.rglob("*")) if p.is_file()]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--classpath", required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--disk", type=Path, action="append", required=True)
    p.add_argument("--cache-disk", type=Path, action="append", required=True)
    p.add_argument("--dataset-mib", type=int, default=512)
    p.add_argument("--operations", type=int, default=1024)
    p.add_argument("--pairs", type=int, default=3)
    p.add_argument("--timeout", type=int, default=600)
    p.add_argument("--capture-period-ms", type=int, default=300000)
    p.add_argument("--data-sample-every", type=int, default=64)
    p.add_argument("--metadata-sample-every", type=int, default=16)
    p.add_argument("--buffered-reads", action="store_true")
    p.add_argument("--prepare-only", action="store_true")
    a = p.parse_args()
    if len(a.disk) != len(a.cache_disk):
        p.error("Supply a separate matching cache parent for every DB parent")
    if not (1 <= a.dataset_mib <= 1024 and 1 <= a.operations <= 1000000 and 1 <= a.pairs <= 10
            and 10 <= a.timeout <= 3600 and 100 <= a.capture_period_ms <= 86400000):
        p.error("Workload or timeout exceeds supported bounds")
    for sample in (a.data_sample_every, a.metadata_sample_every):
        if not 1 <= sample <= 65536 or sample & (sample - 1):
            p.error("Sampling must be a power of two in 1..65536")
    out = a.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    classes = out / "classes"
    classes.mkdir()
    source_dir = Path(__file__).parent / "java"
    sources = [source_dir / (name + ".java") for name in ("StartupProductionQualification", "StartupEmbeddedQualification")]
    entries = [Path(v).resolve(strict=True) for v in a.classpath.split(os.pathsep)]
    # Capture all actual production class/resource identities, including config defaults and controller.
    artifacts = [{"path": str(f), "sha256": sha256(f)} for entry in entries
                 for f in ([entry] if entry.is_file() else sorted(entry.rglob("*"))) if f.is_file()]
    manifest = {"scope": "Paired local EmbeddedConnection/EmbeddedDB in-process service qualification; network/transport and live production excluded",
                "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                "sources": [{"path": str(s), "sha256": sha256(s)} for s in sources],
                "runner_sha256": sha256(Path(__file__)), "runtime_artifacts": artifacts,
                "machine": platform.uname()._asdict(),
                "parameters": {k: str(v) if isinstance(v, Path) else [str(x) for x in v] if isinstance(v, list) else v for k, v in vars(a).items()},
                "ownership": "New task-owned roots. Sequential exited child JVMs; immutable seed backups restore the same canonical DB/cache paths before every trial. No namespace rebasing.",
                "feedback": "Actual production loader/controller, native-call completion cohorts and scheduler pools. Observer reads counters every100ms; never publishes feedback.",
                "bulk_semantics": "Public eight-key ExistsMulti presence checks; public Get verifies values. Not raw value-returning MultiGet.",
                "counter_attribution": "Start/end deltas bracket foreground, excluding startup and final capture. SSD hits/FindTable calls in that window can include concurrent native opening; logical bytes are not physical I/O.",
                "cold_state": "Fresh JVM, same-path restored immutable fixture; no global cache drop. " + ("Direct data reads bypass OS data page cache; " if not a.buffered_reads else "Buffered data page cache uncontrolled; ") + "copying and hashing can warm metadata/devices. Paired order alternates.",
                "resource_bounds": {"heap_max_bytes": 256 * 1024**2, "primary_cache_bytes": 32 * 1024**2,
                                    "wbm_bytes_charged_within_data_cache": 16 * 1024**2,
                                    "snapshot_budget_bytes": 16 * 1024**2, "snapshot_disk_budget_bytes": 48 * 1024**2},
                "acceptance_predefined": {"minimum_pairs": 3, "material_gain_ratio": 1.10, "protected_p99_max_ratio": 1.20,
                    "periodic_capture": "Every cache candidate requires both roles publishing during foreground that spans two capture periods; post-workload manual capture excluded",
                    "admission": "Every admission candidate requires actual admitted FindTable calls plus sampled foreground overlap",
                    "correctness": "All values/presence results verified, all128writer keys survive fresh-JVM restart, zero failures/timeouts/resource violations",
                    "spread": "Any pair crossing the thresholds is inconclusive or regression"}, "retained_roots": []}
    save(out / "manifest.json", manifest)
    command = ["javac", "--release", "25", "-Xlint:all,-restricted", "-Werror", "-cp", a.classpath,
               "-d", str(classes), *map(str, sources)]
    save(out / "compile.command.json", command)
    with (out / "compile.stdout").open("w") as stdout, (out / "compile.stderr").open("w") as stderr:
        subprocess.run(command, check=True, stdout=stdout, stderr=stderr)
    if a.prepare_only:
        save(out / "result.json", {"status": "PREPARED", "measured": False})
        return
    env = {k: v for k, v in os.environ.items() if not k.startswith(("RSHC_TEST_", "ROCKSDB_TEST_"))}
    runs = []

    def java(label, db, cache, mode):
        command = ["java", "-Xms32m", "-Xmx256m", "--enable-native-access=ALL-UNNAMED", "-cp",
                   str(classes) + os.pathsep + a.classpath, "it.cavallium.rockserver.core.impl.rocksdb.StartupEmbeddedQualification",
                   str(db), str(cache), mode, str(a.dataset_mib), str(a.operations), str(not a.buffered_reads).lower(),
                   str(a.data_sample_every), str(a.metadata_sample_every), str(a.capture_period_ms)]
        save(out / (label + ".command.json"), command)
        started = time.monotonic()
        timed_out = False
        process_io = {}
        with (out / (label + ".stdout")).open("w") as stdout, (out / (label + ".stderr")).open("w") as stderr:
            process = subprocess.Popen(command, stdout=stdout, stderr=stderr, env=env, start_new_session=True)
            while True:
                pid, status, usage = os.wait4(process.pid, os.WNOHANG)
                if pid:
                    process.returncode = os.waitstatus_to_exitcode(status)
                    break
                try:
                    for line in Path(f"/proc/{process.pid}/io").read_text().splitlines():
                        key, value = line.split(":", 1)
                        process_io[key] = max(process_io.get(key, 0), int(value))
                except FileNotFoundError:
                    pass
                if time.monotonic() - started > a.timeout and not timed_out:
                    timed_out = True
                    os.killpg(process.pid, signal.SIGKILL)
                time.sleep(.05)
        save(out / (label + ".resources.json"), {"wall_seconds": time.monotonic() - started,
            "user_cpu_seconds": usage.ru_utime, "system_cpu_seconds": usage.ru_stime, "peak_rss_kib": usage.ru_maxrss,
            "io_input_operations": usage.ru_inblock, "io_output_operations": usage.ru_oublock,
            "sampled_process_io_cumulative_max": process_io, "timeout": timed_out,
            "scope": "Whole child including loading/open/capture/close; final procIO increments may be missed"})
        (out / (label + ".exit")).write_text(str(process.returncode) + "\n")
        if process.returncode or timed_out:
            raise RuntimeError(f"{label}: child failure or timeout; diagnostics retained")
        reports = [json.loads(line) for line in (out / (label + ".stdout")).read_text().splitlines() if line.startswith("{")]
        if len(reports) != 1 or reports[0].get("data_mismatches") != 0:
            raise RuntimeError(f"{label}: missing report or data mismatch")
        report = reports[0]
        for artifact in artifacts:
            if sha256(artifact["path"]) != artifact["sha256"]:
                raise RuntimeError("Runtime artifact changed during qualification")
        counters = {role: json.loads(raw) for role, raw in report.get("cache_stats", {}).items()}
        for role, counter in counters.items():
            if counter["errors"] or counter["rejected"] or counter["snapshot_bytes"] > (12 if role == "data" else 4) * 1024**2 or counter["lookup_reserved_bytes"] > 2 * 1024**2:
                raise RuntimeError(f"{label}: native cache correctness/resource bound")
        report["cache_counters"] = counters
        start_counters = {role: json.loads(raw) for role, raw in report.get("cache_stats_at_foreground_start", {}).items()}
        end_counters = {role: json.loads(raw) for role, raw in report.get("cache_stats_before_final_capture", {}).items()}
        report["actual_secondary_hits"] = sum(c[k] for c in counters.values() for k in ("data_hits", "index_hits", "filter_hits", "dictionary_hits"))
        report["foreground_secondary_hits"] = sum(c[k] - start_counters[role][k] for role, c in end_counters.items()
                                                   for k in ("data_hits", "index_hits", "filter_hits", "dictionary_hits"))
        report["periodic_published_snapshots_during_foreground"] = {role: counter["snapshots"] - start_counters[role]["snapshots"] for role, counter in end_counters.items()}
        report["foreground_find_table_calls_including_hits"] = report.get("admission_at_foreground_end", {}).get("completed_find_table", 0) - report.get("admission_at_foreground_start", {}).get("completed_find_table", 0)
        report["foreground_owned_successful_file_opens"] = report.get("admission_at_foreground_end", {}).get("owned_successful_file_opens", 0) - report.get("admission_at_foreground_start", {}).get("owned_successful_file_opens", 0)
        report["cache_directory_bytes"] = sum(v.stat().st_size for v in cache.rglob("*") if v.is_file()) if cache.exists() else 0
        if report["cache_directory_bytes"] > 48 * 1024**2:
            raise RuntimeError(f"{label}: native disk budget exceeded")
        admission = report.get("admission_before_close", {})
        if admission.get("failed", 0) or admission.get("peak_active", 0) > 4:
            raise RuntimeError(f"{label}: native admission resource/failure bound")
        scheduler = report.get("scheduler_after")
        if scheduler and (scheduler["active"] or scheduler["queued"] or scheduler["accepted"] != scheduler["terminal"]):
            raise RuntimeError(f"{label}: service scheduler did not conserve completed operations")
        save(out / (label + ".json"), report)
        return report

    try:
        for disk_index, (disk, cache_disk) in enumerate(zip(a.disk, a.cache_disk)):
            disk, cache_disk = disk.resolve(strict=True), cache_disk.resolve(strict=True)
            if disk == cache_disk or shutil.disk_usage(disk).free < 3 * a.dataset_mib * 1024**2 + 1024**3 or shutil.disk_usage(cache_disk).free < 1024**3:
                raise RuntimeError("Separate cache parent and reserved fixture/free-space bounds required")
            root = Path(tempfile.mkdtemp(prefix="rockserver-embedded-qualification-", dir=disk))
            cache_root = Path(tempfile.mkdtemp(prefix="rockserver-embedded-cache-qualification-", dir=cache_disk))
            for owned in (root, cache_root):
                (owned / ".task-owned").write_text("startup-embedded-qualification-v1\n")
            manifest["retained_roots"].extend(map(str, (root, cache_root)))
            save(out / "manifest.json", manifest)
            for label, owned in (("db", root), ("cache", cache_root)):
                (out / f"disk-{disk_index}-{label}.findmnt.txt").write_text(subprocess.check_output(["findmnt", "-T", str(owned)], text=True))
            db, cache = root / "db", cache_root / "cache"
            seed = java(f"disk-{disk_index}-seed", db, cache, "seed")
            if not seed.get("fixture_live_sst_files"):
                raise RuntimeError("Seed fixture has no SST files")
            backup_db, backup_cache = root / "immutable-seed-db", cache_root / "immutable-seed-cache"
            shutil.copytree(db, backup_db)
            shutil.copytree(cache, backup_cache)
            seed_db_manifest, seed_cache_manifest = tree_manifest(backup_db), tree_manifest(backup_cache)
            save(out / f"disk-{disk_index}-seed-db.files.json", seed_db_manifest)
            save(out / f"disk-{disk_index}-seed-cache.files.json", seed_cache_manifest)
            for pair in range(a.pairs):
                order = MODES if pair % 2 == 0 else tuple(reversed(MODES))
                for mode in order:
                    # Only these exact newly created owned paths are replaced, after their child exited.
                    for target, backup, expected in ((db, backup_db, seed_db_manifest), (cache, backup_cache, seed_cache_manifest)):
                        if target.parent not in (root, cache_root) or not (target.parent / ".task-owned").is_file():
                            raise RuntimeError("Task fixture ownership lost")
                        if target.exists():
                            shutil.rmtree(target)
                        if target == db or mode in ("cache", "both"):
                            shutil.copytree(backup, target)
                            if tree_manifest(target) != expected:
                                raise RuntimeError("Restored same-path fixture differs from immutable seed")
                    label = f"disk-{disk_index}-pair-{pair}-{mode}"
                    report = java(label, db, cache, mode)
                    verified = java(label + "-restart", db, cache, "verify")
                    if verified.get("persisted_writes_verified") != 128:
                        raise RuntimeError("Fresh JVM writer persistence incomplete")
                    save(out / (label + ".final-db.files.json"), tree_manifest(db))
                    runs.append({"disk": disk_index, "pair": pair, "mode": mode, "order": list(order), "label": label, "report": report})
                    save(out / "runs.json", runs)
        comparisons = []
        for disk_index in range(len(a.disk)):
            modes = {mode: [r["report"] for r in runs if r["disk"] == disk_index and r["mode"] == mode] for mode in MODES}
            for mode in MODES[1:]:
                baseline, candidate = modes["none"], modes[mode]
                gains = [x["foreground_elapsed_ns"] / y["foreground_elapsed_ns"] for x, y in zip(baseline, candidate)]
                tails = [max(y[k]["p99_upper_ns"] / max(1, x[k]["p99_upper_ns"]) for k in ("point", "hot_point", "exists_multi", "writes")) for x, y in zip(baseline, candidate)]
                exercised = all(r["writes_during_read_tasks"] >= 128
                    and (mode not in ("cache", "both") or (r["foreground_secondary_hits"] > 0 and r.get("capture_status") == 0
                         and r["foreground_elapsed_ns"] >= 2 * a.capture_period_ms * 1000000
                         and all(r["periodic_published_snapshots_during_foreground"].get(role, 0) > 0 for role in ("data", "metadata"))))
                    and (mode not in ("admission", "both") or (r["opening_overlap"]["operations"] > 0
                         and r["foreground_find_table_calls_including_hits"] > 0)) for r in candidate)
                result = "PASS" if a.pairs >= 3 and min(gains) >= 1.10 and max(tails) <= 1.20 else "INCONCLUSIVE_OR_REGRESSION"
                if not exercised:
                    result = "INCONCLUSIVE_MECHANISM_NOT_EXERCISED"
                comparisons.append({"disk": disk_index, "mode": mode, "mechanism_exercised": exercised,
                    "paired_throughput_ratios": gains, "protected_p99_ratios": tails, "median_throughput_ratio": statistics.median(gains),
                    "performance_acceptance": result})
        save(out / "result.json", {"correctness_status": "PASS", "scope": manifest["scope"], "performance_comparisons": comparisons, "runs": runs})
    except (Exception, KeyboardInterrupt) as failure:
        save(out / "result.json", {"correctness_status": "FAIL_OR_INCOMPLETE", "error": str(failure), "runs": runs})
        raise
    print(out)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Serialized paired fresh-JVM qualification through EmbeddedConnection's production service path."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import stat
import statistics
import subprocess
import tempfile
import time
import zipfile
import xml.etree.ElementTree as ET
from urllib.parse import unquote, urlparse
from experiment_support import sha256

MODES = ("none", "cache", "admission", "both")
DIAGNOSTIC_EVENTS = ("rockserver.StartupIoPolicy", "rockserver.StartupIoStorageFeedback",
                     "rockserver.ServicePut", "rockserver.NativePut",
                     "rockserver.QualificationWrite", "rockserver.QualificationForeground",
                     "rockserver.QualificationRead", "rockserver.NativeRead")


def diagnostic_settings(path):
    config = ET.Element("configuration", version="2.0", label="Startup qualification diagnostics",
                        description="Private timing and actual policy events; no request payloads", provider="Rockserver")
    for name in (*DIAGNOSTIC_EVENTS, "jdk.GarbageCollection", "jdk.GCPhasePause", "jdk.ThreadPark", "jdk.DataLoss"):
        event = ET.SubElement(config, "event", name=name)
        ET.SubElement(event, "setting", name="enabled").text = "true"
        ET.SubElement(event, "setting", name="stackTrace").text = "false"
        if name == "jdk.ThreadPark":
            ET.SubElement(event, "setting", name="threshold").text = "100 us"
        elif name not in ("rockserver.StartupIoPolicy", "rockserver.StartupIoStorageFeedback", "jdk.DataLoss"):
            ET.SubElement(event, "setting", name="threshold").text = "0 ns"
    ET.ElementTree(config).write(path, encoding="utf-8", xml_declaration=True)


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def tree_manifest(root):
    return [{"name": str(p.relative_to(root)), "bytes": p.stat().st_size, "sha256": sha256(p)}
            for p in sorted(root.rglob("*")) if p.is_file()]


def closed_fixture_bytes(root):
    if not root.is_dir() or root.is_symlink():
        raise RuntimeError(f"Unsupported task fixture directory: {root}")
    logical = allocated = regular_files = sst_files = 0
    for file in root.rglob("*"):
        info = file.lstat()
        if stat.S_ISDIR(info.st_mode):
            continue
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError(f"Unsupported nonregular task fixture entry: {file}")
        logical += info.st_size
        allocated += info.st_blocks * 512
        regular_files += 1
        sst_files += file.suffix == ".sst"
    return {"logical_regular_file_bytes": logical, "allocated_regular_file_bytes": allocated,
            "regular_file_count": regular_files, "sst_file_count_on_disk": sst_files,
            "allocation_scope": "Sum of per-file st_blocks*512, without inode/extent deduplication; not device consumption"}


def flush_task_fixture(root, owner):
    """Flush only a verified owned clone, before any child opens it."""
    if owner.is_symlink() or root.parent != owner or (owner / ".task-owned").read_text() != "startup-embedded-qualification-v1\n":
        raise RuntimeError("Fixture flush lost task-owned path identity")
    accounting = closed_fixture_bytes(root)  # Reject nonregular entries before opening files.
    started = time.monotonic()
    directories = [root, owner]
    files = 0
    for path in sorted(root.rglob("*")):
        if path.is_dir():
            directories.append(path)
            continue
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise RuntimeError(f"Nonregular fixture entry during flush: {path}")
            os.fsync(fd)
            files += 1
        finally:
            os.close(fd)
    for path in sorted(directories, key=lambda path: len(path.parts), reverse=True):
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return {"regular_files_flushed": files, "directories_flushed": len(directories),
            "logical_regular_file_bytes": accounting["logical_regular_file_bytes"],
            "wall_seconds": time.monotonic() - started,
            "scope": "Task-owned clone files and directories, including owned parent; no global sync or cache drop"}


def require_data_clone_space(destination, owner, accounting, remaining_bytes):
    if destination.parent != owner or (owner / ".task-owned").read_text() != "startup-embedded-qualification-v1\n":
        raise RuntimeError("Data clone guard lost task-owned path identity")
    clone_bytes = max(accounting["logical_regular_file_bytes"], accounting["allocated_regular_file_bytes"])
    free = shutil.disk_usage(owner).free
    required = clone_bytes + remaining_bytes
    if free < required:
        raise RuntimeError(f"Insufficient data clone space: {free} available, {required} required")
    return {"destination": str(destination), "available_bytes_before_copy": free,
            "conservative_clone_bytes": clone_bytes, "declared_remaining_data_and_headroom_bytes": remaining_bytes,
            "required_available_bytes": required}


def classpath_resource(entries, resource):
    for entry in entries:
        if entry.is_dir():
            file = entry / resource
            if file.is_file():
                return {"classpath_entry": str(entry), "resource": resource, "sha256": sha256(file)}
        elif zipfile.is_zipfile(entry):
            with zipfile.ZipFile(entry) as jar:
                if resource in jar.namelist():
                    with jar.open(resource) as content:
                        digest = hashlib.file_digest(content, "sha256").hexdigest()
                    return {"classpath_entry": str(entry), "resource": resource, "sha256": digest}
    raise RuntimeError(f"Required classpath resource missing: {resource}")


def code_source_path(location):
    source = urlparse(location)
    if source.scheme != "file" or source.netloc not in ("", "localhost"):
        raise RuntimeError(f"Unsupported runtime CodeSource: {location}")
    return str(Path(unquote(source.path)).resolve(strict=True))


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
    p.add_argument("--read-ceiling-mib", type=int, default=8, help="DATA/METADATA read MiB/s; default8 preserves benchmark profile,0 means unlimited")
    p.add_argument("--data-sample-every", type=int, default=64)
    p.add_argument("--metadata-sample-every", type=int, default=16)
    p.add_argument("--seed-hot-keys", type=int, default=4096, help="Declared seed keys, clipped to dataset; original default4096")
    p.add_argument("--seed-passes", type=int, default=1, help="Verified Get passes; repeated passes deterministically shuffled")
    p.add_argument("--startup-window-ms", type=int, default=5000, help="Separate post-open public request-start window,100..60000ms")
    p.add_argument("--buffered-reads", action="store_true")
    p.add_argument("--prepare-only", action="store_true")
    p.add_argument("--diagnostics-jfr", action="store_true", help="Opt-in bounded private recording; diagnostic results cannot qualify performance")
    a = p.parse_args()
    if not 0 <= a.read_ceiling_mib <= (2**63 - 1) // (1024**2):
        p.error("Read ceiling must be nonnegative and representable in signed bytes/second")
    read_ceiling_bytes = a.read_ceiling_mib * 1024**2
    if len(a.disk) != len(a.cache_disk):
        p.error("Supply a separate matching cache parent for every DB parent")
    if not (1 <= a.dataset_mib <= 8192 and 1 <= a.operations <= 1000000 and 1 <= a.pairs <= 10
            and 10 <= a.timeout <= 3600 and 100 <= a.capture_period_ms <= 86400000):
        p.error("Workload or timeout exceeds supported bounds")
    for sample in (a.data_sample_every, a.metadata_sample_every):
        if not 1 <= sample <= 65536 or sample & (sample - 1):
            p.error("Sampling must be a power of two in 1..65536")
    effective_seed_keys = min(a.dataset_mib * 1024**2 // 4096, a.seed_hot_keys)
    if not 1 <= a.seed_hot_keys <= 262144 or not 1 <= a.seed_passes <= 1024 or effective_seed_keys * a.seed_passes > 1048576:
        p.error("Declared history must be bounded to1048576 verified Gets, keys1..262144, passes1..1024")
    if not 100 <= a.startup_window_ms <= 60000:
        p.error("Startup request window must be100..60000ms")
    out = a.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    classes = out / "classes"
    classes.mkdir()
    jfr_settings = out / "diagnostics.jfc"
    if a.diagnostics_jfr:
        diagnostic_settings(jfr_settings)
    source_dir = Path(__file__).parent / "java"
    sources = [source_dir / (name + ".java") for name in ("StartupProductionQualification", "StartupEmbeddedQualification")]
    entries = [Path(v).resolve(strict=True) for v in a.classpath.split(os.pathsep)]
    native_artifact = classpath_resource(entries, "librocksdbjni-linux64.so")
    expected_sources = {name: classpath_resource(entries, name.replace(".", "/") + ".class")["classpath_entry"]
                        for name in ("org.rocksdb.RocksDB", "org.rocksdb.NativeLibraryLoader",
                                     "it.cavallium.rockserver.core.impl.EmbeddedDB")}
    # Capture all actual production class/resource identities, including config defaults and controller.
    artifacts = [{"path": str(f), "sha256": sha256(f)} for entry in entries
                 for f in ([entry] if entry.is_file() else sorted(entry.rglob("*"))) if f.is_file()]
    manifest = {"scope": "Paired local EmbeddedConnection/EmbeddedDB in-process service qualification; network/transport and live production excluded",
                "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                "sources": [{"path": str(s), "sha256": sha256(s)} for s in sources],
                "diagnostics_jfr": {"enabled": a.diagnostics_jfr, "events": DIAGNOSTIC_EVENTS,
                    "maximum_recording_bytes": 256 * 1024**2,
                    "scope": "Diagnostic timings only; public read workload classes and native point/bulk distributions are separate. Concurrent reads cannot be uniquely matched by temporal containment. Recording overhead and possible data loss preclude performance acceptance"},
                "runner_sha256": sha256(Path(__file__)), "runtime_artifacts": artifacts,
                "expected_native_artifact": native_artifact, "expected_code_sources": expected_sources,
                "java_option_environment": {name: os.environ.get(name) for name in
                    ("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS")},
                "native_identity_scope": "Fresh per-child user.home; actual dladdr-selected SO SHA and real CodeSources checked. Loading/hash precedes DB-open/request timers equally in all modes; whole-child resource receipts include it.",
                "machine": platform.uname()._asdict(),
                "parameters": {k: str(v) if isinstance(v, Path) else [str(x) for x in v] if isinstance(v, list) else v for k, v in vars(a).items()},
                "declared_seed_history": {"requested_keys": a.seed_hot_keys, "effective_keys": effective_seed_keys,
                    "passes": a.seed_passes, "verified_gets": effective_seed_keys * a.seed_passes,
                    "order": "sequential" if a.seed_passes == 1 else "deterministic per-pass shuffle",
                    "permission": "Actual controller idle before history and before/after manual capture; never override feedback"},
                "read_ceiling": {"mib_per_second": a.read_ceiling_mib, "bytes_per_second": read_ceiling_bytes,
                    "roles": ["data", "metadata"], "scope": "unlimited, matching production default" if read_ceiling_bytes == 0 else "explicit standalone benchmark profile"},
                "startup_window_scope": "Separate public request-start window after EmbeddedConnection open returns; service latency includes queue/native/future wait; full-workload thresholds unchanged",
                "data_disk_reservation": {"initial_required_bytes": 5 * a.dataset_mib * 1024**2 + 1024**3,
                    "remaining_data_and_headroom_bytes": a.dataset_mib * 1024**2 + 1024**3,
                    "clone_scope": "Measured closed-seed regular logical/allocated bytes; max used conservatively before data copies, never treated as deduplicated device consumption"},
                "fd_resource_scope": "At most one parent /proc/<pid>/fd sample per second; observed peak can miss short peaks; unavailable/exited samples are not zero",
                "ownership": "New task-owned roots. Sequential exited child JVMs; immutable seed backups restore the same canonical DB/cache paths before every trial. No namespace rebasing.",
                "feedback": "Actual production loader/controller, native-call completion cohorts and scheduler pools. Observer reads counters every100ms; never publishes feedback.",
                "bulk_semantics": "Public async eight-key ExistsMulti presence checks; async Get verifies values. One in-flight future per caller lane. Not raw value-returning MultiGet.",
                "latency_histogram": "128 subdivisions per power of two; below 0.8% bucket width, fixed 64 KiB counters per cohort plus object overhead; p99 lower/upper bounds retained",
                "fixture_flush": "Task-owned restored files and directories fsynced before child launch; per-clone receipts, no global sync/cache drop",
                "scheduler_gate": "Accepted/terminal deltas must conserve and cover all verified public request completions; queue/active demand sampled without injection",
                "timeout_scope": "Single monotonic foreground deadline equals --timeout; whole child also has the same timeout including open/capture/close",
                "counter_attribution": "Start/end deltas bracket foreground, excluding startup and final capture. SSD hits/FindTable calls in that window can include concurrent native opening; logical bytes are not physical I/O.",
                "cold_state": "Fresh JVM, same-path restored immutable fixture; no global cache drop. " + ("Direct data reads bypass OS data page cache; " if not a.buffered_reads else "Buffered data page cache uncontrolled; ") + "copying and hashing can warm metadata/devices. Paired order alternates.",
                "resource_bounds": {"heap_max_bytes": 256 * 1024**2, "primary_cache_bytes": 32 * 1024**2,
                                    "wbm_bytes_charged_within_data_cache": 16 * 1024**2,
                                    "snapshot_budget_bytes": 16 * 1024**2, "snapshot_disk_budget_bytes": 48 * 1024**2},
                "acceptance_predefined": {"minimum_pairs": 3, "material_gain_ratio": 1.10, "protected_p99_max_ratio": 1.20,
                    "quantile_guard": "Candidate p99 upper / control p99 lower must also stay within 1.20; reported upper/upper ratios retained",
                    "periodic_capture": "Every cache candidate requires both roles publishing during foreground that spans two capture periods; post-workload manual capture excluded",
                    "admission": "Every admission candidate requires actual owned successful file opens during foreground plus sampled overlap; FindTable cache hits alone do not qualify",
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
        home = out / (label + ".user-home")
        home.mkdir(exist_ok=False)
        command = ["java", "-Xms32m", "-Xmx256m", "--enable-native-access=ALL-UNNAMED",
                   "-Duser.home=" + str(home),
                   "-Drockserver.startup.expected-user-home=" + str(home),
                   "-Drockserver.startup.expected-native-sha256=" + native_artifact["sha256"], "-cp",
                   str(classes) + os.pathsep + a.classpath, "it.cavallium.rockserver.core.impl.rocksdb.StartupEmbeddedQualification",
                   str(db), str(cache), mode, str(a.dataset_mib), str(a.operations), str(not a.buffered_reads).lower(),
                   str(a.data_sample_every), str(a.metadata_sample_every), str(a.capture_period_ms), str(a.timeout),
                   str(a.seed_hot_keys), str(a.seed_passes), str(a.startup_window_ms), str(a.read_ceiling_mib)]
        if a.diagnostics_jfr:
            command.insert(4, "-XX:StartFlightRecording=name=startup,settings=" + str(jfr_settings)
                           + ",filename=" + str(out / (label + ".jfr")) + ",maxsize=256m,dumponexit=true")
        save(out / (label + ".command.json"), command)
        save(out / (label + ".identity-expected.json"), {"user_home": str(home),
            "native_artifact": native_artifact, "code_sources": expected_sources,
            "java_option_environment": {name: env.get(name) for name in
                ("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS")}})
        started = time.monotonic()
        timed_out = False
        process_io = {}
        peak_fds = None
        fd_samples = fd_unavailable = 0
        last_fd_sample = started
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
                now = time.monotonic()
                if now - last_fd_sample >= 1:
                    last_fd_sample = now
                    try:
                        observed_fds = sum(1 for _ in Path(f"/proc/{process.pid}/fd").iterdir())
                    except OSError:
                        fd_unavailable += 1
                    else:
                        if observed_fds == 0:  # A just-exited process can expose an empty FD directory.
                            fd_unavailable += 1
                        else:
                            fd_samples += 1
                            peak_fds = observed_fds if peak_fds is None else max(peak_fds, observed_fds)
                if now - started > a.timeout and not timed_out:
                    timed_out = True
                    os.killpg(process.pid, signal.SIGKILL)
                time.sleep(.05)
        save(out / (label + ".resources.json"), {"wall_seconds": time.monotonic() - started,
            "user_cpu_seconds": usage.ru_utime, "system_cpu_seconds": usage.ru_stime, "peak_rss_kib": usage.ru_maxrss,
            "io_input_operations": usage.ru_inblock, "io_output_operations": usage.ru_oublock,
            "sampled_process_io_cumulative_max": process_io, "timeout": timed_out,
            "peak_observed_fd_count": peak_fds, "fd_sample_count": fd_samples, "fd_unavailable_sample_count": fd_unavailable,
            "fd_scope": "Once/second observed process FD entries; can miss brief peaks; missing/exited samples are unavailable, not zero",
            "scope": "Whole child including loading/open/capture/close; final procIO increments may be missed"})
        (out / (label + ".exit")).write_text(str(process.returncode) + "\n")
        if process.returncode or timed_out:
            raise RuntimeError(f"{label}: child failure or timeout; diagnostics retained")
        reports = [json.loads(line) for line in (out / (label + ".stdout")).read_text().splitlines() if line.startswith("{")]
        if len(reports) != 1 or reports[0].get("data_mismatches") != 0:
            raise RuntimeError(f"{label}: missing report or data mismatch")
        report = reports[0]
        identity = report.get("native_identity", {})
        if identity.get("loaded_native_sha256") != native_artifact["sha256"] or identity.get("expected_native_sha256") != native_artifact["sha256"]:
            raise RuntimeError(f"{label}: loaded native bytes differ from first classpath SO resource")
        if identity.get("user_home") != str(home):
            raise RuntimeError(f"{label}: effective user.home differs from isolated child home")
        actual_sources = identity.get("code_sources", {})
        if set(actual_sources) != set(expected_sources) or any(code_source_path(actual_sources[name]) != expected
                for name, expected in expected_sources.items()):
            raise RuntimeError(f"{label}: runtime CodeSources differ from first classpath class owners")
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
        report["admission_foreground_classification"] = ("actual owned file opens" if report["foreground_owned_successful_file_opens"] > 0
            else "late foreground winner/table-cache hits; no owned open observed" if report["foreground_find_table_calls_including_hits"] > 0
            else "no foreground-window FindTable call observed")
        report["cache_directory_bytes"] = sum(v.stat().st_size for v in cache.rglob("*") if v.is_file()) if cache.exists() else 0
        if report["cache_directory_bytes"] > 48 * 1024**2:
            raise RuntimeError(f"{label}: native disk budget exceeded")
        admission = report.get("admission_before_close", {})
        if admission.get("failed", 0) or admission.get("peak_active", 0) > 4:
            raise RuntimeError(f"{label}: native admission resource/failure bound")
        scheduler = report.get("scheduler_after")
        if scheduler and (scheduler["active"] or scheduler["queued"] or scheduler["accepted"] != scheduler["terminal"]
                          or report.get("scheduler_accepted_delta", 0) < report.get("verified_public_request_completions", 1)
                          or report.get("scheduler_accepted_delta") != report.get("scheduler_terminal_delta")):
            raise RuntimeError(f"{label}: service scheduler did not conserve public async admission")
        save(out / (label + ".json"), report)
        return report

    try:
        for disk_index, (disk, cache_disk) in enumerate(zip(a.disk, a.cache_disk)):
            disk, cache_disk = disk.resolve(strict=True), cache_disk.resolve(strict=True)
            if disk == cache_disk or shutil.disk_usage(disk).free < 5 * a.dataset_mib * 1024**2 + 1024**3 or shutil.disk_usage(cache_disk).free < 1024**3:
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
            seed_bytes = closed_fixture_bytes(db)
            save(out / f"disk-{disk_index}-closed-seed-storage.json", {
                "data": seed_bytes, "cache": closed_fixture_bytes(cache),
                "live_entry_cf_sst_count_before_close": seed["fixture_live_sst_files"],
                "scope": "Regular files after seed JVM exit, before copying; on-disk SST count can include other CFs/obsolete files"})
            backup_db, backup_cache = root / "immutable-seed-db", cache_root / "immutable-seed-cache"
            remaining_bytes = a.dataset_mib * 1024**2 + 1024**3
            save(out / f"disk-{disk_index}-seed-backup-space.json",
                 require_data_clone_space(backup_db, root, seed_bytes, remaining_bytes))
            shutil.copytree(db, backup_db)
            shutil.copytree(cache, backup_cache)
            save(out / f"disk-{disk_index}-seed-backup-flush.json", {
                "data": flush_task_fixture(backup_db, root),
                "cache": flush_task_fixture(backup_cache, cache_root)})
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
                            if target == db:
                                # Actual free space is checked after removing the previous owned target, before copying.
                                save(out / f"disk-{disk_index}-pair-{pair}-{mode}.restore-space.json",
                                     require_data_clone_space(target, root, seed_bytes, remaining_bytes))
                            shutil.copytree(backup, target)
                            if tree_manifest(target) != expected:
                                raise RuntimeError("Restored same-path fixture differs from immutable seed")
                            save(out / f"disk-{disk_index}-pair-{pair}-{mode}-{target.name}.flush.json",
                                 flush_task_fixture(target, target.parent))
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
                tail_bounds = [max(y[k]["p99_upper_ns"] / max(1, x[k]["p99_lower_ns"]) for k in ("point", "hot_point", "exists_multi", "writes")) for x, y in zip(baseline, candidate)]
                exercised = all(r["writes_during_read_tasks"] >= 128
                    and (mode not in ("cache", "both") or (r["foreground_secondary_hits"] > 0 and r.get("capture_status") == 0
                         and r["foreground_elapsed_ns"] >= 2 * a.capture_period_ms * 1000000
                         and all(r["periodic_published_snapshots_during_foreground"].get(role, 0) > 0 for role in ("data", "metadata"))))
                    and (mode not in ("admission", "both") or (r["opening_overlap"]["operations"] > 0
                         and r["foreground_owned_successful_file_opens"] > 0)) for r in candidate)
                result = "PASS" if a.pairs >= 3 and min(gains) >= 1.10 and max(tails) <= 1.20 and max(tail_bounds) <= 1.20 else "INCONCLUSIVE_OR_REGRESSION"
                if not exercised:
                    result = "INCONCLUSIVE_MECHANISM_NOT_EXERCISED"
                if a.diagnostics_jfr:
                    result = "DIAGNOSTIC_ONLY"
                comparisons.append({"disk": disk_index, "mode": mode, "mechanism_exercised": exercised,
                    "paired_throughput_ratios": gains, "protected_p99_ratios": tails, "median_throughput_ratio": statistics.median(gains),
                    "protected_actual_p99_ratio_upper_bounds": tail_bounds,
                    "performance_acceptance": result})
        save(out / "result.json", {"correctness_status": "PASS", "scope": manifest["scope"], "performance_comparisons": comparisons, "runs": runs})
    except (Exception, KeyboardInterrupt) as failure:
        save(out / "result.json", {"correctness_status": "FAIL_OR_INCOMPLETE", "error": str(failure), "runs": runs})
        raise
    print(out)


if __name__ == "__main__":
    main()

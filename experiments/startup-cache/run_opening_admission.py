#!/usr/bin/env python3
"""Exercise opt-in native opening admission with real C API reads in fresh JVMs."""
import argparse
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("library", type=Path)
    parser.add_argument("output", type=Path, help="New isolated fixture and evidence directory")
    args = parser.parse_args()
    library = args.library.resolve(strict=True)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    classes = output / "classes"
    classes.mkdir()
    source = Path(__file__).parent / "java" / "OpeningAdmissionExperiment.java"
    subprocess.run(["javac", "--release", "25", "-d", str(classes), str(source)], check=True, timeout=60)
    reports = {}
    for mode in ["seed", "baseline", "adaptive"]:
        baseline = reports.get("baseline", {}).get("last_window_p95_nanos", 0)
        command = ["java", "--enable-native-access=ALL-UNNAMED", "-cp", str(classes),
                   "OpeningAdmissionExperiment", str(library), str(output / "db"), mode, str(baseline)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=60)
        (output / f"{mode}.stdout").write_text(result.stdout)
        (output / f"{mode}.stderr").write_text(result.stderr)
        (output / f"{mode}.exit").write_text(str(result.returncode) + "\n")
        if result.returncode:
            raise RuntimeError(f"{mode} failed; see {output / (mode + '.stderr')}")
        candidates = [line for line in result.stdout.splitlines() if line.startswith("{")]
        if len(candidates) != 1:
            raise RuntimeError(f"Expected one report from {mode}")
        report = json.loads(candidates[0])
        reports[mode] = report
        (output / f"{mode}.json").write_text(json.dumps(report, indent=2) + "\n")
        if mode != "seed" and (report["reads"] <= 0 or report["last_window_p95_nanos"] <= 0):
            raise AssertionError(f"{mode} did not complete measured reads")
    candidate = reports["adaptive"]
    if candidate["admitted"] <= 0 or candidate["completed"] != candidate["admitted"]:
        raise AssertionError("The admitted background opens did not finish")
    if not 1 <= candidate["peak_active"] <= 4:
        raise AssertionError("The opening concurrency bound was not exercised")
    if candidate["healthy_steps"] + candidate["pressure_steps"] <= 0:
        raise AssertionError("No measured feedback was applied")
    if candidate["active_after_close"] or candidate["waiters_after_close"]:
        raise AssertionError("Admission work survived close")
    (output / "result.json").write_text(json.dumps({"status": "PASS", "scope":
        "Small local debug-library admission/feedback fixture; not a representative HDD performance result",
        "runs": reports}, indent=2) + "\n")
    print(output / "result.json")


if __name__ == "__main__":
    main()

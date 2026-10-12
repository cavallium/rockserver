"""Bounded standalone-JVM execution and retained evidence for these fixtures."""
import hashlib
import json
from pathlib import Path
import subprocess


def sha256(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


class Experiment:
    def __init__(self, library, output, driver):
        self.library = Path(library).resolve(strict=True)
        if not self.library.is_file():
            raise ValueError("Library must be a regular file")
        self.output = Path(output).resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.classes = self.output / "classes"
        self.classes.mkdir()
        source = Path(__file__).parent / "java" / (driver + ".java")
        self.driver = driver
        self.library_hash = sha256(self.library)
        self.runs = []
        # Restricted FFM calls are intentional; reject every other lint warning.
        self.process("compile", ["javac", "--release", "25", "-Xlint:all,-restricted", "-Werror",
                                 "-d", str(self.classes), str(source)])
        self.process("java-version", ["java", "-version"])
        self.metadata = {"library": str(self.library), "library_sha256": self.library_hash,
                         "driver": driver, "driver_source_sha256": sha256(source),
                         "jvm_heap_max_bytes": 256 * 1024 * 1024,
                         "process_timeout_seconds": 60}

    def process(self, label, command, expected_exit=0, env=None):
        stdout, stderr = self.output / (label + ".stdout"), self.output / (label + ".stderr")
        with stdout.open("wb") as out, stderr.open("wb") as err:
            try:
                result = subprocess.run(command, stdout=out, stderr=err, timeout=60, env=env)
            except subprocess.TimeoutExpired:
                (self.output / (label + ".exit")).write_text("TIMEOUT\n")
                raise RuntimeError(f"{label} timed out; partial diagnostics retained in {self.output}") from None
        (self.output / (label + ".exit")).write_text(str(result.returncode) + "\n")
        (self.output / (label + ".command.json")).write_text(json.dumps(command) + "\n")
        if result.returncode != expected_exit:
            raise RuntimeError(f"{label} exited {result.returncode}, expected {expected_exit}; see {stderr}")
        for path in (stdout, stderr):
            if path.stat().st_size > 64 * 1024:
                raise RuntimeError(f"{label} emitted excessive diagnostics; retained at {path}")
        return stdout.read_text(), stderr.read_text()

    def java(self, label, arguments, expected_exit=0, env=None, report=True):
        command = ["java", "-Xms32m", "-Xmx256m", "--enable-native-access=ALL-UNNAMED",
                   "-cp", str(self.classes), self.driver, *map(str, arguments)]
        stdout, stderr = self.process(label, command, expected_exit, env)
        if sha256(self.library) != self.library_hash:
            raise RuntimeError("Native library changed during the experiment")
        if not report:
            return stdout, stderr
        candidates = [line for line in stdout.splitlines() if line.startswith("{")]
        if len(candidates) != 1:
            raise RuntimeError(f"{label} must emit exactly one JSON report")
        result = json.loads(candidates[0])
        if not isinstance(result, dict):
            raise RuntimeError(f"{label} report is not an object")
        (self.output / (label + ".json")).write_text(json.dumps(result, indent=2) + "\n")
        self.runs.append({"label": label, "report": result})
        return result

    def finish(self, scope, **extra):
        result = {"status": "PASS", "scope": scope, **self.metadata, **extra, "runs": self.runs}
        path = self.output / "result.json"
        path.write_text(json.dumps(result, indent=2) + "\n")
        print(path)


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def integer(report, name, minimum=0):
    value = report[name]
    require(type(value) is int and value >= minimum, f"Invalid {name}: {value!r}")
    return value

"""Check the scheduler's public call contract with the declared checker."""

import json
import os
import subprocess
import sys
from pathlib import Path


def test_enqueue_infers_run_parameters_and_result(tmp_path):
    root = Path(__file__).resolve().parents[1]
    fixture = root / "tests" / "typing" / "schedulers.py"
    config = tmp_path / "pyrightconfig.json"
    config.write_text(
        json.dumps(
            {
                "include": [os.path.relpath(fixture, tmp_path)],
                "extraPaths": [str(root)],
                "pythonVersion": "3.13",
                "typeCheckingMode": "strict",
            }
        )
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pyright",
            "--pythonpath",
            sys.executable,
            "-p",
            str(config),
            "--outputjson",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    report = json.loads(result.stdout)
    expected = {
        index
        for index, line in enumerate(fixture.read_text().splitlines())
        if "# error" in line
    }
    diagnostics = report["generalDiagnostics"]
    assert result.returncode == 1, (result.stderr, report)
    assert report["summary"]["filesAnalyzed"] == 1, report
    assert report["summary"]["warningCount"] == 0, report
    assert all(Path(d["file"]) == fixture for d in diagnostics), report
    assert all(d["severity"] == "error" for d in diagnostics), report
    assert {d["range"]["start"]["line"] for d in diagnostics} == expected, (
        report
    )

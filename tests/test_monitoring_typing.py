import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


def test_public_monitoring_provider_broker_and_factory_types():
    pytest.importorskip("opentelemetry.sdk")
    pytest.importorskip("faststream.nats")
    executable = shutil.which("pyright", path=str(Path(sys.executable).parent))
    if executable is None:
        pytest.skip("Requires declared Pyright development dependency")
    path = Path(__file__).parent / "typing" / "monitoring.py"
    result = subprocess.run(
        [
            executable,
            "--pythonpath",
            sys.executable,
            "--outputjson",
            str(path),
        ],
        text=True,
        capture_output=True,
        timeout=60,
    )
    report = json.loads(result.stdout)
    expected = {
        number
        for number, line in enumerate(path.read_text().splitlines())
        if "# expect-error" in line
    }
    diagnostics = report["generalDiagnostics"]
    assert {
        item["range"]["start"]["line"] for item in diagnostics
    } == expected, report
    assert len(diagnostics) == len(expected), report

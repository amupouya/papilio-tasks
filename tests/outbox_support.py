"""Invocation-owned PostgreSQL, or an explicitly supplied isolated test DB."""

import getpass
import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def pg_url():
    selected = os.environ.get("TEST_POSTGRES_URL")
    if selected:
        yield selected
        return
    candidates = sorted(Path("/usr/lib/postgresql").glob("*/bin/initdb"))
    initdb = shutil.which("initdb") or (
        str(candidates[-1]) if candidates else None
    )
    if initdb is None or getpass.getuser() == "root":
        pytest.skip("Requires isolated TEST_POSTGRES_URL or local PostgreSQL")
    binaries = Path(initdb).parent
    with tempfile.TemporaryDirectory(prefix="papilio-outbox-") as directory:
        data = Path(directory) / "data"
        subprocess.run(
            [initdb, "-D", str(data), "-A", "trust", "-U", "papilio_tasks"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=True,
        )
        with (Path(directory) / "postgres.log").open("w") as output:
            server = subprocess.Popen(
                [
                    str(binaries / "postgres"),
                    "-D",
                    str(data),
                    "-k",
                    directory,
                    "-h",
                    "",
                    "-p",
                    "5432",
                    "-c",
                    "fsync=off",
                    "-c",
                    "max_connections=80",
                ],
                stdout=output,
                stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 15
                while not (Path(directory) / ".s.PGSQL.5432").exists():
                    if (
                        server.poll() is not None
                        or time.monotonic() > deadline
                    ):
                        raise RuntimeError("Owned PostgreSQL failed to start")
                    time.sleep(0.05)
                yield (
                    "postgresql+asyncpg://papilio_tasks@/postgres?host="
                    + directory
                )
            finally:
                subprocess.run(
                    [
                        str(binaries / "pg_ctl"),
                        "-D",
                        str(data),
                        "-m",
                        "immediate",
                        "-w",
                        "stop",
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                server.wait(timeout=10)

import os
import shlex
import subprocess

import pytest

from caracal import PCKGDIR
from caracal.dispatch_crew.config_parser import config_parser
from caracal.workers.worker_administrator import WorkerAdministrator

from . import TESTDIR, InitTest


@pytest.fixture
def fixture():
    return InitTest()


def test_help():
    result = subprocess.run(["caracal", "--help"], capture_output=True, text=True)
    assert result.returncode == 0
    assert "Welcome to CARACal" in result.stdout


def test_config_setup(fixture):
    config_file = fixture.random_named_file(suffix=".yaml")
    cmdline = shlex.split(f"caracal -gdt meerkat_continuum -gd {config_file}")
    result = subprocess.run(cmdline, capture_output=True, text=True)

    assert result.returncode == 0

    parser = config_parser()
    config_content, _ = parser.validate_config(config_file)
    _, config = parser.update_config_from_args(config_content, [])

    # update input/msdir/output to avoid missing file errors
    # this also ensures that generated files get stored in TESTDIR
    # instead of the working directory
    config = fixture.update_config_dirs(config)

    pipeline = WorkerAdministrator(
        config,
        workers_directory=os.path.join(PCKGDIR, "workers"),
        configFileName=config_file,
        singularity_image_dir=None,
        container_tech="singularity",
        generate_reports=False,
        end_worker="obsconf",
        partial_init=True,
    )

    assert pipeline.input == os.path.join(TESTDIR, "input")

    for worker in config:
        if "__" in worker:
            continue
        cmdline = shlex.split(f"caracal -wh {worker}")
        result = subprocess.run(cmdline, capture_output=True, text=True)
        assert result.returncode == 0

        assert f"--{worker}" in result.stdout

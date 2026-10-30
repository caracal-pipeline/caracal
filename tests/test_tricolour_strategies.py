from __future__ import annotations

import glob
import os.path

import pytest

import caracal
from caracal import utils

STRATEGY_DIR = os.path.join(os.path.dirname(caracal.__file__), "data", "meerkat_files")
STRATEGIES = sorted(glob.glob(os.path.join(STRATEGY_DIR, "*.yaml")))

# Float-valued sum_threshold parameters, passed straight to Tricolour's jitted kernel
FLOAT_PARAMS = (
    "outlier_nsigma",
    "background_reject",
    "spike_width_time",
    "spike_width_freq",
    "flag_all_time_frac",
    "flag_all_freq_frac",
    "rho",
)


@pytest.mark.parametrize("path", STRATEGIES, ids=os.path.basename)
def test_sum_threshold_parameter_types(path):
    """Tricolour compiles its SumThreshold kernel once per combination of argument
    types, so an int in one step where another step has a float (spike_width_time: 2
    next to 6.5) costs a second compilation of about 30-50 s (ratt-ru/tricolour#111)."""
    strategy = utils.load_yaml(path)
    steps = [step for step in strategy.get("strategies", []) if step.get("task") == "sum_threshold"]
    for name in FLOAT_PARAMS:
        values = [step["kwargs"][name] for step in steps if name in step.get("kwargs", {})]
        types = {"float" if isinstance(value, float) else type(value).__name__ for value in values}
        assert len(types) <= 1, f"{name} is written as {' and '.join(sorted(types))}"

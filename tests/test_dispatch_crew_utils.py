import os.path

import pytest

from caracal import utils
from caracal.dispatch_crew import utils as dc_utils

from . import TESTDIR

MSDICT = utils.load_yaml(os.path.join(TESTDIR, "obsinfo", "ms_summary.json"))


def test_Fields():
    # fields = dc_utils.field_observation_length(MSDICT, 1)

    fields = dc_utils.Fields(
        names=MSDICT["FIELD"]["NAME"],
        ids=MSDICT["FIELD"]["SOURCE_ID"],
        dirs=MSDICT["FIELD"]["REFERENCE_DIR"],
    )
    assert len(fields.names) == len(fields.ids) == len(fields.dirs)
    assert all([isinstance(item, str) for item in fields.names])
    assert all([isinstance(item, int) for item in fields.ids])
    assert all([isinstance(item, list) for item in fields.dirs])

    idx = fields.nfields - 1
    assert fields.index(fields.names[idx]) == fields.index(fields.ids[idx])
    assert fields.name_from_id(fields.ids[idx]) == fields.names[idx]
    assert fields.id_from_name(fields.names[idx]) == fields.ids[idx]

    with pytest.raises(ValueError) as exc1:
        fields.index(sum(fields.ids))

    with pytest.raises(ValueError) as exc2:
        fields.id_from_name("_".join(fields.names))

    with pytest.raises(ValueError) as exc3:
        fields.name_from_id(sum(fields.ids))

    assert all(exc.type is ValueError for exc in [exc1, exc2, exc3])


def test_fieldinfo():
    fields = dc_utils.Fields(
        names=MSDICT["FIELD"]["NAME"],
        ids=MSDICT["FIELD"]["SOURCE_ID"],
        dirs=MSDICT["FIELD"]["REFERENCE_DIR"],
    )

    obs_times = [dc_utils.field_observation_length(MSDICT, fld) for fld in fields.ids]
    longest_observed = dc_utils.observed_longest(MSDICT, fields.ids)

    assert max(obs_times) == pytest.approx(dc_utils.field_observation_length(MSDICT, longest_observed), rel=1e-6)

    obs_length = dc_utils.field_observation_length(MSDICT, longest_observed, return_scans=True)

    assert len(obs_length) == 2
    assert obs_length[0] == pytest.approx(sum(obs_length[1]), rel=1e-6)

    assert not dc_utils.closeby([0, 0], [0, 3.14 / 2])
    assert dc_utils.closeby([0, 0], [0, 3.14 / 10], 3.14 / 2)

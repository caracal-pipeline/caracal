import json
import numpy as np


def get_refant(pipeline, recipe, prefix, msname, fields, min_baseline, max_dist, index):
    """Get reference antenna based on max distances to the array centre,
       min baseline length and amount of flagged data."""
    step = "antenna_flag_summary"
    filename = "{0:s}-flag-{1:s}.json".format(prefix, step)
    recipe.add('cab/flagstats', step, {
        "msname"  : msname,
        "outfile" : filename
    },
        input=pipeline.input,
        output=pipeline.msdir,
        label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))
    recipe.run()
    recipe.jobs = []

    flag_stats = get_antenna_data(pipeline.msdir, filename)
    core_ants = _get_core_antennas(flag_stats, min_baseline, max_dist)
    # Sort antenna by increasing flag data percentage
    sorted_ants = sorted(core_ants.items(), key=lambda x: x[1])
    ref_ants = _prioritised_antennas(sorted_ants)
    return ref_ants


def get_antenna_data(directory, filename):
    """Extract antenna data from the json summary file"""
    with open(f"{directory}/{filename}") as f:
        flag_stats = json.load(f)
    return flag_stats

def _prioritised_antennas(sorted_ants):
    """Get top 1,2 or 3 antennas with minimum flags"""
    if len(sorted_ants) > 2:
        ref_ants_info = sorted_ants[:3]
    elif len(sorted_ants) > 1:
        ref_ants_info = sorted_ants[:2]
    elif len(sorted_ants) > 0:
        ref_ants_info = sorted_ants[:1]
    else:
        return ''
    ref_ants = [ref_ant[1][0] for ref_ant in ref_ants_info]
    return ','.join(ref_ants)


def _get_core_antennas(flag_stats, min_baseline, max_dist):
    """Select antenna with a array centre distance less than max_dist
       and baseline lengths greater than min_baseline"""
    core_ants = {}
    min_base_ants = {}
    for i, ant in flag_stats.items():
        name = ant['name']
        flagged = ant['frac']
        array_centre_dist = ant['array_centre_dist']
        position = ant['position']
        if array_centre_dist <= max_dist:
            core_ants[i] = (name, flagged, position, array_centre_dist)
    for i, ant in core_ants.items():
        baselines = _baseline_calculator(flag_stats, i)
        if all(baseline >= min_baseline for baseline in baselines):
            min_base_ants[i] = ant
    return min_base_ants

def _baseline_calculator(flag_stats, ant_id):
    """Get list of baseline lengths for ant_id"""

    def distance(xyz1, xyz2):
        """Distance between two points in a three dimension coordinate system"""
        x = xyz2[0] - xyz1[0]
        y = xyz2[1] - xyz1[1]
        z = xyz2[2] - xyz1[2]
        d2 = (x * x) + (y * y) + (z * z)
        d = np.sqrt(d2)
        return d

    baselines = []
    ant1_pos = flag_stats[ant_id]['position']
    for i, ant in flag_stats.items():
        if i != ant_id:
            baselines.append(distance(ant1_pos, ant['position']))
    return baselines

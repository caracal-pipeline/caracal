import ruamel.yaml
import math
import numpy

def angular_dist_pos_angle (ra1, dec1, ra2, dec2):
  	"""Computes the angular distance between the two points on a sphere, and
  	the position angle (North through East) of the direction from 1 to 2."""

  	# Knicked from ska-sa/tigger
  	ra = ra2 - ra1
  	sind0, sind, cosd0, cosd = numpy.sin(dec1), numpy.sin(dec2), numpy.cos(dec1), numpy.cos(dec2)
  	sina, cosa = numpy.sin(ra)*cosd, numpy.cos(ra)*cosd
  	x = cosa*sind0 - sind*cosd0
  	y = sina
  	z = cosa*cosd0 + sind*sind0
  	PA = numpy.arctan2(y,-x)
  	R = numpy.arccos(z)

  	return R,PA

def categorize_fields(msinfo):
    with open(msinfo, 'r') as f:
        info = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader)

    names = info['FIELD']['NAME']
    ids = info['FIELD']['SOURCE_ID']
    intents = info['FIELD']['INTENTS']

    mapping = {
'fcal' : (['CALIBRATE_FLUX'], []),
'gcal' : (['CALIBRATE_AMPL', 'CALIBRATE_PHASE'], []),
'bpcal': (['CALIBRATE_BANDPASS'], []),
'target': (['TARGET'], []),
}

    for i, field in enumerate(names):
        ints = intents[i].split(',')
        for intent in ints:
            for ftype in mapping:
                if intent in mapping[ftype][0]:
                    mapping[ftype][-1].append(field)

    return mapping


def select_gcal(msinfo, targets, calibrators, mode='nearest'):
    """
      Automatically select gain calibrator
    """
    with open(msinfo, 'r') as f:
        info = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader)

    names = info['FIELD']['NAME']
    ids = info['FIELD']['SOURCE_ID']
    dirs = info['FIELD']['REFERENCE_DIR']

    def index(field):
        if isinstance(field, str):
            idx = names.index(field)
        elif isinstance(field, int):
            idx = ids.index(field)
        return idx

    if mode == 'most_scans':
        most_scans = 0
        gcal = None
        for fid in calibrators:
            idx = index(fid)
            field = str(ids(idx))
            if most_scans < len(info['SCAN'][field]):
                most_scans = len(info['SCAN'][field])
                gcal = names[idx]
                
    elif mode == 'nearest':
        tras = []
        tdecs = []
        for target in targets:
            idx = index(target)

            tras.append(dirs[idx][0][0])
            tdecs.append(dirs[idx][0][1])

        mean_ra = numpy.mean(tras)
        mean_dec = numpy.mean(tdecs)
        
        nearest_dist = numpy.inf
        gcal = None
        for field in calibrators:
            idx = index(field)
            ra = dirs[idx][0][0]
            dec = dirs[idx][0][1]
            distance = angular_dist_pos_angle(mean_ra, mean_dec, ra, dec)[0]
            if nearest_dist > distance:
                nearest_dist = distance
                gcal = names[idx]

    return gcal

def observed_longest(msinfo, bpcals):
    """
      Automatically select bandpass calibrator
    """
    with open(msinfo, 'r') as f:
        info = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader)

    names = info['FIELD']['NAME']
    ids = info['FIELD']['SOURCE_ID']
    dirs = info['FIELD']['REFERENCE_DIR']

    def index(field):
        if isinstance(field, str):
            idx = names.index(field)
        elif isinstance(field, int):
            idx = ids.index(field)
        return idx

    most_time = 0
    field = None
    for bpcal in bpcals:
        idx = index(bpcal)
        bpcal = str(ids[idx])
        total_time = numpy.sum(info['SCAN'][bpcal])
        if total_time > most_time:
            most_time = total_time
            field = names[idx]

    return field

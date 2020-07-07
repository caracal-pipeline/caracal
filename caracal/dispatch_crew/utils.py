import ruamel.yaml
import math
import numpy
import yaml
import caracal
import caracal.dispatch_crew.caltables as mkct
import re
import astropy.io.fits as fitsio
import codecs
import os

def angular_dist_pos_angle(ra1, dec1, ra2, dec2):
    """Computes the angular distance between the two points on a sphere, and
    the position angle (North through East) of the direction from 1 to 2."""

    # Knicked from ska-sa/tigger
    ra = ra2 - ra1
    sind0, sind, cosd0, cosd = numpy.sin(dec1), numpy.sin(
        dec2), numpy.cos(dec1), numpy.cos(dec2)
    sina, cosa = numpy.sin(ra)*cosd, numpy.cos(ra)*cosd
    x = cosa*sind0 - sind*cosd0
    y = sina
    z = cosa*cosd0 + sind*sind0
    PA = numpy.arctan2(y, -x)
    R = numpy.arccos(z)

    return R, PA


def categorize_fields(info):
    if type(info) is str:
        with open(info, 'r') as f:
            info = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader)

    names = info['FIELD']['NAME']
    ids = info['FIELD']['SOURCE_ID']
    intents = info['FIELD']['INTENTS']
    intent_ids = info['FIELD']['STATE_ID']

    mapping = {
        'fcal': (['CALIBRATE_FLUX'], []),
        'gcal': (['CALIBRATE_AMPL', 'CALIBRATE_PHASE'], []),
        'bpcal': (['CALIBRATE_BANDPASS'], []),
        'target': (['TARGET'], []),
        'xcal': (['CALIBRATE_POLARIZATION'], [])
    }
    if intents:
        for i, field in enumerate(names):
            ints = intents[intent_ids[i]].split(',')
            for intent in ints:
                # for the intents with #, the string after the # does not look useful for us
                # This can be reviewed if need be (Issue 1130)
                intent = intent.split("#")[0]
                for ftype in mapping:
                    if intent in mapping[ftype][0]:
                        mapping[ftype][-1].append(field)

    return mapping


def get_field_id(info, field_name):
    """ Gets field id """
    if not isinstance(field_name, str) and not isinstance(field_name, list):
        raise ValueError(
            "field_name argument must be comma-separated string or list")
    if type(info) is str:
        with open(info, 'r') as f:
            info = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader)
    names = info['FIELD']['NAME']
    ids = info['FIELD']['SOURCE_ID']
    results = []
    for fn in field_name.split(",") if isinstance(field_name, str) else field_name:
        if fn not in names:
            raise KeyError("Could not find field '{0:s}' in the field list {1:}".format(fn,names))
        else:
            results.append(names.index(fn))
    return results


def select_gcal(info, targets, calibrators, mode='nearest'):
    """
      Automatically select gain calibrator
    """
    if type(info) is str:
        with open(info, 'r') as f:
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


def observed_longest(info, bpcals):
    """
      Automatically select bandpass calibrator
    """
    if type(info) is str:
        with open(info, 'r') as f:
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
        total_time = numpy.sum(list(info['SCAN'][bpcal].values()))
        if total_time > most_time:
            most_time = total_time
            field = names[idx]

    return field


def field_observation_length(info, field):
    if type(info) is str:
        with open(info, 'r') as f:
            info = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader)

    names = info['FIELD']['NAME']
    ids = info['FIELD']['SOURCE_ID']

    def index(field):
        if isinstance(field, str):
            idx = names.index(field)
        elif isinstance(field, int):
            idx = ids.index(field)
        else:
            raise ValueError("Field cannot be a {0:s}".format(type(field)))
        return idx
    field = str(ids[index(field)])

    return numpy.sum(list(info['SCAN'][field].values()))

def closeby(radec_1, radec_2, tol=2.9E-3):
    """
    Rough estimate whether two points on celestial sphere are closeby

    Parameters:
    radec_1 (pair of float): Right ascension and Declination of point 1 in rad
    radec_2 (pair of float): Right ascension and Declination of point 2 in rad
    tol: Tolerance in rad (default: 10 arcmin)
    """
    if  numpy.power((radec_1[0]-radec_2[0])*numpy.cos(
            (radec_1[0]-radec_2[0])/2),2)+numpy.power(radec_1[1]-radec_2[1],2
            ) < numpy.power(tol,2):
        return True
    return False


def hetfield(info, field, db, tol=2.9E-3):
    """
    Find match of fields in info

    Parameters:
    info (dict): dictionary of obsinfo as read by yaml
    field (str): field name
    db (dict):   calibrator data base as returned by
                 calibrator_database()

    Go through all calibrators in db and return the first that matches
    the coordinates of field in msinfo. Return empty string if not
    found.
    """

    # Get position of field in msinfo
    ind = info['FIELD']['NAME'].index(field)
    firade = info['FIELD']['DELAY_DIR'][ind][0]
    firade[0] = numpy.mod(firade[0],2*numpy.pi)
    
    dbcp = db.db
    for key in dbcp.keys():
        carade = [dbcp[key]['ra'],dbcp[key]['decl']]
        if closeby(carade, firade, tol=tol):
            return key
    return False

def find_in_native_calibrators(info, field, mode = 'both'):
    """Check if field is in the South Calibrators database.
       Return model if it is. Return lsm if an lsm is available.
       Otherwise, return False.
    """
    if type(info) is str:
        with open(info, 'r') as f:
            info = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader)

    returnsky = False
    returnmod = False
    if mode == 'both':
        returnsky = True
        returnmod = True
    if mode == 'sky':
        returnsky = True
    if mode == 'mod':
        returnmod = True

    db = mkct.calibrator_database()

    fielddb = hetfield(info, field, db)

    if fielddb == False:
        return False

    ref = info['SPW']['REF_FREQUENCY'][0]  # Centre frequency of first channel
    bw = info['SPW']['TOTAL_BANDWIDTH'][0]
    nchan = info['SPW']['NUM_CHAN'][0]

    src = db.db[fielddb]
    aghz = src["a_casa"]
    bghz = src["b_casa"]
    cghz = src["c_casa"]
    dghz = src["d_casa"]
    if "lsm" in src and returnsky:
        return src["lsm"]
    elif returnmod:
        return dict(I=src['S_v0'],
                    a=src['a_casa'],
                    b=src['b_casa'],
                    c=src['c_casa'],
                    d=src['d_casa'],
                    ref=src['v0'])
    else:
        return False

def find_in_casa_calibrators(info, field):
    """Check if field is in the CASA NRAO Calibrators database.
       Return model if it is. Else, return False.
    """

    if type(info) is str:
        with open(info, 'r') as f:
            info = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader)

    with open(caracal.pckgdir + '/data/casa_calibrators.yml') as stdrb:
        db = yaml.safe_load(stdrb)

    dbc = mkct.casa_calibrator_database()

    # Identify field with a standard name
    field_dbc = hetfield(info, field, dbc)
    if field_dbc == False:
        return False

    for src in list(db['models'].values()):
        if field_dbc == src['3C']:
            standards = src['standards']
            break
    standard = standards.split(',')[0]
    return db['standards'][int(standard)]


def meerkat_refant(obsinfo):
    """ get reference antenna. Only works for MeerKAT observations downloaded through CARACal"""

    with open(obsinfo) as stdr:
        info = yaml.safe_load(stdr)
    return info['RefAntenna']


def estimate_solints(msinfo, skymodel, Tsys_eta, dish_diameter, npol, gain_tol=0.05, j=3, save=False):
    if isinstance(skymodel, str):
        skymodel = [skymodel]
    flux = 0
    for name in skymodel:
        with fitsio.open(name) as hdu:
            model = hdu[1].data
        # Get total flux from model
        flux += model['Total_flux'].sum()

    # Get number of antennas
    with open(msinfo, 'r') as f:
        info = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader)
    nant = len(info['ANT']['NAME'])

    # Get time and frequency resoltion of data
    dtime = info['EXPOSURE']
    bw = sum(info['SPW']['TOTAL_BANDWIDTH'])
    nchans = sum(info['SPW']['NUM_CHAN'])
    dfreq = bw/nchans

    k_b = 1.38e-23  # Boltzman's constant
    Jy = 1e-26  # 1 Jansky

    # estimate noise needed for a gain error of 'gain_tol' using Sandeep Sirothia's Equation (priv comm).
    visnoise = flux * numpy.sqrt(nant - j) * gain_tol
    # calculate dt*df (solution intervals) needed to get that noise
    effective_area = numpy.pi * (dish_diameter / 2.0)**2
    dt_dfreq = (2 * k_b * Tsys_eta / (Jy * numpy.sqrt(npol)
                                      * effective_area * visnoise))**2

    # return/save dt*df and the time, frequency resolution of the data
    if save:
        with codecs.open(msinfo, 'w', 'utf8') as yw:
            info['DTDF'] = dt_dfreq
            yaml.dump(info, yw, default_flow_style=False)

    return dt_dfreq, dtime, dfreq


def imaging_params(info, spwid=0):
    if type(info) is str:
        with open(info, 'r') as f:
            info = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader)

    maxbl = info['MAXBL']
    dish_size = numpy.mean(info['ANTENNA']['DISH_DIAMETER'])
    freq = info['SPW']["REF_FREQUENCY"][spwid]
    wavelength = 2.998e8/freq

    FoV = numpy.rad2deg(1.22 * wavelength / dish_size)
    max_res = numpy.rad2deg(wavelength / maxbl)

    return max_res, FoV


def filter_name(string):  # change field names into alphanumerical format for naming output files
    string = string.replace('+', '_p_')
    return re.sub('[^0-9a-zA-Z]', '_', string)



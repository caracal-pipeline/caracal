import ruamel.yaml
import math
import numpy
import yaml
import meerkathi
import meerkathi.dispatch_crew.caltables as mkct 
import re
import astropy.io.fits as fitsio
import codecs

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
    intent_ids = info['FIELD']['STATE_ID']

    mapping = {
        'fcal' : (['CALIBRATE_FLUX'], []),
        'gcal' : (['CALIBRATE_AMPL', 'CALIBRATE_PHASE'], []),
        'bpcal': (['CALIBRATE_BANDPASS'], []),
        'target': (['TARGET'], []),
    }

    for i, field in enumerate(names):
        ints = intents[intent_ids[i]].split(',')
        for intent in ints:
            for ftype in mapping:
                if intent in mapping[ftype][0]:
                    mapping[ftype][-1].append(field)

    return mapping

def get_field_id(msinfo, field_name):
    """ Gets field id """
    with open(msinfo, 'r') as f:
        info = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader)
    names = info['FIELD']['NAME']
    ids = info['FIELD']['SOURCE_ID']
    if field_name not in names:
        raise KeyError("Could not find field '%s'" % field_name)
    return ids[names.index(field_name)]

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
        info = yaml.load(f)

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
        total_time = numpy.sum(info['SCAN'][bpcal].values())
        if total_time > most_time:
            most_time = total_time
            field = names[idx]

    return field

def field_observation_length(msinfo, field):
    with open(msinfo, 'r') as f:
        info = yaml.load(f)
    
    names = info['FIELD']['NAME']
    ids = info['FIELD']['SOURCE_ID']
    
    def index(field):
        if isinstance(field, str):
            idx = names.index(field)
        elif isinstance(field, int):
            idx = ids.index(field)
        return idx
    field = str(ids[index(field)])

    return  numpy.sum(info['SCAN'][field].values())


def find_in_native_calibrators(msinfo, field):
    """Check if field is in the South Calibrators database. 
       Return model if it is. Else, return False. 
    """

    db = mkct.calibrator_database()
    if field not in db.db.keys():
        return False

    with open(msinfo, 'r') as stdr:
        info = yaml.load(stdr)

    ref = info['SPW']['REF_FREQUENCY'][0] # Centre frequency of first channel
    bw = info['SPW']['TOTAL_BANDWIDTH'][0]
    nchan = info['SPW']['NUM_CHAN'][0]
    
    src = db.db[field]
    aghz = src["a_casa"]
    bghz = src["b_casa"]
    cghz = src["c_casa"]
    dghz = src["d_casa"]

    return dict(I=src['S_v0'], 
	a=src['a_casa'], 
	b=src['b_casa'], 
	c=src['c_casa'], 
	d=src['d_casa'], 
	ref=src['v0'])

def meerkat_refant(obsinfo):
    """ get reference antenna. Only works for MeerKAT observations downloaded through meerkathi"""

    with open(obsinfo) as stdr:
        info = yaml.load(stdr)
    return info['RefAntenna']


def find_in_casa_calibrators(msinfo, field):
    """Check if field is in the CASA NRAO Calibrators database. 
       Return model if it is. Else, return False. 
    """
   
    with open(meerkathi.pckgdir +'/data/casa_calibrators.yml') as stdr:
        db = yaml.load(stdr)
    
    found = False
    for src in db['models'].values():
        if field == src['3C']:
            found = True
            standards = src['standards']
            break
        else:
            _field = re.findall(r'\d+', field)
            if len(_field) == 2:
                for name in [src[d] for d in ['B1950', 'J2000', 'ALT']]:
                    _name = re.findall(r'\d+', name)
                    if ''.join(_field) == ''.join(_name) or ''.join(_field[:-1]) == ''.join(_name[:-1]):
                        found = True
                        standards = src['standards']
                        break
    if found:
        standard = standards.split(',')[0]
        return db['standards'][int(standard)]
    else:
        return False

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
    with open(msinfo) as yr:
        info = yaml.load(yr)
    nant = len(info['ANT']['NAME'])

    # Get time and frequency resoltion of data
    dtime = info['EXPOSURE']
    bw = sum(info['SPW']['TOTAL_BANDWIDTH'])
    nchans = sum(info['SPW']['NUM_CHAN'])
    dfreq = bw/nchans
    
    k_b =  1.38e-23 # Boltzman's constant
    Jy = 1e-26 # 1 Jansky

    # estimate noise needed for a gain error of 'gain_tol' using Sandeep Sirothia's Equation (priv comm). 
    visnoise = flux * numpy.sqrt(nant - j) * gain_tol
    # calculate dt*df (solution intervals) needed to get that noise
    effective_area = numpy.pi * (dish_diameter / 2.0)**2
    dt_dfreq = ( 2 * k_b * Tsys_eta  / (Jy * numpy.sqrt(npol) * effective_area * visnoise) )**2 

    # return/save dt*df and the time, frequency resolution of the data
    if save:
        with codecs.open(msinfo, 'w', 'utf8') as yw:
            info['DTDF'] = dt_dfreq
            yaml.dump(data, yw, default_flow_style=False)
    
    return dt_dfreq, dtime, dfreq


def imaging_params(msinfo, spwid=0):
    with open(msinfo) as yr:
        info = yaml.load(yr)

    maxbl = info['MAXBL']
    dish_size = numpy.mean(info['ANTENNA']['DISH_DIAMETER'])
    freq = info['SPW']["REF_FREQUENCY"][spwid]
    wavelegnth = 2.998e8/freq
    
    FoV = numpy.rad2deg( 1.22 * wavelength / dish_size  )
    max_res = numpy.rad2deg( wavelength / maxbl )
    
    return max_res, FoV

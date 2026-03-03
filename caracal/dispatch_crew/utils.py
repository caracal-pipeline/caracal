import codecs
import os.path
import re
from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Any, Dict, List, Tuple, Union

import astropy.io.fits as fitsio
import numpy

import caracal
import caracal.dispatch_crew.caltables as mkct
from caracal import utils

np = numpy


@dataclass
class Fields:
    ids: List[int]
    names: List[str]
    dirs: List[Any] = dc_field(default_factory=list)
    nfields: int = dc_field(init=False)

    def __post_init__(self):
        self.nfields = len(self.ids)

    def index(self, field_val: Union[str, int]) -> int:
        if isinstance(field_val, str):
            return self.names.index(field_val)
        else:
            return self.ids.index(field_val)

    def name_from_id(self, fid):
        return self.names[self.index(fid)]

    def id_from_name(self, name):
        return self.ids[self.index(name)]


def angular_dist_pos_angle(ra1, dec1, ra2, dec2):
    """Computes the angular distance between the two points on a sphere, and
    the position angle (North through East) of the direction from 1 to 2."""

    # Knicked from ska-sa/tigger
    ra = ra2 - ra1
    sind0, sind, cosd0, cosd = numpy.sin(dec1), numpy.sin(dec2), numpy.cos(dec1), numpy.cos(dec2)
    sina, cosa = numpy.sin(ra) * cosd, numpy.cos(ra) * cosd
    x = cosa * sind0 - sind * cosd0
    y = sina
    z = cosa * cosd0 + sind * sind0
    PA = numpy.arctan2(y, -x)
    R = numpy.arccos(z)

    return R, PA


def categorize_fields(info):
    if isinstance(info, str):
        info = utils.load_yaml(info)

    names = info["FIELD"]["NAME"]
    intents = info["FIELD"]["INTENTS"]
    intent_ids = info["FIELD"]["STATE_ID"]

    mapping = {
        "fcal": (["CALIBRATE_FLUX"], []),
        "gcal": (["CALIBRATE_AMPL", "CALIBRATE_PHASE"], []),
        "bpcal": (["CALIBRATE_BANDPASS"], []),
        "target": (["TARGET"], []),
        "xcal": (["CALIBRATE_POLARIZATION"], []),
    }
    if intents:
        for i, field in enumerate(names):
            ints = intents[intent_ids[i]].split(",")
            for intent in ints:
                # for the intents with #, the string after the # does not look useful for us
                # This can be reviewed if need be (Issue 1130)
                intent = intent.split("#")[0]
                for ftype in mapping:
                    if intent in mapping[ftype][0]:
                        mapping[ftype][-1].append(field)

    return mapping


def get_field_id(info, field_name):
    """Gets field id"""
    if not isinstance(field_name, (list, str)):
        raise ValueError("field_name argument must be comma-separated string or list")
    elif isinstance(field_name, str):
        field_name = field_name.split(",")

    if isinstance(info, str):
        info = utils.load_yaml(info)

    names = info["FIELD"]["NAME"]
    results = []
    for fn in field_name:
        if fn not in names:
            raise KeyError(f"Could not find field '{fn}' in the field list {names}")
        else:
            results.append(names.index(fn))
    return results


def select_gcal(info, targets, calibrators, mode="nearest"):
    """
    Automatically select gain calibrator
    """
    if isinstance(info, str):
        info = utils.load_yaml(info)

    fields = Fields(
        names=info["FIELD"]["NAME"],
        ids=info["FIELD"]["SOURCE_ID"],
        dirs=info["FIELD"]["REFERENCE_DIR"],
    )

    if mode == "most_scans":
        most_scans = 0
        gcal = None
        for fid in calibrators:
            idx = fields.index(fid)
            field_id = str(fields.ids[idx])
            if most_scans < len(info["SCAN"][field_id]):
                most_scans = len(info["SCAN"][field_id])
                gcal = fields.names[idx]
    elif mode == "nearest":
        tras = []
        tdecs = []
        for target in targets:
            idx = fields.index(target)

            tras.append(fields.dirs[idx][0][0])
            tdecs.append(fields.dirs[idx][0][1])

        mean_ra = numpy.mean(tras)
        mean_dec = numpy.mean(tdecs)

        nearest_dist = numpy.inf
        gcal = None
        for field in calibrators:
            idx = fields.index(field)
            ra = fields.dirs[idx][0][0]
            dec = fields.dirs[idx][0][1]
            distance = angular_dist_pos_angle(mean_ra, mean_dec, ra, dec)[0]
            if nearest_dist > distance:
                nearest_dist = distance
                gcal = fields.names[idx]
    else:
        raise ValueError(f"Unkown mode '{mode}' for select_gcal() ")

    return gcal


def observed_longest(info: Dict, calfields: List[Union[str, int]]):
    """
    Select field with longest observation length from a
    MSUtils.summary() dictionary

    :param info: Dictionary with information about the fields
    :type info: Dict
    :param calfields: List of fields
    :type calfields: List[Union[str, int]]
    """

    if isinstance(info, str):
        info = utils.load_yaml(info)

    fields = Fields(
        names=info["FIELD"]["NAME"],
        ids=info["FIELD"]["SOURCE_ID"],
        dirs=info["FIELD"]["REFERENCE_DIR"],
    )

    most_time = 0
    field = None
    for calfield in calfields:
        idx = fields.index(calfield)
        _calfield = str(fields.ids[idx])
        total_time = numpy.sum(list(info["SCAN"][_calfield].values()))
        if total_time > most_time:
            most_time = total_time
            field = fields.names[idx]

    return field


def field_observation_length(info: Dict, field: Union[str, int], return_scans: bool = False) -> Union[Tuple[float, List], float]:
    """
    Calculate observation for a field from a MSUtils.summary() dictionary

    :param info: Dictionarty with field information
    :type info: Dict
    :param field: Field name or ID
    :type field: Union[str, int]
    :param return_scans: Return scans corresponding to field
    :type return_scans: bool
    :return: observation length in seconds or the observation length and the scan lengths
    :rtype: Tuple[float, List] | float
    """
    if isinstance(info, str):
        info = utils.load_yaml(info)

    fields = Fields(
        names=info["FIELD"]["NAME"],
        ids=info["FIELD"]["SOURCE_ID"],
    )

    field = str(fields.ids[fields.index(field)])
    scans = list(info["SCAN"][field].values())
    tobs = numpy.sum(scans)
    if return_scans:
        return tobs, scans
    else:
        return tobs


def closeby(radec_1: List[float], radec_2: List[float], tol: float = 2.9e-3) -> bool:
    """
    Estimate whether two points on the celestial sphere are close to each other

    :param radec_1: Right ascension and Declination of point 1 in rad
    :param radec_2: Right ascension and Declination of point 2 in rad
    :param tol : Tolerance in rad (default: 10 arcmin)
    :return: True | False
    """
    ang_dist = angular_dist_pos_angle(radec_1[0], radec_1[1], radec_2[0], radec_2[1])[0]

    if ang_dist < tol:
        return True
    else:
        return False


def hetfield(info, field, db, tol=2.9e-3):
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
    ind = info["FIELD"]["NAME"].index(field)
    firade = info["FIELD"]["DELAY_DIR"][ind][0]
    firade[0] = numpy.mod(firade[0], 2 * numpy.pi)

    dbcp = db.db
    for key in dbcp.keys():
        carade = [dbcp[key]["ra"], dbcp[key]["decl"]]
        if closeby(carade, firade, tol=tol):
            return key
    return False


def find_in_native_calibrators(info, field, mode="both"):
    """Check if field is in the South Calibrators database.
    Return model if it is. Return lsm if an lsm is available.
    Return a crystalball model if specified and available.
    Otherwise, return False.
    """
    if isinstance(info, str):
        info = utils.load_yaml(info)

    returnsky = False
    returnmod = False
    returncrystal = False
    if mode == "both":
        returnsky = True
        returnmod = True
        returncrystal = True
    if mode == "sky":
        returnsky = True
    if mode == "mod":
        returnmod = True
    if mode == "crystal":
        returncrystal = True

    db = mkct.calibrator_database()

    fielddb = hetfield(info, field, db)

    if not fielddb:
        return False

    src = db.db[fielddb]

    if "lsm" in src and returnsky:
        return src["lsm"]
    if "crystal" in src and returncrystal:
        return src["crystal"]
    elif returnmod:
        return dict(I=src["S_v0"], a=src["a_casa"], b=src["b_casa"], c=src["c_casa"], d=src["d_casa"], ref=src["v0"])
    else:
        return False


def find_in_casa_calibrators(info, field):
    """Check if field is in the CASA NRAO Calibrators database.
    Return model if it is. Else, return False.
    """

    if isinstance(info, str):
        info = utils.load_yaml(info)
    db = utils.load_yaml(os.path.join(caracal.pckgdir, "data/casa_calibrators.yml"))

    dbc = mkct.casa_calibrator_database()

    # Identify field with a standard name
    field_dbc = hetfield(info, field, dbc)
    if not field_dbc:
        return False

    for src in list(db["models"].values()):
        if field_dbc == src["3C"]:
            standards = src["standards"]
            break
    else:
        raise
    standard = standards.split(",")[0]
    return db["standards"][int(standard)]


def read_taylor_legodi_row(info, field):
    """
    Read the model from `taylor_legodi_2024.txt`
    """
    if isinstance(info, str):
        info = utils.load(info)

    file_path = caracal.pckgdir + "/data/taylor_legodi_2024.txt"

    with open(file_path, mode="r", encoding="utf-8") as file:
        lines = file.readlines()

        if not lines:
            raise ValueError(f"File '{file_path}' is empty.")

        data_rows = [line.strip().split() for line in lines[1:]]

        for row in data_rows:
            if row[0] == field:
                head = ["fluxdensity", "spix", "reffreq", "polindex", "polangle", "rotmeas"]
                return dict(
                    zip(
                        head,
                        [
                            float(row[1]),
                            float(row[3]),
                            "1.4GHz",
                            float(row[7]) / 100.0,
                            float(row[9]) * numpy.pi / 180,
                            float(row[13]),
                        ],
                    )
                )
        raise ValueError("Field not found in Taylor-Legodi file.")


def meerkat_refant(obsinfo):
    """get reference antenna. Only works for MeerKAT observations downloaded through CARACal"""

    return utils.load_yaml(obsinfo)["RefAntenna"]


def estimate_solints(msinfo, skymodel, Tsys_eta, dish_diameter, npol, gain_tol=0.05, j=3, save=False):
    if isinstance(skymodel, str):
        skymodel = [skymodel]
    flux = 0
    for name in skymodel:
        with fitsio.open(name) as hdu:
            model = hdu[1].data
        # Get total flux from model
        flux += model["Total_flux"].sum()

    # Get number of antennas
    info = utils.load_yaml(msinfo)

    nant = len(info["ANT"]["NAME"])

    # Get time and frequency resoltion of data
    dtime = info["EXPOSURE"]
    bw = sum(info["SPW"]["TOTAL_BANDWIDTH"])
    nchans = sum(info["SPW"]["NUM_CHAN"])
    dfreq = bw / nchans

    k_b = 1.38e-23  # Boltzman's constant
    Jy = 1e-26  # 1 Jansky

    # estimate noise needed for a gain error of 'gain_tol' using Sandeep Sirothia's Equation (priv comm).
    visnoise = flux * numpy.sqrt(nant - j) * gain_tol
    # calculate dt*df (solution intervals) needed to get that noise
    effective_area = numpy.pi * (dish_diameter / 2.0) ** 2
    dt_dfreq = (2 * k_b * Tsys_eta / (Jy * numpy.sqrt(npol) * effective_area * visnoise)) ** 2

    # return/save dt*df and the time, frequency resolution of the data
    if save:
        with codecs.open(msinfo, "w", "utf8") as yw:
            info["DTDF"] = dt_dfreq
            utils.write_yaml(info, yw)

    return dt_dfreq, dtime, dfreq


def imaging_params(info, spwid=0):
    if isinstance(info, str):
        info = utils.load(info)

    maxbl = info["MAXBL"]
    dish_size = numpy.mean(info["ANTENNA"]["DISH_DIAMETER"])
    freq = info["SPW"]["REF_FREQUENCY"][spwid]
    wavelength = 2.998e8 / freq

    FoV = numpy.rad2deg(1.22 * wavelength / dish_size)
    max_res = numpy.rad2deg(wavelength / maxbl)

    return max_res, FoV


def filter_name(string):  # change field names into alphanumerical format for naming output files
    string = string.replace("+", "_p_")
    return re.sub("[^0-9a-zA-Z]", "_", string)

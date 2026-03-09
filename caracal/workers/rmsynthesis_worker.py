import os
from glob import glob

import numpy as np
from astropy.io import fits

from caracal import log

NAME = "Rotation Measure Synthesis"
LABEL = "rmsynthesis"
PIPELINE = None
WCONFIG = None


def file_exists(fname):
    return os.path.isfile(fname)


def stimela_output(fname):
    return os.path.join(*fname.split(os.sep)[1:]) + ":output"


def check_stokes_v(cubes):
    if "v" in cubes:
        log.info("Ignoring specified Stokes V as they're not accepted as RM-tools inputs.")
        del cubes["v"]
    return cubes

def collect_single_polzn_images(imdir, label_in):
    """
    Collect the single per-channel polarization images
    """
    log.info("Collecting per-channel polarization images")

    cubes = dict()
    stokes_params = "iquv" if WCONFIG["use_i"] else "quv"

    # Check for user input
    for stokes in stokes_params:
        search_str = WCONFIG["singles"].get(f"{stokes}_prefix", None)
        if search_str in ["", None]:
            continue
        else:
            cubes[stokes] = sorted(glob(search_str))

    if len(cubes) == 0:
        # if not, check for the caracal generated ones
        for stokes in stokes_params:
            search_str = os.path.join(PIPELINE.polarization, f"{PIPELINE.prefix}_{PIPELINE.field}-[0-9][0-9][0-9][0-9]-{stokes.upper()}*.convolved.fits")
            # Natural sort possible because of name with 0000 structure
            images = glob(search_str)
            if len(images) == 0:
                continue
            else:
                cubes[stokes] = sorted(list(map(stimela_output, images)))

    if cubes:
        cubes = check_stokes_v(cubes)
        chans = {len(v) for v in cubes.values()}
        if len(chans) > 1:
            log.exception(", ".join([f"{k.upper()}: {len(v)} images" for k, v in cubes.items()]))
            log.exception("Please ensure there's an equal number of {} images".format(", ".join(cubes.keys())))
            raise Exception("Stokes input channel counts do not match.")

        for stokes in cubes.keys():
            log.info(f"Stokes {stokes.upper()} images sorted in the following order:\n" + "\n".join([f"{os.path.basename(_)}" for _ in cubes[stokes]]))
            log.info(f"Stokes {stokes.upper()} images found: {len(cubes[stokes])}")
        return cubes
    else:
        raise Exception("No input cubes were found. Stopping RM-synthesis worker")


def make_cube_from_singles(recipe, singles):

    cubes = dict()
    for stokes, images in singles.items():
        step = f"Make-Stokes-{stokes}-cube"

        stokes = stokes.upper()
        oname = os.path.join(PIPELINE.polarization, "{}_{}-{}-image.fits".format(PIPELINE.prefix, PIPELINE.field, stokes))
        recipe.add(
            "cab/fitstool",
            step,
            {
                "image": list(map(stimela_output, images)),
                "output": stimela_output(oname),
                "stack": True,
                "delete-files": False,
                "fits-axis": "FREQ",
                "force": True,
            },
            input=PIPELINE.output,
            output=PIPELINE.output,
            label=f"{step}::Make Stokes cube",
        )
        recipe.run()
        recipe.jobs = []
        cubes[stokes.lower()] = oname
    return cubes


def collect_polzn_cubes(imdir, label_in):
    """
    Collect the polarisation cubes
    """
    log.info("Collecting polarisation cubes")

    stokes_params = "iquv" if WCONFIG["use_i"] else "quv"

    # check if there are user supplied input cubes
    cubes = {_: WCONFIG["cubes"][f"{_}_cube"] for _ in stokes_params if WCONFIG["cubes"][f"{_}_cube"] != "" and file_exists(WCONFIG["cubes"][f"{_}_cube"])} or None

    # if none, use carac generated onees
    if cubes is None:
        cubes = dict()
        log.info("Fingding CARACal generated cubes")
        for stokes in stokes_params:
            # expecting images of the format
            # [PREFIX]_[TARGET]-I-0.0035-0.0035-0.0_image.convolved.fits

            search_str = os.path.join(imdir, f"{label_in}_{PIPELINE.field}-{stokes.upper()}-image.fits")

            if file_exists(search_str):
                cubes[f"{stokes}"] = search_str

    if cubes:
        cubes = check_stokes_v(cubes)
        log.debug("The following cubes were found:")
        log.debug("".join([f"{k.upper()}: {v}\n" for k, v in cubes.items()]))
        return cubes
    else:
        raise Exception("No input cubes were found. Stopping RM-synthesis worker")


def read_freq_from_single_files_header(fname):
    hdr = fits.getheader(fname)
    freq = hdr.get("CRVAL3")
    return freq


def read_freq_from_cube_header(fname):
    hdr = fits.getheader(fname)
    n_chans = hdr.get("NAXIS3")
    start_freq = hdr.get("CRVAL3")
    delta = hdr.get("CDELT3")
    freqs = np.arange(start_freq, start_freq + (n_chans * delta), delta)
    return freqs


def generate_freq_file(cube):
    """
    Create frequency file required by the RMSYNTH script
    The frequencies are extrapolated by the FITS header and the number of
    channels available in a cube
    """

    log.info("Creating frequency file")

    freq_file = os.path.join(PIPELINE.polarization, "freqs.dat")

    if isinstance(cube, list):
        # read headers from each of the list and write them to freq.dat
        freqs = np.array(list(map(read_freq_from_single_files_header, cube)))
    else:
        # read headers from the cube
        freqs = read_freq_from_cube_header(cube)

    np.savetxt(freq_file, freqs, delimiter="")
    log.info(f"Saving frequency file at: {freq_file}")
    return freq_file


def do_rm_synthesis(recipe, cubes, freqfile, prefix):
    """
    The actual RM-synthesis step
    """
    step = "rm-synthesis"
    # todo: investigate why the input is not being redirected to output
    recipe.add(
        "cab/rmsynth3d",
        step,
        {
            "fitsq": stimela_output(cubes["q"]),
            "fitsu": stimela_output(cubes["u"]),
            "fitsi": stimela_output(cubes["i"]) if cubes.get("i") else None,
            "freqs": stimela_output(freqfile),
            "s": WCONFIG["n_samples"],
            "phimax-radm2": WCONFIG["max_phi"],
            "prefixout": prefix,
            "v": True,
        },
        input=PIPELINE.input,
        output=PIPELINE.output,
        label=f"{step}:: Perform RM synthesis",
    )
    recipe.run()
    recipe.jobs = []
    return


def do_rm_clean(recipe, prefix):
    """
    Do RM clean
    """

    step = "RM-CLEAN"
    recipe.add(
        "cab/rmclean3d",
        step,
        {
            "dirty-pdf": stimela_output(os.path.join(PIPELINE.polarization, f"{prefix}FDF_tot_dirty.fits")),
            "rmsf-fwhm": stimela_output(os.path.join(PIPELINE.polarization, f"{prefix}RMSF_tot.fits")),
            "cutoff": WCONFIG.get("cutoff", None),
            "prefixout": prefix,
            "v": True,
            "gain": 0.1,
            "ncores": WCONFIG["ncpus"], 
            "maxiter": WCONFIG["max_iter"],
        },
        input=PIPELINE.input,
        output=PIPELINE.output,
        label=f"{step}:: Perform RM CLEAN",
    )

    recipe.run()
    recipe.jobs = []
    return


# main signature function
def worker(pipeline, recipe, config):
    global PIPELINE, WCONFIG
    PIPELINE = pipeline
    WCONFIG = config

    all_targets, _, _ = pipeline.get_target_mss()

    pipeline.polarization = os.path.join(pipeline.continuum, "polarization")

    # label_in = config["label_in"]

    for target in all_targets:
        pipeline.field = target

        if not WCONFIG["use_i"]:
            log.info("Excluding available Stokes I cubes from RM-synthesis as per user request")

        if WCONFIG["cubes"]["enable"]:
            cubes = collect_polzn_cubes(pipeline.polarization, pipeline.prefix) or None

        if WCONFIG["singles"]["enable"]:
            cubes = collect_single_polzn_images(pipeline.polarization, pipeline.prefix) or None
            cubes = make_cube_from_singles(recipe, cubes)

        if WCONFIG["freq_file"] and file_exists(WCONFIG["freq_file"]):
            freq_file = WCONFIG["freq_file"]
        else:
            log.info("Frequency file was not found.")
            log.info("Autogenerating one.")

            freq_file = generate_freq_file(cubes["q"])
 
        rm_prefix = f"{PIPELINE.prefix}-{WCONFIG['prefix']}-"

        do_rm_synthesis(recipe, cubes, freq_file, rm_prefix)
        do_rm_clean(recipe, rm_prefix)

        return

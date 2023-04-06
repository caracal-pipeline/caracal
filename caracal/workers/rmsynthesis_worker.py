import os
import re
import numpy as np

from glob import glob
from astropy.io import fits
from astropy.wcs import WCS

from caracal import log

NAME = 'Rotation Measure Synthesis'
LABEL = 'rmsynthesis'


def file_exists(fname):
    return os.path.isfile(fname)


def collect_polzn_cubes(imdir, label_in):
    """
    Collect the polarisation cubes
    """
    log.info("Collecting polarisation cubes")   

    cubes = dict()
    for stokes in "iqu":
        # expecting images of the format
        # [PREFIX]_[TARGET]-I-0.0035-0.0035-0.0_image.convolved.fits
        search_str = f"{imdir}/{label_in}*-{stokes}-[0-9]*_image.*fits"

        if len(glob(search_str))> 0:
            cubes[f"{stokes}"], = glob(search_str)
        else:
            log.info(f"Stokes {stokes.capitalize()} cube was not found")
    return cubes


def make_freq_file(cube, fname=None):
    """
    Create frequency file required by the RMSYNTH script
    The frequencies are extrapolated by the FITS header and the number of
    channels available in a cube
    """
    
    log.info("Creating frequency file")
    if not file_exists(cube):
        # try checking for it in output dir
        cube = os.path.join("output", cube)
        
    hdr = fits.getheader(cube)
    wcs = WCS(hdr)
    n_chan = wcs.spectral.array_shape[0]
    freqs = wcs.spectral.pixel_to_world(np.arange(n_chan)).value

    np.savetxt(fname, freqs, delimiter="")
    log.info(f"Saving frequency file at: {fname}")
    return os.path.join(*fname.split("/")[1:])


def do_rm_synthesis(pipeline, recipe, cubes, freqfile, max_phi, prefix):
    """
    The actual RM-synthesis step
    """
    step = "rm-synthesis"
    # todo: investigate why the input is not being redirected to output
    recipe.add(
        'cab/rmsynth3d',
        step,
        {
            "fitsq": f'{cubes["q"]}:output',
            "fitsu": f'{cubes["u"]}:output',
            "fitsi": f'{cubes.get("i", None)}:output',
            "freqs": f"{freqfile}:output",
            "s": 1,
            "phimax-radm2": max_phi,
            "prefixout": prefix + "-",
            "v": True,
        },
        input=pipeline.input,
        output=pipeline.output,
        label=f'{step}:: Perform RM synthesis')
    return recipe

def do_rm_clean(pipeline, recipe, prefix):
    """
    Do RM clean
    """
    
    step = "RM-CLEAN"
    pol_dir = os.path.join(*pipeline.polarization.split("/")[1:])
    recipe.add(
        'cab/rmclean3d',
        step,
        {
            "dirty-pdf": os.path.join(pol_dir, f'{prefix}-FDF_tot_dirty.fits:output'),
            "rmsf-fwhm": os.path.join(pol_dir, f'{prefix}-RMSF_tot.fits:output'),
            # "cutoff": None,
            "prefixout": prefix + "-",
            "v": True,
            "gain": 0.1,
            # "ncores": 10,
            "maxiter": "1000"
        },
        input=pipeline.input,
        output=pipeline.output,
        label=f'{step}:: Perform RM CLEAN')
    return recipe


# main signature function
def worker(pipeline, recipe, config):
    pipeline.polarization = os.path.join(pipeline.continuum, "polarization")

    # label_in = config['label_in']
    max_phi = config['max_phi']

    if config["cubes"]["enable"]:
        cubes = {_: config["cubes"][f"{_}_cube"]
                    for _ in "iqu"}
    else:
        cubes = collect_polzn_cubes(pipeline.polarization, pipeline.prefix)
   
    if config["freq_file"] and file_exists(config['freq_file']) :
        freq_file = config['freq_file']
    else:
        log.info(f"Frequency file was not found.")
        log.info(" Autogenerating one.")

        freq_file = make_freq_file(cubes["q"], 
                        os.path.join(pipeline.polarization, "freqs.dat")
                        )
    if config["prefix"]:
        rm_prefix = config["prefix"]
    else:
        # rm_prefix = os.path.join(pipeline.polarization, f"{pipeline.prefix}")
        rm_prefix = pipeline.prefix
    
    recipe = do_rm_synthesis(pipeline, recipe, cubes,
                            freq_file, max_phi, rm_prefix)

    recipe = do_rm_clean(pipeline, recipe, rm_prefix)

    recipe.run()
    
    return

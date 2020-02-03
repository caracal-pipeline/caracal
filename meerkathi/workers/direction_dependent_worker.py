import os, shutil, glob
import sys
import yaml
import json
import meerkathi
import stimela.dismissable as sdm
from meerkathi.dispatch_crew import utils
from astropy.io import fits as fits

NAME = 'Direction-dependent Calibration'


def worker(pipeline, recipe, config):
    npix = config['image_dd'].get('npix')
    cell = config['image_dd'].get('cell')
    niter = config['image_dd'].get('niter')
    robust = config['image_dd'].get('robust')
    nchans = config['image_dd'].get('nchans')
    fit_spectral_pol = config['image_dd'].get('fit_spectral_pol')
    ddsols = config['calibrate_dd'].get('ddsols', [])
    dist_ncpu = config['calibrate_dd'].get('dist_ncpu', [50])
    label = config.get('label')
    pipeline.set_cal_msnames(label)
    #pipeline.set_hires_msnames(hires_label)
    mslist = pipeline.cal_msnames
    hires_mslist = pipeline.hires_msnames
    prefix = pipeline.prefix
    INPUT=pipeline.input
    OUTPUT=pipeline.output
    DDF_LSM = "DDF_lsm.lsm.html"


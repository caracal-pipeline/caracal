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
    

    def make_primary_beam():
        eidos_opts = {
        "prefix"  : prefix,
        "pixels"  : 256,
        "freq"    : "850 1715 30",
        "diameter" : 4.0,
        "coefficients-file": "meerkat_coeff_dict.npy",}

        recipe.add("cab/eidos", "make_primary_beam", eidos_opts,
        input=INPUT,
        output=OUTPUT,
        label="make_primary_beam:: Generate beams from Eidos",)

    def dd_precal_image():
        dd_precal_image_opts = {
        "Data-MS"        : mslist,
        "Data-ColName"   : "CORRECTED_DATA",
        "Image-NPix"     : npix,
        "Image-Cell"     : cell,
        "Weight-ColName" : "WEIGHT_SPECTRUM",
        "Output-Name"    : prefix+"-DD-precal",
        "Facets-NFacets" : 17,
        "Weight-Mode"    : "Briggs",
        "Weight-Robust"  : robust,
        "Output-Cubes"          : 'all',
        "Freq-NBand"     : nchans,
        "Freq-NDegridBand" : 12,
        "Deconv-FluxThreshold"  : 0.0,
       # "Beam-Model"            : "FITS",
       # "Beam-FITSFile"         : prefix+"'_$(corr)_$(reim).fits':output",
       # "Beam-FITSLAxis"        : "-px",
       # "Beam-FITSMAxis"        : "py",
        "Data-ChunkHours"       : 1.5,
        "Cache-Reset"           : True,
        "Log-Boring"            : True,
        "RIME-DecorrMode"       : "FT",
        "CF-wmax"               : 1000.0,
        "Deconv-PeakFactor"     : 0.35,
        "Predict-ColName"       : "MODEL_DATA",
        "Parallel-NCPU"         : 32,
        "Output-Mode"           : "Clean",
        "Deconv-CycleFactor"    : 0,
        "Deconv-MaxMajorIter"   : 25,
        "Deconv-MaxMinorIter"   : niter,
        "Deconv-Mode"           : "Hogbom",
        "Output-Also"           : "all",
        "Facets-PSFOversize"    : 1.5,
        "SSDClean-NEnlargeData" : False,
        "Deconv-RMSFactor"      : 5.000000,
        "Data-Sort"             : True,
        "Mask-Auto"             : True,
        "Selection-UVRangeKm"   : "[0,200]",
        "Cache-Reset"           : False,
        "Log-Memory"            : True,
        "Log-Boring"            : True, }

        recipe.add("cab/ddfacet", "ddf_image_1", dd_precal_image_opts,
        input=INPUT,
        output=OUTPUT,
        label="ddf:: Primary beam corrected image",
        shared_memory="400gb")

    def dd_postcal_image():
        dd_image_opts = {
        "Data-MS"        : mslist,
        "Data-ColName"   : "SUBDD_DATA",
        "Image-NPix"     : npix,
        "Image-Cell"     : cell,
        "Weight-ColName" : "WEIGHT",
        "Output-Name"    : prefix+"-DD-postcal",
        "Facets-NFacets" : 17,
        "Weight-Mode"    : "Briggs",
        "Weight-Robust"  : robust,
        "Output-Cubes"          : 'all',
        "Freq-NBand"     : nchans,
        "Freq-NDegridBand" : 12,
        "Deconv-FluxThreshold"  : 0.0,
        "Beam-Model"            : "FITS",
       # "Beam-FITSFile"         : prefix+"'_$(corr)_$(reim).fits':output",
        "Beam-FITSLAxis"        : "-px",
        "Beam-FITSMAxis"        : "py",
        "Data-ChunkHours"       : 0.1,
        "Deconv-PeakFactor"     : 0.35,
        "Predict-ColName"       : "MODEL_DATA",
        "Parallel-NCPU"         : 32,
        "Output-Mode"           : "Clean",
        "Deconv-CycleFactor"    : 0,
        "Deconv-MaxMajorIter"   : 25,
        "Deconv-MaxMinorIter"   : niter,
        "Deconv-Mode"           : "Hogbom",
        "Output-Also"           : "all",
        "Facets-PSFOversize"    : 1.5,
        "SSDClean-NEnlargeData" : False,
        "Deconv-RMSFactor"      : 3.000000,
        "Data-Sort"             : True,
       # "Mask-Auto"             : False,
        "Cache-Reset"           : False,
        "Log-Memory"            : True,
        "Log-Boring"            : True, }

        recipe.add("cab/ddfacet", "ddf_image_1", dd_image_opts,
        input=INPUT,
        output=OUTPUT,
        label="ddf:: Primary beam corrected image",
        shared_memory="400gb")



import os, shutil, glob
import sys
import yaml
import json
import meerkathi
import stimela.dismissable as sdm
from meerkathi.dispatch_crew import utils
from astropy.io import fits as fits
from stimela.pathformatter import pathformatter as spf

NAME = 'Direction-dependent Calibration'


def worker(pipeline, recipe, config):
    npix = config['image_dd'].get('npix')
    cell = config['image_dd'].get('cell')
    niter = config['image_dd'].get('niter')
    robust = config['image_dd'].get('robust')
    nchans = config['image_dd'].get('nchans')
    fit_spectral_pol = config['image_dd'].get('fit_spectral_pol')
    ddsols_t = config['calibrate_dd'].get('ddsols_time')
    ddsols_f = config['calibrate_dd'].get('ddsols_freq')
    dist_ncpu = config['calibrate_dd'].get('dist_ncpu')
    label = config.get('label')
    usepb = config.get('use_pb')
    pipeline.set_cal_msnames(label)
    mslist = pipeline.cal_msnames
    hires_mslist = pipeline.hires_msnames
    prefix = pipeline.prefix
    INPUT=pipeline.input
    OUTPUT=pipeline.output
    DDF_LSM = "DDF_lsm.lsm.html"
    
    dd_image_opts = {
        "Data-MS"        : mslist,
        "Data-ColName"   : "DATA",
        "Data-ChunkHours"       : 0.5,
        "Output-Mode"           : "Clean",
        #"Output-Cubes"          : 'all',
        "Output-Name"    : prefix+"-DD-precal",
        "Output-Images"  : 'all',
        "Image-NPix"     : npix,
        "Image-Cell"     : cell,
        "Facets-NFacets" : 17,
        "Weight-ColName" : "WEIGHT",
        "Weight-Mode"    : "Briggs",
        "Weight-Robust"  : robust,
        "Freq-NBand"     : nchans,
        "Freq-NDegridBand" : int(nchans/2.0),
        "Deconv-RMSFactor"      : 0,
        "Deconv-PeakFactor"     : 0.25,
        "Deconv-Mode"       : "Hogbom",
        "Deconv-MaxMinorIter"   : niter,
        "Deconv-Gain"          : 0.1,
        "Deconv-FluxThreshold" : 1.0e-6,
        "Deconv-AllowNegative": True,
        "Hogbom-PolyFitOrder": 6,
        "Parallel-NCPU" : 2,
        "Predict-ColName"       : "MODEL_DATA",
        "Log-Memory"            : True,
        "Cache-Reset"           : True,
        "Log-Boring"            : True,}

    def make_primary_beam():
        eidos_opts = {
        "prefix"  : prefix,
        "pixels"  : 256,
        "freq"    : "850 1715 30",
        "diameter" : 4.0,
        "coeff"   : 'me',
        "coefficients-file": "meerkat_beam_coeffs_em_zp_dct.npy",}

        recipe.add("cab/eidos", "make_primary_beam", eidos_opts,
        input=INPUT,
        output=OUTPUT,
        label="make_primary_beam:: Generate beams from Eidos",)

    def dd_precal_image():
        recipe.add("cab/ddfacet", "ddf_image_1", dd_image_opts,
        input=INPUT,
        output=OUTPUT,
        shared_memory="500g",
        label="ddf:: Primary beam corrected image")

    def dd_postcal_image():
        dd_imagename = {"Output-Name": prefix+"-DD-precal"}
        dd_image_opts.update(dd_imagename)

        recipe.add("cab/ddfacet", "ddf_image_1", dd_image_opts,
        input=INPUT,
        output=OUTPUT,
        label="ddf:: Primary beam corrected image",
        shared_memory="400gb")

    def sfind_intrinsic():
        DDF_INT_IMAGE = prefix+"-DD-precal.int.restored.fits:output"
        DDF_APP_IMAGE = prefix+"-DD-precal.app.restored.fits:output"
        if usepb:
           main_image = DDF_INT_IMAGE
        else:
           main_image = DDF_APP_IMAGE

        recipe.add("cab/pybdsm", "intrinsic_sky_model",{
          "filename" : main_image,
          "outfile"  : "DDF_lsm",
          "detection_image" : DDF_APP_IMAGE,
          "thresh_pix"        : 100,
          "clobber"           : True,
          "thresh_isl"        : 30,
          "port2tigger"       : True,
          "clobber"           : True,
          "adaptive_rms_box"  : True,
          "spectralindex_do"  : False,
          },
          input=INPUT,
          output=OUTPUT,
          label="intrinsic_sky_model:: Find sources in the beam-corrected image")

    def dagga():
        "function to tag sources for dd calibration, very smoky"
        key = 'calibrate_dd'
        #make a skymodel with only dE taggable sources.
        de_only_model = 'de-only-model.txt'
        de_sources_mode = config[key].get('de_sources_mode', 'auto')
        print("de_sources_mode:", de_sources_mode)
        if usepb:
           model_cube = prefix+"-DD-precal.cube.int.model.fits"
        else: 
           model_cube = prefix+"-DD-precal.cube.app.model.fits"
        if de_sources_mode == 'auto':
           print("Carrying out automatic source taggig for direction dependent calibration")
           meerkathi.log.info('Carrying out automatic dE tagging')

           catdagger_opts = {
           "noise-map" : prefix+"-DD-precal.app.residual.fits",
           "psf-image" : prefix+"-DD-precal.psf.fits",
           "input-lsm" : "DDF_lsm.lsm.html",
           "remove-tagged-dE-components-from-model-images": model_cube,
           "only-dEs-in-lsm" : True,
           "sigma" : config[key].get('sigma'),
           "min-distance-from-tracking-centre" : config[key].get('min_dist_from_phcentre', 1300),
           }

           recipe.add('cab/catdagger', 'tag_sources_auto_mode', catdagger_opts,input=INPUT,
              output=OUTPUT,label='tag_sources_auto_mode::Tag dE sources with CatDagger')




    def dd_calibrate():
        key = 'calibrate_dd'
        dicomod = prefix+"-DD-precal.DicoModel"
        dereg = "de.reg"
        for ms in mslist:
           mspref = ms.split('.ms')[0].replace('-','_')
           step = 'dd_calibrate_{0:s}'.format(mspref)
           recipe.add('cab/cubical', step, {
              "data-ms"           : ms,
              "data-column"       : "CORRECTED_DATA",
              "out-column"        : "SUBDD_DATA",
              "weight-column"     : "WEIGHT_SPECTRUM",
              "sol-jones"         : "G,DD",  # Jones terms to solve
              "sol-min-bl"        : config[key].get('sol_min_bl'),  # only solve for |uv| > 300 m
              "g-type"            : "complex-2x2",
              "g-clip-high"       : 1.5,
              "g-clip-low"        : 0.5,
              "g-solvable"        : True,
              "g-update-type"     : "phase-diag",
              "g-max-prior-error" : 0.35,
              "dd-max-prior-error" : 0.35,
              "g-max-post-error"  : 0.35,
              "dd-max-post-error"  : 0.35,
              #"g-time-int"        : gsols[0],
              "g-time-int"        : 5,
              "g-freq-int"        : 20000,
              #"g-freq-int"        : gsols[1],
              "dist-ncpu"         :  dist_ncpu,
              "dist-nworker"      : 5,
            #  "model-beam-pattern": prefix+"'_$(corr)_$(reim).fits':output",
            #  "montblanc-feed-type": "linear",
            #  "model-beam-l-axis" : "px",
            #  "model-beam-m-axis" : "py",
             # "g-save-to"         : "g_final-cal_{0:s}.parmdb".format(mspref),
              "dd-save-to"        : "dd_cal_final_{0:s}.parmdb".format(mspref),
              "dd-type"           : "complex-2x2",
              "dd-clip-high"      : 0.0,
              "dd-clip-low"       : 0.0,
              "dd-solvable"       : True,
              "dd-time-int"       : ddsols_t,
              "dd-freq-int"       : ddsols_f,
              "dd-dd-term"        : True,
              "dd-prop-flags"     : 'always',
              "dd-fix-dirs"       : "0",
              "out-subtract-dirs" : "1:",
              "model-list"        : spf("MODEL_DATA+-{{}}{}@{{}}{}:{{}}{}@{{}}{}".format(dicomod, dereg, dicomod, dereg), "output", "output", "output", "output"),
              "out-name"          : prefix + "dE_sub",
              "out-mode"          : 'sr',
              "data-freq-chunk"   : 4*ddsols_f,
              "data-time-chunk"   : 4*ddsols_t,
              "sol-term-iters"    : "[50,90,50,90]",
              "madmax-plot"       : False,
              "out-plots"          : True,
              "madmax-enable"     : config[key].get('madmax_enable'),
              "madmax-threshold"  : config[key].get('madmax_threshold'),
              "madmax-global-threshold": config[key].get('madmax_global_threshold'),
              "madmax-estimate"   : "corr",
              "out-casa-gaintables" : True,
              "degridding-NDegridBand": int(nchans/2.0),
              'degridding-MaxFacetSize': 0.15,
               },
               input=INPUT,
               output=OUTPUT,
               shared_memory="400gb",
               label='dd_calibrate_{0:s}:: Carry out DD calibration'.format(mspref))
     
    if usepb:
        make_primary_beam()
    dd_precal_image()
    sfind_intrinsic()
    dagga()
    dd_calibrate()
    dd_postcal_image()


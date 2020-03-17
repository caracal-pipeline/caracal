import os, shutil, glob, copy
import sys
import yaml
import json
import meerkathi
import stimela.dismissable as sdm
from meerkathi.dispatch_crew import utils
from astropy.io import fits as fits
from astropy.coordinates import Angle, SkyCoord
from astropy import units as u 
from astropy.wcs import WCS 
from regions import PixCoord, write_ds9, PolygonPixelRegion
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
    DD_DIR = "3GC"
    OUTPUT=pipeline.output+"/"+DD_DIR
    DDF_LSM = "DDF_lsm.lsm.html"
    all_targets, all_msfile, ms_dict = utils.target_to_msfiles(
        pipeline.target, pipeline.msnames, label)
    print("All_targes", all_targets)
    print("All_msfiles", all_msfile)
    print("ms_dict",ms_dict)
    dd_image_opts = {
        "Data-MS"        : mslist,
        "Data-ColName"   : "DATA",
        "Data-ChunkHours"       : 0.5,
        "Output-Mode"           : "Clean",
        #"Output-Cubes"          : 'all',
        "Output-Name"    : prefix+"-DD-precal",
        "Output-Images"  : 'dmcri',
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

    def dd_precal_image(field,ms_list):
        dd_image_opts_precal = copy.deepcopy(dd_image_opts)
        image_prefix_precal = prefix+"_"+field
        dd_ms_list = {"Data-MS" : ms_list}
        dd_imagename = {"Output-Name": image_prefix_precal+"-DD-precal"}
        dd_image_opts_precal.update(dd_imagename)
        dd_image_opts_precal.update(dd_ms_list)
        recipe.add("cab/ddfacet", "ddf_image_{0:s}".format(field), dd_image_opts_precal,
        input=INPUT,
        output=OUTPUT,
        shared_memory="500gb",
        label="ddf_image_{0:s}:: Primary beam corrected image".format(field))
        recipe.run()
        recipe.jobs = []
    def dd_postcal_image(field,mslist):
        dd_image_opts_postcal = copy.deepcopy(dd_image_opts)
        image_prefix_postcal = prefix+"_"+field
        dd_ms_list = {"Data-MS" : ms_list}
        dd_imagename = {"Output-Name": image_prefix_postcal+"-DD-postcal"}
        dd_imagecol = {"Data-ColName": "SUBDD_DATA"}
        dd_image_opts_postcal.update(dd_ms_list)
        dd_image_opts_postcal.update(dd_imagename)
        dd_image_opts_postcal.update(dd_imagecol)

        recipe.add("cab/ddfacet", "ddf_image_{0:s}".format(field), dd_image_opts_postcal,
        input=INPUT,
        output=OUTPUT,
        label="ddf_image_{0:s}:: Primary beam corrected image".format(field),
        shared_memory="500gb")

#    def sfind_intrinsic():
#        DDF_INT_IMAGE = prefix+"-DD-precal.int.restored.fits:output"
#        DDF_APP_IMAGE = prefix+"-DD-precal.app.restored.fits:output"
#        if usepb:
#           main_image = DDF_INT_IMAGE
#        else:
#           main_image = DDF_APP_IMAGE
#
#        recipe.add("cab/pybdsm", "intrinsic_sky_model",{
#          "filename" : main_image,
#          "outfile"  : "DDF_lsm",
#          "detection_image" : DDF_APP_IMAGE,
#          "thresh_pix"        : 100,
#          "clobber"           : True,
#          "thresh_isl"        : 30,
#          "port2tigger"       : True,
#          "clobber"           : True,
#          "adaptive_rms_box"  : True,
#          "spectralindex_do"  : False,
#          },
#          input=INPUT,
#          output=OUTPUT,
#          label="intrinsic_sky_model:: Find sources in the beam-corrected image")

    def dagga(field):
        "function to tag sources for dd calibration, very smoky"
        key = 'calibrate_dd'
        #make a skymodel with only dE taggable sources.
        #de_only_model = 'de-only-model.txt'
        de_sources_mode = config[key].get('de_sources_mode', 'auto')
        print("de_sources_mode:", de_sources_mode)
       # if usepb:
       #    model_cube = prefix+"-DD-precal.cube.int.model.fits"
       # else: 
       #    model_cube = prefix+"-DD-precal.cube.app.model.fits"
        if de_sources_mode == 'auto':
           print("Carrying out automatic source taggig for direction dependent calibration")
           meerkathi.log.info('Carrying out automatic dE tagging')

           catdagger_opts = {
            "ds9-reg-file": "de-{0:s}.reg:output".format(field),
            "ds9-tag-reg-file" : "de-clusterleads-{0:s}.reg:output".format(field),
            "noise-map" : prefix+"_"+field+"-DD-precal.app.residual.fits",
            "sigma" : config[key].get('sigma'),
            "min-distance-from-tracking-centre" : config[key].get('min_dist_from_phcentre'),
           }

           recipe.add('cab/catdagger', 'tag_sources_auto_mode', catdagger_opts,input=INPUT,
              output=OUTPUT,label='tag_sources_auto_mode::Tag dE sources with CatDagger')
        if de_sources_mode == 'manual':
           img = prefix+"_"+field+"-DD-precal.app.restored.fits"
           imagefile = os.path.join(pipeline.output,DD_DIR,img)
           print("Imagefile",imagefile)
           print("Pipeline output", pipeline.output)
           w = WCS(imagefile)
           coords =  config[key].get('de_sources_manual')
           size = coords.split(",")[2]
           coords_str = coords.split(",")[0]+" "+coords.split(",")[1] 
           print("Coordinate String", coords_str)
           centre = SkyCoord(coords_str, unit='deg') 
           separation = int(size) * u.arcsec 
           print("Size",separation)
           xlist = []
           ylist = []
           for i in range(5):
              ang_sep = (306/5)*i*u.deg
              p = centre.directional_offset_by(ang_sep,separation) 
              pix = PixCoord.from_sky(p,w)
              xlist.append(pix.x)
              ylist.append(pix.y)
           vertices = PixCoord(x=xlist, y=ylist)
           reg = PolygonPixelRegion(vertices=vertices)
           regfile = "de-{0:s}.reg".format(field)
           ds9_file = os.path.join(OUTPUT,DD_DIR,regfile)
           write_ds9([reg],ds9_file,coordsys='physical') 

    def dd_calibrate(field,mslist):
        key = 'calibrate_dd'
        dicomod = prefix+"_"+field+"-DD-precal.DicoModel"
        dereg = "de-{0:s}.reg".format(field)
        for ms in mslist:
           mspref = ms.split('.ms')[0].replace('-','_')
           step = 'dd_calibrate_{0:s}_{1:s}'.format(mspref,field)
           recipe.add('cab/cubical', step, {
              "data-ms"           : ms,
              "data-column"       : "DATA",
              "out-column"        : "SUBDD_DATA",
              "weight-column"     : "WEIGHT_SPECTRUM",
              "sol-jones"         : "G,DD",  # Jones terms to solve
              "sol-min-bl"        : config[key].get('sol_min_bl'),  # only solve for |uv| > 300 m
              "sol-stall-quorum"  : 0.95,
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
              "out-model-column"  : "MODEL_OUT",
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
               label='dd_calibrate_{0:s}_{1:s}:: Carry out DD calibration'.format(mspref,field))
    
    for target in all_targets:
       mslist = ms_dict[target]
       field = utils.filter_name(target)
 
    #if usepb:
    #    make_primary_beam()
       dd_precal_image(field,mslist)
    #sfind_intrinsic()
       dagga(field)
       dd_calibrate(field,mslist)
       dd_postcal_image(field,mslist)


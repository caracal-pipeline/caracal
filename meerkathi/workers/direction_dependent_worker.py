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

    def sfind_intrinsic():
        #DDF_CUBE = prefix+"-DD-precal.cube.int.restored.fits:output"
        DDF_INT_IMAGE = prefix+"-DD-precal.int.restored.fits:output"
        DDF_APP_IMAGE = prefix+"-DD-precal.app.restored.fits:output"
        recipe.add("cab/pybdsm", "intrinsic_sky_model",{
          "filename" : DDF_INT_IMAGE,
          #"filename" : DDF_CUBE,
          "outfile"  : "DDF_lsm",
          "detection_image" : DDF_APP_IMAGE,
          "thresh_pix"        : 40,
          "clobber"           : True,
          "thresh_isl"        : 3,
         # "minpix_isl"        : 25,
         # "catalog_type"      : 'srl',
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
        print "de_sources_mode:", de_sources_mode
        print 'de_sources_mode:', de_sources_mode
        if de_sources_mode == 'auto':
           print "Carrying out automatic source taggig for direction dependent calibration"
           meerkathi.log.info('Carrying out automatic dE tagging')
           #make a cube of the pre-dd-cal model images

           catdagger_opts = {
           "noise-map" : prefix+"-DD-precal.app.residual.fits",
           "psf-image" : prefix+"-DD-precal.psf.fits",
           "input-lsm" : "DDF_lsm.lsm.html",
           "remove-tagged-dE-components-from-model-images": prefix+"-DD-precal.cube.int.model.fits",
           "only-dEs-in-lsm" : True,
           "sigma" : config[key].get('sigma'),
           "min-distance-from-tracking-centre" : config[key].get('min_dist_from_phcentre', 1300),
           }

           recipe.add('cab/catdagger', 'tag_sources_auto_mode', catdagger_opts,input=INPUT,
              output=OUTPUT,label='tag_sources_auto_mode::Tag dE sources with CatDagger')


           #re-predict the dE-subtracted Model Data
           dd_precal_image_opts = {
           "Data-MS"        : mslist,
           "Data-ColName"   : "CORRECTED_DATA",
           "Image-NPix"     : npix,
           "Image-Cell"     : cell,
           "Weight-ColName" : "WEIGHT",
           "Output-Name"    : prefix+"-DD-precal",
           "Facets-NFacets" : 17,
           "Weight-Mode"    : "Briggs",
           "Predict-FromImage" : prefix+"-DD-precal.cube.int.model.fits:output",
           "Weight-Robust"  : robust,
           "Output-Cubes"          : 'all',
           "Freq-NDegridBand" : nchans,
           "Deconv-FluxThreshold"  : 0.0,
           "Beam-Model"            : "FITS",
           "Beam-FITSFile"         : prefix+"'_$(corr)_$(reim).fits':output",
           "Beam-FITSLAxis"        : "-px",
           "Beam-FITSMAxis"        : "py",
           "Data-ChunkHours"       : 0.5,
           "Deconv-PeakFactor"     : 0.35,
           "Predict-ColName"       : "MODEL_DATA",
           "Parallel-NCPU"         : 32,
           "Output-Mode"           : "Predict",
           "Deconv-CycleFactor"    : 0,
           "Deconv-MaxMajorIter"   : 25,
           "Deconv-MaxMinorIter"   : niter,
           "Cache-ResetWisdom"     : True,
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

           for ms in mslist:
               mspref = ms.split('.ms')[0].replace('-','_')
               step = "repredict_{0:s}".format(mspref)
               recipe.add('cab/ddfacet',step,dd_precal_image_opts,input=INPUT,
               output=OUTPUT,
               label="repredict_{0:s}:: Primary beam corrected image".format(mspref),
               shared_memory="400gb")
        else:
           de_sources_manual = config[key].get('de_sources_manual')
           with open(os.path.join(pipeline.input, de_only_model), 'w') as stdw:
                   stdw.write('#format: ra_d dec_d i tags...\n')
                   for i in range(len(de_sources_manual)):
                       de_str =  de_sources[i]+"  dE"
                       print "de_str=", de_str
                       stdw.write(de_str)
           recipe.add('cab/tigger_tag', 'transfer_tags', {
              "skymodel" : de_only_model,
              "output-skymodel" : 'DDF_lsm_manual.lsm.html:input',
              "tag"    : "dE",
              "force"  : True,
              "transfer-tags" : True,
              "tolerance" : 5,
              },
              input=INPUT,
              output=OUTPUT,
              label="transfer_tags: Transfer dE tags to the complete lsm")




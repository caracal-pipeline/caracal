import os, shutil, glob
import sys
import yaml
import json
import os
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
   # gsols = config['calibrate_dd'].get('gsols', [])
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
    
    def init():
        #This belongs in the self-cal worker
        flagms_opts = {
        "create"  : True,
        "flag"    : "final_2gc_flags",
        "flagged-any": ["+L"],}
        for ms in mslist:
            mspref = ms.split(".ms")[0]
            mspref = mspref.replace("-","_")
            flagms_opts.update({"msname": ms})
            recipe.add("cab/flagms", "save_2gc_flags_{0:s}".format(mspref),flagms_opts,
            input=INPUT,
            output=OUTPUT,
            label="save_2gc_flags_{0:s}:: Save 2GC flags".format(mspref))
   
    def restore():
        for ms in mslist:    
            mspref = ms.split(".ms")[0].replace("-","_")
            recipe.add("cab/flagms", "remove_3gc_flags_{0:s}".format(mspref),
            {
              "msname" : ms,
              "remove" : "final_3gc_flags",
            },
            input=INPUT,
            output=OUTPUT,
            label="remove_3gc_flags_{0:s}:: Remove 3GC flags".format(mspref))
           
            recipe.add("cab/cubical", "reapply_gains", {
              "data-ms"           : ms,
              "data-column"       : "DATA",
              "out-column"        : "CORRECTED_DATA",
              "weight-column"     : "WEIGHT",
              "out-mode"          : 'ac',
              "dist-ncpu"         :  5,
              "bbc-save-to"       : '',
              "dist-nworker"      : 2,
              "g-xfer-from"       : "g-gains-{0:d}-{1:s}.parmdb:output".format(4,ms.split('.ms')[0]),
              },
              input=INPUT,
              output=OUTPUT,
              label="reapply_gains:: Reapply 2GC gains")

           


    def make_primary_beam():
        eidos_opts = {
        "prefix"  : prefix,
        "pixels"  : 256,
        "freq"    : "850 1715 30",
        "diameter" : 3.0,
        "coefficients-file": "meerkat_coeff_dict.npy",}

        recipe.add("cab/eidos", "make_primary_beam", eidos_opts,
        input=INPUT,
        output=OUTPUT,
        label="make_primary_beam:: Generate beams from Eidos",)

    def dd_precal_image():
        dd_image_opts = {
        "Data-MS"        : mslist,
        "Data-ColName"   : "CORRECTED_DATA",
        "Image-NPix"     : npix,
        "Image-Cell"     : cell,
        "Weight-ColName" : "WEIGHT",
        "Output-Name"    : prefix+"-DD-precal",
        "Facets-NFacets" : 17,
        "Weight-Mode"    : "Briggs",
        "Weight-Robust"  : robust,
        "Output-Cubes"          : 'all',
        "Freq-NBand"     : nchans,
        "Freq-NDegridBand" : 12,  
        "Deconv-FluxThreshold"  : 0.0,
        "Beam-Model"            : "FITS",
        "Beam-FITSFile"         : prefix+"'_$(corr)_$(reim).fits':output",
        "Beam-FITSLAxis"        : "-px",
        "Beam-FITSMAxis"        : "py",
        "Data-ChunkHours"       : 0.5,
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
        "Beam-FITSFile"         : prefix+"'_$(corr)_$(reim).fits':output",
        "Beam-FITSLAxis"        : "-px",
        "Beam-FITSMAxis"        : "py",
        "Data-ChunkHours"       : 0.5,
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
        DDF_CUBE = prefix+"-DD-precal.cube.int.restored.fits:output"
        DDF_APP_IMAGE = prefix+"-DD-precal.app.restored.fits:output"
        recipe.add("cab/pybdsm", "intrinsic_sky_model",{
          "filename" : DDF_CUBE,
          "outfile"  : "DDF_lsm",
          "detection_image" : DDF_APP_IMAGE,
          "thresh_pix"        : 10,
          "clobber"           : True,
          "thresh_isl"        : 4,
          "catalog_type"      : 'srl',
          "port2tigger"       : True,
          "clobber"           : True,
          "adaptive_rms_box"  : False,
          "spectralindex_do"  : False,
          },
          input=INPUT,
          output=OUTPUT,
          label="intrinsic_sky_model:: Find sources in the beam-corrected image")
 
    def tag_sources():
        key = 'calibrate_dd'
        #make a skymodel with only dE taggable sources.
        de_only_model = 'de-only-model.txt'
        de_sources = config[key].get('de_sources')
        with open(os.path.join(pipeline.input, de_only_model), 'w') as stdw:
                stdw.write('#format: ra_d dec_d i tags...\n')
                for i in range(len(de_sources)):
                    de_str =  de_sources[i]+"  dE"
                    print "de_str=", de_str
                    stdw.write(de_str)
        recipe.add('cab/tigger_tag', 'transfer_tags', {
           "skymodel" : de_only_model,
           "output-skymodel" : 'DDF_lsm.lsm.html:output',
           "tag"    : "dE",
           "force"  : True,
           "transfer-tags" : True,
           "tolerance" : 5,
           },
           input=INPUT,
           output=OUTPUT,
           label="transfer_tags: Transfer dE tags to the complete lsm")
        
      
    def dd_calibrate():
        key = 'calibrate_dd'
        DDF_LSM = "DDF_lsm.lsm.html"
        flagms_postcal_opts = {
         "create"  : True,
         "flag"    : "final_3gc_flags",
         "flagged-any": ["+L"],}

        for ms in mslist:
           mspref = ms.split('.ms')[0].replace('-','_')
           step = 'dd_calibrate_{0:s}'.format(mspref)
           recipe.add('cab/cubical', step, {
              "data-ms"           : ms,
              "data-column"       : "CORRECTED_DATA",
              "out-column"        : "SUBDD_DATA",
              "weight-column"     : "WEIGHT_SPECTRUM",
              "sol-jones"         : "DD",  # Jones terms to solve
              "sol-min-bl"        : 300,  # only solve for |uv| > 300 m
             # "g-type"            : "complex-diag",
             # "g-clip-high"       : 1.5,
             # "g-clip-low"        : 0.5,
             # "g-solvable"        : True,
             # "g-time-int"        : gsols[0],
             # "g-freq-int"        : gsols[1],
              "dist-ncpu"         :  dist_ncpu,
              "dist-nworker"      : 5,
              "model-beam-pattern": prefix+"'_$(corr)_$(reim).fits':output",
              "montblanc-feed-type": "linear",
              "model-beam-l-axis" : "px",
              "model-beam-m-axis" : "py",
             # "g-save-to"         : "g_final-cal_{0:s}.parmdb".format(mspref),
              "dd-save-to"        : "dd_cal_final_{0:s}.parmdb".format(mspref),
              "dd-type"           : "complex-diag",
              "dd-clip-high"      : 0.0,
              "dd-clip-low"       : 0.0,
              "dd-solvable"       : True,
              "dd-time-int"       : ddsols[0],
              "dd-freq-int"       : ddsols[1],
              "dd-dd-term"        : True,
              "dd-fix-dirs"       : "0",
              "out-subtract-dirs" : "1:",
              "model-list"        : [DDF_LSM+"@dE"],
              "out-name"          : prefix + "dE_sub",
              "out-mode"          : 'sr',
              "data-freq-chunk"   : 4*ddsols[1],
              "data-time-chunk"   : 4*ddsols[0],
              "sol-term-iters"    : "200",
              "madmax-enable"     : True,
              "madmax-plot"       : False,
              "out-plots"          : True,
              "madmax-threshold"  : [50, 40, 30, 20, 10],
              "madmax-global-threshold": [40, 30, 20, 10, 5],
              "madmax-estimate"   : "corr",
              "out-casa-gaintables" : True,
               },
               input=INPUT,
               output=OUTPUT,
               label='dd_calibrate_{0:s}:: Carry out DD calibration'.format(mspref))
           flagms_postcal_opts.update({"msname" : ms})
#           recipe.add("cab/flagms", "save_3gc_flags_{0:s}".format(mspref),flagms_postcal_opts,
#              input=INPUT,
#              output=OUTPUT,
#              label="save_3gc_flags_{0:s}:: Save 3GC flags".format(mspref))
             
    if pipeline.enable_task(config, 'init'):
       init()
    if pipeline.enable_task(config, 'restore'):
       restore()
    make_primary_beam()
    dd_precal_image()
    sfind_intrinsic()
    tag_sources()
    dd_calibrate()
    dd_postcal_image()

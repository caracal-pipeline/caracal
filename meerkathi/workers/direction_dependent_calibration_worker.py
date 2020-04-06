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
    niter = config['image_dd'].get('deconv-maxminoriter')
    colname = config['image_dd'].get('column')
    fit_spectral_pol = config['image_dd'].get('fit_spectral_pol')
    ddsols_t = config['calibrate_dd'].get('ddsols_time')
    ddsols_f = config['calibrate_dd'].get('ddsols_freq')
    dist_ncpu = config['calibrate_dd'].get('dist_ncpu')
    label = config.get('label')
    USEPB = config.get('use_pb')
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
    print("All_targets", all_targets)
    #print("All_msfiles", all_msfile)
    #print("ms_dict",ms_dict)
    if not os.path.exists(OUTPUT):
       os.mkdir(OUTPUT)
    de_sources_mode = config['calibrate_dd'].get('de_sources_mode')
    if de_sources_mode == 'manual' :
        de_targets =  config['calibrate_dd'].get('de_target_manual')
        de_sources =  config['calibrate_dd'].get('de_sources_manual')
        if len(de_targets)!=len(de_sources):
            meerkathi.log.error("The number of targets for de calibration does not match sources, please recheck, and Also, Kshitij can't haz the Snowleopard.")
            sys.exit(1)
        de_dict = dict(zip(de_targets, de_sources))
    else: 
        de_targets = all_targets
    
    print(de_targets)
    
    dd_image_opts = {
        "Data-MS"        : mslist,
        "Data-ColName"   : config['image_dd'].get('data_colname'),
        "Data-ChunkHours"       : config['image_dd'].get('data_chunkhours'),
        "Output-Mode"           : config['image_dd'].get('output_mode'),
        "Output-Name"    : prefix+"-DD-precal",
        "Output-Images"  : 'dmcri',
        "Image-NPix"     : npix,
        "Image-Cell"     : cell,
        "Facets-NFacets" : config['image_dd'].get('facets_nfacets'),
        "Weight-ColName" : config['image_dd'].get('weight_column'),
        "Weight-Mode"    : config['image_dd'].get('weight_mode'),
        "Weight-Robust"  : config['image_dd'].get('weight_robust'),
        "Freq-NBand"     : config['image_dd'].get('freq_nband'),
        "Freq-NDegridBand" : config['image_dd'].get('freq_ndegridband'),
        "Deconv-RMSFactor"      : config['image_dd'].get('deconv_rmsfactor'),
        "Deconv-PeakFactor"     : config['image_dd'].get('deconv_peakfactor'),
        "Deconv-Mode"       : config['image_dd'].get('deconv_mode'),
        "Deconv-MaxMinorIter"   : niter,
        "Deconv-Gain"          : config['image_dd'].get('deconv_gain'),
        "Deconv-FluxThreshold" : config['image_dd'].get('deconv_fluxthreshold'),
        "Deconv-AllowNegative": config['image_dd'].get('deconv_allownegative'),
        "Hogbom-PolyFitOrder": config['image_dd'].get('hogbom_polyfitorder'),
        "Parallel-NCPU" : config['image_dd'].get('parallel_ncpu'),
        "Predict-ColName"       : config['image_dd'].get("predict_colname"), 
        "Log-Memory"            : config['image_dd'].get("log_memory"),
        "Cache-Reset"           : config['image_dd'].get("cache_reset"),
        "Log-Boring"            : config["image_dd"].get("log_boring"),}

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
        outdir = field+"_ddcal"
        dd_ms_list = {"Data-MS" : ms_list}
        dd_imagename = {"Output-Name": image_prefix_precal+"-DD-precal"}
        dd_image_opts_precal.update(dd_imagename)
        dd_image_opts_precal.update(dd_ms_list)
        recipe.add("cab/ddfacet", "ddf_image_{0:s}".format(field), dd_image_opts_precal,
        input=INPUT,
        output=OUTPUT+"/"+outdir,
        shared_memory="500gb",
        label="ddf_image_{0:s}:: Primary beam corrected image".format(field))
        recipe.run()
        recipe.jobs = []
    def dd_postcal_image(field,ms_list):
        dd_image_opts_postcal = copy.deepcopy(dd_image_opts)
        outdir = field+"_ddcal"
        image_prefix_postcal = "/"+outdir+"/"+prefix+"_"+field
        dd_ms_list = {"Data-MS" : ms_list}
        dd_imagename = {"Output-Name": image_prefix_postcal+"-DD-postcal"}
        dd_imagecol = {"Data-ColName": "SUBDD_DATA"}
        dd_beamopts = {"Beam-Model": "FITS", "Beam-FITSFile":prefix+"'_$(corr)_$(reim).fits':output", "Beam-FITSLAxis": 'px', "Beam-FITSMAxis":"py", "Output-Images": 'dmcriDMCRI'}
        dd_image_opts_postcal.update(dd_ms_list)
        dd_image_opts_postcal.update(dd_imagename)
        dd_image_opts_postcal.update(dd_imagecol)
        if USEPB:
            dd_image_opts_postcal.update(dd_beamopts)

        recipe.add("cab/ddfacet", "ddf_image_postcal_{0:s}".format(field), dd_image_opts_postcal,
        input=INPUT,
        output=OUTPUT,
        label="ddf_image_postcal_{0:s}:: Primary beam corrected image".format(field),
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
        de_sources_mode = config[key].get('de_sources_mode')
        print("de_sources_mode:", de_sources_mode)
       # if usepb:
       #    model_cube = prefix+"-DD-precal.cube.int.model.fits"
       # else: 
       #    model_cube = prefix+"-DD-precal.cube.app.model.fits"
        outdir = field+"_ddcal"
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
              output=OUTPUT+"/"+outdir,label='tag_sources_auto_mode::Tag dE sources with CatDagger')

        if de_sources_mode == 'manual':
           img = prefix+"_"+field+"-DD-precal.app.restored.fits"
           imagefile = os.path.join(pipeline.output,DD_DIR,outdir,img)
           #print("Imagefile",imagefile)
           #print("Pipeline output", pipeline.output)
           w = WCS(imagefile)
           #coords =  config[key].get('de_sources_manual')
           print(de_dict)
           sources_to_tag = de_dict[field.replace("_","-")]
           reg = []
           for j in range(len(sources_to_tag.split(";"))):
               coords = sources_to_tag.split(";")[j]
               size = coords.split(",")[2]
               coords_str = coords.split(",")[0]+" "+coords.split(",")[1] 
               #print("Coordinate String", coords_str)
               centre = SkyCoord(coords_str, unit='deg') 
               separation = int(size) * u.arcsec 
               #print("Size",separation)
               xlist = []
               ylist = []
               for i in range(5):
                 ang_sep = (306/5)*i*u.deg
                 p = centre.directional_offset_by(ang_sep,separation) 
                 pix = PixCoord.from_sky(p,w)
                 xlist.append(pix.x)
                 ylist.append(pix.y)
               vertices = PixCoord(x=xlist, y=ylist)
               region_dd = PolygonPixelRegion(vertices=vertices)
               reg.append(region_dd)
           regfile = "de-{0:s}.reg".format(field)
           ds9_file = os.path.join(OUTPUT,outdir,regfile)
           write_ds9(reg,ds9_file,coordsys='physical') 

    def dd_calibrate(field,mslist):
        key = 'calibrate_dd'
        outdir = field+"_ddcal"
        dicomod = prefix+"_"+field+"-DD-precal.DicoModel"
        dereg = "de-{0:s}.reg".format(field)
        for ms in mslist:
           mspref = ms.split('.ms')[0].replace('-','_')
           step = 'dd_calibrate_{0:s}_{1:s}'.format(mspref,field)
           recipe.add('cab/cubical', step, {
              "data-ms"           : ms,
              "data-column"       : "CORRECTED_DATA",
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
              "data-freq-chunk"   : 1*ddsols_f,
              "data-time-chunk"   : 1*ddsols_t,
              "sol-term-iters"    : "[50,90,50,90]",
              "madmax-plot"       : False,
              "out-plots"          : True,
              #"madmax-enable"     : config[key].get('madmax_enable'),
              #"madmax-threshold"  : config[key].get('madmax_threshold'),
              #"madmax-global-threshold": config[key].get('madmax_global_threshold'),
              #"madmax-estimate"   : "corr",
              #"out-casa-gaintables" : True,
              "degridding-NDegridBand": int(nchans/2.0),
              'degridding-MaxFacetSize': 0.15,
               },
               input=INPUT,
               output=OUTPUT+"/"+outdir,
               shared_memory="400gb",
               label='dd_calibrate_{0:s}_{1:s}:: Carry out DD calibration'.format(mspref,field))

    def cp_data_column(field,mslist):
        outdir = field+"_ddcal"
        for ms in mslist:
           mspref = ms.split('.ms')[0].replace('-','_')
           step = 'cp_datacol_{0:s}_{1:s}'.format(mspref,field)
           recipe.add('cab/msutils', step, {
               "command" : 'copycol',
               "msname"  : ms,
               "fromcol" : 'SUBDD_DATA',
               "tocol"   : 'CORRECTED_DATA',
                              },
               input=INPUT,
               output=OUTPUT+"/"+outdir,
               label='cp_datacol_{0:s}_{1:s}:: Copy SUBDD_DATA to CORRECTED_DATA'.format(mspref,field))
      
    def img_wsclean(mslist,field):
        key='image_wsclean'
        outdir = field+"_ddcal"
        imweight = config[key].get('img_ws_weight')
        pref = "DD_wsclean"
        for ms in mslist:
           mspref = ms.split('.ms')[0].replace('-','_')
           step = 'img_wsclean_{0:s}_{1:s}'.format(mspref,field)
           recipe.add('cab/wsclean', step, {
               "msname": mslist,
               "column": config[key].get('img_ws_column'),
               "weight": imweight if not imweight == 'briggs' else 'briggs {}'.format(config[key].get('img_ws_robust')),
               "nmiter": sdm.dismissable(config[key].get('img_ws_nmiter')),
               "npix": config[key].get('img_ws_npix'),
               "padding": config[key].get('img_ws_padding'),
               "scale": config[key].get('img_ws_cell', cell),
               "prefix": '{0:s}_{1:s}'.format(pref, field),
               "niter": config[key].get('img_ws_niter'),
               "mgain": config[key].get('img_ws_mgain'),
               "pol": config[key].get('img_ws_pol'),
               "taper-gaussian": sdm.dismissable(config[key].get('img_ws_uvtaper')),
               "channelsout": config[key].get('img_ws_nchans'),
               "joinchannels": config[key].get('img_ws_joinchannels'),
               "local-rms": config[key].get('img_ws_local_rms'),
               "fit-spectral-pol": config[key].get('img_ws_fit_spectral_pol'),
               "auto-threshold": config[key].get('img_ws_auto_threshold'),
               "auto-mask": config[key].get('img_ws_auto_mask'),
               "multiscale": config[key].get('img_ws_multi_scale'),
               "multiscale-scales": sdm.dismissable(config[key].get('img_ws_multi_scale_scales')),
               "savesourcelist": True if config[key].get('img_ws_niter')>0 else False,
             },
               input=INPUT,
               output=OUTPUT+"/"+outdir,
               label='img_wsclean_{0:s}_{1:s}:: Image DD-calibrated data with WSClean'.format(mspref,field))

    def run_crystalball(mslist,field):
        key='transfer_model_dd'
        outdir = field+"_ddcal"
        pref = "DD_wsclean"
        crystalball_model = '{0:s}_{1:s}-sources.txt'.format(pref, field)
        for ms in mslist:
           mspref = ms.split('.ms')[0].replace('-','_')
           step = 'run_crystalball_{0:s}_{1:s}'.format(mspref,field)
           recipe.add('cab/crystalball', step, {
               "ms": ms,
               "sky-model": crystalball_model+':output',
               "spectra": config[key].get('dd_spectra'),
               "row-chunks": config[key].get('dd_row_chunks'),
               "model-chunks": config[key].get('dd_model_chunks'),
               "exp-sign-convention": config[key].get('dd_exp_sign_convention'),
               "within": sdm.dismissable(config[key].get('dd_within') or None),
               "points-only": config[key].get('dd_points_only'),
               "num-sources": sdm.dismissable(config[key].get('dd_num_sources')),
               "num-workers": sdm.dismissable(config[key].get('dd_num_workers')),
               "memory-fraction": config[key].get('dd_memory_fraction'),
             },
               input=INPUT,
               output=OUTPUT+"/"+outdir,
               label='run_crystalball_{0:s}_{1:s}:: Run Crystalball'.format(mspref,field))

    for target in de_targets:
       mslist = ms_dict[target]
       field = utils.filter_name(target)
       print("Processing field",field,"for de calibration:")
#       print(mslist)
#       print(field)
       if USEPB:
          make_primary_beam()
       if pipeline.enable_task(config,'image_dd'):
          dd_precal_image(field,mslist)
    #sfind_intrinsic()
       dagga(field)
       if pipeline.enable_task(config,'calibrate_dd'):
          dd_calibrate(field,mslist)
       if pipeline.enable_task(config,'image_dd'):
          dd_postcal_image(field,mslist)
       if pipeline.enable_task(config, 'copy_data'):
          cp_data_column(field,mslist)
       if pipeline.enable_task(config, 'image_wsclean'):
          img_wsclean(mslist,field)
       if pipeline.enable_task(config,'transfer_model_dd'):
          run_crystalball(mslist,field)


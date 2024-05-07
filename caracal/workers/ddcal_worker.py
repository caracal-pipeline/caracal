# -*- coding: future_fstrings -*-
import os
import copy
import numpy as np
import sys
import caracal
import stimela.dismissable as sdm
from caracal.dispatch_crew import utils
from stimela.pathformatter import pathformatter as spf
from caracal.utils.requires import extras

NAME = 'Direction-dependent Calibration'
LABEL = "ddcal"


@extras(packages=["astropy", "regions"])
def worker(pipeline, recipe, config):
    from astropy.coordinates import SkyCoord
    from astropy import units as u
    from astropy.wcs import WCS
    from regions import PixCoord, Regions, PolygonPixelRegion
    npix = config['image_dd']['npix']
    cell = config['image_dd']['cell']
    use_mask = config['image_dd']['use_mask']
    ddsols_t = config['calibrate_dd']['dd_dd_timeslots_int']
    ddsols_f = config['calibrate_dd']['dd_dd_chan_int']
    dist_ncpu = config['calibrate_dd']['dist_ncpu']
    label = config['label_in']
    USEPB = config['use_pb']

    prefix = pipeline.prefix
    INPUT = pipeline.input
    DD_DIR = "3GC"
    OUTPUT = os.path.join(pipeline.output, DD_DIR)
    DDF_LSM = "DDF_lsm.lsm.html"
    shared_mem = str(config['shared_mem']) + 'gb'
    all_targets, all_msfile, ms_dict = pipeline.get_target_mss(label)
    caracal.log.info("All_targets", all_targets)
    caracal.log.info("All_msfiles", all_msfile)

    if not os.path.exists(OUTPUT):
        os.mkdir(OUTPUT)
    de_sources_mode = config['calibrate_dd']['de_sources_mode']

    if de_sources_mode == 'manual':
        de_targets = config['calibrate_dd']['de_target_manual']
        de_sources = config['calibrate_dd']['de_sources_manual']
        if len(de_targets) != len(de_sources):
            caracal.log.error("The number of targets for de calibration does not match sources, please recheck.")
            sys.exit(1)
        de_dict = dict(zip(de_targets, de_sources))
    else:
        de_targets = all_targets

    dd_image_opts = {
        "Data-MS": all_msfile,
        "Data-ColName": config['image_dd']['data_colname'],
        "Data-ChunkHours": config['image_dd']['data_chunkhours'],
        "Output-Mode": config['image_dd']['output_mode'],
        "Output-Name": prefix + "-DD-precal",
        "Output-Images": 'dmcrioekzp',
        "Image-NPix": npix,
        "Image-Cell": cell,
        "Facets-NFacets": config['image_dd']['facets_nfacets'],
        "Weight-ColName": config['image_dd']['weight_col'],
        "Weight-Mode": config['image_dd']['weight_mode'],
        "Weight-Robust": config['image_dd']['weight_robust'],
        "Freq-NBand": config['image_dd']['freq_nband'],
        "Freq-NDegridBand": config['image_dd']['freq_ndegridband'],
        "Deconv-RMSFactor": config['image_dd']['deconv_rmsfactor'],
        "Deconv-PeakFactor": config['image_dd']['deconv_peakfactor'],
        "Deconv-Mode": config['image_dd']['deconv_mode'],
        "Deconv-MaxMinorIter": config['image_dd']['deconv_maxminoriter'],
        "Deconv-Gain": config['image_dd']['deconv_gain'],
        "Deconv-FluxThreshold": config['image_dd']['deconv_fluxthr'],
        "Deconv-AllowNegative": config['image_dd']['deconv_allownegative'],
        "Hogbom-PolyFitOrder": config['image_dd']['hogbom_polyfitorder'],
        "Parallel-NCPU": config['image_dd']['parallel_ncpu'],
        "Predict-ColName": config['image_dd']["predict_colname"],
        "Log-Memory": config['image_dd']["log_memory"],
        "Cache-Reset": config['image_dd']["cache_reset"],
        "Log-Boring": config["image_dd"]["log_boring"], }

    def make_primary_beam():
        eidos_opts = {
            "prefix": prefix,
            "pixels": 256,
            "freq": "850 1715 30",
            "diameter": 4.0,
            "coeff": 'me',
            "coefficients-file": "meerkat_beam_coeffs_em_zp_dct.npy", }

        recipe.add("cab/eidos", "make-pb", eidos_opts,
                   input=INPUT,
                   output=OUTPUT,
                   label="make-pb:: Generate primary beams from Eidos",
                   shared_memory=shared_mem)

    def dd_precal_image(field, ms_list):
        dd_image_opts_precal = copy.deepcopy(dd_image_opts)
        outdir = field + "_ddcal/"
        image_prefix_precal = "/" + outdir + "/" + prefix + "_" + field  # Add the output subdirectory to the imagename
        dd_ms_list = {"Data-MS": ms_list}
        dd_image_opts_precal.update(dd_ms_list)
        if (use_mask):
            dd_imagename = {"Output-Name": image_prefix_precal + "-DD-masking"}  # Add the mask image prefix
            dd_image_opts_precal.update(dd_imagename)
            recipe.add("cab/ddfacet", "ddf_image-for_mask-{0:s}".format(field), dd_image_opts_precal,
                       input=INPUT,
                       output=OUTPUT,
                       label="ddf_image-for_mask-{0:s}:: DDFacet image for masking".format(field),
                       shared_memory=shared_mem)

            imname = '{0:s}{1:s}.app.restored.fits'.format(image_prefix_precal, "-DD-masking")
            output_folder = "/" + outdir
            recipe.add("cab/cleanmask", "mask_ddf-precal-{0:s}".format(field), {
                'image': '{0:s}:output'.format(imname),
                'output': '{0:s}mask_ddf_precal_{1:s}.fits'.format(output_folder, field),
                'sigma': config['image_dd']['mask_sigma'],
                'boxes': config['image_dd']['mask_boxes'],
                'iters': config['image_dd']['mask_niter'],
                'overlap': config['image_dd']['mask_overlap'],
                'no-negative': True,
                'tolerance': config['image_dd']['mask_tol'],
            }, input=INPUT, output=OUTPUT, label='mask_ddf-precal-{0:s}:: Make a mask for the initial ddf image'.format(field), shared_memory=shared_mem)
            recipe.run()
            recipe.jobs = []
        dd_imagename = {"Output-Name": image_prefix_precal + "-DD-precal"}
        dd_image_opts_precal.update(dd_imagename)
        if use_mask:
            dd_maskopt = {"Mask-External": "{0:s}mask_ddf_precal_{1:s}.fits:output".format(output_folder, field)}
            dd_image_opts_precal.update(dd_maskopt)
        recipe.add("cab/ddfacet", "ddf_image-{0:s}".format(field), dd_image_opts_precal,
                   input=INPUT,
                   output=OUTPUT,
                   label="ddf_image-{0:s}:: DDFacet initial image for DD calibration".format(field), shared_memory=shared_mem)
        recipe.run()
        recipe.jobs = []

    def dd_postcal_image(field, ms_list):
        dd_image_opts_postcal = copy.deepcopy(dd_image_opts)
        outdir = field + "_ddcal/"
        image_prefix_postcal = "/" + outdir + "/" + prefix + "_" + field
        dd_ms_list = {"Data-MS": ms_list}
        dd_image_opts_postcal.update(dd_ms_list)
        caracal.log.info("Imaging", ms_list)
        postcal_datacol = config['image_dd']['data_colname_postcal']
        dd_imagecol = {"Data-ColName": postcal_datacol}
        dd_image_opts_postcal.update(dd_imagecol)
        if (use_mask):
            dd_imagename = {"Output-Name": image_prefix_postcal + "-DD-masking"}
            dd_image_opts_postcal.update(dd_imagename)
            recipe.add("cab/ddfacet", "ddf_image-postcal-{0:s}".format(field), dd_image_opts_postcal,
                       input=INPUT,
                       output=OUTPUT,
                       label="ddf_image-postcal-{0:s}:: Primary beam corrected image".format(field),
                       shared_memory=shared_mem)
            imname = '{0:s}{1:s}.app.restored.fits'.format(image_prefix_postcal, "-DD-masking")
            output_folder = "/" + outdir
            recipe.add("cab/cleanmask", "mask_ddf-postcal-{0:s}".format(field), {
                'image': '{0:s}:output'.format(imname),
                'output': '{0:s}mask_ddf_postcal_{1:s}.fits:output'.format(output_folder, field),
                'sigma': config['image_dd']['mask_sigma'],
                'boxes': config['image_dd']['mask_boxes'],
                'iters': config['image_dd']['mask_niter'],
                'overlap': config['image_dd']['mask_overlap'],
                'no-negative': True,
                'tolerance': config['image_dd']['mask_tol'],
            }, input=INPUT, output=OUTPUT, label='mask_ddf-postcal-{0:s}:: Make a mask for the initial ddf image'.format(field), shared_memory=shared_mem)
            recipe.run()
            recipe.jobs = []

        dd_imagename = {"Output-Name": image_prefix_postcal + "-DD-postcal"}
        dd_image_opts_postcal.update(dd_imagename)

        if use_mask:
            dd_maskopt = {"Mask-External": "{0:s}mask_ddf_postcal_{1:s}.fits:output".format(output_folder, field)}
            dd_image_opts_postcal.update(dd_maskopt)

        dd_beamopts = {"Beam-Model": "FITS", "Beam-FITSFile": prefix + "'_$(corr)_$(reim).fits':output", "Beam-FITSLAxis": 'px', "Beam-FITSMAxis": "py", "Output-Images": 'dmcriDMCRIPMRIikz'}
        if USEPB:
            dd_image_opts_postcal.update(dd_beamopts)

        recipe.add("cab/ddfacet", "ddf_image-postcal-{0:s}".format(field), dd_image_opts_postcal,
                   input=INPUT,
                   output=OUTPUT,
                   label="ddf_image-postcal-{0:s}:: Primary beam corrected image".format(field),
                   shared_memory=shared_mem)

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
        # make a skymodel with only dE taggable sources.
        # de_only_model = 'de-only-model.txt'
        de_sources_mode = config[key]['de_sources_mode']
        print("de_sources_mode:", de_sources_mode)
        # if usepb:
        #    model_cube = prefix+"-DD-precal.cube.int.model.fits"
        # else:
        #    model_cube = prefix+"-DD-precal.cube.app.model.fits"
        outdir = field + "_ddcal"
        if de_sources_mode == 'auto':
            caracal.log.info("Carrying out automatic source taggig for direction dependent calibration")
            caracal.log.info('Carrying out automatic dE tagging')

            catdagger_opts = {
                "ds9-reg-file": "de-{0:s}.reg:output".format(field),
                "ds9-tag-reg-file": "de-clusterleads-{0:s}.reg:output".format(field),
                "noise-map": prefix + "_" + field + "-DD-precal.app.residual.fits",
                "sigma": config[key]['sigma'],
                "min-distance-from-tracking-centre": config[key]['min_dist_from_phcentre'],
            }

            recipe.add('cab/catdagger', 'tag_sources-auto_mode', catdagger_opts, input=INPUT,
                       output=OUTPUT + "/" + outdir, label='tag_sources-auto_mode::Tag dE sources with CatDagger', shared_memory=shared_mem)

        if de_sources_mode == 'manual':
            img = prefix + "_" + field + "-DD-precal.app.restored.fits"
            imagefile = os.path.join(pipeline.output, DD_DIR, outdir, img)
            # print("Imagefile",imagefile)
            # print("Pipeline output", pipeline.output)
            w = WCS(imagefile)
            # coords =  config[key]['de_sources_manual']
            print(de_dict)
            sources_to_tag = de_dict[field.replace("_", "-")]
            reg = []
            for j in range(len(sources_to_tag.split(";"))):
                coords = sources_to_tag.split(";")[j]
                size = coords.split(",")[2]
                coords_str = coords.split(",")[0] + " " + coords.split(",")[1]
                # print("Coordinate String", coords_str)
                centre = SkyCoord(coords_str, unit='deg')
                separation = int(size) * u.arcsec
                # print("Size",separation)
                xlist = []
                ylist = []
                for i in range(5):
                    ang_sep = (306 / 5) * i * u.deg
                    p = centre.directional_offset_by(ang_sep, separation)
                    pix = PixCoord.from_sky(p, w)
                    xlist.append(pix.x)
                    ylist.append(pix.y)
                vertices = PixCoord(x=xlist, y=ylist)
                region_dd = PolygonPixelRegion(vertices=vertices)
                reg.append(region_dd)
            regfile = "de-{0:s}.reg".format(field)
            ds9_file = os.path.join(OUTPUT, outdir, regfile)
            Regions(reg).write(ds9_file, format='ds9', overwrite=True)

    def dd_calibrate(field, mslist):
        key = 'calibrate_dd'
        outdir = field + "_ddcal"
        dicomod = prefix + "_" + field + "-DD-precal.DicoModel"
        dereg = "de-{0:s}.reg".format(field)
        output_cubical = OUTPUT + "/" + outdir
        test_path = spf("MODEL_DATA")
        for ms in mslist:
            mspref = os.path.splitext(ms)[0].replace('-', '_')
            step = 'dd_calibrate-{0:s}-{1:s}'.format(mspref, field)
            recipe.add('cab/cubical_ddf', step, {
                "data-ms": ms,
                "data-column": config[key]['dd_data_col'],
                "out-column": config[key]['dd_out_data_col'],
                "weight-column": config[key]['dd_weight_col'],
                "sol-jones": "G,DD",  # Jones terms to solve
                "sol-min-bl": config[key]['sol_min_bl'],  # only solve for |uv| > 300 m
                "sol-stall-quorum": config[key]['dd_sol_stall_quorum'],
                "g-type": config[key]['dd_g_type'],
                "g-clip-high": config[key]['dd_g_clip_high'],
                "g-clip-low": config[key]['dd_g_clip_low'],
                "g-solvable": True,
                "g-update-type": config[key]['dd_g_update_type'],
                "g-max-prior-error": config[key]['dd_g_max_prior_error'],
                "dd-max-prior-error": config[key]['dd_dd_max_prior_error'],
                "g-max-post-error": config[key]['dd_g_max_post_error'],
                "dd-max-post-error": config[key]['dd_dd_max_post_error'],
                "g-time-int": config[key]['dd_g_timeslots_int'],
                "g-freq-int": config[key]['dd_g_chan_int'],
                "dist-ncpu": config[key]['dist_ncpu'],
                "dist-nworker": config[key]['dist_nworker'],
                "dist-max-chunks": config[key]['dist_nworker'],
                "dist-max-chunks": config[key]['dist_nworker'],
                #  "model-beam-pattern": prefix+"'_$(corr)_$(reim).fits':output",
                #  "montblanc-feed-type": "linear",
                #  "model-beam-l-axis" : "px",
                #  "model-beam-m-axis" : "py",
                "g-save-to": "g_final-cal_{0:s}_{1:s}.parmdb".format(mspref, field),
                "dd-save-to": "dd_cal_final_{0:s}_{1:s}.parmdb".format(mspref, field),
                "dd-type": "complex-2x2",
                "dd-clip-high": 0.0,
                "dd-clip-low": 0.0,
                "dd-solvable": True,
                "dd-time-int": ddsols_t,
                "dd-freq-int": ddsols_f,
                "dd-dd-term": True,
                "dd-prop-flags": 'always',
                "dd-fix-dirs": "0",
                "out-subtract-dirs": "1:",
                "model-list": spf("MODEL_DATA+-{{}}{}@{{}}{}:{{}}{}@{{}}{}".format(dicomod, dereg, dicomod, dereg), "output", "output", "output", "output"),
                "out-name": prefix + "dE_sub",
                "out-mode": 'sr',
                "out-model-column": "MODEL_OUT",
                # "data-freq-chunk"   : 1*ddsols_f,
                # "data-time-chunk"   : 1*ddsols_t,
                "data-time-chunk": ddsols_t * int(min(1, config[key]['dist_nworker'])) if (ddsols_f == 0 or config[key]['dd_g_chan_int'] == 0) else ddsols_t * int(min(1, np.sqrt(config[key]['dist_nworker']))),
                "data-freq-chunk": 0 if (ddsols_f == 0 or config[key]['dd_g_chan_int'] == 0) else ddsols_f * int(min(1, np.sqrt(config[key]['dist_nworker']))),
                "sol-term-iters": "[50,90,50,90]",
                "madmax-plot": False,
                "out-plots": True,
                "madmax-enable": config[key]['madmax_enable'],
                "madmax-threshold": config[key]['madmax_thr'],
                "madmax-global-threshold": config[key]['madmax_global_thr'],
                "madmax-estimate": "corr",
                # "out-casa-gaintables" : True,
                "degridding-NDegridBand": config['image_dd']['freq_ndegridband'],
                'degridding-MaxFacetSize': 0.15,
            },
                input=INPUT,
                # output=OUTPUT+"/"+outdir,
                output=output_cubical,
                shared_memory=shared_mem,
                label='dd_calibrate-{0:s}-{1:s}:: Carry out DD calibration'.format(mspref, field))

    def cp_data_column(field, mslist):
        outdir = field + "_ddcal"
        for ms in mslist:
            mspref = os.path.splitext(ms)[0].replace('-', '_')
            step = 'cp_datacol-{0:s}-{1:s}'.format(mspref, field)
            recipe.add('cab/msutils', step, {
                "command": 'copycol',
                "msname": ms,
                "fromcol": 'SUBDD_DATA',
                "tocol": 'CORRECTED_DATA',
            },
                input=INPUT,
                output=OUTPUT + "/" + outdir,
                label='cp_datacol-{0:s}-{1:s}:: Copy SUBDD_DATA to CORRECTED_DATA'.format(mspref, field), shared_memory=shared_mem)

    def img_wsclean(mslist, field):
        key = 'image_wsclean'
        outdir = field + "_ddcal"
        imweight = config[key]['img_ws_weight']
        pref = "DD_wsclean"
        mspref = os.path.splitext(mslist[0])[0].replace('-', '_')
        step = 'img_wsclean-{0:s}-{1:s}'.format(mspref, field)
        recipe.add('cab/wsclean', step, {
            "msname": mslist,
            "column": config[key]['img_ws_col'],
            "weight": imweight if not imweight == 'briggs' else 'briggs {}'.format(config[key]['img_ws_robust']),
            "nmiter": sdm.dismissable(config[key]['img_ws_nmiter']),
            "npix": config[key]['img_ws_npix'],
            "padding": config[key]['img_ws_padding'],
            "scale": config[key]['img_ws_cell'],
            "prefix": '{0:s}_{1:s}'.format(pref, field),
            "niter": config[key]['img_ws_niter'],
            "mgain": config[key]['img_ws_mgain'],
            "pol": config[key]['img_ws_stokes'],
            "taper-gaussian": sdm.dismissable(config[key]['img_ws_uvtaper']),
            "channelsout": config[key]['img_ws_nchans'],
            "joinchannels": config[key]['img_ws_joinchans'],
            "local-rms": config[key]['img_ws_local_rms'],
            "fit-spectral-pol": config[key]['img_ws_specfit_nrcoeff'],
            "auto-threshold": config[key]['img_ws_auto_thr'],
            "auto-mask": config[key]['img_ws_auto_mask'],
            "multiscale": config[key]['img_ws_multi_scale'],
            "multiscale-scales": sdm.dismissable(config[key]['img_ws_multi_scale_scales']),
            "savesourcelist": True if config[key]['img_ws_niter'] > 0 else False,
        },
            input=INPUT,
            output=OUTPUT + "/" + outdir,
            version='2.6' if config[key]['img_ws_multi_scale'] else None,
            label='img_wsclean-{0:s}-{1:s}:: Image DD-calibrated data with WSClean'.format(mspref, field), shared_memory=shared_mem)

    def run_crystalball(mslist, field):
        key = 'transfer_model_dd'
        outdir = field + "_ddcal"
        pref = "DD_wsclean"
        crystalball_model = '{0:s}_{1:s}-sources.txt'.format(pref, field)
        for ms in mslist:
            mspref = os.path.splitext(ms)[0].replace('-', '_')
            step = 'crystalball-{0:s}-{1:s}'.format(mspref, field)
            recipe.add('cab/crystalball', step, {
                "ms": ms,
                "sky-model": crystalball_model + ':output',
                "row-chunks": config[key]['dd_row_chunks'],
                "model-chunks": config[key]['dd_model_chunks'],
                "within": sdm.dismissable(config[key]['dd_within'] or None),
                "points-only": config[key]['dd_points_only'],
                "num-sources": sdm.dismissable(config[key]['dd_num_sources']),
                "num-workers": sdm.dismissable(config[key]['dd_num_workers']),
                "memory-fraction": config[key]['dd_mem_frac'],
            },
                input=INPUT,
                output=OUTPUT + "/" + outdir, shared_memory=shared_mem,
                label='crystalball-{0:s}-{1:s}:: Run Crystalball'.format(mspref, field))

    for target in de_targets:
        mslist = ms_dict[target]
        field = utils.filter_name(target)
        caracal.log.info("Processing field", field, "for de calibration:")
        if USEPB:
            make_primary_beam()
        if pipeline.enable_task(config, 'image_dd'):
            dd_precal_image(field, mslist)

        dagga(field)
        if pipeline.enable_task(config, 'calibrate_dd'):
            dd_calibrate(field, mslist)
        if pipeline.enable_task(config, 'image_dd'):
            dd_postcal_image(field, mslist)
        if pipeline.enable_task(config, 'copy_data'):
            cp_data_column(field, mslist)
        if pipeline.enable_task(config, 'image_wsclean'):
            img_wsclean(mslist, field)
        if pipeline.enable_task(config, 'transfer_model_dd'):
            run_crystalball(mslist, field)

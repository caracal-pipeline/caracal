import os
import shutil
import glob
import sys
import yaml
import json
import re
import copy
import caracal
import numpy as np
import stimela.dismissable as sdm
from caracal.dispatch_crew import utils
from astropy.io import fits as fits
from stimela.pathformatter import pathformatter as spf
from typing import Any
from caracal.workers.utils import manage_flagsets as manflags
import psutil
#import equolver.beach as beach

NAME = 'Polarization Imaging'
LABEL = 'polimg'


def get_dir_path(string, pipeline):
    return string.split(pipeline.output)[1][1:]


def worker(pipeline, recipe, config):
    wname = pipeline.CURRENT_WORKER
    flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
    flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
    rewind_main_ms = config['rewind_flags']["enable"] and (config['rewind_flags']['mode'] == 'reset_worker' or config['rewind_flags']["version"] != 'null')
    taper = str(config['img_taper'])
    beam = float(config['img_beam'])
    maxuvl = config['img_maxuv_l']
    transuvl = maxuvl * config['img_transuv_l'] / 100.
    multiscale = config['img_multiscale']
    multiscale_scales = config['img_multiscale_scales']
    if taper == '':
        taper = None
    if beam == '':
        beam = None
    label = config['label_in']
    min_uvw = config['minuvw_m']
    ncpu = config['ncpu']
    if ncpu == 0:
        ncpu = psutil.cpu_count()
    else:
        ncpu = min(ncpu, psutil.cpu_count())
    #nwlayers_factor = config['img_nwlayers_factor']
    nrdeconvsubimg = ncpu if config['img_nrdeconvsubimg'] == 0 else config['img_nrdeconvsubimg']
    if nrdeconvsubimg == 1:
        wscl_parallel_deconv = None
    else:
        wscl_parallel_deconv = int(np.ceil(config['img_npix'] / np.sqrt(nrdeconvsubimg)))

    #mfsprefix = ["", '-MFS'][int(config['img_nchans'] > 1)]
    all_targets, all_msfile, ms_dict = pipeline.get_target_mss(label)

    i = 0
    for i, m in enumerate(all_msfile):
        # check whether all ms files to be used exist
        if not os.path.exists(os.path.join(pipeline.msdir, m)):
            raise IOError(
                "MS file {0:s} does not exist. Please check that it is where it should be.".format(m))

        # Write/rewind flag versions only if flagging tasks are being
        # executed on these .MS files, or if the user asks to rewind flags
        if rewind_main_ms:
            available_flagversions = manflags.get_flags(pipeline, m)
            if rewind_main_ms:
                if config['rewind_flags']['mode'] == 'reset_worker':
                    version = flags_before_worker
                    stop_if_missing = False
                elif config['rewind_flags']['mode'] == 'rewind_to_version':
                    version = config['rewind_flags']['version']
                    if version == 'auto':
                        version = flags_before_worker
                    stop_if_missing = True
                if version in available_flagversions:
                    if flags_before_worker in available_flagversions and available_flagversions.index(
                            flags_before_worker) < available_flagversions.index(version) and not config[
                        'overwrite_flagvers']:
                        manflags.conflict('rewind_too_little', pipeline, wname, m, config, flags_before_worker,
                                          flags_after_worker)
                    substep = 'version-{0:s}-ms{1:d}'.format(version, i)
                    manflags.restore_cflags(pipeline, recipe, version, m, cab_name=substep)
                    if version != available_flagversions[-1]:
                        substep = 'delete-flag_versions-after-{0:s}-ms{1:d}'.format(version, i)
                        manflags.delete_cflags(pipeline, recipe,
                                               available_flagversions[available_flagversions.index(version) + 1],
                                               m, cab_name=substep)
                    if version != flags_before_worker:
                        substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                        manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                            m, cab_name=substep, overwrite=config['overwrite_flagvers'])
                elif stop_if_missing:
                    manflags.conflict('rewind_to_non_existing', pipeline, wname, m, config, flags_before_worker,
                                      flags_after_worker)
                # elif flag_main_ms:
                #     substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                #     manflags.add_cflags(pipeline, recipe, flags_before_worker,
                #         m, cab_name=substep, overwrite=config['overwrite_flagvers'])
            else:
                if flags_before_worker in available_flagversions and not config['overwrite_flagvers']:
                    manflags.conflict('would_overwrite_bw', pipeline, wname, m, config, flags_before_worker,
                                      flags_after_worker)
                else:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                        m, cab_name=substep, overwrite=config['overwrite_flagvers'])

    i += 1
    prefix = pipeline.prefix

    # rename single stokes fits files
    def rename_single_stokes(img_dir, field, stokes):
        posname = '{0:s}/{1:s}/{2:s}_{3:s}'.format(pipeline.output, img_dir, prefix, field)
        llist = list(set(glob.glob('{0:s}_{1:s}'.format(posname, '*psf.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*I-psf.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*Q-psf.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*U-psf.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*V-psf.fits'))))
        for fname in llist:
            os.rename(fname, fname[:-8] + stokes + "-psf.fits")
        llist = list(set(glob.glob('{0:s}_{1:s}'.format(posname, '*dirty.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*I-dirty.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*Q-dirty.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*U-dirty.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*V-dirty.fits'))))
        for fname in llist:
            os.rename(fname, fname[:-10] + stokes + "-dirty.fits")
        llist = list(set(glob.glob('{0:s}_{1:s}'.format(posname, '*image.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*I-image.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*Q-image.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*U-image.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*V-image.fits'))))
        for fname in llist:
            os.rename(fname, fname[:-10] + stokes + "-image.fits")
        llist = list(set(glob.glob('{0:s}_{1:s}'.format(posname, '*model.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*I-model.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*Q-model.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*U-model.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*V-model.fits'))))
        for fname in llist:
            os.rename(fname, fname[:-10] + stokes + "-model.fits")
        llist = list(set(glob.glob('{0:s}_{1:s}'.format(posname, '*residual.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*I-residual.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*Q-residual.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*U-residual.fits'))) -
                     set(glob.glob('{0:s}_{1:s}'.format(posname, '*V-residual.fits'))))
        for fname in llist:
            os.rename(fname, fname[:-13] + stokes + "-residual.fits")

    def image(img_dir, mslist, field):
        caracal.log.info("Number of threads used by WSClean for gridding:")
        caracal.log.info(ncpu)
        step = 'image-pol'
        image_opts = {
            "msname": mslist,
            "column": config['col'],
            "weight": config['img_weight'] if not config['img_weight'] == 'briggs' else 'briggs {}'.format(
                config['img_robust']),
            "nmiter": sdm.dismissable(config['img_nmiter']),
            "npix": config['img_npix'],
            "padding": config['img_padding'],
            "scale": config['img_cell'],
            "prefix": '{0:s}/{1:s}_{2:s}'.format(img_dir, prefix, field),
            "niter": config['img_niter'],
            "gain": config["img_gain"],
            "mgain": config['img_mgain'],
            "pol": config['img_stokes'],
            "channelsout": config['img_nchans'],
            "joinchannels": config['img_joinchans'],
            "squared-channel-joining": config['img_squared_chansjoin'],
            "auto-threshold": config['clean_cutoff'],
            "parallel-deconvolution": sdm.dismissable(wscl_parallel_deconv),
            #"nwlayers-factor": nwlayers_factor,
            "threads": ncpu,
            "absmem": config['absmem'],
        }

        if config['img_chan_range']:
            image_opts.update({"channel-range": list(map(int,config['img_chan_range'].split(',')))})

        #join polarization only if they will be imaged together
        joinpol=False
        if len(config['img_stokes']) > 1:
            joinpol = config['img_join_polarizations']
            image_opts['join-polarizations']=joinpol

        if joinpol is False and config['img_specfit_nrcoeff'] > 0:
            image_opts["fit-spectral-pol"] = config['img_specfit_nrcoeff']
            if config['img_niter'] > 0 and config['img_stokes'] == 'I':
                image_opts["savesourcelist"] = True
        if not config['img_mfs_weighting']:
            image_opts["nomfsweighting"] = True
        if maxuvl > 0.:
            image_opts.update({
                "maxuv-l": maxuvl,
                "taper-tukey": transuvl,
            })
        if float(taper) > 0.:
            image_opts.update({
                "taper-gaussian": taper,
            })
        if float(beam) > 0. :
            image_opts.update({
                "beam-size": beam,
            })
        if min_uvw > 0:
            image_opts.update({"minuvw-m": min_uvw})
        if multiscale:
            image_opts.update({"multiscale": multiscale})
            if multiscale_scales:
                image_opts.update({"multiscale-scales": list(map(int, multiscale_scales.split(',')))})

        mask_key = config['cleanmask_method']
        if mask_key == 'wsclean':
            image_opts.update({
                "auto-mask": config['cleanmask_thr'],
                "local-rms": config['cleanmask_localrms'],
            })
            if config['cleanmask_localrms']:
                image_opts.update({
                    "local-rms-window": config['cleanmask_localrms_window'],
                })
        else:
            fits_mask = 'masking/{0:s}.fits'.format(mask_key)
            if not os.path.isfile('{0:s}/{1:s}'.format(pipeline.output, fits_mask)):
                    raise caracal.ConfigurationError(
                        "Clean mask {0:s}/{1:s} not found. Please make sure that you have given the correct mask label" \
                        " in cleanmask_method, and that the mask exists.".format(pipeline.output, fits_mask))
            image_opts.update({
                    "fitsmask": '{0:s}:output'.format(fits_mask),
                    "local-rms": False,
                })
        recipe.add('cab/wsclean', step,
            image_opts,
            input=pipeline.input,
            output=pipeline.output,
            label='{:s}:: Make wsclean image'.format(step))

        recipe.run()
        recipe.jobs = []

        alone = ["I", "Q", "U", "V"]
        stokes = config['img_stokes']
        if stokes in alone:
            rename_single_stokes(get_dir_path(image_path, pipeline), field, stokes)

    for target in all_targets:
        mslist = ms_dict[target]
        field = utils.filter_name(target)
        image_path = "{0:s}/polarization".format(pipeline.continuum)
        if not os.path.exists(image_path):
            os.mkdir(image_path)

        img_dir = get_dir_path(image_path, pipeline)
        image(img_dir, mslist, field)

        # if config['convolve']:
        #     tar_beam = config['convl_beam']
        #     equolver_args = {
        #         "threads": ncpu,
        #     }
        #     if config['convolve']['convolve_images']:
        #         for stokes in config['pol']:
        #             #step = 'convl-images'.format(stokes)
        #             for ch in config['img_nchans']:
        #                 im_name = '{0:s}/{1:s}-{2:s}-{3:d}-{4:d}-image.fits:output'.format(
        #                     img_dir, prefix, field, ch, stokes)
        #                 equolver_args["inc_cubes"] = im_name
        #                 beach.Beach(equolver_args)
        #
        # if config['make_cubes']:
        #     for stokes in config['pol']:
        #         inp_images = '{0:s}/{1:s}-{2:s}-*-{3:d}-image.fits:output'.format(
        #             img_dir, prefix, field, stokes)
        #         if config['convolve'] and config['convolve']['convolve_cubes']:
        #             output_images = '{0:s}/tmp_{1:s}-{2:s}-{3:d}-image.fits:output'.format(
        #                 img_dir, prefix, field, stokes)
        #         else:
        #             output_images = '{0:s}/{1:s}-{2:s}-{3:d}-image.fits:output'.format(
        #                 img_dir, prefix, field, stokes)
        #         step = 'make-cube-'.format(stokes)
        #         recipe.add(
        #             'cab/fitstool',
        #             step,
        #             {
        #                 "file_pattern": inp_images,
        #                 "output": output_images,
        #                 "stack": True,
        #                 "delete-files": False,
        #                 "fits-axis": 'FREQ',
        #             },
        #             input=pipeline.input,
        #             output=pipeline.output,
        #             label='{0:s}:: Make {1:s} cube from wsclean {1:s} channels'.format(
        #                 step,stokes))
        #         recipe.run()
        #         recipe.jobs = []
        #         if config['convolve'] and config['convolve']['convolve_cubes']:
        #             tar_beam = config['convl_beam']
        #             equolver_args = {
        #                 "threads": ncpu,
        #             }
        #             im_name = '{0:s}/{1:s}-{2:s}-{3:d}-image.fits:output'.format(
        #                     img_dir, prefix, field, stokes)
        #             equolver_args["inc_cubes"] = im_name
        #             beach.Beach(equolver_args)


        for i, msname in enumerate(mslist):
            if pipeline.enable_task(config, 'flagging_summary'):
                step = 'flagging_summary-selfcal-ms{0:d}'.format(i)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'summary',
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))


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

# import equolver.beach as beach

NAME = 'Polarization Imaging'
LABEL = 'polimg'


def get_dir_path(string, pipeline):
    return string.split(pipeline.output)[1][1:]


def worker(pipeline, recipe, config):
    wname = pipeline.CURRENT_WORKER
    flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
    flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
    rewind_main_ms = config['rewind_flags']["enable"] and (
            config['rewind_flags']['mode'] == 'reset_worker' or config['rewind_flags']["version"] != 'null')

    label = config['label_in']
    ncpu = config['ncpu']
    pol = config['stokes']
    if ncpu == 0:
        ncpu = psutil.cpu_count()
    else:
        ncpu = min(ncpu, psutil.cpu_count())

    # mfsprefix = ["", '-MFS'][int(config['img_nchans'] > 1)]
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
            "column": config['make_images']['col'],
            "weight": config['make_images']['img_weight'] if not config['make_images']['img_weight'] == 'briggs' else 'briggs {}'.format(
                config['make_images']['img_robust']),
            "nmiter": sdm.dismissable(config['make_images']['img_nmiter']),
            "npix": config['make_images']['img_npix'],
            "scale": config['make_images']['img_cell'],
            "prefix": '{0:s}/{1:s}_{2:s}'.format(img_dir, prefix, field),
            "niter": config['make_images']['img_niter'],
            "mgain": config['make_images']['img_mgain'],
            "pol": pol,
            "channelsout": config['make_images']['img_nchans'],
            "joinchannels": config['make_images']['img_joinchans'],
            "squared-channel-joining": config['make_images']['img_squared_chansjoin'],
            "auto-threshold": config['make_images']['clean_cutoff'],
            "parallel-deconvolution": sdm.dismissable(wscl_parallel_deconv),
            "threads": ncpu,
            "absmem": config['make_images']['absmem'],
        }
        if nwlayers_factor != -1:
            image_opts.update({"nwlayers-factor": nwlayers_factor})

        if config['make_images']['img_padding'] != -1:
            image_opts.update({"padding": config['make_images']['img_padding']})

        if config['make_images']["img_gain"] != -1:
            image_opts.update({"gain": config['make_images']["img_gain"]})

        if config['make_images']['img_chan_range']:
            image_opts.update({"channel-range": list(map(int, config['make_images']['img_chan_range'].split(',')))})

        # join polarization only if they will be imaged together
        joinpol = False
        if len(pol) > 1:
            joinpol = config['make_images']['img_join_polarizations']
            image_opts['join-polarizations'] = joinpol

        if joinpol is False and config['make_images']['img_specfit_nrcoeff'] > 0:
            image_opts["fit-spectral-pol"] = config['make_images']['img_specfit_nrcoeff']
            if config['make_images']['img_niter'] > 0 and pol == 'I':
                image_opts["savesourcelist"] = True
        if not config['make_images']['img_mfs_weighting']:
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
        # if float(beam) > 0. :
        #     image_opts.update({
        #         "beam-size": beam,
        #     })
        if min_uvw > 0:
            image_opts.update({"minuvw-m": min_uvw})
        if multiscale:
            image_opts.update({"multiscale": multiscale})
            if multiscale_scales:
                image_opts.update({"multiscale-scales": list(map(int, multiscale_scales.split(',')))})

        mask_key = config['make_images']['cleanmask_method']
        if mask_key == 'wsclean':
            image_opts.update({
                # "auto-mask": config['cleanmask_thr'],
                "local-rms": config['make_images']['cleanmask_localrms'],
            })
            if config['make_images']['cleanmask_thr'] != -1:
                image_opts.update({"auto-mask": config['make_images']['cleanmask_thr']})
            if config['make_images']['cleanmask_localrms']:
                image_opts.update({
                    "local-rms-window": config['make_images']['cleanmask_localrms_window'],
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
        if pol in alone:
            rename_single_stokes(get_dir_path(image_path, pipeline), field, stokes)

    def make_mauchian_pb(filename, freq):
        with fits.open(filename) as image:
            headimage = image[0].header
            ang_offset = np.indices(
                (headimage['naxis2'], headimage['naxis1']), dtype=np.float32)
            ang_offset[0] -= (headimage['crpix2'] - 1)
            ang_offset[1] -= (headimage['crpix1'] - 1)
            ang_offset = np.sqrt((ang_offset ** 2).sum(
                axis=0))  # Using offset in x and y direction to calculate the total offset from the pointing centre
            ang_offset = ang_offset * np.abs(headimage['cdelt1'])  # Now offset is in units of deg
            FWHM_pb = (57.5 / 60) * (
                    freq / 1.5e9) ** -1  # Eqn 4 of Mauch et al. (2020), but in deg   # freq is just a float for the 2D case
            pb_image = (np.cos(1.189 * np.pi * (ang_offset / FWHM_pb)) / (
                    1 - 4 * (1.189 * ang_offset / FWHM_pb) ** 2)) ** 2  # Eqn 3 of Mauch et al. (2020)
            pb_image = np.expand_dims(pb_image, axis=0)
            fits.writeto(filename.replace('image.fits', 'pb.fits'),
                         pb_image, header=headimage, overwrite=True)
            caracal.log.info('Created Mauchian primary-beam  FITS {0:s}'.format(
                filename.replace('image.fits', 'pb.fits')))

    for target in all_targets:
        mslist = ms_dict[target]
        field = utils.filter_name(target)
        image_path = "{0:s}/polarization".format(pipeline.continuum)
        if not os.path.exists(image_path):
            os.mkdir(image_path)

        img_dir = get_dir_path(image_path, pipeline)

        if config['make_images']['enable']:
            taper = config['make_images']['img_taper']
            if taper == '':
                taper = None
            # beam = config['img_beam']
            maxuvl = config['make_images']['img_maxuv_l']
            transuvl = maxuvl * config['make_images']['img_transuv_l'] / 100.
            multiscale = config['make_images']['img_multiscale']
            multiscale_scales = config['make_images']['img_multiscale_scales']
            min_uvw = config['make_images']['minuvw_m']
            nwlayers_factor = config['make_images']['img_nwlayers_factor']
            nrdeconvsubimg = ncpu if config['make_images']['img_nrdeconvsubimg'] == 0 else config['make_images'][
                'img_nrdeconvsubimg']
            if nrdeconvsubimg == 1:
                wscl_parallel_deconv = None
            else:
                wscl_parallel_deconv = int(np.ceil(config['make_images']['img_npix'] / np.sqrt(nrdeconvsubimg)))
            image(img_dir, mslist, field)

        # if you want a pb cube then enable the make_extra_images segment and make pb images
        if config['make_cubes']['enable'] and config['make_cubes']['make_pb_cubes']:
            do_extra = True
            do_pb = True
        else:
            do_extra = config['make_extra_images']['enable']
            do_pb = config['make_extra_images']['make_pb_images']

        if do_extra:
            for stokes in pol:
                # derive target beam from first image
                if config['make_extra_images']['enable'] and config['make_extra_images']['convl_images'] and config['make_extra_images']['convl_beam'] == '':
                    image_path = '{0:s}/{1:s}'.format(pipeline.output, img_dir)
                    im_name = '{0:s}/{1:s}_{2:s}-{3:04d}-{4:s}-image.fits'.format(
                        image_path, prefix, field, 0, stokes)
                    header = fits.open(im_name)[0].header
                    tar_beam = (header['bmaj'], header['bmin'], header['bpa'])
                # set target beam from schema
                elif config['make_extra_images']['enable'] and config['make_extra_images']['convl_images']:
                    tar_beam = config['make_extra_images']['convl_beam']
                for ch in range(0, config['make_images']['img_nchans']):
                    image_path = '{0:s}/{1:s}'.format(pipeline.output, img_dir)
                    im_name = '{0:s}/{1:s}_{2:s}-{3:04d}-{4:s}-image.fits'.format(
                        image_path, prefix, field, ch, stokes)
                    # make pb images
                    if do_pb:
                        head = fits.open(im_name)[0].header
                        freq = head['crval3']
                        make_mauchian_pb(im_name, freq)
                    # make convolved images
                    if config['make_extra_images']['enable'] and config['make_extra_images']['convl_images']:
                        caracal.log.info('Convolving images')
                        step = 'make-convolved-{0:s}-images'.format(stokes)
                        inp_name = '{0:s}/{1:s}_{2:s}-{3:04d}-{4:s}-image.fits:output'.format(img_dir, prefix, field,
                                                                                              ch, stokes)
                        # recipe.add(
                        #     'cab/spimple',
                        #     step,
                        #     {
                        #         "image": inp_name,
                        #         "o": inp_name.replace('.fits','')
                        #         "pp": tar_beam,
                        #         "cp": config['make_exta_images']['circular_beam'],
                        #         "nthreads": ncpu,
                        #     },
                        #     input=pipeline.output,
                        #     output=pipeline.output,
                        #     label='{0:s}:: Make convolved {1:s} images'.format(step,stokes))
                        # remove not convolved images
                        if config['make_extra_images']['remove_originals']:
                            caracal.log.info('Removing the original not convolved images')
                            os.remove(im_name)

        if config['make_cubes']['enable']:
            for stokes in pol:
                # make image cubes
                inp_images = '{0:s}/{1:s}_{2:s}-*-{3:s}-image.fits:output'.format(img_dir, prefix, field, stokes)
                out_images = '{0:s}/{1:s}_{2:s}-{3:s}-image.fits:output'.format(
                    img_dir, prefix, field, stokes)
                step = 'make-cube-{0:s}'.format(stokes)
                recipe.add(
                    'cab/fitstool',
                    step,
                    {
                        "file_pattern": inp_images,
                        "output": out_images,
                        "stack": True,
                        "delete-files": True if config['make_cubes']['remove_originals'] else False,
                        "fits-axis": 'FREQ',
                        "force": True,
                    },
                    input=pipeline.output,
                    output=pipeline.output,
                    label='{0:s}:: Make {1:s} cube from wsclean channels'.format(step, stokes))
                # make pb cubes
                if config['make_cubes']['make_pb_cubes']:
                    inp_images = '{0:s}/{1:s}_{2:s}-*-{3:s}-pb.fits:output'.format(img_dir, prefix, field, stokes)
                    out_images = '{0:s}/{1:s}_{2:s}-{3:s}-pb.fits:output'.format(
                        img_dir, prefix, field, stokes)
                    step = 'make-pb-cube-{0:s}'.format(stokes)
                    recipe.add(
                        'cab/fitstool',
                        step,
                        {
                            "file_pattern": inp_images,
                            "output": out_images,
                            "stack": True,
                            "delete-files": True if not config['make_extra_images']['make_pb_images'] else False,
                            "fits-axis": 'FREQ',
                        },
                        input=pipeline.output,
                        output=pipeline.output,
                        label='{0:s}:: Make {1:s} PB cube from wsclean channels'.format(step, stokes))
                    recipe.run()
                    recipe.jobs = []
                    if not config['make_extra_images']['make_pb_images']:
                        image_path = '{0:s}/{1:s}'.format(pipeline.output, img_dir)
                        im_name = '{0:s}/{1:s}_{2:s}-*-{3:s}-pb.fits'.format(image_path, prefix, field, stokes)
                        os.remove(im_name)
                if config['make_cubes']['convl_cubes']:
                    #  derive target beam from first channel
                    if config['make_cubes']['convl_beam'] == '':
                        image_path = '{0:s}/{1:s}'.format(pipeline.output, img_dir)
                        im_name = '{0:s}/{1:s}_{2:s}-{3:s}-image.fits'.format(
                            image_path, prefix, field, stokes)
                        header = fits.open(im_name)[0].header
                        tar_beam = (header['bmaj1'], header['bmin1'], header['bpa1'])
                    # set target beam from schema
                    else:
                        tar_beam = config['make_cubes']['convl_beam']
                    # make convolved cubes
                    inp_name = '{0:s}/{1:s}_{2:s}-{3:s}-image.fits:output'.format(
                        img_dir, prefix, field, stokes)
                    caracal.log.info('Convolving cubes')
                    step = 'make-convolved-{0:s}-cubes'.format(stokes)
                    # recipe.add(
                    #     'cab/spimple',
                    #     step,
                    #     {
                    #         "image": inp_name,
                    #         "o": inp_name.replace('.fits',''),
                    #         "pp": config['make_cubes']['convl_beam'],
                    #         "cp": tar_beam,
                    #         "nthreads": ncpu,
                    #     },
                    #     input=pipeline.output,
                    #     output=pipeline.output,
                    #     label='{0:s}:: Make convolved {1:s} cubes'.format(step,stokes))
                    # remove original cube and images
                    if config['make_cubes']['remove_originals']:
                        image_path = '{0:s}/{1:s}'.format(pipeline.output, img_dir)
                        im_name = '{0:s}/{1:s}_{2:s}-*{3:s}-image.fits'.format(image_path, prefix, field, stokes)
                        caracal.log.info('Removing the original not convolved images and cube')
                        os.remove(im_name)

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

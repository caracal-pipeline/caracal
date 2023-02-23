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
        llist = list(set(glob.glob('{0:s}{1:s}'.format(posname, '*dirty.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-I-dirty.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-Q-dirty.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-U-dirty.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-V-dirty.fits'))))
        for fname in llist:
            os.rename(fname, fname[:-10] + stokes + "-dirty.fits")
        llist = list(set(glob.glob('{0:s}{1:s}'.format(posname, '*image.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-I-image.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-Q-image.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-U-image.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-V-image.fits'))))
        for fname in llist:
            os.rename(fname, fname[:-10] + stokes + "-image.fits")
        llist = list(set(glob.glob('{0:s}{1:s}'.format(posname, '*model.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-I-model.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-Q-model.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-U-model.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-V-model.fits'))))
        for fname in llist:
            os.rename(fname, fname[:-10] + stokes + "-model.fits")
        llist = list(set(glob.glob('{0:s}{1:s}'.format(posname, '*residual.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-I-residual.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-Q-residual.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-U-residual.fits'))) -
                     set(glob.glob('{0:s}{1:s}'.format(posname, '*-V-residual.fits'))))
        for fname in llist:
            os.rename(fname, fname[:-13] + stokes + "-residual.fits")

    def fix_freq(nch):
        summary = f'{mslist[0][:-3]}-summary.json'
        summary_file = json.load(open(os.path.join(pipeline.msdir, summary)))
        freq0 = float(summary_file["SPW"]["REF_FREQUENCY"][0])
        bw = float(summary_file["SPW"]["TOTAL_BANDWIDTH"][0])
        nchan = int(summary_file["SPW"]["NUM_CHAN"][0])
        res = bw/nchan
        chout=config['make_images']['img_nchans']
        if config['make_images']['img_chan_range']:
            chrange = config['make_images']['img_chan_range']
            ch = list(map(int, chrange.split(',')))
            ch1,ch2 = ch[0],ch[1]
        else:
            ch1,ch2 = 0,nchan
        subbw = (ch2-ch1)*res/chout
        return(freq0+(subbw/2.)+(nch*subbw),subbw)

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

        if config['make_images']['weighting_rank_filter']:
            image_opts.update({"weighting-rank-filter": config['make_images']['weighting_rank_filter']})

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
            rename_single_stokes(get_dir_path(image_path, pipeline), field, pol)

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
        do_convl = config['make_extra_images']['convl_images']
        do_pb = config['make_extra_images']['make_pb_images']

        if config['make_extra_images']['enable']:
            for stokes in pol:
                # derive target beam from first image
                img_path = '{0:s}/{1:s}'.format(pipeline.output, img_dir)
                im_name = '{0:s}/{1:s}_{2:s}-{3:04d}-{4:s}-image.fits'.format(img_path, prefix, field, 0, stokes)
                if do_convl and config['make_extra_images']['convl_beam'] == '':
                    header = fits.open(im_name)[0].header
                    tar_beam = str(header['bmaj'])+','+str(header['bmin'])+','+str(header['bpa'])
                # set target beam from schema
                elif do_convl:
                    tar_beam = config['make_extra_images']['convl_beam']
                skipped_ch=0
                # for every channel
                for ch in range(0, config['make_images']['img_nchans']):
                    im_name = '{0:s}/{1:s}_{2:s}-{3:04d}-{4:s}-image.fits'.format(img_path, prefix, field, ch, stokes)
                    # make pb images
                    if do_pb:
                        #fix header mess
                        freq,bw = fix_freq(ch)
                        head = fits.open(im_name)[0].header
                        head['crval3'] = freq
                        head['cdelt3'] = bw
                        fits.writeto(im_name, fits.open(im_name)[0].data, header=head, overwrite=True)
                        #now pb images
                        make_mauchian_pb(im_name, freq)
                    # make convolved images
                    if do_convl and config['make_extra_images']['schema'] != 'cube':
                        im_name = '{0:s}/{1:s}_{2:s}-{3:04d}-{4:s}-image.fits'.format(img_path, prefix, field, ch, stokes)
                        bmaj=max(tar_beam.split(',')[0],tar_beam.split(',')[1])
                        header = fits.open(im_name)[0].header
                        # skip and make a nan image if beam is larger
                        if float(header['bmaj']) > float(bmaj):
                            out_name = im_name.replace('.fits','_'+str(tar_beam.replace(',','_')))
                            header['bmaj'],header['bmin'],header['bpa']=tar_beam.split(',')[0],tar_beam.split(',')[1],tar_beam.split(',')[2]
                            fits.writeto(out_name, fits.open(im_name)[0].data*np.nan, header=header)
                            skipped_ch=skipped_ch+1
                        # do convl if tar beam is larger than image beam
                        else:
                            step = 'make-convolved-{0:s}-images'.format(stokes)
                            inp_name = '{0:s}/{1:s}_{2:s}-{3:04d}-{4:s}-image.fits:output'.format(img_dir, prefix, field, ch, stokes)
                            convl_opt = {
                                "image": inp_name,
                                "output-filename": inp_name.replace('image.fits',str(tar_beam.replace(',','_'))+'_image'),
                                "psf-pars": [float(x) for x in tar_beam.split(',')],
                                "nthreads": ncpu,
                                #"circ-psf": config['make_extra_images']['circular_beam'],
                                "band": 'l',
                            }
                            recipe.add(
                                'cab/spimple_imconv',
                                step,
                                convl_opt,
                                input=pipeline.output,
                                output=pipeline.output,
                                label='{0:s}:: Make convolved {1:s} images'.format(step,stokes))
                im_name = '{0:s}/{1:s}_{2:s}-MFS-{3:s}-image.fits'.format(img_path, prefix, field, stokes)
                if do_pb:
                    head = fits.open(im_name)[0].header
                    freq = head['crval3']
                    make_mauchian_pb(im_name, freq)
                    if do_convl and config['make_extra_images']['schema'] != 'cube':
                        bmaj=max(tar_beam.split(',')[0],tar_beam.split(',')[1])
                        header = fits.open(im_name)[0].header
                        # skip and make a nan image if beam is larger
                        if float(header['bmaj']) > float(bmaj):
                            out_name = im_name.replace('.fits','_'+str(tar_beam.replace(',','_')))
                            header['bmaj'],header['bmin'],header['bpa']=tar_beam.split(',')[0],tar_beam.split(',')[1],tar_beam.split(',')[2]
                            fits.writeto(out_name, fits.open(im_name)[0].data*np.nan, header=header)
                            skipped_ch=skipped_ch+1
                        # do convl if tar beam is larger than image beam
                        else:
                            step = 'make-convolved-MFS-{0:s}-images'.format(stokes)
                            inp_name = '{0:s}/{1:s}_{2:s}-MFS-{3:s}-image.fits:output'.format(img_dir, prefix, field, stokes)
                            convl_opt = {
                                "image": inp_name,
                                "output-filename": inp_name.replace('image.fits',str(tar_beam.replace(',','_'))+'_image'),
                                "psf-pars": [float(x) for x in tar_beam.split(',')],
                                "nthreads": ncpu,
                                #"circ-psf": config['make_extra_images']['circular_beam'],
                                "band": 'l',
                            }
                            recipe.add(
                                'cab/spimple_imconv',
                                step,
                                convl_opt,
                                input=pipeline.output,
                                output=pipeline.output,
                                label='{0:s}:: Make convolved MFS {1:s} image'.format(step,stokes))
                recipe.run()
                recipe.jobs = []

                if do_convl and config['make_extra_images']['schema'] != 'cube' and skipped_ch>0:
                    caracal.log.info("%d %s channel images out of %d are now nan because target beam is larger than bmaj"%(skipped_ch,stokes,int(config['make_images']['img_nchans'])))

                if config['make_extra_images']['schema'] == 'both' or config['make_extra_images']['schema'] == 'cube':
                    # make PB cubes
                    if do_pb:
                        pb_im_name = '{0:s}/{1:s}_{2:s}-0*-{3:s}-pb.fits:output'.format(img_dir, prefix, field, stokes)
                        pb_out_cube = '{0:s}/{1:s}_{2:s}-{3:s}-pb.fits:output'.format(img_dir, prefix, field, stokes)
                        caracal.log.info('Using the following image to make a PB cube:')
                        caracal.log.info(os.system('ls -1 {0:s}/{1:s}_{2:s}-0*-{3:s}-pb.fits'.format(img_path, prefix, field, stokes)))
                        step = 'make-pb-cube-{0:s}'.format(stokes)
                        recipe.add(
                            'cab/fitstool',
                            step,
                            {
                                "file_pattern": pb_im_name,
                                "output": pb_out_cube,
                                "stack": True,
                                "delete-files": False,
                                #True if config['make_extra_images']['schema'] == 'cube' else False,
                                "fits-axis": 'FREQ',
                            },
                            input=pipeline.output,
                            output=pipeline.output,
                            label='{0:s}:: Make {1:s} PB cube from wsclean channels'.format(step, stokes))
                    # make image cubes from images
                    im_names = '{0:s}/{1:s}_{2:s}-0*-{3:s}-image.fits:output'.format(img_dir, prefix, field, stokes)
                    cube_name = '{0:s}/{1:s}_{2:s}-{3:s}-image.fits:output'.format(img_dir, prefix, field, stokes)
                    caracal.log.info('Using the following image to make a {0:s} cube:'.format(stokes))
                    caracal.log.info(os.system('ls -1 {0:s}/{1:s}_{2:s}-0*-{3:s}-image.fits'.format(img_path, prefix, field, stokes)))
                    step = 'make-cube-{0:s}'.format(stokes)
                    recipe.add(
                        'cab/fitstool',
                        step,
                        {
                            "file_pattern": im_names,
                            "output": cube_name,
                            "stack": True,
                            "delete-files": False,
                            "fits-axis": 'FREQ',
                            "force": True,
                        },
                        input=pipeline.output,
                        output=pipeline.output,
                        label='{0:s}:: Make {1:s} cube from wsclean channels'.format(step, stokes))
                    recipe.run()
                    recipe.jobs = []
                    # make convl
                    if do_convl:
                        # make a nan image if beam is larger
                        inp_cube_name = '{0:s}/{1:s}_{2:s}-{3:s}-image.fits'.format(img_path, prefix, field, stokes)
                        head = fits.open(inp_cube_name)[0].header
                        data = fits.open(inp_cube_name)[0].data
                        bvec = head['bmaj*']
                        bvect=[float(bvec[x])<float(max(tar_beam.split(',')[0],tar_beam.split(',')[1])) for x in range(0,len(bvec))]
                        caracal.log.info("%d channels out of %d in the %s cube are now nan because target beam is larger than bmaj"%(len(bvect)-sum(bvect),int(len(bvect)-1),stokes))
                        for x in range(1,len(bvect)):
                            head['bmaj'+str(x)] = head['bmaj'+str(x)]*bvect[x]
                            head['bmin'+str(x)] = head['bmin'+str(x)]*bvect[x]
                            head['bpa'+str(x)] = head['bpa'+str(x)]*bvect[x]
                            data[0,x-1,:,:] = data[0,x-1,:,:]*bvect[x]
                            if ~np.any(data[0,x-1,:,:]):
                                data[0,x-1,:,:]=data[0,x-1,:,:]*np.nan
                        fits.writeto(inp_cube_name, data, header=head, overwrite=True)
                        step = 'make-convolved-{0:s}-cubes'.format(stokes)
                        recipe.add(
                             'cab/spimple_imconv',
                             step,
                             {
                                 "image": cube_name,
                                 "output-filename": cube_name.replace('image.fits',str(tar_beam.replace(',','-'))+'_image'),
                                 "nthreads": ncpu,
                                 "psf-pars": [float(x) for x in tar_beam.split(',')],
                                 #"circ-psf": config['make_extra_images']['circular_beam'],
                                 "band": 'l',
                             },
                             input=pipeline.output,
                             output=pipeline.output,
                             label='{0:s}:: Make convolved {1:s} cubes'.format(step,stokes))
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

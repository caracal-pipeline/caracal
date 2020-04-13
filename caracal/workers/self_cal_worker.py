import os
import shutil
import glob
import sys
import yaml
import json
import os
import re
import copy
import caracal
import stimela.dismissable as sdm
from caracal.dispatch_crew import utils
from astropy.io import fits as fits
from stimela.pathformatter import pathformatter as spf


NAME = 'Self calibration loop'
LABEL = 'self_cal'


# self_cal_iter_counter is used as a global variable.

# To split out continuum/<dir> from output/continuum/dir


def get_dir_path(string, pipeline): return string.split(pipeline.output)[1][1:]


CUBICAL_OUT = {
    "CORR_DATA": 'sc',
    "CORR_RES": 'sr',
}

CUBICAL_MT = {
    "Gain2x2": 'complex-2x2',
    "GainDiag": 'complex-2x2',  # TODO:: Change this. Ask cubical to support this mode
    "GainDiagPhase": 'phase-diag',
    "ComplexDiag": 'complex-diag'
}

SOL_TERMS = {
    "G": "50",
    "B": "50",
    "DD": "20",
}


def worker(pipeline, recipe, config):
    npix = config['img_npix']
    padding = config['img_padding']
    spwid = config.get('spwid')
    cell = config['img_cell']
    mgain = config['img_mgain']
    niter = config['img_niter']
    imgweight = config['img_weight']
    img_nmiter = config['img_nmiter']
    robust = config['img_robust']
    nchans = config['img_nchans']
    pol = config.get('img_pol')
    joinchannels = config['img_joinchannels']
    fit_spectral_pol = config['img_fit_spectral_pol']
    taper = config.get('img_uvtaper')
    multiscale = config.get('img_multi_scale')
    if multiscale == True:
        multiscale_scales = sdm.dismissable(config.get('img_multi_scale_scales'))
    if taper == '':
        taper = None
    label = config['label']
    time_chunk = config.get('cal_timeslots_chunk')
    #If user sets value that is not -1 use leave that
    if int(time_chunk) < 0 and  pipeline.enable_task(config, 'calibrate'):
        # We're always doing gains
        all_time_solution = config['calibrate'].get('Gsols_timeslots')
        # add the various sections
        if config['calibrate'].get('Bjones', False):
            all_time_solution.append(config[key].get('Bsols_timeslots')[:])
        if config['calibrate'].get('DDjones', False):
            all_time_solution.append(config[key].get('DDsols_timeslots')[:])
        if  config['calibrate'].get('two_step',False):
            for val in config['calibrate'].get('DDsols_timeslots'):
                if int(val) >= 0:
                    all_time_solution.append(val)
        if min(all_time_solution) == 0:
            time_chunk = 0
        else:
            time_chunk = max(all_time_solution)
    freq_chunk = config.get('cal_channel_chunk')

    #If user sets value that is not -1 then use that
    if int(freq_chunk) < 0 and  pipeline.enable_task(config, 'calibrate'):
        # We're always doing gains
        all_freq_solution = config['calibrate'].get('Gsols_channel')
        # add the various sections
        if config['calibrate'].get('Bjones', False):
            all_freq_solution.append(config[key].get('Bsols_channel')[:])
        if config['calibrate'].get('DDjones', False):
            all_freq_solution.append(config[key].get('DDsols_channel')[:])
        if  config['calibrate'].get('two_step',False):
            for val in config['calibrate'].get('DDsols_channel'):
                if int(val) >= 0:
                    all_freq_solution.append(val)
        if min(all_freq_solution) == 0:
            freq_chunk = 0
        else:
            freq_chunk = int(max(all_freq_solution))

    min_uvw = config.get('minuvw_m')
    ncpu = config.get('ncpu')
    mfsprefix = ["", '-MFS'][int(nchans > 1)]
    cal_niter = config.get('cal_niter')
    # label of MS where we transform selfcal gaintables
    label_tgain = config['transfer_apply_gains'].get('transfer_to_label')
    # label of MS where we interpolate and transform model column
    label_tmodel = config['transfer_model'].get('transfer_to_label')

    all_targets, all_msfile, ms_dict = utils.target_to_msfiles(
        pipeline.target, pipeline.msnames, label)

    for m in all_msfile:  # check whether all ms files to be used exist
        if not os.path.exists(os.path.join(pipeline.msdir, m)):
            raise IOError(
                "MS file {0:s} does not exist. Please check that it is where it should be.".format(m))

    if pipeline.enable_task(config, 'transfer_apply_gains'):
        t, all_msfile_tgain, ms_dict_tgain = utils.target_to_msfiles(
            pipeline.target, pipeline.msnames, label_tgain)
        for m in all_msfile_tgain:  # check whether all ms files to be used exist
            if not os.path.exists(os.path.join(pipeline.msdir, m)):
                raise IOError(
                    "MS file {0:s}, to transfer gains to, does not exist. Please check that it is where it should be.".format(m))

    if pipeline.enable_task(config, 'transfer_model'):
        t, all_msfile_tmodel, ms_dict_tmodel = utils.target_to_msfiles(
            pipeline.target, pipeline.msnames, label_tmodel)
        for m in all_msfile_tmodel:  # check whether all ms files to be used exist
            if not os.path.exists(os.path.join(pipeline.msdir, m)):
                raise IOError(
                    "MS file {0:s}, to transfer model to, does not exist. Please check that it is where it should be.".format(m))

    #caracal.log.info("Processing {0:s}".format(",".join(mslist)))
#    hires_mslist = filter(lambda ms: isinstance(ms, str), hires_mslist)
#    hires_mslist = filter(lambda ms: os.path.exists(os.path.join(pipeline.msdir, ms)), hires_mslist)

    prefix = pipeline.prefix

    # Define image() extract_sources() calibrate()
    # functions for convenience


    def cleanup_files(mask_name):

        if os.path.exists(pipeline.output+'/'+mask_name):
            shutil.move(pipeline.output+'/'+mask_name,
                        pipeline.output+'/masking/'+mask_name)

        casafiles = glob.glob(pipeline.output+'/*.image')
        for i in range(0, len(casafiles)):
            shutil.rmtree(casafiles[i])

    def change_header(filename, headfile, copy_head):
        pblist = fits.open(filename)

        dat = pblist[0].data

        if copy_head == True:
            hdrfile = fits.open(headfile)
            head = hdrfile[0].header
        elif copy_head == False:

            head = pblist[0].header

            if 'ORIGIN' in head:
                del head['ORIGIN']
            if 'CUNIT1' in head:
                del head['CUNIT1']
            if 'CUNIT2' in head:
                del head['CUNIT2']

        fits.writeto(filename,dat,head,overwrite=True)

    #def get_mem():
    #    with open('/proc/meminfo') as p:
    #         memory_available = p.read()
    #         print memory_available
    #    memtotal = re.search(r'^MemTotal:\s+(\d+)', memory_available)
    #    if memtotal:
    #       memtotal_kb = int(memtotal.groups()[0])
    #       print 'memory_in_kb:', memtotal_kb
    #    return memtotal_kb

    def fake_image(trg, num, img_dir, mslist, field):
        key = 'image'
        key_mt = 'calibrate'

        step = 'image_field{0:d}_iter{1:d}'.format(trg, num)
        fake_image_opts = {
            "msname": mslist,
            "column": 'DATA',
            "weight": imgweight if not imgweight == 'briggs' else 'briggs {}'.format(config.get('robust', robust)),
            "nmiter": sdm.dismissable(config['img_nmiter']),
            "npix": config[key].get('npix', npix),
            "padding": config[key].get('padding', padding),
            "scale": config[key].get('cell', cell),
            "prefix": '{0:s}/{1:s}_{2:s}_{3:d}'.format(img_dir, prefix, field, num),
            "niter": niter,
            "mgain": mgain,
            "pol":  pol,
            "taper-gaussian":  taper,
            "channelsout": nchans,
            "joinchannels": joinchannels,
            "fit-spectral-pol":  fit_spectral_pol,
            "local-rms": True,
            "auto-mask": 6,
            "auto-threshold": config[key].get('clean_threshold')[0],
            "savesourcelist": False,
            "fitbeam": False,
        }
        if multiscale==True:
            fake_image_opts.update({"multiscale": multiscale})
            fake_image_opts.update({"multiscale": multiscale_scales})

        recipe.add('cab/wsclean', step,
                   fake_image_opts,
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{:s}:: Make image after first round of calibration'.format(step))

    def image(trg, num, img_dir, mslist, field):
        key = 'image'
        key_mt = 'calibrate'

        if num > 1:
            matrix_type = config[key_mt].get('gain_matrix_type')[
                num - 2 if len(config[key_mt].get('gain_matrix_type')) >= num else -1]
        else:
            matrix_type = 'null'
        # If we have a two_step selfcal and Gaindiag we want to use  CORRECTED_DATA
        if config.get('calibrate_with').lower() == 'meqtrees' and config[key_mt].get('two_step'):
            if matrix_type == 'GainDiag':
                imcolumn = "CORRECTED_DATA"
            # If we do not have gaindiag but do have two step selfcal check against stupidity and that we are actually ending with ampphase cal and written to a special phase column
            elif matrix_type == 'GainDiagPhase' and config[key_mt].get('gain_matrix_type')[-1] == 'GainDiag':
                imcolumn = 'CORRECTED_DATA_PHASE'
            # If none of these apply then do our normal sefcal
            else:
                imcolumn = config[key].get(
                    'column')[num - 1 if len(config[key].get('column')) >= num else -1]
        else:
            imcolumn = config[key].get(
                'column')[num - 1 if len(config[key].get('column')) >= num else -1]

        step = 'image_field{0:d}_iter{1:d}'.format(trg, num)
        image_opts = {
            "msname": mslist,
            "column": imcolumn,
            "weight": imgweight if not imgweight == 'briggs' else 'briggs {}'.format(config.get('robust', robust)),
            "nmiter": sdm.dismissable(config['img_nmiter']),
            "npix": config[key].get('npix', npix),
            "padding": config[key].get('padding', padding),
            "scale": config[key].get('cell', cell),
            "prefix": '{0:s}/{1:s}_{2:s}_{3:d}'.format(img_dir, prefix, field, num),
            "niter": config[key].get('niter', niter),
            "mgain": config[key].get('mgain', mgain),
            "pol": config[key].get('pol', pol),
            "taper-gaussian": sdm.dismissable(config[key].get('uvtaper', taper)),
            "channelsout": nchans,
            "joinchannels": config[key].get('joinchannels', joinchannels),
            "fit-spectral-pol": config[key].get('fit_spectral_pol', fit_spectral_pol),
            "savesourcelist": True if config[key].get('niter', niter)>0 else False,
            "auto-threshold": config[key].get('clean_threshold')[num-1 if len(config[key].get('clean_threshold')) >= num else -1],
        }
        if min_uvw > 0:
            image_opts.update({"minuvw-m": min_uvw})
        if multiscale ==True:
            image_opts.update({"multiscale": multiscale})
            image_opts.update({"multiscale-scales": multiscale_scales})

        mask_key = config[key].get('clean_mask_method')[num-1 if len(config[key].get('clean_mask_method')) >= num else -1]
        if mask_key == 'wsclean':
            image_opts.update({
                "auto-mask": config[key].get('clean_mask_threshold')[num-1 if len(config[key].get('clean_mask_threshold')) >= num else -1],
                "local-rms": config[key].get('clean_mask_local_rms')[num-1 if len(config[key].get('clean_mask_local_rms')) >= num else -1],
                "local-rms-window": config[key].get('clean_mask_local_rms_window')[num-1 if len(config[key].get('clean_mask_local_rms_window')) >= num else -1],
              })
        elif mask_key == 'sofia':
            fitmask_address = 'masking'
            image_opts.update({
                "fitsmask": '{0:s}/{1:s}_{2:s}_{3:d}_clean_mask.fits:output'.format(fitmask_address, prefix,field, num),
                "local-rms": False,
              })
        else:
            fits_mask = '{0:s}/{1:s}_{2:s}.fits'.format(
                'masking', mask_key, field)
            if not os.path.isfile('{0:s}/{1:s}'.format(pipeline.output, fits_mask)):
                caracal.log.error(
                    "No mask is found in output/masking. Please run masking-worker or put a mask in output/masking called clean_mask_method[0].fits format ")
                raise caracal.ConfigurationError("no mask found in output directory")
            image_opts.update({
                "fitsmask": fits_mask+':output',
                "local-rms": False,
              })

        recipe.add('cab/wsclean', step,
                   image_opts,
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{:s}:: Make image after first round of calibration'.format(step))

    def sofia_mask(trg, num, img_dir, field):
        step = 'make_sofia_mask_field{0:d}_iter{1:d}'.format(trg,num)
        key = 'sofia_settings'

        if config['img_joinchannels'] == True:
            imagename = '{0:s}/{1:s}_{2:s}_{3:d}-MFS-image.fits'.format(
                img_dir, prefix, field, num)
        else:
            imagename = '{0:s}/{1:s}_{2:s}_{3:d}-image.fits'.format(
                img_dir, prefix, field, num)

        if config['image'][key].get('fornax_special') == True and config['image'][key].get('fornax_use_sofia') == True:
            forn_kernels = [[80, 80, 0, 'b']]
            forn_thresh = config['image'][key].get('fornax_thresh')[
                num-1 if len(config['image'][key].get('fornax_thresh')) >= num else -1]

            image_opts_forn = {
                "import.inFile": imagename,
                "steps.doFlag": True,
                "steps.doScaleNoise": False,
                "steps.doSCfind": True,
                "steps.doMerge": True,
                "steps.doReliability": False,
                "steps.doParameterise": False,
                "steps.doWriteMask": True,
                "steps.doMom0": False,
                "steps.doMom1": False,
                "steps.doWriteCat": False,
                "parameters.dilateMask": False,
                "parameters.fitBusyFunction": False,
                "parameters.optimiseMask": False,
                "SCfind.kernelUnit": 'pixel',
                "SCfind.kernels": forn_kernels,
                "SCfind.threshold": forn_thresh,
                "SCfind.rmsMode": 'mad',
                "SCfind.edgeMode": 'constant',
                "SCfind.fluxRange": 'all',
                "scaleNoise.method": 'local',
                "scaleNoise.windowSpatial": 51,
                "scaleNoise.windowSpectral": 1,
                "writeCat.basename": 'FornaxA_sofia',
                "merge.radiusX": 3,
                "merge.radiusY": 3,
                "merge.radiusZ": 1,
                "merge.minSizeX": 100,
                "merge.minSizeY": 100,
                "merge.minSizeZ": 1,
            }

        outmask = pipeline.prefix+'_'+field+'_'+str(num+1)+'_clean'
        outmaskName = outmask+'_mask.fits'

        image_opts = {
            "import.inFile": imagename,
            "steps.doFlag": True,
            "steps.doScaleNoise": config['image'].get('clean_mask_local_rms')[num-1 if len(config['image'].get('clean_mask_local_rms')) >= num else -1],
            "steps.doSCfind": True,
            "steps.doMerge": True,
            "steps.doReliability": False,
            "steps.doParameterise": False,
            "steps.doWriteMask": True,
            "steps.doMom0": False,
            "steps.doMom1": False,
            "steps.doWriteCat": True,
            "writeCat.writeASCII": False,
            "writeCat.basename": outmask,
            "writeCat.writeSQL": False,
            "writeCat.writeXML": False,
            "parameters.dilateMask": False,
            "parameters.fitBusyFunction": False,
            "parameters.optimiseMask": False,
            "SCfind.kernelUnit": 'pixel',
            "SCfind.kernels": [[kk, kk, 0, 'b'] for kk in config['image'][key].get('kernels')],
            "SCfind.threshold": config['image'].get('clean_mask_threshold')[num-1 if len(config['image'].get('clean_mask_threshold')) >= num else -1],
            "SCfind.rmsMode": 'mad',
            "SCfind.edgeMode": 'constant',
            "SCfind.fluxRange": 'all',
            "scaleNoise.statistic": 'mad',
            "scaleNoise.method": 'local',
            "scaleNoise.interpolation": 'linear',
            "scaleNoise.windowSpatial": config['image'].get('clean_mask_local_rms_window')[num-1 if len(config['image'].get('clean_mask_local_rms_window')) >= num else -1],
            "scaleNoise.windowSpectral": 1,
            "scaleNoise.scaleX": True,
            "scaleNoise.scaleY": True,
            "scaleNoise.scaleZ": False,
            "scaleNoise.perSCkernel": config['image'].get('clean_mask_local_rms')[num-1 if len(config['image'].get('clean_mask_local_rms')) >= num else -1], # work-around for https://github.com/SoFiA-Admin/SoFiA/issues/172, to be replaced by "True" once the next SoFiA version is in Stimela
            "merge.radiusX": 3,
            "merge.radiusY": 3,
            "merge.radiusZ": 1,
            "merge.minSizeX": 3,
            "merge.minSizeY": 3,
            "merge.minSizeZ": 1,
            "merge.positivity": config['image'][key].get('only_positive_pix'),
        }
        if config['image'][key].get('flag'):
            flags_sof = config['image'][key].get('flagregion')
            image_opts.update({"flag.regions": flags_sof})

        if config['image'][key].get('inputmask'):
            # change header of inputmask so it is the same as image
            mask_name = 'masking/'+config['image'][key].get('inputmask')

            mask_name_casa = mask_name.split('.fits')[0]
            mask_name_casa = mask_name_casa+'.image'

            mask_regrid_casa = mask_name_casa+'_regrid.image'

            imagename_casa = '{0:s}_{1:d}{2:s}-image.image'.format(
                prefix, num, mfsprefix)

            recipe.add('cab/casa_importfits', step,
                       {
                           "fitsimage": imagename,
                           "imagename": imagename_casa,
                           "overwrite": True,
                       },
                       input=pipeline.output,
                       output=pipeline.output,
                       label='Image in casa format')

            recipe.add('cab/casa_importfits', step,
                       {
                           "fitsimage": mask_name+':output',
                           "imagename": mask_name_casa,
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Mask in casa format')

            step = '3'
            recipe.add('cab/casa_imregrid', step,
                       {
                           "template": imagename_casa+':output',
                           "imagename": mask_name_casa+':output',
                           "output": mask_regrid_casa,
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Regridding mosaic to size and projection of dirty image')

            step = '4'
            recipe.add('cab/casa_exportfits', step,
                       {
                           "fitsimage": mask_name+':output',
                           "imagename": mask_regrid_casa+':output',
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Extracted regridded mosaic')

            step = '5'
            recipe.add(change_header, step,
                       {
                           "filename": pipeline.output+'/'+mask_name,
                           "headfile": pipeline.output+'/'+imagename,
                           "copy_head": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Extracted regridded mosaic')

            image_opts.update({"import.maskFile": mask_name})
            image_opts.update({"import.inFile": imagename})

        if config['image'][key].get('fornax_special') == True and config['image'][key].get('fornax_use_sofia') == True:

            recipe.add('cab/sofia', step,
                       image_opts_forn,
                       input=pipeline.output,
                       output=pipeline.output+'/masking/',
                       label='{0:s}:: Make SoFiA mask'.format(step))

            fornax_namemask = 'masking/FornaxA_sofia_mask.fits'
            image_opts.update({"import.maskFile": fornax_namemask})

        elif config['image'][key].get('fornax_special') == True and config['image'][key].get('fornax_use_sofia') == False:

            # this mask should be regridded to correct f.o.v.

            fornax_namemask = 'masking/Fornaxa_vla_mask_doped.fits'
            fornax_namemask_regr = 'masking/Fornaxa_vla_mask_doped_regr.fits'

            mask_name_casa = fornax_namemask.split('.fits')[0]
            mask_name_casa = fornax_namemask+'.image'

            mask_regrid_casa = fornax_namemask+'_regrid.image'

            imagename_casa = '{0:s}_{1:d}{2:s}-image.image'.format(
                prefix, num, mfsprefix)

            recipe.add('cab/casa_importfits', step,
                       {
                           "fitsimage": imagename,
                           "imagename": imagename_casa,
                           "overwrite": True,
                       },
                       input=pipeline.output,
                       output=pipeline.output,
                       label='Image in casa format')

            recipe.add('cab/casa_importfits', step,
                       {
                           "fitsimage": fornax_namemask+':output',
                           "imagename": mask_name_casa,
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Mask in casa format')

            step = '3'
            recipe.add('cab/casa_imregrid', step,
                       {
                           "template": imagename_casa+':output',
                           "imagename": mask_name_casa+':output',
                           "output": mask_regrid_casa,
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Regridding mosaic to size and projection of dirty image')

            step = '4'
            recipe.add('cab/casa_exportfits', step,
                       {
                           "fitsimage": fornax_namemask_regr+':output',
                           "imagename": mask_regrid_casa+':output',
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Extracted regridded mosaic')

            step = '5'
            recipe.add(change_header, step,
                       {
                           "filename": pipeline.output+'/'+fornax_namemask_regr,
                           "headfile": pipeline.output+'/'+imagename,
                           "copy_head": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Extracted regridded mosaic')

            image_opts.update({"import.maskFile": fornax_namemask_regr})

        print('########################################')
        print(image_opts)
        print('########################################')
        recipe.add('cab/sofia', step,
                   image_opts,
                   input=pipeline.output,
                   output=pipeline.output+'/masking/',
                   label='{0:s}:: Make SoFiA mask'.format(step))

    def make_cube(num, img_dir, field, imtype='model'):
        im = '{0:s}/{1:s}_{2:s}_{3}-cube.fits:output'.format(
            img_dir, prefix, field, num)
        step = 'makecube_{}'.format(num)
        images = ['{0:s}/{1:s}_{2:s}_{3}-{4:04d}-{5:s}.fits:output'.format(
            img_dir, prefix, field, num, i, imtype) for i in range(nchans)]
        recipe.add('cab/fitstool', step,
                   {
                       "image": images,
                       "output": im,
                       "stack": True,
                       "fits-axis": 'FREQ',
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Make convolved model'.format(step))

        return im

    def extract_sources(trg, num, img_dir, field):
        key = 'extract_sources'
        if config[key].get('detection_image'):
            step = 'detection_image_field{0:d}_iter{1:d}'.format(trg, num)
            detection_image = '{0:s}/{1:s}-detection_image_{0:s}_{1:d}.fits:output'.format(
                img_dir, prefix, field, num)
            recipe.add('cab/fitstool', step,
                       {
                           "image": ['{0:s}/{1:s}_{2:s}_{3:d}{4:s}-{5:s}.fits:output'.format(img_dir, prefix, field, num, im, mfsprefix) for im in ('image', 'residual')],
                           "output": detection_image,
                           "diff": True,
                           "force": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Make convolved model'.format(step))
        else:
            detection_image = None

        sourcefinder = config[key].get('sourcefinder')
        if (sourcefinder == 'pybdsm' or sourcefinder == 'pybdsf'):
            spi_do = config[key].get('spi')
            if spi_do:
                im = make_cube(num, get_dir_path(
                    pipeline.continuum, pipeline) + '/' + img_dir.split("/")[-1], field, 'image')
                im = im.split("/")[-1]
            else:
                im = '{0:s}_{1:s}_{2:d}{3:s}-image.fits:output'.format(
                    prefix, field, num, mfsprefix)

            step = 'extract_field{0:d}_iter{1:d}'.format(trg, num)
            calmodel = '{0:s}_{1:s}_{2:d}-pybdsm'.format(prefix, field, num)

            if detection_image:
                blank_limit = 1e-9
            else:
                blank_limit = None
            try:
                os.remove(
                    '{0:s}/{1:s}/{2:s}.fits'.format(pipeline.output, img_dir, calmodel))
            except:
                print('No Previous fits log found.')
            try:
                os.remove(
                    '{0:s}/{1:s}/{2:s}.lsm.html'.format(pipeline.output, img_dir, calmodel))
            except:
                print('No Previous lsm.html found.')
            recipe.add('cab/pybdsm', step,
                       {
                           "image": im,
                           "thresh_pix": config[key].get('thresh_pix')[num-1 if len(config[key].get('thresh_pix')) >= num else -1],
                           "thresh_isl": config[key].get('thresh_isl')[num-1 if len(config[key].get('thresh_isl')) >= num else -1],
                           "outfile": '{:s}.gaul:output'.format(calmodel),
                           "blank_limit": sdm.dismissable(blank_limit),
                           "adaptive_rms_box": config[key].get('local_rms'),
                           "port2tigger": False,
                           "format": 'ascii',
                           "multi_chan_beam": spi_do,
                           "spectralindex_do": spi_do,
                           "detection_image": sdm.dismissable(detection_image),
                       },
                       input=pipeline.input,
                       # Unfortuntaly need to do it this way for pybdsm
                       output=pipeline.output + '/' + img_dir,
                       label='{0:s}:: Extract sources'.format(step))
            # In order to make sure that we actually find stuff in the images we execute the recipe here
            recipe.run()
            # Empty job que after execution
            recipe.jobs = []
            # and then check the proper file is produced
            if not os.path.isfile('{0:s}/{1:s}/{2:s}.gaul'.format(pipeline.output, img_dir, calmodel)):
                caracal.log.error(
                    "No model file is found after the PYBDSM run. This probably means no sources were found either due to a bad calibration or to stringent values. ")
                raise caracal.BadDataError("No model file found after the PyBDSM run")

            step = 'convert_extract_field{0:d}_iter{1:d}'.format(trg, num)
            recipe.add('cab/tigger_convert', step,
                       {
                           "input-skymodel": '{0:s}/{1:s}.gaul:output'.format(img_dir, calmodel),
                           "output-skymodel": '{0:s}/{1:s}.lsm.html:output'.format(img_dir, calmodel),
                           "type": 'Gaul',
                           "output-type": 'Tigger',
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Convert extracted sources to tigger model'.format(step))


    def predict_from_fits(num, model, index, img_dir, mslist, field):
        if isinstance(model, str) and len(model.split('+')) == 2:
            combine = True
            mm = model.split('+')
            # Combine FITS models if more than one is given
            step = 'combine_models_' + '_'.join(map(str, mm))
            calmodel = '{0:s}/{1:s}_{2:s}_{3:d}-FITS-combined.fits:output'.format(
                img_dir, prefix, field, num)
            cubes = [make_cube(n, img_dir, field, 'model') for n in mm]
            recipe.add('cab/fitstool', step,
                       {
                           "image": cubes,
                           "output": calmodel,
                           "sum": True,
                           "force": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Add clean components'.format(step))
        else:
            calmodel = make_cube(num, img_dir, field)

        step = 'predict_fromfits_{}'.format(num)
        recipe.add('cab/lwimager', 'predict', {
            "msname": mslist[index],
            "simulate_fits": calmodel,
            "column": 'MODEL_DATA',
            "img_nchan": nchans,
            "img_chanstep": 1,
            # TODO: This should consider SPW IDs
            "nchan": pipeline.nchans[index],
            "cellsize": cell,
            "chanstep": 1,
        },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Predict from FITS ms={1:s}'.format(step, mslist[index]))

    def combine_models(models, num, img_dir, field, enable=True):
      #  model_names = ['{0:s}/{1:s}_{2:s}_{3:s}-pybdsm.lsm.html:output'.format(img_dir,
                         #                                                      prefix, field, m) for m in models]
        model_names = ['{0:s}/{1:s}_{2:s}_{3:s}-pybdsm.lsm.html:output'.format(get_dir_path("{0:s}/image_{1:d}".format(pipeline.continuum, int(m)),pipeline), prefix, field, m) for m in models]

        model_names_fits = ['{0:s}/{1:s}_{2:s}_{3:s}-pybdsm.fits'.format(get_dir_path("{0:s}/image_{1:d}".format(pipeline.continuum, int(m)),pipeline), prefix, field, m) for m in models]
        calmodel = '{0:s}/{1:s}_{2:d}-pybdsm-combined.lsm.html:output'.format(
            img_dir, prefix, num)

        if enable:
            step = 'combine_models_' + '_'.join(map(str, models))
            recipe.add('cab/tigger_convert', step,
                       {
                           "input-skymodel": model_names[0],
                           "append": model_names[1],
                           "output-skymodel": calmodel,
                           "rename": True,
                           "force": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Combined models'.format(step))

        return calmodel, model_names_fits

    def calibrate_meqtrees(trg, num, prod_path, img_dir, mslist, field):
        key = 'calibrate'
        global reset_cal, trace_SN, trace_matrix
        if num == cal_niter:
            vismodel = config[key].get('add_vis_model')
        else:
            vismodel = False
        # force to calibrate with model data column if specified by user

        # If the mode is pybdsm_vis then we want to add the clean component model only at the last step,
        # which is anyway achieved by the **above** statement; no need to further specify vismodel.

        if config[key].get('model_mode') == 'pybdsm_vis':
            model = config[key].get('model')[num-1]

            modelcolumn = 'MODEL_DATA'
            if isinstance(model, str) and len(model.split('+')) > 1:
                mm = model.split('+')
                calmodel, fits_model = combine_models(mm, num, img_dir, field,
                                                      enable=False if pipeline.enable_task(
                                                          config, 'aimfast') else True)
            else:
                model = int(model)
                calmodel = '{0:s}/{1:s}_{2:s}_{3:d}-pybdsm.lsm.html:output'.format(
                    img_dir, prefix, field, model)
                fits_model = '{0:s}/{1:s}_{2:s}_{3:d}-pybdsm.fits'.format(
                    img_dir, prefix, field, model)
        # If the mode is pybdsm_only, don't use any clean components. So, the same as above, but with
        #vismodel =False
        elif config[key].get('model_mode') == 'pybdsm_only':
            vismodel = False
            model = config[key].get('model', num)[num-1]
            if isinstance(model, str) and len(model.split('+')) > 1:
                mm = model.split('+')
                calmodel, fits_model = combine_models(mm, num, img_dir, field,
                                                      enable=False if pipeline.enable_task(
                                                          config, 'aimfast') else True)
            else:
                model = int(model)
                calmodel = '{0:s}/{1:s}_{2:s}_{3:d}-pybdsm.lsm.html:output'.format(
                    img_dir, prefix, field, model)
                fits_model = '{0:s}/{1:s}_{2:s}_{2:d}-pybdsm.fits'.format(
                    img_dir, prefix, field, model)

            modelcolumn = ''
        # If the mode is vis_only, then there is need for an empty sky model (since meqtrees needs one).
        # In this case, vis_model is always true, the model_column is always MODEL_DATA.
        elif config[key].get('model_mode') == 'vis_only':
            vismodel = True
            modelcolumn = 'MODEL_DATA'
            calmodel = '{0:s}_{1:d}-nullmodel.txt'.format(prefix, num)
            with open(os.path.join(pipeline.input, calmodel), 'w') as stdw:
                stdw.write('#format: ra_d dec_d i\n')
                stdw.write('0.0 -30.0 1e-99')
        # Let's see the matrix type we are dealing with
        if not config[key].get('two_step'):
            matrix_type = config[key].get('gain_matrix_type')[
                num - 1 if len(config[key].get('gain_matrix_type')) >= num else -1]
        # If we have a two_step selfcal and Gaindiag we want to use CORRECTED_DATA_PHASE as input and write to CORRECTED_DATA

        outcolumn = "CORRECTED_DATA"
        incolumn = "DATA"

        for i, msname in enumerate(mslist):
             # Let's see the matrix type we are dealing with

            gsols_ = [config[key].get('Gsols_timeslots')[num-1 if num <= len(config[key].get('Gsols_timeslots')) else -1],
                      config[key].get('Gsols_channel')[num-1 if num <= len(config[key].get('Gsols_channel')) else -1]]
            # If we have a two_step selfcal  we will calculate the intervals
            matrix_type = config[key].get('gain_matrix_type')[
                num - 1 if len(config[key].get('gain_matrix_type')) >= num else -1]
            if config[key].get('two_step') and config[key].get('aimfast'):
                if num == 1:
                    matrix_type = 'GainDiagPhase'
                    SN = 3
                else:
                    matrix_type = trace_matrix[num-2]
                    SN = trace_SN[num-2]
                fidelity_data = get_aimfast_data()
                obs_data = get_obs_data(msname)
                int_time = obs_data['EXPOSURE']
                tot_time = 0.0

                for scan_key in obs_data['SCAN']['0']:
                    tot_time += obs_data['SCAN']['0'][scan_key]
                no_ant = len(obs_data['ANT']['DISH_DIAMETER'])
                DR = fidelity_data['{0}_{1}-residual'.format(
                    prefix, num)]['{0}_{1}-model'.format(prefix, num)]['DR']
                Noise = fidelity_data['{0}_{1}-residual'.format(
                    prefix, num)]['STDDev']
                flux = DR * Noise
                solvetime = int(Noise**2 * SN**2 * tot_time *
                                no_ant / (flux**2 * 2.) / int_time)

                if num > 1:
                    DR = fidelity_data['{0}_{1}-residual'.format(
                        prefix, num-1)]['{0}_{1}-model'.format(prefix, num-1)]['DR']
                    flux = DR*Noise
                    prev_solvetime = int(
                        Noise**2*SN**2*tot_time*no_ant/(flux**2*2.)/int_time)
                else:
                    prev_solvetime = solvetime+1

                if (solvetime >= prev_solvetime or reset_cal == 1) and matrix_type == 'GainDiagPhase':
                    matrix_type = 'GainDiag'
                    SN = 8
                    solvetime = int(Noise**2*SN**2*tot_time *
                                    no_ant/(flux**2*2.)/int_time)
                    gsols_[0] = int(solvetime/num)
                elif solvetime >= prev_solvetime and matrix_type == 'GainDiag':
                    gsols_[0] = int(prev_solvetime/num)
                    reset_cal = 2
                else:
                    gsols_[0] = int(solvetime/num)
                if matrix_type == 'GainDiagPhase':
                    minsolvetime = int(30./int_time)
                else:
                    minsolvetime = int(30.*60./int_time)
                if minsolvetime > gsols_[0]:
                    gsols_[0] = minsolvetime
                    if matrix_type == 'GainDiag':
                        reset_cal = 2
                trace_SN.append(SN)
                trace_matrix.append(matrix_type)
                if matrix_type == 'GainDiagPhase' and config[key].get('two_step'):
                    outcolumn = "CORRECTED_DATA_PHASE"
                    incolumn = "DATA"
                elif config[key].get('two_step'):
                    outcolumn = "CORRECTED_DATA"
                    incolumn = "CORRECTED_DATA_PHASE"
            elif config[key].get('two_step'):
                if matrix_type == 'GainDiagPhase':
                    outcolumn = "CORRECTED_DATA_PHASE"
                    incolumn = "DATA"
                else:
                    outcolumn = "CORRECTED_DATA"
                    incolumn = "CORRECTED_DATA_PHASE"

            bsols_ = [config[key].get('Bsols_timeslots')[num-1 if num <= len(config[key].get('Bsols_timeslots')) else -1],
                      config[key].get('Bsols_channel')[num-1 if num <= len(config[key].get('Bsols_channel')) else -1]]
            step = 'calibrate_field{0:d}_iter{1:d}_ms{2:d}'.format(trg, num, i)
            recipe.add('cab/calibrator', step,
                       {
                           "skymodel": calmodel,
                           "add-vis-model": vismodel,
                           "model-column": modelcolumn,
                           "msname": msname,
                           "threads": ncpu,
                           "column": incolumn,
                           "output-data": config[key].get('output_data')[num-1 if len(config[key].get('output_data')) >= num else -1],
                           "output-column": outcolumn,
                           "prefix": '{0:s}/{1:s}_{2:s}_{3:d}_meqtrees'.format(get_dir_path(prod_path, pipeline),
                                                                               pipeline.dataid[i], msname[:-3], num),
                           "label": 'cal{0:d}'.format(num),
                           "read-flags-from-ms": True,
                           "read-flagsets": "-stefcal",
                           "write-flagset": "stefcal",
                           "write-flagset-policy": "replace",
                           "Gjones": True,
                           "Gjones-solution-intervals": sdm.dismissable(gsols_ or None),
                           "Gjones-matrix-type": matrix_type,
                           "Gjones-ampl-clipping": True,
                           "Gjones-ampl-clipping-low": config.get('cal_gain_amplitude_clip_low'),
                           "Gjones-ampl-clipping-high": config.get('cal_gain_amplitude_clip_high'),
                           "Bjones": config[key].get('Bjones'),
                           "Bjones-solution-intervals": sdm.dismissable(bsols_ or None),
                           "Bjones-ampl-clipping": config[key].get('Bjones'),
                           "Bjones-ampl-clipping-low": config.get('cal_gain_amplitude_clip_low'),
                           "Bjones-ampl-clipping-high": config.get('cal_gain_amplitude_clip_high'),
                           "make-plots": True,
                           "tile-size": time_chunk,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label="{0:s}:: Calibrate step {1:d} ms={2:s}".format(step, num, msname))

    def calibrate_cubical(trg, num, prod_path, img_dir, mslist, field):
        key = 'calibrate'

        modellist = []
        model = config[key].get('model', num)[num-1]
        ### Defines the pybdsf models (and fitsmodels for some weird reasons)
        # If the model string contains a +, then combine the appropriate models
        if isinstance(model, str) and len(model.split('+')) > 1:
            mm = model.split('+')
            print(mm,num, img_dir, field)
            calmodel, fits_model = combine_models(mm, num, img_dir, field)
        # If it doesn't then don't combine.
        else:
            model = int(model)
            calmodel = '{0:s}/{1:s}_{2:s}_{3:d}-pybdsm.lsm.html:output'.format(
                img_dir, prefix, field, model)
            fits_model = '{0:s}/{1:s}_{2:s}_{3:d}-pybdsm.fits'.format(
                img_dir, prefix, field, model)
        ## In pybdsm_vis mode, add the calmodel (pybdsf) and the MODEL_DATA.
        if config[key].get('model_mode') == 'pybdsm_vis':
            if (num == cal_niter):
                cmodel = calmodel.split(":output")[0]
                modellist = spf("MODEL_DATA+"+'{}/'+cmodel,"output")

                #modellist = [calmodel, 'MODEL_DATA']
        # otherwise, just calmodel (pybdsf)
            else:
                #modellist = [calmodel]
                cmodel = calmodel.split(":output")[0]
                modellist = spf("{}/"+cmodel,"output")
            # This is incorrect and will result in the lsm being used in the first direction
            # and the model_data in the others. They need to be added as + however
            # that messes up the output identifier structure
        if config[key].get('model_mode') == 'pybdsm_only':
            cmodel = calmodel.split(":output")[0]
            modellist = spf('{}/'+cmodel,"output")
        if config[key].get('model_mode') == 'vis_only':
            modellist = spf("MODEL_DATA")
        matrix_type = config[key].get('gain_matrix_type')[
            num-1 if len(config[key].get('gain_matrix_type')) >= num else -1]
        if matrix_type == 'GainDiagPhase' or config[key].get('two_step'):
            gupdate = 'phase-diag'
            bupdate = 'phase-diag'
            dupdate = 'phase-diag'
        else:
            gupdate = 'full'
            bupdate = 'full'
            dupdate = 'full'

        jones_chain = 'G'
        gsols_ = [config[key].get('Gsols_timeslots')[num - 1 if num <= len(config[key].get('Gsols_timeslots')) else -1],
                  config[key].get('Gsols_channel')[
                      num - 1 if num <= len(config[key].get('Gsols_channel')) else -1]]
        bsols_ = [config[key].get('Bsols_timeslots')[num - 1 if num <= len(config[key].get('Bsols_timeslots')) else -1],
                  config[key].get('Bsols_channel')[
                      num - 1 if num <= len(config[key].get('Bsols_channel')) else -1]]
        ddsols_ = [
            config[key].get('DDsols_timeslots')[num - 1 if num <=
                                           len(config[key].get('DDsols_timeslots')) else -1],
            config[key].get('DDsols_channel')[
                num - 1 if num <= len(config[key].get('DDsols_channel')) else -1]]

        if (config[key].get('two_step') and ddsols_[0] != -1) or config[key].get('ddjones'):
            jones_chain += ',DD'
            matrix_type = 'Gain2x2'
        elif config[key].get('DDjones') and config[key].get('two_step'):
            raise ValueError(
                'You cannot do a DD-gain calibration and a split amplitude-phase calibration all at once')
        if config[key].get('Bjones'):
            jones_chain += ',B'
            matrix_type = 'Gain2x2'

        sol_term_iters = config[key].get('sol_term_iters')
        if sol_term_iters == 'auto':
           sol_terms_add = []
           for term in jones_chain.split(","):
               sol_terms_add.append(SOL_TERMS[term])
           sol_terms = ','.join(sol_terms_add)
        else:
           sol_terms = sol_term_iters
        #Determine the memory requirements of cubical automatically

        for i,msname in enumerate(mslist):
        #    print "sol-term-iters:", config[key].get('sol_term_iters')
        #    print "prefix, field, label=",prefix, field, label
        #    ms_prefix = pipeline.prefixes[i]
        #    print 'ms_prefix', ms_prefix
        #    observation_data = get_obs_data(ms_prefix, field, label)
        #    n_correlations = observation_data['NCOR']
        #    print 'n_correlations', n_correlations
        #    n_ant = len(observation_data['ANT']['NAME'])
        #    n_baseline = n_ant*(n_ant-1)/2
        #    print observation_data.keys()
        #    n_chan = observation_data['SPW']['NUM_CHAN'][0]
        #    print n_chan
        #    sol_t = observation_data['FIELD']['PERIOD'][0]/observation_data['EXPOSURE']
        #    print sol_t, n_baseline, n_correlations
        #    membyte = get_mem()
        #    print "denomitor:", 16.0*n_correlations*n_baseline*n_chan*sol_t
        #    maxtiles = max(1,int(membyte*1.0*0.5/16.0*n_correlations*n_baseline*n_chan*sol_t))
        #    print "memory in kb, number of tiles:", membyte, maxtiles
        #    tilesize = max(sol_t, min(nworker*sol_t, membyte*0.5/(16.0*n_correlations*n_chan*n_baseline)))
        #    print tilesize
            # Determine the number of tiles

            step = 'calibrate_cubical_field{0:d}_iter{1:d}_ms{2:d}'.format(trg, num, i)

            matrix_type = config['calibrate'].get('gain_matrix_type')[
            num-1 if len(config['calibrate'].get('gain_matrix_type')) >= num else -1]

            cubical_opts = {
                "data-ms": msname,
                "data-column": 'DATA',
                "model-list": modellist,
                "data-time-chunk": time_chunk,
                "data-freq-chunk": freq_chunk,
                "sel-ddid": sdm.dismissable(config[key].get('spwid')),
                "dist-ncpu": ncpu,
                "sol-jones": jones_chain,
                "sol-term-iters": ",".join(sol_terms),
                "out-name": '{0:s}/{1:s}_{2:s}_{3:d}_cubical'.format(get_dir_path(prod_path,
                                                                                  pipeline), prefix, msname[:-3], num),
                "out-mode": CUBICAL_OUT[config[key].get('output_data')[num-1 if len(config[key].get('output_data')) >= num else -1]],
                "out-plots": True,
                "dist-max-chunks": config[key].get('dist_max_chunks'),
                "out-casa-gaintables": True,
                "weight-column": config[key].get('weight_column'),
                "montblanc-dtype": 'float',
                "g-solvable": True,
                "g-type": CUBICAL_MT[matrix_type],
                "g-time-int": int(gsols_[0]),
                "g-freq-int": int(gsols_[1]),
                "out-overwrite": True,
                "g-save-to": "{0:s}/g-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                           pipeline), num, msname.split('.ms')[0]),
                "bbc-save-to": "{0:s}/bbc-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                               pipeline), num, msname.split('.ms')[0]),
                "g-clip-low": config.get('cal_gain_amplitude_clip_low'),
                "g-clip-high": config.get('cal_gain_amplitude_clip_high'),
                "madmax-enable": config[key].get('madmax_flagging'),
                "madmax-plot": True if (config[key].get('madmax_flagging')) else False,
                "madmax-threshold": config[key].get('madmax_flag_thresh'),
                "madmax-estimate": 'corr',
                "log-boring": True,
                "dd-dd-term": False,
                "model-ddes": 'never',
            }
            if min_uvw > 0:
                cubical_opts.update({"sol-min-bl": min_uvw})
            if config[key].get('two_step', False) and ddsols_[0] != -1:
                cubical_opts.update({
                    "g-update-type": gupdate,
                    "dd-update-type": 'amp-diag',
                    "dd-solvable": True,
                    "dd-type": CUBICAL_MT[matrix_type],
                    "dd-time-int": int(ddsols_[0]),
                    "dd-freq-int": int(ddsols_[1]),
                    "dd-save-to": "g-amp-gains-{0:d}-{1:s}.parmdb:output".format(num, msname.split('.ms')[0]),
                    "dd-clip-low": config.get('cal_gain_amplitude_clip_low'),
                    "dd-clip-high": config.get('cal_gain_amplitude_clip_high'),
                })
            if config[key].get('Bjones', False):
                cubical_opts.update({
                    "g-update-type": gupdate,
                    "b-update-type": bupdate,
                    "b-solvable": True,
                    "b-time-int": int(bsols_[0]),
                    "b-freq-int": int(bsols_[1]),
                    "b-type": CUBICAL_MT[matrix_type],
                    "b-clip-low": config.get('cal_gain_amplitude_clip_low'),
                    "b-save-to": "{0:s}/b-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                               pipeline), num, msname.split('.ms')[0]),
                    "b-clip-high": config.get('cal_gain_amplitude_clip_high')})


            if config[key].get('DDjones', False):
                cubical_opts.update({"g-update-type": gupdate,
                                     "dd-update-type": dupdate,
                                     "dd-solvable": True,
                                     "dd-time-int": int(ddsols_[0]),
                                     "dd-freq-int": int(ddsols_[1]),
                                     "dd-type": CUBICAL_MT[matrix_type],
                                     "dd-clip-low": config.get('cal_gain_amplitude_clip_low'),
                                     "dd-clip-high": config.get('cal_gain_amplitude_clip_high'),
                                     "dd-dd-term": True,
                                     "dd-save-to": "{0:s}/dE-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                                  pipeline), num, msname.split('.ms')[0]), })

            recipe.add('cab/cubical', step, cubical_opts,
                       input=pipeline.input,
                       output=pipeline.output,
                       shared_memory=config[key].get('shared_memory'),
                       label="{0:s}:: Calibrate step {1:d} ms={2:s}".format(step, num, msname))

    def restore(num, prod_path, mslist_out, enable_inter=True):
        key = 'calibrate'
        calwith = config.get('calibrate_with').lower()
        # First check we are using Cubical
        if (calwith == 'meqtrees'):
            if enable_inter:
                caracal.log.info(
                    'Gains cannot be interpolated with MeqTrees, please switch to CubiCal')
                raise ValueError("Gains cannot be interpolated with MeqTrees, please switch to CubiCal")
            else:
                caracal.log.info(
                    "We cannot reapply MeqTrees calibration at a given step. Hence you will need to do a full selfcal loop.")
                raise ValueError(
                    "We cannot reapply MeqTrees calibration at a given step. Hence you will need to do a full selfcal loop.")
        # to achieve accurate restauration we need to reset all parameters properly
        matrix_type = config[key].get('gain_matrix_type')[
            num - 1 if len(config[key].get('gain_matrix_type')) >= num else -1]
        # Decide if take diagonal terms into account
        if matrix_type == 'Gain2x2':
            take_diag_terms = False
        else:
            take_diag_terms = True

        # set the update type correctly
        if matrix_type == 'GainDiagPhase':
            gupdate = 'phase-diag'
        elif matrix_type == 'GainDiagAmp':
            gupdate = 'amp-diag'
        elif matrix_type == 'GainDiag':
            gupdate = 'diag'
        elif matrix_type == 'Gain2x2':
            gupdate = 'full'
        else:
            raise ValueError('{} is not a viable matrix_type'.format(matrix_type))

        jones_chain = 'G'
        gsols_ = [config[key].get('Gsols_timeslots')[num - 1 if num <= len(config[key].get('Gsols_timeslots')) else -1],
                  config[key].get('Gsols_channel')[
                      num - 1 if num <= len(config[key].get('Gsols_channel')) else -1]]
        bsols_ = [config[key].get('Bsols_timeslots')[num - 1 if num <= len(config[key].get('Bsols_timeslots')) else -1],
                  config[key].get('Bsols_channel')[
                      num - 1 if num <= len(config[key].get('Bsols_channel')) else -1]]
        gasols_ = [
            config[key].get('DDsols_timeslots')[num - 1 if num <=
                                                      len(config[key].get('DDsols_timeslots')) else -1],
            config[key].get('DDsols_channel')[num - 1 if num <=
                                                         len(config[key].get('DDsols_channel')) else -1]]

        # If we want to interpolate our we get the interpolation interval
        if enable_inter and config['transfer_apply_gains']['interpolate'].get('enable'):
            if int(config['transfer_apply_gains']['interpolate'].get('timeslots_int')) >= 0:
                gsols_[0] = config['transfer_apply_gains']['interpolate'].get('timeslots_int')
            if int(config['transfer_apply_gains']['interpolate'].get('channel_int')) >= 0:
                gsols_[1] = config['transfer_apply_gains']['interpolate'].get('channel_int')
            time_chunk_in = gsols_[0] if (
                        gsols_[0] > config['transfer_apply_gains']['interpolate'].get('timeslots_chunk') != 0.) else \
                config['transfer_apply_gains']['interpolate'].get('timeslots_chunk')
            freq_chunk_in = gsols_[1] if (
                        gsols_[1] > config['transfer_apply_gains']['interpolate'].get('channel_chunk') != 0) else \
                config['transfer_apply_gains']['interpolate'].get('channel_chunk')
        else:
            time_chunk_in = gsols_[0] if (gsols_[0] > time_chunk != 0.) else \
                time_chunk
            freq_chunk_in = gsols_[1] if (gsols_[1] > freq_chunk != 0.) else \
                freq_chunk
        if config[key].get('Bjones'):
            jones_chain += ',B'
            matrix_type = 'Gain2x2'
            bupdate = gupdate
            if enable_inter and config['transfer_apply_gains']['interpolate'].get('enable'):
                if int(config['transfer_apply_gains']['interpolate'].get('timeslots_int')) >= 0:
                    bsols_[0] = config['transfer_apply_gains']['interpolate'].get('timeslots_int')
                if int(config['transfer_apply_gains']['interpolate'].get('channel_int')) >= 0:
                    bsols_[1] = config['transfer_apply_gains']['interpolate'].get('channel_int')
            if bsols_[0] > time_chunk_in:
                time_chunk_in = bsols_[0]
            if bsols_[1] > freq_chunk_in:
                freq_chunk_in = bsols_[1]
        # If we are doing a calibration of phases and amplitudes on different timescale G is always phase
        # This cannot be combined with the earlier statement as bupdate needs to be equal to the original matrix.
        if config[key].get('two_step'):
            gupdate = 'phase-diag'
            if gasols_[0] != -1:
                jones_chain += ',DD'
                matrix_type = 'Gain2x2'
                if enable_inter and config['transfer_apply_gains']['interpolate'].get('enable'):
                    if int(config['transfer_apply_gains']['interpolate'].get('timeslots_int')) >= 0:
                        gasols_[0] = config['transfer_apply_gains']['interpolate'].get('timeslots_int')
                    if int(config['transfer_apply_gains']['interpolate'].get('channel_int')) >= 0:
                        gasols_[1] = config['transfer_apply_gains']['interpolate'].get('channel_int')
                if gasols_[0] > time_chunk_in:
                    time_chunk_in = gasols_[0]
                if gasols_[1] > freq_chunk_in:
                    freq_chunk_in = gasols_[1]
        # if we are interpolating to 1 without setting the chunks th
        if freq_chunk_in == 1:
            freq_chunk_in = freq_chunk
        if time_chunk_in == 1:
            time_chunk_in = time_chunk

        # select the right datasets
        if enable_inter:
            apmode = 'ac'
        else:
            if CUBICAL_OUT[
                config[key].get('output_data')[num - 1 if len(config[key].get('output_data')) >= num else -1]] == 'sr':
                apmode = 'ar'
            else:
                apmode = 'ac'
        # Cubical does not at the moment apply the gains when the matrix is not complex2x2 (https://github.com/ratt-ru/CubiCal/issues/324).
        # Hence the following fix. This should be removed once the fix makes it into stimela.
        matrix_type = 'Gain2x2'
        # Does sol_term_iters  matter for applying?????
        sol_term_iters = config[key].get('sol_term_iters')
        if sol_term_iters == 'auto':
            sol_terms_add = []
            for term in jones_chain.split(","):
                sol_terms_add.append(SOL_TERMS[term])
            sol_terms = ','.join(sol_terms_add)
        else:
            sol_terms = sol_term_iters
        # loop through measurement sets
        for i, msname_out in enumerate(mslist_out):
            print(gsols_, gasols_)
            print("It was here")
            # Python is really the dumbest language ever so need deep copies else the none apply variables change along with apply
            gsols_apply = copy.deepcopy(gsols_)
            bsols_apply = copy.deepcopy(bsols_)
            gasols_apply = copy.deepcopy(gasols_)
            time_chunk_apply = copy.deepcopy(time_chunk_in)
            freq_chunk_apply = copy.deepcopy(freq_chunk_in)
            ##Read the time and frequency channels of the  'fullres'
            fullres_data = get_obs_data(msname_out)
            print(fullres_data.keys())
            int_time_fullres = fullres_data['EXPOSURE']
            channelsize_fullres = fullres_data['SPW']['TOTAL_BANDWIDTH'][0] / fullres_data['SPW']['NUM_CHAN'][0]
            print("Integration time of full-resolution data is:", int_time_fullres)
            print("Channel size of full-resolution data is:", channelsize_fullres)
            # Corresponding numbers for the self-cal -ed MS:
            avg_data = get_obs_data(mslist[i])
            int_time_avg = avg_data['EXPOSURE']
            channelsize_avg = avg_data['SPW']['TOTAL_BANDWIDTH'][0] / avg_data['SPW']['NUM_CHAN'][0]
            print("Integration time of averaged data is:", int_time_avg)
            print("Channel size of averaged data is:", channelsize_avg)
            # Compare the channel and timeslot ratios:
            ratio_timeslot = int_time_avg / int_time_fullres
            ratio_channelsize = channelsize_avg / channelsize_fullres
            # If we are restoring a step we need to restore the flags of the 2gc process
            if not enable_inter:
                fromname = msname_out
                # First remove the later flags
                counter = num + 1
                remainder_flags = "step_{0:d}_2gc_flags".format(counter)

                while counter < cal_niter:
                    counter += 1
                    remainder_flags += ",step_{0:d}_2gc_flags".format(counter)
                mspref = msname_out.split(".ms")[0].replace("-", "_")
                recipe.add("cab/flagms", "remove_2gc_flags_{0:s}".format(mspref),
                           {
                               "msname": msname_out,
                               "remove": remainder_flags,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label="remove_2gc_flags_{0:s}:: Remove 2GC flags".format(mspref))
            else:
                fromname = msname_out.replace(label_tgain, label)
                if not config['transfer_apply_gains']['interpolate'].get('enable'):
                    gsols_apply[0] = int(ratio_timeslot * gsols_[0])
                    gsols_apply[1] = int(ratio_channelsize * gsols_[1])
                    time_chunk_apply = int(max(gsols_[0], time_chunk_in)) if not (
                                int(gsols_[0]) == 0 or time_chunk_in == 0) else 0
                    freq_chunk_apply = int(max(gsols_[1], freq_chunk_in)) if not (
                                int(gsols_[1]) == 0 or freq_chunk_in == 0) else 0

            # build cubical commands
            cubical_gain_interp_opts = {
                "data-ms": msname_out,
                "data-column": 'DATA',
                "sel-ddid": sdm.dismissable(config[key].get('spwid')),
                "sol-jones": jones_chain,
                "sol-term-iters": ",".join(sol_terms),
                "sel-diag": take_diag_terms,
                "dist-ncpu": ncpu,
                "out-name": '{0:s}/{1:s}-{2:s}_{3:d}_restored_cubical'.format(get_dir_path(prod_path,
                                                                                           pipeline), prefix,
                                                                              msname_out[:-3], num),
                "out-mode": apmode,
                #"out-overwrite": config[key].get('overwrite'),
                "out-overwrite": True,
                "weight-column": config[key].get('weight_column'),
                "montblanc-dtype": 'float',
                "g-solvable": True,
                "g-update-type": gupdate,
                "g-type": CUBICAL_MT[matrix_type],
                "g-time-int": int(gsols_apply[0]),
                "g-freq-int": int(gsols_apply[1]),
                "madmax-enable": config[key].get('madmax_flagging'),
                "madmax-plot": False,
                "madmax-threshold": config[key].get('madmax_flag_thresh'),
                "madmax-estimate": 'corr',
                "madmax-offdiag": False,
                "dd-dd-term": False,
                "model-ddes": 'never',
            }
            if config['transfer_apply_gains']['interpolate'].get('enable'):
                cubical_gain_interp_opts.update({
                    "g-xfer-from": "{0:s}/g-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                 pipeline), num,
                                                                                    fromname.split('.ms')[0])
                })
            else:
                cubical_gain_interp_opts.update({
                    "g-load-from": "{0:s}/g-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                 pipeline), num,
                                                                                    fromname.split('.ms')[0])
                })

            # expand
            if config[key].get('Bjones'):
                if enable_inter and not config['transfer_apply_gains']['interpolate'].get('enable'):
                    bsols_apply[0] = int(ratio_timeslot * bsols_[0])
                    bsols_apply[1] = int(ratio_channelsize * bsols_[1])
                    time_chunk_apply = int(max(bsols_[0], time_chunk_in)) if not (
                                int(bsols_[0]) == 0 or time_chunk_in == 0) else 0
                    freq_chunk_apply = int(max(bsols_[1], freq_chunk_in)) if not (
                                int(bsols_[1]) == 0 or freq_chunk_in == 0) else 0
                cubical_gain_interp_opts.update({
                    "b-update-type": bupdate,
                    "b-type": CUBICAL_MT[matrix_type],
                    "b-time-int": int(bsols_apply[0]),
                    "b-freq-int": int(bsols_apply[1]),
                    "b-solvable": False
                })
                if config['transfer_apply_gains']['interpolate'].get('enable'):
                    cubical_gain_interp_opts.update({
                        "b-xfer-from": "{0:s}/b-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                     pipeline), num,
                                                                                        fromname.split('.ms')[0])
                    })
                else:
                    cubical_gain_interp_opts.update({
                        "b-load-from": "{0:s}/b-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                     pipeline), num,
                                                                                        fromname.split('.ms')[0])
                    })
            if config[key].get('two_step') and gasols_[0] != -1:
                if enable_inter and not config['transfer_apply_gains']['interpolate'].get('enable'):
                    gasols_apply[0] = int(ratio_timeslot * gasols_[0])
                    gasols_apply[1] = int(ratio_channelsize * gasols_[1])
                    time_chunk_apply = int(max(gasols_[0], time_chunk_in)) if not (
                                int(gasols_[0]) == 0 or time_chunk_in == 0) else 0
                    freq_chunk_apply = int(max(gasols_[1], freq_chunk_in)) if not (
                                int(gasols_[1]) == 0 or freq_chunk_in == 0) else 0
                cubical_gain_interp_opts.update({
                    "dd-update-type": 'amp-diag',
                    "dd-type": CUBICAL_MT[matrix_type],
                    "dd-time-int": int(gasols_apply[0]),
                    "dd-freq-int": int(gasols_apply[1]),
                    "dd-solvable": False
                })
                if config['transfer_apply_gains']['interpolate'].get('enable'):
                    cubical_gain_interp_opts.update({
                        "dd-xfer-from": "{0:s}/g-amp-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                          pipeline),
                                                                                             num,
                                                                                             fromname.split('.ms')[0])
                    })
                else:
                    cubical_gain_interp_opts.update({
                        "dd-load-from": "{0:s}/g-amp-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                          pipeline),
                                                                                             num,
                                                                                             fromname.split('.ms')[0])
                    })
            cubical_gain_interp_opts.update({
                "data-time-chunk": time_chunk_apply,
                "data-freq-chunk": freq_chunk_apply
            })
            # ensure proper logging for restore or interpolation
            if not enable_inter:
                step = 'restore_cubical_gains_{0:d}_{1:d}'.format(num, i)
                stim_label = "{0:s}:: restore cubical gains ms={1:s}".format(step, msname_out)
            else:
                step = 'apply_cubical_gains_{0:d}_{1:d}'.format(num, i)
                stim_label = "{0:s}:: Apply cubical gains ms={1:s}".format(step, msname_out)
            recipe.add('cab/cubical', step, cubical_gain_interp_opts,
                       input=pipeline.input,
                       output=pipeline.output,
                       shared_memory=config[key].get('shared_memory'),
                       label=stim_label)
            recipe.run()
            # Empty job que after execution
            recipe.jobs = []

    def get_aimfast_data(filename='{0:s}/fidelity_results.json'.format(pipeline.output)):
        "Extracts data from the json data file"
        with open(filename) as f:
            data = json.load(f)
        return data

    def get_obs_data(msname):
        "Extracts data from the json data file"
        filename='{0:s}/{1:s}-obsinfo.json'.format(pipeline.output,msname[:-3])
        with open(filename) as f:
            data = json.load(f)
        return data

    def quality_check(n, field, enable=True):
        "Examine the aimfast results to see if they meet specified conditions"
        # If total number of iterations is reached stop
        global reset_cal
        if enable:
            # The recipe has to be executed at this point to get the image fidelity results

            recipe.run()
            # Empty job que after execution
            recipe.jobs = []
            if reset_cal >= 2:
                return False
            key = 'aimfast'
            tolerance = config[key].get('tolerance')
            fidelity_data = get_aimfast_data()
            # Ensure atleast one iteration is ran to compare previous and subsequent images
            if n >= 2:
                conv_crit = config[key].get('convergence_criteria')
                conv_crit = [cc.upper() for cc in conv_crit]
                # Ensure atleast one iteration is ran to compare previous and subsequent images
                residual0 = fidelity_data['{0}_{1}_{2}-residual'.format(
                    prefix, field, n-1)]
                residual1 = fidelity_data['{0}_{1}_{2}-residual'.format(
                    prefix, field, n)]
                # Unlike the other ratios DR should grow hence n-1/n < 1.

                if not pipeline.enable_task(config, 'extract_sources'):
                    drratio = fidelity_data['{0}_{1}_{2}-restored'.format(prefix, field, n-1)]['DR']/fidelity_data[
                        '{0}_{1}_{2}-restored'.format(prefix, field, n)]['DR']
                else:
                    drratio = residual0['{0}_{1}_{2}-model'.format(prefix, field,
                                                                   n-1)]['DR']/residual1['{0}_{1}_{2}-model'.format(prefix, field, n)]['DR']

                # Dynamic range is important,
                if any(cc == "DR" for cc in conv_crit):
                    drweight = 0.8
                else:
                    drweight = 0.
                # The other parameters should become smaller, hence n/n-1 < 1
                skewratio = residual1['SKEW']/residual0['SKEW']
                # We care about the skewness when it is large. What is large?
                # Let's go with 0.005 at that point it's weight is 0.5
                if any(cc == "SKEW" for cc in conv_crit):
                    skewweight = residual1['SKEW']/0.01
                else:
                    skewweight = 0.
                kurtratio = residual1['KURT']/residual0['KURT']
                # Kurtosis goes to 3 so this way it counts for 0.5 when normal distribution
                if any(cc == "KURT" for cc in conv_crit):
                    kurtweight = residual1['KURT']/6.
                else:
                    kurtweight = 0.
                meanratio = residual1['MEAN']/residual0['MEAN']
                # We only care about the mean when it is large compared to the noise
                # When it deviates from zero more than 20% of the noise this is a problem
                if any(cc == "MEAN" for cc in conv_crit):
                    meanweight = residual1['MEAN']/(residual1['STDDev']*0.2)
                else:
                    meanweight = 0.
                noiseratio = residual1['STDDev']/residual0['STDDev']
                # The noise should not change if the residuals are gaussian in n-1.
                # However, they should decline in case the residuals are non-gaussian.
                # We want a weight that goes to 0 in both cases
                if any(cc == "STDDEV" for cc in conv_crit):
                    if residual0['KURT']/6. < 0.52 and residual0['SKEW'] < 0.01:
                        noiseweight = abs(1.-noiseratio)
                    else:
                        # If declining then noiseratio is small and that's good, If rising it is a real bad thing.
                        #  Hence we can just square the ratio
                        noiseweight = noiseratio
                else:
                    noiseweight = 0.
                # A huge increase in DR can increase the skew and kurtosis significantly which can mess up the calculations
                if drratio < 0.6:
                    skewweight = 0.
                    kurtweight = 0.

                # These weights could be integrated with the ratios however while testing I
                #  kept them separately such that the idea behind them is easy to interpret.
                # This  combines to total weigth of 1.2+0.+0.5+0.+0. so our total should be LT 1.7*(1-tolerance)
                # it needs to be slightly lower to avoid keeping fitting without improvement
                # Ok that is the wrong philosophy. Their weighted mean should be less than 1-tolerance that means improvement.
                # And the weights control how important each parameter is.
                HolisticCheck = (drratio*drweight+skewratio*skewweight+kurtratio*kurtweight+meanratio*meanweight+noiseratio*noiseweight) \
                    / (drweight+skewweight+kurtweight+meanweight+noiseweight)
                if (1 - tolerance) < HolisticCheck:
                    caracal.log.info(
                        'Stopping criterion: '+' '.join([cc for cc in conv_crit]))
                    caracal.log.info('The calculated ratios DR={:f}, Skew={:f}, Kurt={:f}, Mean={:f}, Noise={:f} '.format(
                        drratio, skewratio, kurtratio, meanratio, noiseratio))
                    caracal.log.info('The weights used DR={:f}, Skew={:f}, Kurt={:f}, Mean={:f}, Noise={:f} '.format(
                        drweight, skewweight, kurtweight, meanweight, noiseweight))
                    caracal.log.info('{:f} < {:f}'.format(
                        1-tolerance, HolisticCheck))
                #   If we stop we want change the final output model to the previous iteration
                    global self_cal_iter_counter
                    reset_cal += 1
                    if reset_cal == 1:
                        self_cal_iter_counter -= 1
                    else:
                        self_cal_iter_counter -= 2

                    if self_cal_iter_counter < 1:
                        self_cal_iter_counter = 1
                    return True
        # If we reach the number of iterations we want to stop.
        if n == cal_niter + 1:
            caracal.log.info(
                'Number of iterations reached: {:d}'.format(cal_niter))
            return False
        # If no condition is met return true to continue
        return True

    def image_quality_assessment(num, img_dir, field):
        # Check if more than two calibration iterations to combine successive models
        # Combine models <num-1> (or combined) to <num> creat <num+1>-pybdsm-combine
        # This was based on thres_pix but change to model as when extract_sources = True is will take the last settings
        if len(config['calibrate'].get('model')) >= num:
            model = config['calibrate'].get('model', num)[num-1]
            if isinstance(model, str) and len(model.split('+')) == 2:
                mm = model.split('+')
                combine_models(mm, num, img_dir, field)
        # in case we are in the last round, imaging has made a model that is longer then the expected model column
        # Therefore we take this last model if model is not defined
        if num == cal_niter+1:
            try:
                model.split()
            except NameError:
                model = str(num)

        step = 'aimfast'
        aimfast_settings = {
            "residual-image": '{0:s}/{1:s}_{2:s}_{3:d}{4:s}-residual.fits:output'.format(img_dir, prefix, field, num, mfsprefix),
            "normality-test": config[step].get('normality_model'),
            "area-factor": config[step].get('area_factor'),
            "label": "{0:s}_{1:s}_{2:d}".format(prefix, field, num),
        }

        # if we run pybdsm we want to use the  model as well. Otherwise we want to use the image.

        if pipeline.enable_task(config, 'extract_sources'):
            if config['calibrate'].get('model_mode', None) == 'vis_only':
                aimfast_settings.update({"tigger-model": '{0:s}/{1:s}_{2:s}_{3:d}-pybdsm{4:s}.lsm.html:output'.format(img_dir,
                                                                                                                      prefix, field, num, '')})
            else:
                aimfast_settings.update({"tigger-model": '{0:s}/{1:s}_{2:s}_{3:d}-pybdsm{4:s}.lsm.html:output'.format(img_dir,
                                                                                                                      prefix, field, num if num <= len(config['calibrate'].get('model', num))
                                                                                                                      else len(config['calibrate'].get('model', num)),
                                                                                                                      '-combined' if len(model.split('+')) >= 2 else '')})

        else:
            # Use the image
            if config['calibrate'].get('output_data')[num-1 if num <= len(config['calibrate'].get('output_data')) else -1] == "CORR_DATA":
                aimfast_settings.update({"restored-image" : '{0:s}/{1:s}_{2:s}_{3:d}{4:s}-image.fits:output'.format(img_dir, prefix, field, num, mfsprefix)})

            else:
                try:
                    im = config['calibrate'].get(
                        'output_data').index("CORR_RES") + 1
                except ValueError:
                    im = num
                aimfast_settings.update({"restored-image": '{0:s}/{1:s}_{2:s}_{3:d}{4:s}-image.fits:output'.format(img_dir,
                                                                                                                   prefix, field, im, mfsprefix)})
        recipe.add('cab/aimfast', step,
                   aimfast_settings,
                   input=pipeline.output,
                   output=pipeline.output,
                   label="{0:s}_{1:d}:: Image fidelity assessment for {2:d}".format(step, num, num))

    def aimfast_plotting(field):
        """Plot comparisons of catalogs and residuals"""

        cont_dir = get_dir_path(pipeline.continuum, pipeline)
        # Get residuals to compare
        res_files = []
        for ii in range(0, cal_niter + 1):
            res_file = glob.glob("{0:s}/image_{1:d}/{2:s}_{3:s}_?-MFS-residual.fits".format(
                pipeline.continuum, ii+1, prefix, field))
            res_files.append(res_file)

        res_files = sorted(res_files)
        res_files = [res for res_files in res_files for res in res_files]

        residuals = []
        for ii in range(0, len(res_files)-1):
            residuals.append('{0:s}:{1:s}:output'.format(res_files[ii].split(
                'output/')[-1], res_files[ii + 1].split('output/')[-1]))

        # Get models to compare - maybe this needs changing?
        model_files = []
        for ii in range(0, cal_niter + 1):
            model_file = glob.glob(
                "{0:s}/image_{1:d}/{2:s}_{3:s}_?.lsm.html".format(pipeline.continuum, ii+1, prefix, field))
            model_files.append(model_file)

        model_files = sorted(model_files)
        model_files = [
            mod for model_files in model_files for mod in model_files]

        models = []
        for ii in range(0, len(model_files)-1):
            models.append('{0:s}:{1:s}:output'.format(model_files[ii].split(
                'output/')[-1], model_files[ii + 1].split('output/')[-1]))

        if len(model_files) > 1:
            step = "aimfast_comparing_models"

            recipe.add('cab/aimfast', step,
                       {
                           "compare-models": models,
                           "area-factor": config['aimfast'].get('area_factor')
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label="Plotting model comparisons")

        if len(res_files) > 1:
            step = "aimfast_comparing_random_residuals"

            recipe.add('cab/aimfast', step,
                       {
                           "compare-residuals": residuals,
                           "area-factor": config['aimfast'].get('area_factor'),
                           "data-points": 100
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label="Plotting random residuals comparisons")

        if len(res_files) > 1 and len(model_files) > 1:
            step = "aimfast_comparing_source_residuals"

            recipe.add('cab/aimfast', step,
                       {
                           "compare-residuals": residuals,
                           "area-factor": config['aimfast'].get('area_factor'),
                           "tigger-model": '{:s}:output'.format(model_files[-1].split('output/')[-1])
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label="Plotting source residuals comparisons")

    def ragavi_plotting_cubical_tables():
        """Plot self-cal gain tables"""

        B_tables = glob.glob('{0:s}/{1:s}/{2:s}/{3:s}'.format(pipeline.output,
                                                              get_dir_path(pipeline.continuum, pipeline), 'selfcal_products', 'g-gains*B.casa'))
        if len(B_tables) > 1:
            step = 'plot_G_gain_table'

            gain_table_name = config['calibrate']['ragavi_plot'].get('table',
                                                                     [table.split('output/')[-1] for table in B_tables])  # This probably needs changing?
            recipe.add('cab/ragavi', step,
                       {
                           "table": [tab+":output" for tab in gain_table_name],
                           "gaintype": config['calibrate']['ragavi_plot'].get('gaintype'),
                           "field": config['calibrate']['ragavi_plot'].get('field'),
                           "htmlname": '{0:s}/{1:s}/{2:s}_self-cal_G_gain_plots'.format(get_dir_path(pipeline.diagnostic_plots,
                                                                                                     pipeline), 'selfcal', prefix)
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Plot gaincal phase : {1:s}'.format(step, ' '.join(B_tables)))

        D_tables = glob.glob('{0:s}/{1:s}/{2:s}/{3:s}'.format(pipeline.output,
                                                              get_dir_path(pipeline.continuum, pipeline), 'selfcal_products', 'g-gains*D.casa'))
        if len(D_tables) > 1:
            step = 'plot_D_gain_table'

            gain_table_name = config['calibrate']['ragavi_plot'].get('table',
                                                                     [table.split('output/')[-1] for table in D_tables])
            recipe.add('cab/ragavi', step,
                       {
                           "table": [tab+":output" for tab in gain_table_name],
                           "gaintype": config['calibrate']['ragavi_plot'].get('gaintype'),
                           "field": config['calibrate']['ragavi_plot'].get('field'),
                           "htmlname": '{0:s}/{1:s}/{2:s}_self-cal_D_gain_plots'.format(get_dir_path(pipeline.diagnostic_plots,
                                                                                                     pipeline), 'selfcal', prefix)
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Plot gain tables : {1:s}'.format(step, ' '.join(D_tables)))

    # decide which tool to use for calibration
    calwith = config.get('calibrate_with').lower()
    if calwith == 'meqtrees':
        calibrate = calibrate_meqtrees
    elif calwith == 'cubical':
        calibrate = calibrate_cubical

    # if we do not run pybdsm we always need to output the corrected data column
    if not pipeline.enable_task(config, 'extract_sources'):
        config['calibrate']['output_data'] = [k.replace(
            'CORR_RES', 'CORR_DATA') for k in config['calibrate'].get('output_data')]

    if pipeline.enable_task(config, 'aimfast'):
        # If aimfast plotting is enabled run source finder
        if config['aimfast'].get('plot'):
            config['extract_sources']['enable'] = True

    target_iter=0
    for target in all_targets:
        mslist = ms_dict[target]
        field = utils.filter_name(target)

        global self_cal_iter_counter
        self_cal_iter_counter = config.get('start_at_iter')
        global reset_cal
        reset_cal = 0
        global trace_SN
        trace_SN = []
        global trace_matrix
        trace_matrix = []

        image_path = "{0:s}/image_{1:d}".format(
            pipeline.continuum, self_cal_iter_counter)
        if not os.path.exists(image_path):
            os.mkdir(image_path)

        mask_key = config['image'].get('clean_mask_method')[0]
        #print(mask_key)
        if pipeline.enable_task(config, 'image'):
            if config['calibrate'].get('hires_interpol') == True:
                caracal.log.info("Interpolating gains")
            if mask_key == 'sofia':
                image_path = "{0:s}/image_0".format(
                    pipeline.continuum, self_cal_iter_counter)
                if not os.path.exists(image_path):
                    os.mkdir(image_path)
                fake_image(target_iter, 0, get_dir_path(
                image_path, pipeline), mslist, field)
                sofia_mask(target_iter, 0, get_dir_path(
                image_path, pipeline), field)
                config['image']['clean_mask_method'].insert(1,config['image']['clean_mask_method'][self_cal_iter_counter if len(config['image'].get('clean_mask_method')) > self_cal_iter_counter else -1])
                image_path = "{0:s}/image_{1:d}".format(
                    pipeline.continuum, self_cal_iter_counter)
                image(target_iter, self_cal_iter_counter, get_dir_path(
                image_path, pipeline), mslist, field)
            else:
                image(target_iter, self_cal_iter_counter, get_dir_path(
                image_path, pipeline), mslist, field)
                #if config['image'].get('clean_mask_method')[self_cal_iter_counter if len(config['image'].get('clean_mask_method')) > self_cal_iter_counter else -1]=='sofia':
                #    config['image'].get('clean_mask_threshold')[0]=config['image'].get('clean_mask_threshold')[1]
                #    sofia_mask(self_cal_iter_counter, get_dir_path(
                #        image_path, pipeline), field)
                #sofia_mask(self_cal_iter_counter, get_dir_path(
                #image_path, pipeline), field)
        ### to enable eventually if one wants only to run caracal to produce sofia mask
        #if pipeline.enable_task(config, 'image') == False and mask_key =='sofia':
        #    sofia_mask(0, get_dir_path(
        #        image_path, pipeline), field)
        #        config['image']['clean_mask_method'].insert(1,config['image']['clean_mask_method'][1])
        if pipeline.enable_task(config, 'extract_sources'):
            extract_sources(target_iter, self_cal_iter_counter, get_dir_path(
                image_path, pipeline), field)
        if pipeline.enable_task(config, 'aimfast'):
            image_quality_assessment(
                self_cal_iter_counter, get_dir_path(image_path, pipeline), field)

        while quality_check(self_cal_iter_counter, field, enable=pipeline.enable_task(config, 'aimfast')):
            if pipeline.enable_task(config, 'calibrate'):
                selfcal_products = "{0:s}/{1:s}".format(
                    pipeline.continuum, 'selfcal_products')
                if not os.path.exists(selfcal_products):
                    os.mkdir(selfcal_products)
                calibrate(target_iter, self_cal_iter_counter, selfcal_products,
                          get_dir_path(image_path, pipeline), mslist, field)
            if reset_cal < 2:
                mask_key=config['image'].get('clean_mask_method')[self_cal_iter_counter if len(config['image'].get('clean_mask_method')) > self_cal_iter_counter else -1]
                if mask_key=='sofia' and self_cal_iter_counter != cal_niter+1:
                    sofia_mask(target_iter, self_cal_iter_counter, get_dir_path(
                        image_path, pipeline), field)
                self_cal_iter_counter += 1
                image_path = "{0:s}/image_{1:d}".format(
                     pipeline.continuum, self_cal_iter_counter)
                if not os.path.exists(image_path):
                    os.mkdir(image_path)
                if pipeline.enable_task(config, 'image'):
                    image(target_iter, self_cal_iter_counter, get_dir_path(
                        image_path, pipeline), mslist, field)
                if pipeline.enable_task(config, 'extract_sources'):
                    extract_sources(target_iter, self_cal_iter_counter, get_dir_path(
                        image_path, pipeline), field)
                if pipeline.enable_task(config, 'aimfast'):
                    image_quality_assessment(
                        self_cal_iter_counter, get_dir_path(image_path, pipeline), field)

        # Copy plots from the selfcal_products to the diagnotic plots IF calibrate OR transfer_gains is enabled
        if pipeline.enable_task(config, 'calibrate') or pipeline.enable_task(config, 'transfer_apply_gains'):
            selfcal_products = "{0:s}/{1:s}".format(
                pipeline.continuum, 'selfcal_products')
            plot_path = "{0:s}/{1:s}".format(
                pipeline.diagnostic_plots, 'selfcal')
            if not os.path.exists(plot_path):
                os.mkdir(plot_path)

            selfcal_plots = glob.glob(
                "{0:s}/{1:s}".format(selfcal_products, '*.png'))
            for plot in selfcal_plots:
                shutil.copy(plot, plot_path)

        if pipeline.enable_task(config, 'transfer_apply_gains'):
            mslist_out = ms_dict_tgain[target]
            if (self_cal_iter_counter > cal_niter):
                restore(
                    self_cal_iter_counter-1, selfcal_products, mslist_out, enable_inter=True)
            else:
                restore(
                    self_cal_iter_counter, selfcal_products, mslist_out, enable_inter=True)

        if pipeline.enable_task(config, 'aimfast'):
            if config['aimfast']['plot']:
                aimfast_plotting(field)
                recipe.run()
                # Empty job que after execution
                recipe.jobs = []

            #  Move the aimfast html plots
                plot_path = "{0:s}/{1:s}".format(
                    pipeline.diagnostic_plots, 'selfcal')
                if not os.path.exists(plot_path):
                    os.mkdir(plot_path)
                aimfast_plots = glob.glob(
                    "{0:s}/{1:s}".format(pipeline.output, '*.html'))
                for plot in aimfast_plots:
                    shutil.move(
                        plot, '{0:s}/{1:s}'.format(plot_path, plot.split('output/')[-1]))

        if pipeline.enable_task(config, 'calibrate'):
            if config['calibrate']['ragavi_plot']['enable']:
                ragavi_plotting_cubical_tables()

        if pipeline.enable_task(config, 'restore_model'):
            if config['restore_model']['model']:
                num = config['restore_model']['model']
                if isinstance(num, str) and len(num.split('+')) == 2:
                    mm = num.split('+')
                    if int(mm[-1]) > self_cal_iter_counter:
                        num = str(self_cal_iter_counter)
            else:
                extract_sources = len(config['extract_sources'].get(
                    'thresh_isl', [self_cal_iter_counter]))
                if extract_sources > 1:
                    num = '{:d}+{:d}'.format(self_cal_iter_counter -
                                             1, self_cal_iter_counter)
                else:
                    num = self_cal_iter_counter
            if isinstance(num, str) and len(num.split('+')) == 2:
                mm = num.split('+')
                models = ['{0:s}/image_{1:s}/{2:s}_{3:s}_{4:s}-pybdsm.lsm.html\
:output'.format(get_dir_path(pipeline.continuum, pipeline),
                          m, prefix, field, m) for m in mm]
                final = '{0:s}/image_{1:s}/{2:s}_{3:s}_final-pybdsm.lsm.html\
:output'.format(get_dir_path(pipeline.continuum, pipeline), mm[-1], prefix, field)

                step = 'create_final_lsm_{0:s}_{1:s}'.format(*mm)
                recipe.add('cab/tigger_convert', step,
                           {
                               "input-skymodel": models[0],
                               "append": models[1],
                               "output-skymodel": final,
                               "rename": True,
                               "force": True,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Combined models'.format(step))

            elif isinstance(num, str) and num.isdigit():
                inputlsm = '{0:s}/image_{1:s}/{2:s}_{3:s}_{4:s}-pybdsm.lsm.html\
:output'.format(get_dir_path(pipeline.continuum, pipeline), num, prefix, field, num)
                final = '{0:s}/image_{1:s}/{2:s}_{3:s}_final-pybdsm.lsm.html\
:output'.format(get_dir_path(pipeline.continuum, pipeline), num, prefix, field)
                step = 'create_final_lsm_{0:s}'.format(num)
                recipe.add('cab/tigger_convert', step,
                           {
                               "input-skymodel": inputlsm,
                               "output-skymodel": final,
                               "rename": True,
                               "force": True,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Combined models'.format(step))
            else:
                raise ValueError(
                    "restore_model_model should be integer-valued string or indicate which models to be appended, eg. 2+3")

            if config['restore_model'].get('clean_model'):
                num = int(config['restore_model'].get('clean_model'))
                if num > self_cal_iter_counter:
                    num = self_cal_iter_counter

                conv_model = '{0:s}/image_{1:d}/{2:s}_{3:s}-convolved_model.fits\
:output'.format(get_dir_path(pipeline.continuum, pipeline), num, prefix, field)
                recipe.add('cab/fitstool', step,
                           {
                               "image": ['{0:s}/image_{1:d}/{2:s}_{3:s}_{4:d}{5:s}-{6:s}.fits\
:output'.format(get_dir_path(pipeline.continuum, pipeline), num, prefix, target, num,
                                   mfsprefix, im) for im in ('image', 'residual')],
                               "output": conv_model,
                               "diff": True,
                               "force": True,
                           },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Make convolved model'.format(step))

                with_cc = '{0:s}/image_{1:d}/{2:s}_{3:s}-with_cc.fits:output'.format(get_dir_path(pipeline.continuum,
                                                                                                  pipeline), num, prefix, field)
                recipe.add('cab/fitstool', step,
                           {
                               "image": ['{0:s}/image_{1:d}/{2:s}_{3:s}_{4:d}{5:s}-image.fits:output'.format(get_dir_path(pipeline.continuum,
                                                                                                                          pipeline), num, prefix, field, num, mfsprefix), conv_model],
                               "output": with_cc,
                               "sum": True,
                               "force": True,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Add clean components'.format(step))

                recipe.add('cab/tigger_restore', step,
                           {
                               "input-image": with_cc,
                               "input-skymodel": final,
                               "output-image": '{0:s}/image_{1:d}/{2:s}_{3:s}.fullrest.fits'.format(get_dir_path(pipeline.continuum,
                                                                                                                 pipeline), num, prefix, field),
                               "force": True,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Add extracted skymodel'.format(step))

        for i, msname in enumerate(mslist):
            if pipeline.enable_task(config, 'flagging_summary'):
                step = 'flagging_summary_image_selfcal_{0:d}'.format(i)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'summary',
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'transfer_model'):
#            caracal.log.info('Transfer the model {0:s}/{1:s}_{2:d}-sources.txt to all input \
#    .MS files with label {3:s}'.format(get_dir_path(image_path, pipeline),
#                                       prefix, self_cal_iter_counter, config['transfer_model'].get('transfer_to_label')))
            crystalball_model = config['transfer_model'].get('model')
            mslist_out = ms_dict_tmodel[target]
            if crystalball_model == 'auto':
                crystalball_model = '{0:s}/{1:s}_{2:s}_{3:d}-sources.txt'.format(get_dir_path(image_path,
                                                                            pipeline), prefix, field, self_cal_iter_counter)

            for i, msname in enumerate(mslist_out):
                step = 'transfer_model_field{0:d}_ms{1:d}'.format(target_iter,i)
                recipe.add('cab/crystalball', step,
                           {
                               "ms": msname,
                               "sky-model": crystalball_model+':output',
                               "spectra": config['transfer_model'].get('spectra'),
                               "row-chunks": config['transfer_model'].get('row_chunks'),
                               "model-chunks": config['transfer_model'].get('model_chunks'),
                               "exp-sign-convention": config['transfer_model'].get('exp_sign_convention'),
                               "within": sdm.dismissable(config['transfer_model'].get('within') or None),
                               "points-only": config['transfer_model'].get('points_only'),
                               "num-sources": sdm.dismissable(config['transfer_model'].get('num_sources')),
                               "num-workers": sdm.dismissable(config['transfer_model'].get('num_workers')),
                               "memory-fraction": config['transfer_model'].get('memory_fraction'),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Transfer model {2:s} to ms={1:s}'.format(step, msname, crystalball_model))

        target_iter+=1


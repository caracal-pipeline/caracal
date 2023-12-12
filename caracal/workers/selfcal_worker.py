# -*- coding: future_fstrings -*-
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
from caracal.utils.requires import extras
from stimela.pathformatter import pathformatter as spf
from typing import Any
from caracal.workers.utils import manage_flagsets as manflags
from casacore.tables import table
import psutil

NAME = 'Continuum Imaging and Self-calibration Loop'
LABEL = 'selfcal'


# self_cal_iter_counter is used as a global variable.

# To split out continuum/<dir> from output/continuum/dir


def get_dir_path(string, pipeline):
    return string.split(pipeline.output)[1][1:]


CUBICAL_OUT = {
    "CORRECTED_DATA": 'sc',
    "CORR_DATA": 'sc',
    "CORR_RES": 'sr',
    "PA_DATA": 'ac',
}

CUBICAL_MT = {
    "Gain2x2": 'complex-2x2',
    "GainDiag": 'complex-2x2',  # TODO:: Change this. Ask cubical to support this mode
    "GainDiagAmp": 'complex-2x2',
    "GainDiagPhase": 'phase-diag',
    "ComplexDiag": 'complex-diag',
    "Fslope": 'f-slope',
}

SOL_TERMS_INDEX = {
    "G": 0,
    "B": 1,
    "DD": 2,
}


def check_config(config, name):
    """
    Optional function to check consistency of config, invoked before the pipeline runs.
    its purpose is to log warnings, or raise exceptions on bad errors.
    """
    # First let' check that we are not using transfer gains with meqtrees or not starting at the start with meqtrees
    if config['calibrate_with'].lower() == 'meqtrees':
        if config['transfer_apply_gains']['enable']:
            raise caracal.ConfigurationError(
                'Gains cannot be interpolated with MeqTrees, please switch to CubiCal. Exiting.')
        if int(config['start_iter']) != 1:
            raise caracal.ConfigurationError(
                "We cannot reapply MeqTrees calibration at a given step. Hence you will need to do a full selfcal loop.")
        if int(config['cal_cubical']['chan_chunk']) != -1:
            caracal.log.info("The channel chunk has no effect on MeqTrees.")
        if 'Fslope' in config['calibrate']['gain_matrix_type']:
            caracal.log.info("Delay selfcal does not work with MeqTrees, please switch to Cubical. Exiting.")
    else:
        if int(config['start_iter']) != 1:
            raise caracal.ConfigurationError(
                "We cannot reapply Cubical calibration at a given step. Hence you will need to do a full selfcal loop.")
    # First check we are actually running a calibrate
    if config['calibrate']['enable']:
        # Running with a model shorter than the output type is dengerous with 'CORR_RES'
        if 'CORR_RES' in config['calibrate']['output_data']:
            if len(config['calibrate']['model']) < config['cal_niter']:
                raise caracal.ConfigurationError(
                    "You did not set a model to use for every iteration while using residuals. This is too dangerous for CARACal to execute.")

        # Make sure we are not using two_step with CubiCal
        if config['calibrate_with'].lower() == 'cubical' and config['cal_meqtrees']['two_step']:
            raise caracal.ConfigurationError(
                "Two_Step calibration is an experimental mode only available for meqtrees at the moment.")
        # Then let's check that the solutions are reasonable and fit in our time chunks
        #!!!!!! Remainder solutions are not checked to be a full solution block!!!!!!!!
        #  we check there are enough solution
        if len(config['calibrate']['gsols_timeslots']) < int(config['cal_niter']):
            amount_sols = len(config['calibrate']['gsols_timeslots'])
        else:
            amount_sols = int(config['cal_niter'])
        #  we collect all time solutions
        solutions = config['calibrate']['gsols_timeslots'][:amount_sols]
        # if we do Bjones we add those
        if config['cal_bjones']:
            if len(config['calibrate']['bsols_timeslots']) < int(config['cal_niter']):
                amount_sols = len(config['calibrate']['bsols_timeslots'])
            else:
                amount_sols = int(config['cal_niter'])
            solutions.append(config['calibrate']['bsols_timeslots'][:amount_sols])
        # Same for GA solutions
        if len(config['calibrate']['gain_matrix_type']) < int(config['cal_niter']):
            amount_matrix = len(config['calibrate']['gain_matrix_type'])
        else:
            amount_matrix = int(config['cal_niter'])
        if 'GainDiag' in config['calibrate']['gain_matrix_type'][:amount_matrix] or \
                'Gain2x2' in config['calibrate']['gain_matrix_type'][:amount_matrix]:
            if len(config['calibrate']['gasols_timeslots']) < int(config['cal_niter']):
                amount_sols = len(config['calibrate']['gasols_timeslots'])
            else:
                amount_sols = int(config['cal_niter'])
            for i, val in enumerate(config['calibrate']['gasols_timeslots'][:amount_sols]):
                if val >= 0:
                    solutions.append(val)
        # then we assign the timechunk
        if config['cal_timeslots_chunk'] == -1:
            if np.min(solutions) != 0.:
                time_chunk = np.max(solutions)
            else:
                time_chunk = 0
        else:
            time_chunk = config['cal_timeslots_chunk']
        # if time_chunk is not 0 all solutions should fit in there.
        # if it is 0 then it does not matter as we are not checking remainder intervals
        if time_chunk != 0:
            if 0. in solutions:
                caracal.log.error("You are using all timeslots in your solutions (i.e. 0) but have set cal_timeslots_chunk, please set it to 0 for using all timeslots.")
                caracal.log.error("Your timeslots chunk = {}".format(time_chunk))
                caracal.log.error("Your timeslots solutions to be applied are {}".format(', '.join([str(x) for x in solutions])))
                raise caracal.ConfigurationError("Inconsistent selfcal chunking")
            sol_int_array = float(time_chunk) / np.array(solutions, dtype=float)
            for val in sol_int_array:
                if val != int(val):
                    caracal.log.error("Not all applied time solutions fit in the timeslot_chunk.")
                    caracal.log.error("Your timeslot chunk = {}".format(time_chunk))
                    caracal.log.error("Your time solutions to be applied are {}".format(', '.join([str(x) for x in solutions])))
                    raise caracal.ConfigurationError("Inconsistent selfcal chunking")
        # Then we repeat for the channels, as these arrays do not have to be the same length as the timeslots this can not be combined
        # This is not an option for meqtrees
        if config['calibrate_with'].lower() == 'cubical':
            if len(config['calibrate']['gsols_chan']) < int(config['cal_niter']):
                amount_sols = len(config['calibrate']['gsols_chan'])
            else:
                amount_sols = int(config['cal_niter'])
            #  we collect all time solutions
            solutions = config['calibrate']['gsols_chan'][:amount_sols]
            # if we do bjones we add those
            if config['cal_bjones']:
                if len(config['calibrate']['bsols_chan']) < int(config['cal_niter']):
                    amount_sols = len(config['calibrate']['bsols_chan'])
                else:
                    amount_sols = int(config['cal_niter'])
                solutions.append(config['calibrate']['bsols_chan'][:amount_sols])
            # Same for GA solutions
            if 'GainDiag' in config['calibrate']['gain_matrix_type'][:amount_matrix] or \
                    'Gain2x2' in config['calibrate']['gain_matrix_type'][:amount_matrix]:
                if len(config['calibrate']['gasols_chan']) < int(config['cal_niter']):
                    amount_sols = len(config['calibrate']['gasols_chan'])
                else:
                    amount_sols = int(config['cal_niter'])
                for i, val in enumerate(config['calibrate']['gasols_chan'][:amount_sols]):
                    if val >= 0:
                        solutions.append(val)
            # then we assign the timechunk
            if config['cal_cubical']['chan_chunk'] == -1:
                if np.min(solutions) != 0.:
                    chan_chunk = max(solutions)
                else:
                    chan_chunk = 0
            else:
                chan_chunk = config['cal_cubical']['chan_chunk']
            # if chan_chunk is not 0 all solutions should fit in there.
            # if it is 0 then it does not matter as we are not checking remainder intervals
            if chan_chunk != 0:
                if 0. in solutions:
                    caracal.log.error("You are using all channels in your solutions (i.e. 0) but have set chan_chunk, please set it to 0 for using all channels.")
                    caracal.log.error("Your channel chunk = {} \n".format(chan_chunk))
                    caracal.log.error("Your channel solutions to be applied are {}".format(', '.join([str(x) for x in solutions])))
                    raise caracal.ConfigurationError("Inconsistent selfcal chunking")
                sol_int_array = float(chan_chunk) / np.array(solutions, dtype=float)
                for val in sol_int_array:
                    if val != int(val):
                        caracal.log.error("Not all applied channel solutions fit in the chan_chunk.")
                        caracal.log.error("Your channel chunk = {} \n".format(chan_chunk))
                        caracal.log.error("Your channel solutions to be applied are {}".format(', '.join([str(x) for x in solutions])))
                        raise caracal.ConfigurationError("Inconsistent selfcal chunking")
    # Check some imaging stuff
    if config['image']['enable']:
        if config['img_maxuv_l'] > 0. and config['img_taper'] > 0.:
            caracal.UserInputError(
                "You are trying to image with a Gaussian taper as well as a Tukey taper. Please remove one. ")

def worker(pipeline, recipe, config):
    wname = pipeline.CURRENT_WORKER
    flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
    flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
    flag_main_ms = pipeline.enable_task(config, 'calibrate') and config['cal_niter'] >= config['start_iter']
    rewind_main_ms = config['rewind_flags']["enable"] and (config['rewind_flags']['mode'] == 'reset_worker' or config['rewind_flags']["version"] != 'null')
    rewind_transf_ms = config['rewind_flags']["enable"] and (config['rewind_flags']['mode'] == 'reset_worker' or config['rewind_flags']["transfer_apply_gains_version"] != 'null')
    spwid = str(config['spwid'])
    niter = config['img_niter']
    imgweight = config['img_weight']
    robust = config['img_robust']
    taper = config['img_taper']
    maxuvl = config['img_maxuv_l']
    transuvl = maxuvl * config['img_transuv_l'] / 100.
    multiscale = config['img_multiscale']
    multiscale_scales = config['img_multiscale_scales']
    if taper == '':
        taper = None

    label = config['label_in']
    cal_niter = config['cal_niter']
    time_chunk = config['cal_timeslots_chunk']
    # If user sets value that is not -1 use  that
    if len(config['calibrate']['gain_matrix_type']) < int(cal_niter):
        amount_matrix = len(config['calibrate']['gain_matrix_type'])
    else:
        amount_matrix = int(cal_niter)
    if int(time_chunk) < 0 and pipeline.enable_task(config, 'calibrate'):
        # We're always doing gains
        if len(config['calibrate']['gsols_timeslots']) < cal_niter:
            g_amount_sols = len(config['calibrate']['gsols_timeslots'])
        else:
            g_amount_sols = cal_niter
        all_time_solution = config['calibrate']['gsols_timeslots'][:g_amount_sols]
        # add the various sections
        if config['cal_bjones']:
            if len(config['calibrate']['bsols_timeslots']) < cal_niter:
                b_amount_sols = len(config['calibrate']['bsols_timeslots'])
            else:
                b_amount_sols = cal_niter
            all_time_solution.append(config['calibrate']['bsols_timeslots'][:b_amount_sols])
        if 'GainDiag' in config['calibrate']['gain_matrix_type'][:amount_matrix] or \
                'Gain2x2' in config['calibrate']['gain_matrix_type'][:amount_matrix]:
            if len(config['calibrate']['gasols_timeslots']) < cal_niter:
                amount_sols = len(config['calibrate']['gasols_timeslots'])
            else:
                amount_sols = int(cal_niter)
            for val in config['calibrate']['gasols_timeslots'][:amount_sols]:
                if int(val) >= 0:
                    all_time_solution.append(val)
        if min(all_time_solution) == 0:
            time_chunk = 0
        else:
            time_chunk = max(all_time_solution)
    # And for the frequencies
    freq_chunk = config['cal_cubical']['chan_chunk']
    # If user sets value that is not -1 then use that
    if int(freq_chunk) < 0 and pipeline.enable_task(config, 'calibrate'):
        # We're always doing gains
        if len(config['calibrate']['gsols_chan']) < cal_niter:
            g_amount_sols = len(config['calibrate']['gsols_chan'])
        else:
            g_amount_sols = cal_niter
        all_freq_solution = config['calibrate']['gsols_chan'][:g_amount_sols]
        # add the various sections
        if config['cal_bjones']:
            if len(config['calibrate']['bsols_chan']) < cal_niter:
                b_amount_sols = len(config['calibrate']['bsols_chan'])
            else:
                b_amount_sols = cal_niter
            all_freq_solution.append(config['calibrate']['bsols_chan'][:b_amount_sols])
        if 'GainDiag' in config['calibrate']['gain_matrix_type'][:amount_matrix] or \
                'Gain2x2' in config['calibrate']['gain_matrix_type'][:amount_matrix]:
            if len(config['calibrate']['gasols_chan']) < cal_niter:
                amount_sols = len(config['calibrate']['gasols_chan'])
            else:
                amount_sols = int(cal_niter)
            for val in config['calibrate']['gasols_chan'][:amount_sols]:
                if int(val) >= 0:
                    all_freq_solution.append(val)
        if min(all_freq_solution) == 0:
            freq_chunk = 0
        else:
            freq_chunk = int(max(all_freq_solution))

    min_uvw = config['minuvw_m']

    ncpu = config['ncpu']
    if ncpu == 0:
        ncpu = psutil.cpu_count()
    else:
        ncpu = min(ncpu, psutil.cpu_count())
    nwlayers_factor = config['img_nwlayers_factor']
    nrdeconvsubimg = ncpu if config['img_nrdeconvsubimg'] == 0 else config['img_nrdeconvsubimg']
    if nrdeconvsubimg == 1:
        wscl_parallel_deconv = None
    else:
        wscl_parallel_deconv = int(np.ceil(config['img_npix'] / np.sqrt(nrdeconvsubimg)))

    mfsprefix = ["", '-MFS'][int(config['img_nchans'] > 1)]

    # label of MS where we transform selfcal gaintables
    label_tgain = config['transfer_apply_gains']['transfer_to_label']
    # label of MS where we interpolate and transform model column
    label_tmodel = config['transfer_model']['transfer_to_label']

    all_targets, all_msfile, ms_dict = pipeline.get_target_mss(label)

    i = 0
    for i, m in enumerate(all_msfile):
        # check whether all ms files to be used exist
        if not os.path.exists(os.path.join(pipeline.msdir, m)):
            raise IOError(
                "MS file {0:s} does not exist. Please check that it is where it should be.".format(m))

        # Write/rewind flag versions only if flagging tasks are being
        # executed on these .MS files, or if the user asks to rewind flags
        if flag_main_ms or rewind_main_ms:
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
                    if flags_before_worker in available_flagversions and available_flagversions.index(flags_before_worker) < available_flagversions.index(version) and not config['overwrite_flagvers']:
                        manflags.conflict('rewind_too_little', pipeline, wname, m, config, flags_before_worker, flags_after_worker)
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
                    manflags.conflict('rewind_to_non_existing', pipeline, wname, m, config, flags_before_worker, flags_after_worker)
                elif flag_main_ms:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                        m, cab_name=substep, overwrite=config['overwrite_flagvers'])
            else:
                if flags_before_worker in available_flagversions and not config['overwrite_flagvers']:
                    manflags.conflict('would_overwrite_bw', pipeline, wname, m, config, flags_before_worker, flags_after_worker)
                else:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                        m, cab_name=substep, overwrite=config['overwrite_flagvers'])

    i += 1
    if pipeline.enable_task(config, 'transfer_apply_gains'):
        t, all_msfile_tgain, ms_dict_tgain = pipeline.get_target_mss(label_tgain)

        for j, m in enumerate(all_msfile_tgain):
            # check whether all ms files to be used exist
            if not os.path.exists(os.path.join(pipeline.msdir, m)):
                raise IOError(
                    "MS file {0:s}, to transfer gains to, does not exist. Please check that it is where it should be.".format(m))
            # Check if a model subtraction has already been done
            with table('{0:s}/{1:s}'.format(pipeline.msdir, m), readonly=False) as ms_table:
                try:
                    caracal.log.info(f"Transferring the gains to {m}")
                    nModelSub = 0
                    ms_table.putcolkeyword('CORRECTED_DATA', 'modelSub', nModelSub)
                    caracal.log.info(f"Reseting the counter to {nModelSub}")
                except RuntimeError:
                    caracal.log.info(f"No previous model subtraction found in {m}")

            # Write/rewind flag versions
            available_flagversions = manflags.get_flags(pipeline, m)
            if rewind_transf_ms:
                if config['rewind_flags']['mode'] == 'reset_worker':
                    version = flags_before_worker
                    stop_if_missing = False
                elif config['rewind_flags']['mode'] == 'rewind_to_version':
                    version = config['rewind_flags']['transfer_apply_gains_version']
                    if version == 'auto':
                        version = flags_before_worker
                    stop_if_missing = True
                if version in available_flagversions:
                    if flags_before_worker in available_flagversions and available_flagversions.index(flags_before_worker) < available_flagversions.index(version) and not config['overwrite_flagvers']:
                        manflags.conflict('rewind_too_little', pipeline, wname, m, config, flags_before_worker, flags_after_worker, read_version='transfer_apply_gains_version')
                    substep = 'version_{0:s}_ms{1:d}'.format(version, i)
                    manflags.restore_cflags(pipeline, recipe, version, m, cab_name=substep)
                    if version != available_flagversions[-1]:
                        substep = 'delete-flag_versions-after-{0:s}-ms{1:d}'.format(version, i)
                        manflags.delete_cflags(pipeline, recipe,
                                               available_flagversions[available_flagversions.index(version) + 1],
                                               m, cab_name=substep)
                    if version != flags_before_worker:
                        substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i + j)
                        manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                            m, cab_name=substep, overwrite=config['overwrite_flagvers'])
                elif stop_if_missing:
                    manflags.conflict('rewind_to_non_existing', pipeline, wname, m, config, flags_before_worker, flags_after_worker, read_version='transfer_apply_gains_version')
                else:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i + j)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                        m, cab_name=substep, overwrite=config['overwrite_flagvers'])
            else:
                if flags_before_worker in available_flagversions and not config['overwrite_flagvers']:
                    manflags.conflict('would_overwrite_bw', pipeline, wname, m, config, flags_before_worker, flags_after_worker, read_version='transfer_apply_gains_version')
                else:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i + j)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                        m, cab_name=substep, overwrite=config['overwrite_flagvers'])

    if pipeline.enable_task(config, 'transfer_model'):
        t, all_msfile_tmodel, ms_dict_tmodel = pipeline.get_target_mss(label_tmodel)
        for m in all_msfile_tmodel:  # check whether all ms files to be used exist
            if not os.path.exists(os.path.join(pipeline.msdir, m)):
                raise IOError(
                    "MS file {0:s}, to transfer model to, does not exist. Please check that it is where it should be.".format(m))

    prefix = pipeline.prefix

    def cleanup_files(mask_name):
        # This function is never used
        if os.path.exists(pipeline.output + '/' + mask_name):
            shutil.move(pipeline.output + '/' + mask_name,
                        pipeline.output + '/masking/' + mask_name)

        casafiles = glob.glob(pipeline.output + '/*.image')
        for i in range(0, len(casafiles)):
            shutil.rmtree(casafiles[i])

    @extras("astropy")
    def change_header_and_type(filename, headfile, copy_head):
        import astropy.io.fits as fits

        pblist = fits.open(filename)
        dat = pblist[0].data
        pblist.close()
        if copy_head:
            head = fits.getheader(headfile, 0)
        else:
            head = fits.getheader(filename, 0)
            # delete ORIGIN, CUNIT1, CUNIT2
            if 'ORIGIN' in head:
                del head['ORIGIN']
            if 'CUNIT1' in head:
                del head['CUNIT1']
            if 'CUNIT2' in head:
                del head['CUNIT2']
            # copy CRVAL3 from headfile to filename
            template_head = fits.getheader(headfile, 0)
            if 'crval3' in template_head:
                head['crval3'] = template_head['crval3']
        fits.writeto(filename, dat.astype('int32'), head, overwrite=True)

    def fake_image(trg, num, img_dir, mslist, field):
        key = 'image'
        key_mt = 'calibrate'
        ncpu_img = config[key]['ncpu_img'] if config[key]['ncpu_img'] else ncpu
        absmem = config[key]['absmem']
        step = 'image-field{0:d}-iter{1:d}'.format(trg, num)
        fake_image_opts = {
            "msname": mslist,
            "column": config[key]['col'][0],
            "weight": imgweight if not imgweight == 'briggs' else 'briggs {}'.format(robust),
            "nmiter": sdm.dismissable(config['img_nmiter']),
            "npix": config['img_npix'],
            "padding": config['img_padding'],
            "scale": config['img_cell'],
            "prefix": '{0:s}/{1:s}_{2:s}_{3:d}'.format(img_dir, prefix, field, num),
            "niter": config['img_niter'],
            "gain": config["img_gain"],
            "mgain": config['img_mgain'],
            "pol": config['img_stokes'],
            "channelsout": config['img_nchans'],
            "joinchannels": config['img_joinchans'],
            "local-rms": False,
            "auto-mask": 6,
            "auto-threshold": config[key]['clean_cutoff'][0],
            "fitbeam": False,
            "parallel-deconvolution": sdm.dismissable(wscl_parallel_deconv),
            "nwlayers-factor": nwlayers_factor,
            "threads": ncpu_img,
            "absmem": absmem,
            "parallel-gridding": config[key]['nr_parallel_grid'],
            "use-wgridder": config[key]['use_wgridder']
        }
        if config['img_specfit_nrcoeff'] > 0:
            fake_image_opts["fit-spectral-pol"] = config['img_specfit_nrcoeff']
        if not config['img_mfs_weighting']:
            fake_image_opts["nomfsweighting"] = True
        if maxuvl > 0.:
            fake_image_opts.update({
                "maxuv-l": maxuvl,
                "taper-tukey": transuvl,
            })
        if float(taper) > 0.:
            fake_image_opts.update({
                "taper-gaussian": taper,
            })
        if min_uvw > 0:
            fake_image_opts.update({"minuvw-m": min_uvw})
        if multiscale:
            fake_image_opts.update({"multiscale": multiscale})
            if multiscale_scales:
                fake_image_opts.update({"multiscale-scales": list(map(int, multiscale_scales.split(',')))})
        if len(config['img_channelrange']) == 2:
            fake_image_opts.update({"channelrange": config['img_channelrange']})

        recipe.add('cab/wsclean', step,
                   fake_image_opts,
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{:s}:: Make image after first round of calibration'.format(step))

    def image(trg, num, img_dir, mslist, field):
        key = 'image'
        key_mt = 'calibrate'

        ncpu_img = config[key]['ncpu_img'] if config[key]['ncpu_img'] else ncpu
        absmem = config[key]['absmem']
        caracal.log.info("Number of threads used by WSClean for gridding:")
        caracal.log.info(ncpu_img)
        if num > 1:
            matrix_type = config[key_mt]['gain_matrix_type'][
                num - 2 if len(config[key_mt]['gain_matrix_type']) >= num else -1]
        else:
            matrix_type = 'null'
        # If we have a two_step selfcal and Gaindiag we want to use  CORRECTED_DATA
        if config['calibrate_with'].lower() == 'meqtrees' and config['cal_meqtrees']['two_step'] and num > 1:
            if trace_matrix[-1] == 'GainDiag':
                imcolumn = "CORRECTED_DATA"
            # If we do not have gaindiag but do have two step selfcal check against stupidity and that we are actually ending with ampphase cal and written to a special phase column
            elif trace_matrix[-1] == 'GainDiagPhase':
                imcolumn = 'CORRECTED_DATA_PHASE'
            # If none of these apply then do our normal sefcal
            else:
                raise RuntimeError("Something has gone wrong in the two step processing")
        else:
            imcolumn = config[key][
                'col'][num - 1 if len(config[key]['col']) >= num else -1]

        step = 'image-field{0:d}-iter{1:d}'.format(trg, num)
        image_opts = {
            "msname": mslist,
            "column": imcolumn,
            "weight": imgweight if not imgweight == 'briggs' else 'briggs {}'.format(robust),
            "nmiter": sdm.dismissable(config['img_nmiter']),
            "npix": config['img_npix'],
            "padding": config['img_padding'],
            "scale": config['img_cell'],
            "prefix": '{0:s}/{1:s}_{2:s}_{3:d}'.format(img_dir, prefix, field, num),
            "niter": config['img_niter'],
            "gain": config["img_gain"],
            "mgain": config['img_mgain'],
            "pol": config['img_stokes'],
            "channelsout": config['img_nchans'],
            "joinchannels": config['img_joinchans'],
            "auto-threshold": config[key]['clean_cutoff'][num - 1 if len(config[key]['clean_cutoff']) >= num else -1],
            "parallel-deconvolution": sdm.dismissable(wscl_parallel_deconv),
            "nwlayers-factor": nwlayers_factor,
            "threads": ncpu_img,
            "absmem": absmem,
            "parallel-gridding": config[key]['nr_parallel_grid'],
            "use-wgridder": config[key]['use_wgridder']
        }
        if config['img_specfit_nrcoeff'] > 0:
            image_opts["fit-spectral-pol"] = config['img_specfit_nrcoeff']
            if config['img_niter'] > 0:
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
        if min_uvw > 0:
            image_opts.update({"minuvw-m": min_uvw})
        if multiscale:
            image_opts.update({"multiscale": multiscale})
            if multiscale_scales:
                image_opts.update({"multiscale-scales": list(map(int, multiscale_scales.split(',')))})
        if len(config['img_channelrange']) == 2:
            image_opts.update({"channelrange": config['img_channelrange']})

        mask_key = config[key]['cleanmask_method'][num - 1 if len(config[key]['cleanmask_method']) >= num else -1]
        if mask_key == 'wsclean':
            image_opts.update({
                "auto-mask": config[key]['cleanmask_thr'][num - 1 if len(config[key]['cleanmask_thr']) >= num else -1],
                "local-rms": config[key]['cleanmask_localrms'][num - 1 if len(config[key]['cleanmask_localrms']) >= num else -1],
            })
            if config[key]['cleanmask_localrms'][num - 1 if len(config[key]['cleanmask_localrms']) >= num else -1]:
                image_opts.update({
                    "local-rms-window": config[key]['cleanmask_localrms_window'][num - 1 if len(config[key]['cleanmask_localrms_window']) >= num else -1],
                })
        elif mask_key == 'sofia':
            fits_mask = 'masking/{0:s}_{1:s}_{2:d}_clean_mask.fits'.format(
                prefix, field, num)
            if not os.path.isfile('{0:s}/{1:s}'.format(pipeline.output, fits_mask)):
                raise caracal.ConfigurationError("SoFiA clean mask {0:s}/{1:s} not found. Something must have gone wrong with the SoFiA run"
                                                 " (maybe the detection threshold was too high?). Please check the logs.".format(pipeline.output, fits_mask))
            image_opts.update({
                "fitsmask": '{0:s}:output'.format(fits_mask),
                "local-rms": False,
            })
        elif mask_key == 'breizorro':
            fits_mask = 'masking/{0:s}_{1:s}_{2:d}_clean_mask.fits'.format(
                prefix, field, num)
            if not os.path.isfile('{0:s}/{1:s}'.format(pipeline.output, fits_mask)):
                raise caracal.ConfigurationError("Breizorro clean mask {0:s}/{1:s} not found. Something must have gone wrong with the Breizorro run"
                                                 " (maybe the detection threshold was too high?). Please check the logs.".format(pipeline.output, fits_mask))
            image_opts.update({
                "fitsmask": '{0:s}:output'.format(fits_mask),
                "local-rms": False,
            })
        else:
            fits_mask = 'masking/{0:s}_{1:s}.fits'.format(
                mask_key, field)
            if not os.path.isfile('{0:s}/{1:s}'.format(pipeline.output, fits_mask)):
                raise caracal.ConfigurationError("Clean mask {0:s}/{1:s} not found. Please make sure that you have given the correct mask label"
                                                 " in cleanmask_method, and that the mask exists.".format(pipeline.output, fits_mask))
            image_opts.update({
                "fitsmask": '{0:s}:output'.format(fits_mask),
                "local-rms": False,
            })

        recipe.add('cab/wsclean', step,
                   image_opts,
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{:s}:: Make wsclean image (selfcal iter {})'.format(step, num))
        recipe.run()
        # Empty job que after execution
        recipe.jobs = []

    def sofia_mask(trg, num, img_dir, field):
        step = 'make-sofia_mask-field{0:d}-iter{1:d}'.format(trg, num)
        key = 'img_sofia_settings'

        if config['img_joinchans']:
            imagename = '{0:s}/{1:s}_{2:s}_{3:d}-MFS-image.fits'.format(
                img_dir, prefix, field, num)
        else:
            imagename = '{0:s}/{1:s}_{2:s}_{3:d}-image.fits'.format(
                img_dir, prefix, field, num)

        if config[key]['fornax_special'] and config[key]['fornax_sofia']:
            forn_kernels = [[80, 80, 0, 'b']]
            forn_thresh = config[key]['fornax_thr'][
                num if len(config[key]['fornax_thr']) >= num + 1 else -1]

            sofia_opts_forn = {
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

        outmask = pipeline.prefix + '_' + field + '_' + str(num + 1) + '_clean'
        outmaskName = outmask + '_mask.fits'

        sofia_opts = {
            "import.inFile": imagename,
            "steps.doFlag": True,
            "steps.doScaleNoise": config['image']['cleanmask_localrms'][num if len(config['image']['cleanmask_localrms']) >= num + 1 else -1],
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
            "SCfind.kernels": [[kk, kk, 0, 'b'] for kk in config[key]['kernels']],
            "SCfind.threshold": config['image']['cleanmask_thr'][num if len(config['image']['cleanmask_thr']) >= num + 1 else -1],
            "SCfind.rmsMode": 'mad',
            "SCfind.edgeMode": 'constant',
            "SCfind.fluxRange": 'all',
            "scaleNoise.statistic": 'mad',
            "scaleNoise.method": 'local',
            "scaleNoise.interpolation": 'linear',
            "scaleNoise.windowSpatial": config['image']['cleanmask_localrms_window'][num if len(config['image']['cleanmask_localrms_window']) >= num + 1 else -1],
            "scaleNoise.windowSpectral": 1,
            "scaleNoise.scaleX": True,
            "scaleNoise.scaleY": True,
            "scaleNoise.scaleZ": False,
            "scaleNoise.perSCkernel": config['image']['cleanmask_localrms'][num if len(config['image']['cleanmask_localrms']) >= num + 1 else -1],  # work-around for https://github.com/SoFiA-Admin/SoFiA/issues/172, to be replaced by "True" once the next SoFiA version is in Stimela
            "merge.radiusX": 3,
            "merge.radiusY": 3,
            "merge.radiusZ": 1,
            "merge.minSizeX": 3,
            "merge.minSizeY": 3,
            "merge.minSizeZ": 1,
            "merge.positivity": config[key]['pospix'],
        }
        if config[key]['flag']:
            flags_sof = config[key]['flagregion']
            sofia_opts.update({"flag.regions": flags_sof})

        if config[key]['inputmask']:
            mask_fits = 'masking/' + config[key]['inputmask']
            mask_casa = mask_fits.replace('.fits', '.image')
            mask_regrid_casa = mask_fits.replace('.fits', '_regrid.image')
            mask_regrid_fits = mask_fits.replace('.fits', '_regrid.fits')
            imagename_casa = imagename.split('/')[-1].replace('.fits', '.image')

            recipe.add('cab/casa_importfits', step + "-import-image",
                       {
                           "fitsimage": imagename,
                           "imagename": imagename_casa,
                           "overwrite": True,
                       },
                       input=pipeline.output,
                       output=pipeline.output,
                       label='Import image in casa format')

            recipe.add('cab/casa_importfits', step + "-import-mask",
                       {
                           "fitsimage": mask_fits + ':output',
                           "imagename": mask_casa,
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Import mask in casa format')

            recipe.add('cab/casa_imregrid', step + "-regrid-mask",
                       {
                           "template": imagename_casa + ':output',
                           "imagename": mask_casa + ':output',
                           "output": mask_regrid_casa,
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Regrid mask to image')

            recipe.add('cab/casa_exportfits', step + "-export-mask",
                       {
                           "fitsimage": mask_regrid_fits + ':output',
                           "imagename": mask_regrid_casa + ':output',
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Export regridded mask to fits')

            recipe.add(change_header_and_type, step + "-copy-header",
                       {
                           "filename": pipeline.output + '/' + mask_regrid_fits,
                           "headfile": pipeline.output + '/' + imagename,
                           "copy_head": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Copy image header to mask')

            sofia_opts.update({"import.maskFile": mask_regrid_fits})
            sofia_opts.update({"import.inFile": imagename})

        if config[key]['fornax_special'] and config[key]['fornax_sofia']:

            recipe.add('cab/sofia', step + "-fornax_special",
                       sofia_opts_forn,
                       input=pipeline.output,
                       output=pipeline.output + '/masking/',
                       label='{0:s}:: Make SoFiA mask'.format(step))

            fornax_namemask = 'masking/FornaxA_sofia_mask.fits'
            sofia_opts.update({"import.maskFile": fornax_namemask})

        elif config[key]['fornax_special'] and config[key]['fornax_sofia'] == False:

            # this mask should be regridded to correct f.o.v.

            fornax_namemask = 'masking/Fornaxa_vla_mask_doped.fits'
            fornax_namemask_regr = 'masking/Fornaxa_vla_mask_doped_regr.fits'

            mask_casa = fornax_namemask.split('.fits')[0]
            mask_casa = fornax_namemask + '.image'

            mask_regrid_casa = fornax_namemask + '_regrid.image'

            imagename_casa = '{0:s}_{1:d}{2:s}-image.image'.format(
                prefix, num, mfsprefix)

            recipe.add('cab/casa_importfits', step + "-fornax_special-import-image",
                       {
                           "fitsimage": imagename,
                           "imagename": imagename_casa,
                           "overwrite": True,
                       },
                       input=pipeline.output,
                       output=pipeline.output,
                       label='Image in casa format')

            recipe.add('cab/casa_importfits', step + "-fornax_special-import-image",
                       {
                           "fitsimage": fornax_namemask + ':output',
                           "imagename": mask_casa,
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Mask in casa format')

            recipe.add('cab/casa_imregrid', step + "-fornax_special-regrid",
                       {
                           "template": imagename_casa + ':output',
                           "imagename": mask_casa + ':output',
                           "output": mask_regrid_casa,
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Regridding mosaic to size and projection of dirty image')

            recipe.add('cab/casa_exportfits', step + "-fornax_special-export-mosaic",
                       {
                           "fitsimage": fornax_namemask_regr + ':output',
                           "imagename": mask_regrid_casa + ':output',
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Extracted regridded mosaic')

            recipe.add(change_header_and_type, step + "-fornax_special-change_header",
                       {
                           "filename": pipeline.output + '/' + fornax_namemask_regr,
                           "headfile": pipeline.output + '/' + imagename,
                           "copy_head": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Extracted regridded mosaic')

            sofia_opts.update({"import.maskFile": fornax_namemask_regr})

        recipe.add('cab/sofia', step,
                   sofia_opts,
                   input=pipeline.output,
                   output=pipeline.output + '/masking/',
                   label='{0:s}:: Make SoFiA mask'.format(step))

    def breizorro_mask(trg, num, img_dir, field):
        step = 'make-breizorro_mask-field{0:d}-iter{1:d}'.format(trg, num)
        key = 'img_breizorro_settings'

        if config['img_joinchans']:
            imagename = '{0:s}/{1:s}_{2:s}_{3:d}-MFS-image.fits'.format(
                img_dir, prefix, field, num)
        else:
            imagename = '{0:s}/{1:s}_{2:s}_{3:d}-image.fits'.format(
                img_dir, prefix, field, num)

        outmask = pipeline.prefix + '_' + field + '_' + str(num + 1) + '_clean'
        outmaskName = outmask + '_mask.fits'

        breizorro_opts = {
            "restored-image": imagename,
            "outfile": outmaskName,
            "threshold": config['image']['cleanmask_thr'][num if len(config['image']['cleanmask_thr']) >= num + 1 else -1],
            "boxsize": config[key]['boxsize'],
            "dilate": config[key]['dilate'],
            "fill-holes": config[key]['fill_holes']
        }

        recipe.add('cab/breizorro', step,
                   breizorro_opts,
                   input=pipeline.output,
                   output=pipeline.output + '/masking/',
                   label='{0:s}:: Make Breizorro'.format(step))

    def make_cube(num, img_dir, field, imtype='model'):
        im = '{0:s}/{1:s}_{2:s}_{3}-cube.fits:output'.format(
            img_dir, prefix, field, num)
        step = 'makecube-{}'.format(num)
        images = ['{0:s}/{1:s}_{2:s}_{3}-{4:04d}-{5:s}.fits:output'.format(
            img_dir, prefix, field, num, i, imtype) for i in range(config['img_nchans'])]
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
        if config[key]['detection_image']:
            step = 'detection_image-field{0:d}-iter{1:d}'.format(trg, num)
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

        sourcefinder = config[key]['sourcefinder']
        if (sourcefinder == 'pybdsm' or sourcefinder == 'pybdsf'):
            spi_do = config[key]['spi']
            if spi_do:
                im = make_cube(num, get_dir_path(
                    pipeline.continuum, pipeline) + '/' + img_dir.split("/")[-1], field, 'image')
                im = im.split("/")[-1]
            else:
                im = '{0:s}_{1:s}_{2:d}{3:s}-image.fits:output'.format(
                    prefix, field, num, mfsprefix)

            if config[key]['breizorro_image']['enable']:
                step = "Breizorro_masked_image"
                outmask_image = im.replace('image.fits:output', 'breiz-image.fits')
                recipe.add('cab/breizorro', step,
                           {
                               "restored-image": im,
                               "outfile": f'{outmask_image}:output',
                               "threshold": config[key]['thr_pix'][num - 1 if len(config[key]['thr_pix']) >= num else -1],
                               "sum-peak": config[key]['breizorro_image']['sum_to_peak'],
                               "fill-holes": True
                           },
                           input=pipeline.input,
                           output=pipeline.output + '/' + img_dir,
                           label='{0:s}:: Make Breizorro'.format(step))
                im = '{}:{}'.format(outmask_image, 'output')
                # In order to make sure that we actually find stuff in the images we execute the rec ipe here
                recipe.run()
                # Empty job que after execution
                recipe.jobs = []
                caracal.log.info(im)


            step = 'extract-field{0:d}-iter{1:d}'.format(trg, num)
            calmodel = '{0:s}_{1:s}_{2:d}-pybdsm'.format(prefix, field, num)

            if detection_image:
                blank_limit = 1e-9
            else:
                blank_limit = None
            try:
                os.remove(
                    '{0:s}/{1:s}/{2:s}.fits'.format(pipeline.output, img_dir, calmodel))
            except BaseException:
                caracal.log.info('No Previous fits log found.')
            try:
                os.remove(
                    '{0:s}/{1:s}/{2:s}.lsm.html'.format(pipeline.output, img_dir, calmodel))
            except BaseException:
                caracal.log.info('No Previous lsm.html found.')
            recipe.add('cab/pybdsm', step,
                       {
                           "image": sdm.dismissable(im),
                           "thresh_pix": config[key]['thr_pix'][num - 1 if len(config[key]['thr_pix']) >= num else -1],
                           "thresh_isl": config[key]['thr_isl'][num - 1 if len(config[key]['thr_isl']) >= num else -1],
                           "outfile": '{:s}.gaul:output'.format(calmodel),
                           "blank_limit": sdm.dismissable(blank_limit),
                           "adaptive_rms_box": config[key]['local_rms'],
                           "port2tigger": False,
                           "format": 'ascii',
                           "multi_chan_beam": spi_do,
                           "spectralindex_do": spi_do,
                           "detection_image": sdm.dismissable(detection_image),
                           "ncores": ncpu,
                       },
                       input=pipeline.input,
                       # Unfortuntaly need to do it this way for pybdsm
                       output=pipeline.output + '/' + img_dir,
                       label='{0:s}:: Extract sources'.format(step))
            # In order to make sure that we actually find stuff in the images we execute the rec ipe here
            recipe.run()
            # Empty job que after execution
            recipe.jobs = []
            # and then check the proper file is produced
            if not os.path.isfile('{0:s}/{1:s}/{2:s}.gaul'.format(pipeline.output, img_dir, calmodel)):
                caracal.log.error(
                    "No model file is found after the PYBDSM run. This probably means no sources were found either due to a bad calibration or to stringent values. ")
                raise caracal.BadDataError("No model file found after the PyBDSM run")

            step = 'convert-field{0:d}-iter{1:d}'.format(trg, num)
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
            step = 'combine_models-' + '_'.join(map(str, mm))
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

        step = 'predict_from_fits-{}'.format(num)
        recipe.add('cab/lwimager', 'predict', {
            "msname": mslist[index],
            "simulate_fits": calmodel,
            "column": 'MODEL_DATA',
            "img_nchan": config['img_nchans'],
            "img_chanstep": 1,
            # TODO: This should consider SPW IDs
            "nchan": pipeline.nchans[index],
            "cellsize": config['img_cell'],
            "chanstep": 1,
        },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Predict from FITS ms={1:s}'.format(step, mslist[index]))

    def combine_models(models, num, img_dir, field, enable=True):
        model_names = ['{0:s}/{1:s}_{2:s}_{3:s}-pybdsm.lsm.html:output'.format(get_dir_path("{0:s}/image_{1:d}".format(pipeline.continuum, int(m)), pipeline), prefix, field, m) for m in models]

        model_names_fits = ['{0:s}/{1:s}_{2:s}_{3:s}-pybdsm.fits'.format(get_dir_path("{0:s}/image_{1:d}".format(pipeline.continuum, int(m)), pipeline), prefix, field, m) for m in models]
        calmodel = '{0:s}/{1:s}_{2:d}-pybdsm-combined.lsm.html:output'.format(
            img_dir, prefix, num)

        if enable:
            step = 'combine_models-' + '_'.join(map(str, models))
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

        # force to calibrate with model data column if specified by user

        # If the mode is pybdsm_vis then we want to add the clean component model only at the last step,
        # which is anyway achieved by the **above** statement; no need to further specify vismodel.

        if config['cal_model_mode'] == 'pybdsm_vis':
            if num == cal_niter:
                vismodel = True
            else:
                vismodel = False

            if len(config[key]['model']) >= num:
                model = config[key]['model'][num - 1]
            else:
                model = str(num)

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
        # vismodel =False
        elif config['cal_model_mode'] == 'pybdsm_only':
            vismodel = False
            if len(config[key]['model']) >= num:
                model = config[key]['model'][num - 1]
            else:
                model = str(num)

            if isinstance(model, str) and len(model.split('+')) > 1:
                mm = model.split('+')
                calmodel, fits_model = combine_models(mm, num, img_dir, field,
                                                      enable=False if pipeline.enable_task(
                                                          config, 'aimfast') else True)
            else:
                model = int(model)
                calmodel = '{0:s}/{1:s}_{2:s}_{3:d}-pybdsm.lsm.html:output'.format(
                    img_dir, prefix, field, model)
                fits_model = '{0:s}/{1:s}_{2:s}_{2:s}-pybdsm.fits'.format(
                    img_dir, prefix, field, model)

            modelcolumn = ''
        # If the mode is vis_only, then there is need for an empty sky model (since meqtrees needs one).
        # In this case, vis_model is always true, the model_column is always MODEL_DATA.
        elif config['cal_model_mode'] == 'vis_only':
            vismodel = True
            modelcolumn = 'MODEL_DATA'
            calmodel = '{0:s}_{1:d}-nullmodel.txt'.format(prefix, num)
            with open(os.path.join(pipeline.output, img_dir, calmodel), 'w') as stdw:
                stdw.write('#format: ra_d dec_d i\n')
                stdw.write('0.0 -30.0 1e-99')
        # Let's see the matrix type we are dealing with
        if not config['cal_meqtrees']['two_step']:
            matrix_type = config[key]['gain_matrix_type'][
                num - 1 if len(config[key]['gain_matrix_type']) >= num else -1]
        # If we have a two_step selfcal and Gaindiag we want to use CORRECTED_DATA_PHASE as input and write to CORRECTED_DATA

        outcolumn = "CORRECTED_DATA"
        incolumn = "DATA"

        for i, msname in enumerate(mslist):
            # Let's see the matrix type we are dealing with

            gsols_ = [config[key]['gsols_timeslots'][num - 1 if num <= len(config[key]['gsols_timeslots']) else -1],
                      config[key]['gsols_chan'][num - 1 if num <= len(config[key]['gsols_chan']) else -1]]
            # If we have a two_step selfcal  we will calculate the intervals
            matrix_type = config[key]['gain_matrix_type'][
                num - 1 if len(config[key]['gain_matrix_type']) >= num else -1]
            if config['cal_meqtrees']['two_step'] and pipeline.enable_task(config, 'aimfast'):
                if num == 1:
                    matrix_type = 'GainDiagPhase'
                    SN = 3
                else:
                    matrix_type = trace_matrix[num - 2]
                    SN = trace_SN[num - 2]
                fidelity_data = get_aimfast_data()
                obs_data = get_obs_data(msname)
                int_time = obs_data['EXPOSURE']
                tot_time = 0.0

                for scan_key in obs_data['SCAN']['0']:
                    tot_time += obs_data['SCAN']['0'][scan_key]
                no_ant = len(obs_data['ANT']['DISH_DIAMETER'])
                DR = fidelity_data['{0}_{2}_{1}-residual'.format(
                    prefix, num, field)]['{0}_{2}_{1}-model'.format(prefix, num, field)]['DR']
                Noise = fidelity_data['{0}_{2}_{1}-residual'.format(
                    prefix, num, field)]['STDDev']
                flux = DR * Noise
                solvetime = int(Noise**2 * SN**2 * tot_time *
                                no_ant / (flux**2 * 2.) / int_time)

                if num > 1:
                    DR = fidelity_data['{0}_{2}_{1}-residual'.format(
                        prefix, num - 1, field)]['{0}_{2}_{1}-model'.format(prefix, num - 1, field)]['DR']
                    flux = DR * Noise
                    prev_solvetime = int(
                        Noise**2 * SN**2 * tot_time * no_ant / (flux**2 * 2.) / int_time)
                else:
                    prev_solvetime = solvetime + 1

                if (solvetime >= prev_solvetime or reset_cal == 1) and matrix_type == 'GainDiagPhase':
                    matrix_type = 'GainDiag'
                    SN = 8
                    solvetime = int(Noise**2 * SN**2 * tot_time *
                                    no_ant / (flux**2 * 2.) / int_time)
                    gsols_[0] = int(solvetime / num)
                elif solvetime >= prev_solvetime and matrix_type == 'GainDiag':
                    gsols_[0] = int(prev_solvetime / num - 1)
                    reset_cal = 2
                else:
                    gsols_[0] = int(solvetime / num)
                if matrix_type == 'GainDiagPhase':
                    minsolvetime = int(30. / int_time)
                else:
                    minsolvetime = int(30. * 60. / int_time)
                if minsolvetime > gsols_[0]:
                    gsols_[0] = minsolvetime
                    if matrix_type == 'GainDiag':
                        reset_cal = 2
                trace_SN.append(SN)
                trace_matrix.append(matrix_type)
                if matrix_type == 'GainDiagPhase' and config['cal_meqtrees']['two_step']:
                    outcolumn = "CORRECTED_DATA_PHASE"
                    incolumn = "DATA"
                elif config['cal_meqtrees']['two_step']:
                    outcolumn = "CORRECTED_DATA"
                    incolumn = "CORRECTED_DATA_PHASE"

            elif config['cal_meqtrees']['two_step']:
                # This mode is actually not accesible for now as aimfast is swithed on automatically
                gasols_ = [config[key]['gasols_timeslots'][
                    num - 1 if num <= len(config[key]['gasols_timeslots']) else -1],
                    config[key]['gasols_chan'][
                    num - 1 if num <= len(config[key]['gasols_chan']) else -1]]
                if gasols_[0] == -1:
                    outcolumn = "CORRECTED_DATA_PHASE"
                    incolumn = "DATA"
                else:
                    outcolumn = "CORRECTED_DATA"
                    incolumn = "CORRECTED_DATA_PHASE"
                    matrix_type = 'GainDiag'
                    gsols_ = gasols_

            bsols_ = [config[key]['bsols_timeslots'][num - 1 if num <= len(config[key]['bsols_timeslots']) else -1],
                      config[key]['bsols_chan'][num - 1 if num <= len(config[key]['bsols_chan']) else -1]]
            step = 'calibrate-field{0:d}-iter{1:d}-ms{2:d}'.format(trg, num, i)
            outdata = config[key]['output_data'][num - 1 if len(config[key]['output_data']) >= num else -1]
            if outdata == 'CORRECTED_DATA':
                outdata = 'CORR_DATA'
            model_cal = calmodel.split("/")[-1]
            model_cal = model_cal.split(":output")[0]
            inp_dir = pipeline.output + "/" + img_dir + "/"
            op_dir = pipeline.continuum + "/selfcal_products/"
            msbase = os.path.splitext(msname)[0]
            # Check if a model subtraction has already been done
            with table('{0:s}/{1:s}'.format(pipeline.msdir, msname), readonly=False) as ms_table:
                try:
                    caracal.log.info(f"Re-doing the calibration on {msname}")
                    nModelSub = 0
                    ms_table.putcolkeyword('CORRECTED_DATA', 'modelSub', nModelSub)
                    caracal.log.info(f"Reseting the counter to {nModelSub}")
                except RuntimeError:
                    caracal.log.info(f"No subtraction found in {msname}")
            recipe.add('cab/calibrator', step,
                       {
                           "skymodel": model_cal,
                           "add-vis-model": vismodel,
                           "model-column": modelcolumn,
                           "msname": msname,
                           "threads": ncpu,
                           "column": incolumn,
                           "output-data": outdata,
                           "output-column": outcolumn,
                           "prefix": '{0:s}_{1:s}_{2:d}_meqtrees'.format(prefix, msbase, num),
                           "label": 'cal{0:d}'.format(num),
                           "read-flags-from-ms": True,
                           "read-flagsets": "-stefcal",
                           "write-flagset": "stefcal",
                           "write-flagset-policy": "replace",
                           "Gjones": True,
                           "Gjones-solution-intervals": sdm.dismissable(gsols_ or None),
                           "Gjones-matrix-type": matrix_type,
                           "Gjones-ampl-clipping": True,
                           "Gjones-ampl-clipping-low": config['cal_gain_cliplow'],
                           "Gjones-ampl-clipping-high": config['cal_gain_cliphigh'],
                           "Bjones": config['cal_bjones'],
                           "Bjones-solution-intervals": sdm.dismissable(bsols_ or None),
                           "Bjones-ampl-clipping": config['cal_bjones'],
                           "Bjones-ampl-clipping-low": config['cal_gain_cliplow'],
                           "Bjones-ampl-clipping-high": config['cal_gain_cliphigh'],
                           "make-plots": False,
                           "tile-size": time_chunk,
                       },
                       input=inp_dir,
                       output=op_dir,
                       label="{0:s}:: Calibrate step {1:d} ms={2:s}".format(step, num, msname))

    def calibrate_cubical(trg, num, prod_path, img_dir, mslist, field):
        key = 'calibrate'

        modellist = []
        # if model is unset for this iteration then just run with the model
        if len(config[key]['model']) >= num:
            model = config[key]['model'][num - 1]
        else:
            model = str(num)
        # Defines the pybdsf models (and fitsmodels for some weird reasons)
        # If the model string contains a +, then combine the appropriate models
        if isinstance(model, str) and len(model.split('+')) > 1:
            mm = model.split('+')
            calmodel, fits_model = combine_models(mm, num, img_dir, field)
        # If it doesn't then don't combine.
        else:
            model = int(model)
            calmodel = '{0:s}/{1:s}_{2:s}_{3:d}-pybdsm.lsm.html:output'.format(
                img_dir, prefix, field, model)
            fits_model = '{0:s}/{1:s}_{2:s}_{3:d}-pybdsm.fits'.format(
                img_dir, prefix, field, model)
        # In pybdsm_vis mode, add the calmodel (pybdsf) and the MODEL_DATA.
        if config['cal_model_mode'] == 'pybdsm_vis':
            if (num == cal_niter):
                cmodel = calmodel.split(":output")[0]
                modellist = spf("MODEL_DATA+" + '{}/' + cmodel, "output")

                # modellist = [calmodel, 'MODEL_DATA']
        # otherwise, just calmodel (pybdsf)
            else:
                # modellist = [calmodel]
                cmodel = calmodel.split(":output")[0]
                modellist = spf("{}/" + cmodel, "output")
            # This is incorrect and will result in the lsm being used in the first direction
            # and the model_data in the others. They need to be added as + however
            # that messes up the output identifier structure
        if config['cal_model_mode'] == 'pybdsm_only':
            cmodel = calmodel.split(":output")[0]
            modellist = spf('{}/' + cmodel, "output")
        if config['cal_model_mode'] == 'vis_only':
            modellist = spf("MODEL_DATA")
        matrix_type = config[key]['gain_matrix_type'][
            num - 1 if len(config[key]['gain_matrix_type']) >= num else -1]

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
        elif matrix_type == 'Fslope':
            gupdate = 'phase-diag'

        else:
            raise ValueError('{} is not a viable matrix_type'.format(matrix_type))

        jones_chain = 'G'
        gsols_ = [config[key]['gsols_timeslots'][num - 1 if num <= len(config[key]['gsols_timeslots']) else -1],
                  config[key]['gsols_chan'][
                      num - 1 if num <= len(config[key]['gsols_chan']) else -1]]
        bsols_ = [config[key]['bsols_timeslots'][num - 1 if num <= len(config[key]['bsols_timeslots']) else -1],
                  config[key]['bsols_chan'][
                      num - 1 if num <= len(config[key]['bsols_chan']) else -1]]
        gasols_ = [
            config[key]['gasols_timeslots'][num - 1 if num <=
                                            len(config[key]['gasols_timeslots']) else -1],
            config[key]['gasols_chan'][num - 1 if num <=
                                       len(config[key]['gasols_chan']) else -1]]
        if config['cal_bjones']:
            jones_chain += ',B'
            bupdate = gupdate

        second_matrix_invoked = False
        # If we are doing a calibration of phases and amplitudes on different timescale G is always phase
        # This cannot be combined with the earlier statement as bupdate needs to be equal to the original matrix
        # first check if we are doing amplitude and phase
        if (matrix_type == 'GainDiag' or matrix_type == 'Gain2x2'):
            # Then check whether the scales different
            if (gasols_[0] != -1 and gasols_[0] != gsols_[0]) or (gasols_[1] != -1 and gasols_[1] != gsols_[1]):
                gupdate = 'phase-diag'
                jones_chain += ',DD'
                second_matrix_invoked = True
                if gasols_[0] == -1:
                    gasols_[0] = gsols_[0]
                if gasols_[1] == -1:
                    gasols_[1] = gsols_[1]
        # If we are using more than one matrix we need to set the matrix type to Gain2x2
        if len(jones_chain.split(",")) > 1:
            matrix_type = 'Gain2x2'
        # Need to ad the solution term iterations
        solterm_niter = config['cal_cubical']['solterm_niter']
        sol_terms_add = []
        for term in jones_chain.split(","):
            sol_terms_add.append(str(solterm_niter[SOL_TERMS_INDEX[term]]))
        flags = "-cubical"

        for i, msname in enumerate(mslist):
            # Due to a bug in cubical full polarization datasets are not compliant with sel-diag: True
            # Hence this temporary fix.
            corrs = pipeline.get_msinfo(msname)['CORR']['CORR_TYPE']
            if len(corrs) > 2:
                take_diag_terms = False
            # End temp fix
            step = 'calibrate-cubical-field{0:d}-iter{1:d}'.format(trg, num, i)
            if gupdate == 'phase-diag' and matrix_type == 'Fslope':
                g_table_name = "{0:s}/{3:s}-g-delay-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                         pipeline), num, os.path.splitext(msname)[0], prefix)
            elif gupdate == 'phase-diag':
                g_table_name = "{0:s}/{3:s}-g-phase-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                         pipeline), num, os.path.splitext(msname)[0], prefix)
            elif gupdate == 'amp-diag':
                g_table_name = "{0:s}/{3:s}-g-amp-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                       pipeline), num, os.path.splitext(msname)[0], prefix)
            elif gupdate == 'diag':
                g_table_name = "{0:s}/{3:s}-g-amp-phase-diag-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                                  pipeline), num, os.path.splitext(msname)[0], prefix)
            elif gupdate == 'full':
                g_table_name = "{0:s}/{3:s}-g-amp-phase-full-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                                  pipeline), num, os.path.splitext(msname)[0], prefix)
            else:
                raise RuntimeError("Something has corrupted the selfcal run")
            msbase = os.path.splitext(msname)[0]
            # Check if a model subtraction has already been done
            with table('{0:s}/{1:s}'.format(pipeline.msdir, msname), readonly=False) as ms_table:
                try:
                    caracal.log.info(f"Re-doing the calibration on {msname}")
                    nModelSub = 0
                    ms_table.putcolkeyword('CORRECTED_DATA', 'modelSub', nModelSub)
                    caracal.log.info(f"Reseting the counter to {nModelSub}")
                except RuntimeError:
                    caracal.log.info(f"No subtraction found in {msname}")
            cubical_opts = {
                "data-ms": msname,
                "data-column": 'DATA',
                "model-list": modellist if config[key]['output_data'][num - 1 if len(config[key]['output_data']) >= num else -1] not in ['PA_DATA'] else '',
                "model-pa-rotate": config['cal_cubical']['model_pa_rotate'],
                "sel-ddid": sdm.dismissable(spwid),
                "dist-ncpu": ncpu,
                "log-memory": True,
                "sol-jones": jones_chain,
                "sol-term-iters": ",".join(sol_terms_add),
                "sel-diag": take_diag_terms,
                "out-name": '{0:s}/{1:s}_{2:s}_{3:d}_cubical'.format(get_dir_path(prod_path,
                                                                                  pipeline), prefix, msbase, num),
                "out-mode": CUBICAL_OUT[config[key]['output_data'][num - 1 if len(config[key]['output_data']) >= num else -1]],
                "out-plots": True,
                "out-derotate": config['cal_cubical']['out_derotate'],
                "dist-max-chunks": config['cal_cubical']['dist_max_chunks'],
                "out-casa-gaintables": True,
                "weight-column": config['cal_cubical']['weight_col'],
                "montblanc-dtype": 'float',
                "bbc-save-to": "{0:s}/bbc-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                               pipeline), num, msbase),
                "g-solvable": True if config[key]['output_data'][num - 1 if len(config[key]['output_data']) >= num else -1] not in ['PA_DATA'] else False,
                "g-type": CUBICAL_MT[matrix_type],
                "g-update-type": gupdate,
                "g-time-int": int(gsols_[0]),
                "g-freq-int": int(gsols_[1]),
                "out-overwrite": config['cal_cubical']['overwrite'],
                "g-save-to": g_table_name,
                "g-clip-low": config['cal_gain_cliplow'],
                "g-clip-high": config['cal_gain_cliphigh'],
                "g-max-prior-error": config['cal_cubical']['max_prior_error'],
                "g-max-post-error": config['cal_cubical']['max_post_error'],
                "madmax-enable": config['cal_cubical']['flag_madmax'],
                "madmax-plot": True if (config['cal_cubical']['flag_madmax']) else False,
                "madmax-threshold": config['cal_cubical']['madmax_flag_thr'],
                "madmax-estimate": 'corr',
                "log-boring": True,
                "dd-dd-term": False,
                "model-ddes": 'never',
            }
            if config['cal_cubical']['model_feed_rotate']:
                cubical_opts.update({"model-feed-rotate": config['cal_cubical']['model_feed_rotate']}),
            if min_uvw > 0:
                cubical_opts.update({"sol-min-bl": min_uvw})
            if flags != "":
                cubical_opts.update({
                    "flags-apply": flags,
                })
            if second_matrix_invoked:
                cubical_opts.update({
                    "dd-update-type": 'amp-diag',
                    "dd-solvable": True,
                    "dd-type": CUBICAL_MT[matrix_type],
                    "dd-time-int": int(gasols_[0]),
                    "dd-freq-int": int(gasols_[1]),
                    "dd-save-to": "{0:s}/{3:s}-g-amp-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                          pipeline), num, os.path.splitext(msname)[0], prefix),
                    "dd-clip-low": config['cal_gain_cliplow'],
                    "dd-clip-high": config['cal_gain_cliphigh'],
                    "dd-max-prior-error": config['cal_cubical']['max_prior_error'],
                    "dd-max-post-error": config['cal_cubical']['max_post_error'],
                })
            if config['cal_bjones']:
                if bupdate == 'phase-diag':
                    b_table_name = "{0:s}/{3:s}-b-phase-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                             pipeline), num, os.path.splitext(msname)[0], prefix)
                elif bupdate == 'amp-diag':
                    b_table_name = "{0:s}/{3:s}-b-amp-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                           pipeline), num, os.path.splitext(msname)[0], prefix)
                elif bupdate == 'diag':
                    b_table_name = "{0:s}/{3:s}-b-amp-phase-diag-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                                      pipeline), num, os.path.splitext(msname)[0], prefix)
                elif bupdate == 'full':
                    b_table_name = "{0:s}/{3:s}-b-amp-phase-full-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                                      pipeline), num, os.path.splitext(msname)[0], prefix)
                else:
                    raise RuntimeError("Something has corrupted the selfcal run")
                cubical_opts.update({
                    "b-update-type": bupdate,
                    "b-solvable": True,
                    "b-time-int": int(bsols_[0]),
                    "b-freq-int": int(bsols_[1]),
                    "b-type": CUBICAL_MT[matrix_type],
                    "b-clip-low": config['cal_gain_cliplow'],
                    "b-save-to": b_table_name,
                    "b-clip-high": config['cal_gain_cliphigh'],
                    "b-max-prior-error": config['cal_cubical']['max_prior_error'],
                    "b-max-post-error": config['cal_cubical']['max_post_error'], }
                )
            # Time chunk and freq chunk have been checked and approved before so they are what they are
            cubical_opts.update({
                "data-time-chunk": time_chunk,
                "data-freq-chunk": freq_chunk, }
            )
            recipe.add('cab/cubical', step, cubical_opts,
                       input=pipeline.input,
                       output=pipeline.output,
                       shared_memory=config['cal_cubical']['shared_mem'],
                       label="{0:s}:: Calibrate step {1:d} ms={2:s}".format(step, num, msname))

    def restore(num, prod_path, mslist_out, enable_inter=True):
        key = 'calibrate'
        # to achieve accurate restauration we need to reset all parameters properly
        matrix_type = config[key]['gain_matrix_type'][
            num - 1 if len(config[key]['gain_matrix_type']) >= num else -1]
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
        elif matrix_type == 'Fslope':
            gupdate = 'phase-diag'
        else:
            raise ValueError('{} is not a viable matrix_type'.format(matrix_type))

        jones_chain = 'G'
        gsols_ = [config[key]['gsols_timeslots'][num - 1 if num <= len(config[key]['gsols_timeslots']) else -1],
                  config[key]['gsols_chan'][
                      num - 1 if num <= len(config[key]['gsols_chan']) else -1]]
        bsols_ = [config[key]['bsols_timeslots'][num - 1 if num <= len(config[key]['bsols_timeslots']) else -1],
                  config[key]['bsols_chan'][
                      num - 1 if num <= len(config[key]['bsols_chan']) else -1]]
        gasols_ = [
            config[key]['gasols_timeslots'][num - 1 if num <=
                                            len(config[key]['gasols_timeslots']) else -1],
            config[key]['gasols_chan'][num - 1 if num <=
                                       len(config[key]['gasols_chan']) else -1]]
        # If we are doing a calibration of phases and amplitudes on different timescale G is always phase
        # This cannot be combined with the earlier statement as bupdate needs to be equal to the original matrix.
        second_matrix_invoked = False
        if (matrix_type == 'GainDiag' or matrix_type == 'Gain2x2'):
            # Then check whether the scales different
            if (gasols_[0] != -1 and gasols_[0] != gsols_[0]) or (gasols_[1] != -1 and gasols_[1] != gsols_[1]):
                gupdate = 'phase-diag'
                jones_chain += ',DD'
                second_matrix_invoked = True
                if gasols_[0] == -1:
                    gasols_[0] = gsols_[0]
                if gasols_[1] == -1:
                    gasols_[1] = gsols_[1]
        # If we want to interpolate our we get the interpolation interval

        if config['cal_bjones']:
            jones_chain += ',B'
            bupdate = gupdate

        # select the right datasets
        if enable_inter:
            apmode = 'ac'
        else:
            if CUBICAL_OUT[
                    config[key]['output_data'][num - 1 if len(config[key]['output_data']) >= num else -1]] == 'sr':
                apmode = 'ar'
            else:
                apmode = 'ac'
        # if we have more than one matrix set the matrixtype correctly
        if len(jones_chain.split(",")) > 1:
            matrix_type = 'Gain2x2'
        # Cubical does not at the moment apply the gains when the matrix is not complex2x2 (https://github.com/ratt-ru/CubiCal/issues/324).
        # Hence the following fix. This should be removed once the fix makes it into stimela.
        matrix_type = 'Gain2x2'
        # Does solterm_niter  matter for applying?????
        solterm_niter = config['cal_cubical']['solterm_niter']
        sol_terms_add = []
        for term in jones_chain.split(","):
            sol_terms_add.append(str(solterm_niter[SOL_TERMS_INDEX[term]]))
        # loop through measurement sets
        for i, msname_out in enumerate(mslist_out):
            # Due to a bug in cubical full polarization datasets are not compliant with sel-diag: True
            # Hence this temporary fix.
            corrs = pipeline.get_msinfo(msname_out)['CORR']['CORR_TYPE']
            if len(corrs) > 2:
                take_diag_terms = False
            # End temp fix
            # Python is really the dumbest language ever so need deep copies else the none apply variables change along with apply
            gsols_apply = copy.deepcopy(gsols_)
            bsols_apply = copy.deepcopy(bsols_)
            gasols_apply = copy.deepcopy(gasols_)
            if enable_inter and config['transfer_apply_gains']['interpolate']['enable']:
                time_chunk_apply = config['transfer_apply_gains']['interpolate']['timeslots_chunk']
                freq_chunk_apply = config['transfer_apply_gains']['interpolate']['chan_chunk']
            else:
                time_chunk_apply = copy.deepcopy(time_chunk)
                freq_chunk_apply = copy.deepcopy(freq_chunk)
            if enable_inter:
                # Read the time and frequency channels of the  'fullres'
                fullres_data = get_obs_data(msname_out)
                int_time_fullres = fullres_data['EXPOSURE']
                channelsize_fullres = fullres_data['SPW']['TOTAL_BANDWIDTH'][0] / fullres_data['SPW']['NUM_CHAN'][0]
                caracal.log.info("Integration time of full-resolution data is: {}".format(int_time_fullres))
                caracal.log.info("Channel size of full-resolution data is: {}".format(channelsize_fullres))
                # Corresponding numbers for the self-cal -ed MS:
                avg_data = get_obs_data(mslist[i])
                int_time_avg = avg_data['EXPOSURE']
                channelsize_avg = avg_data['SPW']['TOTAL_BANDWIDTH'][0] / avg_data['SPW']['NUM_CHAN'][0]
                caracal.log.info("Integration time of averaged data is: {}".format(int_time_avg))
                caracal.log.info("Channel size of averaged data is:{}".format(channelsize_avg))
                # Compare the channel and timeslot ratios:
                ratio_timeslot = int_time_avg / int_time_fullres
                ratio_channelsize = channelsize_avg / channelsize_fullres
                fromname = msname_out.replace(label_tgain, label)
                if not config['transfer_apply_gains']['interpolate']['enable']:
                    gsols_apply[0] = int(ratio_timeslot * gsols_[0])
                    gsols_apply[1] = int(ratio_channelsize * gsols_[1])
                    time_chunk_apply = int(max(int(ratio_timeslot * gsols_[0]), time_chunk_apply)) if not (
                        int(gsols_[0]) == 0 or time_chunk_apply == 0) else 0
                    freq_chunk_apply = int(max(int(ratio_channelsize * gsols_[1]), freq_chunk_apply)) if not (
                        int(gsols_[1]) == 0 or freq_chunk_apply == 0) else 0
                else:
                    if config['transfer_apply_gains']['interpolate']['timeslots_int'] < 0:
                        gsols_apply[0] = int(ratio_timeslot * gsols_[0])
                    else:
                        gsols_apply[0] = config['transfer_apply_gains']['interpolate']['timeslots_int']
                    if config['transfer_apply_gains']['interpolate']['chan_int'] < 0:
                        gsols_apply[1] = int(ratio_timeslot * gsols_[1])
                    else:
                        gsols_apply[1] = config['transfer_apply_gains']['interpolate']['chan_int']
                    time_chunk_apply = int(max(int(ratio_timeslot * gsols_[0]), time_chunk_apply)) if not (
                        int(gsols_[0]) == 0 or time_chunk_apply == 0) else 0
                    freq_chunk_apply = int(max(int(ratio_channelsize * gsols_[1]), freq_chunk_apply)) if not (
                        int(gsols_[1]) == 0 or freq_chunk_apply == 0) else 0
            else:
                fromname = msname_out
                # First remove the later flags
                counter = num + 1
                remainder_flags = "step_{0:d}_2gc_flags".format(counter)

                while counter < cal_niter:
                    counter += 1
                    remainder_flags += ",step_{0:d}_2gc_flags".format(counter)
                mspref = msname_out.split(".ms")[0].replace("-", "_")
                recipe.add("cab/flagms", "remove_2gc_flags-{0:s}".format(mspref),
                           {
                               "msname": msname_out,
                               "remove": remainder_flags,
                },
                    input=pipeline.input,
                    output=pipeline.output,
                    label="remove-2gc_flags-{0:s}:: Remove 2GC flags".format(mspref))

            # build cubical commands
            msbase = os.path.splitext(msname_out)[0]
            cubical_gain_interp_opts = {
                "data-ms": msname_out,
                "data-column": 'DATA',
                "sel-ddid": sdm.dismissable(spwid),
                "sol-jones": jones_chain,
                "sol-term-iters": ",".join(sol_terms_add),
                "sel-diag": take_diag_terms,
                "dist-ncpu": ncpu,
                "dist-max-chunks": config['cal_cubical']['dist_max_chunks'],
                "log-memory": True,
                "out-name": '{0:s}/{1:s}-{2:s}_{3:d}_restored_cubical'.format(get_dir_path(prod_path,
                                                                                           pipeline), prefix,
                                                                              msbase, num),
                "out-mode": apmode,
                # "out-overwrite": config[key]['overwrite'],
                "out-overwrite": True,
                "weight-column": config['cal_cubical']['weight_col'],
                "montblanc-dtype": 'float',
                "g-solvable": True,
                "g-update-type": gupdate,
                "g-type": CUBICAL_MT[matrix_type],
                "g-time-int": int(gsols_apply[0]),
                "g-freq-int": int(gsols_apply[1]),
                "madmax-enable": config['cal_cubical']['flag_madmax'],
                "madmax-plot": False,
                "madmax-threshold": config['cal_cubical']['madmax_flag_thr'],
                "madmax-estimate": 'corr',
                "madmax-offdiag": False,
                "dd-dd-term": False,
                "model-ddes": 'never',
            }
            # Set the table name
            if gupdate == 'phase-diag' and matrix_type == 'Fslope':
                g_table_name = "{0:s}/{3:s}-g-delay-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                         pipeline), num, os.path.splitext(fromname)[0], prefix)
            elif gupdate == 'phase-diag':
                g_table_name = "{0:s}/{3:s}-g-phase-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                         pipeline), num, os.path.splitext(fromname)[0], prefix)
            elif gupdate == 'amp-diag':
                g_table_name = "{0:s}/{3:s}-g-amp-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                       pipeline), num, os.path.splitext(fromname)[0], prefix)
            elif gupdate == 'diag':
                g_table_name = "{0:s}/{3:s}-g-amp-phase-diag-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                                  pipeline), num, os.path.splitext(fromname)[0], prefix)
            elif gupdate == 'full':
                g_table_name = "{0:s}/{3:s}-g-amp-phase-full-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                                  pipeline), num, os.path.splitext(fromname)[0], prefix)
            else:
                raise RuntimeError("Something has corrupted the application of the tables")
            if config['transfer_apply_gains']['interpolate']['enable']:
                cubical_gain_interp_opts.update({
                    "g-xfer-from": g_table_name
                })
            else:
                cubical_gain_interp_opts.update({
                    "g-load-from": g_table_name
                })

            # expand
            if config['cal_bjones']:
                if enable_inter:
                    if not config['transfer_apply_gains']['interpolate']['enable']:
                        bsols_apply[0] = int(ratio_timeslot * bsols_[0])
                        bsols_apply[1] = int(ratio_channelsize * bsols_[1])
                        time_chunk_apply = int(max(int(ratio_timeslot * bsols_[0]), time_chunk_apply)) if not (
                            int(bsols_[0]) == 0 or time_chunk_apply == 0) else 0
                        freq_chunk_apply = int(max(int(ratio_channelsize * bsols_[1]), freq_chunk_apply)) if not (
                            int(bsols_[1]) == 0 or freq_chunk_apply == 0) else 0
                    else:
                        if config['transfer_apply_gains']['interpolate']['timeslots_int'] < 0:
                            bsols_apply[0] = int(ratio_timeslot * bsols_[0])
                        else:
                            bsols_apply[0] = config['transfer_apply_gains']['interpolate']['timeslots_int']
                        if config['transfer_apply_gains']['interpolate']['chan_int'] < 0:
                            bsols_apply[1] = int(ratio_timeslot * bsols_[1])
                        else:
                            bsols_apply[1] = config['transfer_apply_gains']['interpolate']['chan_int']
                        time_chunk_apply = int(max(int(ratio_timeslot * bsols_[0]), time_chunk_apply)) if not (
                            int(bsols_[0]) == 0 or time_chunk_apply == 0) else 0
                        freq_chunk_apply = int(max(int(ratio_channelsize * bsols_[1]), freq_chunk_apply)) if not (
                            int(bsols_[1]) == 0 or freq_chunk_apply == 0) else 0

                cubical_gain_interp_opts.update({
                    "b-update-type": bupdate,
                    "b-type": CUBICAL_MT[matrix_type],
                    "b-time-int": int(bsols_apply[0]),
                    "b-freq-int": int(bsols_apply[1]),
                    "b-solvable": False
                })
                # Set the table name
                if bupdate == 'phase-diag':
                    b_table_name = "{0:s}/{3:s}-b-phase-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                             pipeline), num, os.path.splitext(fromname)[0], prefix)
                elif bupdate == 'amp-diag':
                    b_table_name = "{0:s}/{3:s}-b-amp-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                           pipeline), num, os.path.splitext(fromname)[0], prefix)
                elif bupdate == 'diag':
                    b_table_name = "{0:s}/{3:s}-b-amp-phase-diag-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                                      pipeline), num, os.path.splitext(fromname)[0], prefix)
                elif bupdate == 'full':
                    b_table_name = "{0:s}/{3:s}-b-amp-phase-full-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                                      pipeline), num, os.path.splitext(fromname)[0], prefix)
                else:
                    raise RuntimeError("Something has corrupted the application of the tables")
                if config['transfer_apply_gains']['interpolate']['enable']:
                    cubical_gain_interp_opts.update({
                        "b-xfer-from": b_table_name
                    })
                else:
                    cubical_gain_interp_opts.update({
                        "b-load-from": b_table_name
                    })
            if second_matrix_invoked:
                if not config['transfer_apply_gains']['interpolate']['enable']:
                    gasols_apply[0] = int(ratio_timeslot * gasols_[0])
                    gasols_apply[1] = int(ratio_channelsize * gasols_[1])
                    time_chunk_apply = int(max(int(ratio_timeslot * gasols_[0]), time_chunk_apply)) if not (
                        int(gasols_[0]) == 0 or time_chunk_apply == 0) else 0
                    freq_chunk_apply = int(max(int(ratio_channelsize * gasols_[1]), freq_chunk_apply)) if not (
                        int(gasols_[1]) == 0 or freq_chunk_apply == 0) else 0
                else:
                    if config['transfer_apply_gains']['interpolate']['timeslots_int'] < 0:
                        gasols_apply[0] = int(ratio_timeslot * gasols_[0])
                    else:
                        gasols_apply[0] = config['transfer_apply_gains']['interpolate']['timeslots_int']
                    if config['transfer_apply_gains']['interpolate']['chan_int'] < 0:
                        gasols_apply[1] = int(ratio_timeslot * gasols_[1])
                    else:
                        gasols_apply[1] = config['transfer_apply_gains']['interpolate']['chan_int']
                    time_chunk_apply = int(max(int(ratio_timeslot * gasols_[0]), time_chunk_apply)) if not (
                        int(gasols_[0]) == 0 or time_chunk_apply == 0) else 0
                    freq_chunk_apply = int(max(int(ratio_channelsize * gasols_[1]), freq_chunk_apply)) if not (
                        int(gasols_[1]) == 0 or freq_chunk_apply == 0) else 0

                cubical_gain_interp_opts.update({
                    "dd-update-type": 'amp-diag',
                    "dd-type": CUBICAL_MT[matrix_type],
                    "dd-time-int": int(gasols_apply[0]),
                    "dd-freq-int": int(gasols_apply[1]),
                    "dd-solvable": False
                })
                if config['transfer_apply_gains']['interpolate']['enable']:
                    cubical_gain_interp_opts.update({
                        "dd-xfer-from": "{0:s}/{3:s}-g-amp-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                                pipeline), num, os.path.splitext(fromname)[0], prefix)
                    })
                else:
                    cubical_gain_interp_opts.update({
                        "dd-load-from": "{0:s}/{3:s}-g-amp-gains-{1:d}-{2:s}.parmdb:output".format(get_dir_path(prod_path,
                                                                                                                pipeline), num, os.path.splitext(fromname)[0], prefix)
                    })
            cubical_gain_interp_opts.update({
                "data-time-chunk": time_chunk_apply,
                "data-freq-chunk": int(freq_chunk_apply)
            })
            # ensure proper logging for restore or interpolation
            if not enable_inter:
                step = 'restore_cubical_gains-{0:d}-{1:d}'.format(num, i)
                stim_label = "{0:s}:: restore cubical gains ms={1:s}".format(step, msname_out)
            else:
                step = 'apply_cubical_gains-{0:d}-{1:d}'.format(num, i)
                stim_label = "{0:s}:: Apply cubical gains ms={1:s}".format(step, msname_out)
            recipe.add('cab/cubical', step, cubical_gain_interp_opts,
                       input=pipeline.input,
                       output=pipeline.output,
                       shared_memory=config['cal_cubical']['shared_mem'],
                       label=stim_label)
            recipe.run()
            # Empty job que after execution
            recipe.jobs = []

    def get_aimfast_data(filename='{0:s}/{1:s}_fidelity_results.json'.format(
            pipeline.output, prefix)):
        "Extracts data from the json data file"
        with open(filename) as f:
            data = json.load(f)
        return data

    def get_obs_data(msname):
        "Extracts data from the json data file"
        return pipeline.get_msinfo(msname)

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
            tol = config[key]['tol']
            conv_crit = config[key]['convergence_criteria']
            # Ensure atleast one iteration is ran to compare previous and subsequent images
            # And atleast one convergence criteria is specified
            if n >= 2 and not config['cal_meqtrees']['two_step'] and conv_crit:
                fidelity_data = get_aimfast_data()
                conv_crit = [cc.upper() for cc in conv_crit]
                # Ensure atleast one iteration is ran to compare previous and subsequent images
                residual0 = fidelity_data['{0}_{1}_{2}-residual'.format(
                    prefix, field, n - 1)]
                residual1 = fidelity_data['{0}_{1}_{2}-residual'.format(
                    prefix, field, n)]
                # Unlike the other ratios DR should grow hence n-1/n < 1.

                if not pipeline.enable_task(config, 'extract_sources'):
                    drratio = fidelity_data['{0}_{1}_{2}-restored'.format(prefix, field, n - 1)]['DR'] / fidelity_data[
                        '{0}_{1}_{2}-restored'.format(prefix, field, n)]['DR']
                else:
                    drratio = residual0['{0}_{1}_{2}-model'.format(prefix, field,
                                                                   n - 1)]['DR'] / residual1['{0}_{1}_{2}-model'.format(prefix, field, n)]['DR']

                # Dynamic range is important,
                if any(cc == "DR" for cc in conv_crit):
                    drweight = 0.8
                else:
                    drweight = 0.
                # The other parameters should become smaller, hence n/n-1 < 1
                skewratio = residual1['SKEW'] / residual0['SKEW']
                # We care about the skewness when it is large. What is large?
                # Let's go with 0.005 at that point it's weight is 0.5
                if any(cc == "SKEW" for cc in conv_crit):
                    skewweight = residual1['SKEW'] / 0.01
                else:
                    skewweight = 0.
                kurtratio = residual1['KURT'] / residual0['KURT']
                # Kurtosis goes to 3 so this way it counts for 0.5 when normal distribution
                if any(cc == "KURT" for cc in conv_crit):
                    kurtweight = residual1['KURT'] / 6.
                else:
                    kurtweight = 0.
                meanratio = residual1['MEAN'] / residual0['MEAN']
                # We only care about the mean when it is large compared to the noise
                # When it deviates from zero more than 20% of the noise this is a problem
                if any(cc == "MEAN" for cc in conv_crit):
                    meanweight = residual1['MEAN'] / (residual1['STDDev'] * 0.2)
                else:
                    meanweight = 0.
                noiseratio = residual1['STDDev'] / residual0['STDDev']
                # The noise should not change if the residuals are gaussian in n-1.
                # However, they should decline in case the residuals are non-gaussian.
                # We want a weight that goes to 0 in both cases
                if any(cc == "STDDEV" for cc in conv_crit):
                    if residual0['KURT'] / 6. < 0.52 and residual0['SKEW'] < 0.01:
                        noiseweight = abs(1. - noiseratio)
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
                # This  combines to total weigth of 1.2+0.+0.5+0.+0. so our total should be LT 1.7*(1-tol)
                # it needs to be slightly lower to avoid keeping fitting without improvement
                # Ok that is the wrong philosophy. Their weighted mean should be less than 1-tol that means improvement.
                # And the weights control how important each parameter is.
                HolisticCheck = (drratio * drweight + skewratio * skewweight + kurtratio * kurtweight + meanratio * meanweight + noiseratio * noiseweight) \
                    / (drweight + skewweight + kurtweight + meanweight + noiseweight)
                if (1 - tol) < HolisticCheck:
                    caracal.log.info(
                        'Stopping criterion: ' + ' '.join([cc for cc in conv_crit]))
                    caracal.log.info('The calculated ratios DR={:f}, Skew={:f}, Kurt={:f}, Mean={:f}, Noise={:f} '.format(
                        drratio, skewratio, kurtratio, meanratio, noiseratio))
                    caracal.log.info('The weights used DR={:f}, Skew={:f}, Kurt={:f}, Mean={:f}, Noise={:f} '.format(
                        drweight, skewweight, kurtweight, meanweight, noiseweight))
                    caracal.log.info('{:f} < {:f}'.format(
                        1 - tol, HolisticCheck))
                #   If we stop we want change the final output model to the previous iteration
                    global self_cal_iter_counter
                    reset_cal += 1
                    if reset_cal == 1:
                        self_cal_iter_counter -= 1
                    else:
                        self_cal_iter_counter -= 2

                    if self_cal_iter_counter < 1:
                        self_cal_iter_counter = 1
                    return False
        # If we reach the number of iterations we want to stop.
        if n == cal_niter + 1:
            caracal.log.info(
                'Number of iterations to be done: {:d}'.format(cal_niter))
            return False
        # If no condition is met return true to continue
        return True

    def image_quality_assessment(num, img_dir, field):
        # Check if more than two calibration iterations to combine successive models
        # Combine models <num-1> (or combined) to <num> creat <num+1>-pybdsm-combine
        # This was based on thres_pix but change to model as when extract_sources = True is will take the last settings
        if len(config['calibrate']['model']) >= num:
            model = config['calibrate']['model'][num - 1]
            if isinstance(model, str) and len(model.split('+')) == 2:
                mm = model.split('+')
                combine_models(mm, num, img_dir, field)
        else:
            model = str(num)
        # in case we are in the last round, imaging has made a model that is longer then the expected model column
        # Therefore we take this last model if model is not defined
        if num == cal_niter + 1:
            try:
                model.split()
            except NameError:
                model = str(num)

        step = 'aimfast'
        aimfast_settings = {
            "residual-image": '{0:s}/{1:s}_{2:s}_{3:d}{4:s}-residual.fits:output'.format(img_dir, prefix, field, num, mfsprefix),
            "normality-test": config[step]['normality_model'],
            "area-factor": config[step]['area_factor'],
            "label": "{0:s}_{1:s}_{2:d}".format(prefix, field, num),
            "outfile": "{0:s}_fidelity_results.json".format(prefix)
        }

        # if we run pybdsm we want to use the  model as well. Otherwise we want to use the image.
        if pipeline.enable_task(config, 'extract_sources'):
            if config['calibrate'].get('output_data')[-1] == 'CORR_DATA':
                aimfast_settings.update(
                    {"tigger-model": '{0:s}/{1:s}_{2:s}_{3:d}-pybdsm.lsm.html:output'.format(
                        img_dir, prefix, field, num)})
            else:
                # In the case of RES_DATA we need the combined models to compute the dynamic range.
                aimfast_settings.update(
                    {"tigger-model": '{0:s}/{1:s}_{2:s}_{3:d}-pybdsm{4:s}.lsm.html:output'.format(
                        img_dir, prefix, field, num if num <= len(config['calibrate'].get('model'))
                        else len(config['calibrate'].get('model')),
                        '-combined' if len(model.split('+')) >= 2 else '')})

        else:
            # Use the image
            if config['calibrate']['output_data'][num - 1 if num <= len(config['calibrate']['output_data']) else -1] == "CORR_DATA" or \
                    config['calibrate']['output_data'][num - 1 if num <= len(config['calibrate']['output_data']) else -1] == "CORRECTED_DATA":
                aimfast_settings.update({"restored-image": '{0:s}/{1:s}_{2:s}_{3:d}{4:s}-image.fits:output'.format(img_dir, prefix, field, num, mfsprefix)})

            else:
                try:
                    im = config['calibrate']['output_data'].index("CORR_RES") + 1
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
        residuals_compare = []
        for ii in range(1, cal_niter + 2):
            res_file = glob.glob("{0:s}/image_{1:d}/{2:s}_{3:s}_?-MFS-residual.fits".format(
                pipeline.continuum, ii, prefix, field))
            if res_file:
                res_files.append(res_file[0])
        res_files = sorted(res_files)

        for ii in range(0, len(res_files) - 1):
            residuals_compare.append('{0:s}:output'.format(
                res_files[ii].split(pipeline.output)[-1]))
            residuals_compare.append('{0:s}:output'.format(
                res_files[ii + 1].split(pipeline.output)[-1]))

        # Get models to compare
        model_files = []
        models_compare = []
        for ii in range(1, cal_niter + 2):
            model_file = glob.glob(
                "{0:s}/image_{1:d}/{2:s}_{3:s}_?-pybdsm.lsm.html".format(pipeline.continuum, ii, prefix, field))
            if model_file:
                model_files.append(model_file[0])
        model_files = sorted(model_files)

        models = []
        for ii in range(0, len(model_files) - 1):
            models_compare.append('{0:s}:output'.format(
                model_files[ii].split(pipeline.output)[-1]))
            models_compare.append('{0:s}:output'.format(
                model_files[ii + 1].split(pipeline.output)[-1]))

        if len(model_files) > 1:
            step = "aimfast-compare-models"

            recipe.add('cab/aimfast', step,
                       {
                           "compare-models": models_compare,
                           "tolerance": config['aimfast']['radius']
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label="Plotting model comparisons")

        if len(res_files) > 1:
            step = "aimfast-compare-random_residuals"

            recipe.add('cab/aimfast', step,
                       {
                           "compare-residuals": residuals_compare,
                           "area-factor": config['aimfast']['area_factor'],
                           "data-points": 100
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label="Plotting random residuals comparisons")

        if len(res_files) > 1 and len(model_files) > 1:
            step = "aimfast-compare-source_residuals"

            recipe.add('cab/aimfast', step,
                       {
                           "compare-residuals": residuals_compare,
                           "area-factor": config['aimfast']['area_factor'],
                           "tigger-model": '{:s}:output'.format(model_files[-1].split(
                               pipeline.output)[-1])
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label="Plotting source residuals comparisons")

    def aimfast_compare_online_catalog(field):
        """Compare local models to online catalog"""
        model_files = []
        # Get models to compare
        for ii in range(1, cal_niter + 2):
            model_file = glob.glob(
                "{0:s}/image_{1:d}/{2:s}_{3:s}_?-pybdsm.lsm.html".format(
                    pipeline.continuum, ii, prefix, field))
            if model_file:
                model_files.append(model_file[0].split(pipeline.output)[-1] + ':output')
        online_compare = sorted(model_files)

        if online_compare:
            step = "aimfast-compare-online_catalog"
            recipe.add('cab/aimfast', step,
                       {
                           "compare-online": online_compare,
                           "online-catalog": config['aimfast']['online_catalog']['catalog_type'],
                       },
                       input=pipeline.continuum,
                       output=pipeline.output,
                       label="Plotting online source catalog comparisons")
            recipe.run()
            recipe.jobs = []

    def ragavi_plotting_cubical_tables():
        """Plot self-cal gain tables"""

        B_tables = glob.glob('{0:s}/{1:s}/{2:s}/{3:s}'.format(pipeline.output,
                                                              get_dir_path(pipeline.continuum, pipeline), 'selfcal_products', 'g-gains*B.casa'))
        if len(B_tables) > 1:
            step = 'plot-btab'

            gain_table_name = [table.split('output/')[-1] for table in B_tables]  # This probably needs changing?
            recipe.add('cab/ragavi', step,
                       {
                           "table": [tab + ":output" for tab in gain_table_name],
                           "gaintype": config['cal_cubical']['ragavi_plot']['gaintype'],
                           "field": config['cal_cubical']['ragavi_plot']['field'],
                           "htmlname": '{0:s}/{1:s}/{2:s}_self-cal_G_gain_plots'.format(get_dir_path(pipeline.diagnostic_plots,
                                                                                                     pipeline), 'selfcal', prefix)
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Plot gaincal phase : {1:s}'.format(step, ' '.join(B_tables)))

        D_tables = glob.glob('{0:s}/{1:s}/{2:s}/{3:s}'.format(pipeline.output,
                                                              get_dir_path(pipeline.continuum, pipeline), 'selfcal_products', 'g-gains*D.casa'))
        if len(D_tables) > 1:
            step = 'plot_dtab'

            gain_table_name = [table.split(pipeline.output)[-1] for table in D_tables]
            recipe.add('cab/ragavi', step,
                       {
                           "table": [tab + ":output" for tab in gain_table_name],
                           "gaintype": config['cal_cubical']['ragavi_plot']['gaintype'],
                           "field": config['cal_cubical']['ragavi_plot']['field'],
                           "htmlname": '{0:s}/{1:s}/{2:s}_self-cal_D_gain_plots'.format(get_dir_path(pipeline.diagnostic_plots,
                                                                                                     pipeline), 'selfcal', prefix)
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Plot gain tables : {1:s}'.format(step, ' '.join(D_tables)))

    # decide which tool to use for calibration
    calwith = config['calibrate_with'].lower()
    if calwith == 'meqtrees':
        calibrate = calibrate_meqtrees
    elif calwith == 'cubical':
        calibrate = calibrate_cubical

    # if we use the new two_step analysis aimfast has to be run
    if config['cal_meqtrees']['two_step'] and calwith == 'meqtrees':
        config['aimfast']['enable'] = True

    # if we do not run pybdsm we always need to output the corrected data column
    if not pipeline.enable_task(config, 'extract_sources'):
        config['calibrate']['output_data'] = [k.replace(
            'CORR_RES', 'CORR_DATA') for k in config['calibrate']['output_data']]

    if pipeline.enable_task(config, 'aimfast'):
        # If aimfast plotting is enabled run source finder
        if config['aimfast']['plot']:
            config['extract_sources']['enable'] = True

    target_iter = 0
    for target in all_targets:
        mslist = ms_dict[target]
        field = utils.filter_name(target)

        global self_cal_iter_counter
        self_cal_iter_counter = config['start_iter']
        global reset_cal
        reset_cal = 0
        global trace_SN
        trace_SN = []
        global trace_matrix
        trace_matrix = []
        image_path = "{0:s}/image_{1:d}".format(
            pipeline.continuum, self_cal_iter_counter)
        # I think it is best to always define selfcal_products as it might be needed for transfer gains or restore

        selfcal_products = "{0:s}/{1:s}".format(
            pipeline.continuum, 'selfcal_products')
        # When we do not start at iteration 1 we need to restore the data set
        if self_cal_iter_counter != 1:
            if not os.path.exists(image_path):
                raise IOError(
                    "Trying to restore step {0:d} but the correct direcory ({1:s}) does not exist.".format(self_cal_iter_counter - 1, image_path))
            restore(self_cal_iter_counter - 1, selfcal_products, mslist, enable_inter=False)

        if not os.path.exists(image_path):
            os.mkdir(image_path)

        mask_key = config['image']['cleanmask_method'][self_cal_iter_counter - 1 if len(config['image']['cleanmask_method']) >= self_cal_iter_counter else -1]
        if pipeline.enable_task(config, 'image'):
            if mask_key == 'sofia':
                image_path = "{0:s}/image_0".format(
                    pipeline.continuum, self_cal_iter_counter)
                if not os.path.exists(image_path):
                    os.mkdir(image_path)
                fake_image(target_iter, 0, get_dir_path(
                    image_path, pipeline), mslist, field)
                sofia_mask(target_iter, 0, get_dir_path(
                    image_path, pipeline), field)
                recipe.run()
                recipe.jobs = []
                config['image']['cleanmask_method'].insert(1, config['image']['cleanmask_method'][self_cal_iter_counter if len(config['image']['cleanmask_method']) > self_cal_iter_counter else -1])
                image_path = "{0:s}/image_{1:d}".format(
                    pipeline.continuum, self_cal_iter_counter)
                image(target_iter, self_cal_iter_counter, get_dir_path(
                    image_path, pipeline), mslist, field)
            elif mask_key == 'breizorro':
                if self_cal_iter_counter == 1:
                    image_path = "{0:s}/image_{1:d}".format(
                        pipeline.continuum, 0)
                    if not os.path.exists(image_path):
                        os.mkdir(image_path)
                    fake_image(target_iter, 0, get_dir_path(
                        image_path, pipeline), mslist, field)
                    breizorro_mask(target_iter, 0, get_dir_path(
                        image_path, pipeline), field)
                    recipe.run()
                    recipe.jobs = []
                    image(target_iter, self_cal_iter_counter, get_dir_path(
                        image_path, pipeline), mslist, field)
                else:
                    image_path = "{0:s}/image_{1:d}".format(
                        pipeline.continuum, self_cal_iter_counter)
                    image(target_iter, self_cal_iter_counter, get_dir_path(
                        image_path, pipeline), mslist, field)
            else:
                image(target_iter, self_cal_iter_counter, get_dir_path(
                    image_path, pipeline), mslist, field)
        if pipeline.enable_task(config, 'extract_sources'):
            extract_sources(target_iter, self_cal_iter_counter, get_dir_path(
                image_path, pipeline), field)
        if pipeline.enable_task(config, 'aimfast'):
            image_quality_assessment(
                self_cal_iter_counter, get_dir_path(image_path, pipeline), field)

        while quality_check(self_cal_iter_counter, field, enable=pipeline.enable_task(config, 'aimfast')):
            if pipeline.enable_task(config, 'calibrate'):
                if not os.path.exists(selfcal_products):
                    os.mkdir(selfcal_products)
                calibrate(target_iter, self_cal_iter_counter, selfcal_products,
                          get_dir_path(image_path, pipeline), mslist, field)
            mask_key = config['image']['cleanmask_method'][self_cal_iter_counter if len(config['image']['cleanmask_method']) > self_cal_iter_counter else -1]
            if mask_key == 'sofia' and self_cal_iter_counter != cal_niter + 1 and pipeline.enable_task(config, 'image'):
                sofia_mask(target_iter, self_cal_iter_counter, get_dir_path(
                    image_path, pipeline), field)
                recipe.run()
                recipe.jobs = []
            elif mask_key == 'breizorro' and self_cal_iter_counter != cal_niter + 1 and pipeline.enable_task(config, 'image'):
                breizorro_mask(target_iter, self_cal_iter_counter,
                               get_dir_path(image_path, pipeline), field)
                recipe.run()
                recipe.jobs = []
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
                "{0:s}/{1:s}*.png".format(selfcal_products, pipeline.prefix))
            for plot in selfcal_plots:
                shutil.copyfile(plot, '{0:s}/{1:s}'.format(plot_path, os.path.basename(plot)))

        if pipeline.enable_task(config, 'transfer_apply_gains'):
            mslist_out = ms_dict_tgain[target]
            if (self_cal_iter_counter > cal_niter):
                restore(
                    self_cal_iter_counter - 1, selfcal_products, mslist_out, enable_inter=True)
            else:
                restore(
                    self_cal_iter_counter, selfcal_products, mslist_out, enable_inter=True)

        if pipeline.enable_task(config, 'aimfast'):
            if config['aimfast']['plot']:
                aimfast_plotting(field)
                recipe.run()
                # Empty job que after execution
                recipe.jobs = []

            if config['aimfast']['online_catalog']:
                aimfast_compare_online_catalog(field)
                recipe.run()
                # Empty job que after execution
                recipe.jobs = []

            # Move the aimfast html plots
            plot_path = "{0:s}/{1:s}".format(
                pipeline.diagnostic_plots, 'selfcal')
            if not os.path.exists(plot_path):
                os.mkdir(plot_path)
            aimfast_plots = glob.glob(
                "{0:s}/{1:s}".format(pipeline.output, '*.html'))
            for plot in aimfast_plots:
                shutil.copyfile(plot, '{0:s}/{1:s}'.format(plot_path, os.path.basename(plot)))
                os.remove(plot)

        if pipeline.enable_task(config, 'calibrate'):
            if config['cal_cubical']['ragavi_plot']['enable']:
                ragavi_plotting_cubical_tables()

        if pipeline.enable_task(config, 'restore_model'):
            if config['restore_model']['model']:
                num = config['restore_model']['model']
                if isinstance(num, str) and len(num.split('+')) == 2:
                    mm = num.split('+')
                    if int(mm[-1]) > self_cal_iter_counter:
                        num = str(self_cal_iter_counter)
            else:
                extract_sources = len(config['extract_sources']['thr_isl'])
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

                step = 'create-final_lsm-{0:s}-{1:s}'.format(*mm)
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
                step = 'create-final_lsm-{0:s}'.format(num)
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

            if config['restore_model']['clean_model']:
                num = int(config['restore_model']['clean_model'])
                if num > self_cal_iter_counter:
                    num = self_cal_iter_counter

                conv_model = '{0:s}/image_{1:d}/{2:s}_{3:s}-convolved_model.fits\
:output'.format(get_dir_path(pipeline.continuum, pipeline), num, prefix, field)
                recipe.add('cab/fitstool', 'subtract-model',
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
                recipe.add('cab/fitstool', 'add-cc',
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

                recipe.add('cab/tigger_restore', 'tigger-restore',
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
                step = 'flagging_summary-selfcal-ms{0:d}'.format(i)
                recipe.add('cab/flagstats', step,
                           {
                               "msname": msname,
                               "plot": True,
                               "outfile": ('{0:s}-{1:s}-'
                                           'selfcal-summary-{2:d}.json').format(
                                   prefix, wname, i),
                               "htmlfile": ('{0:s}-{1:s}-'
                                            'selfcal-summary-plots-{2:d}.html').format(
                                   prefix, wname, i)
                           },
                           input=pipeline.input,
                           output=pipeline.diagnostic_plots,
                           label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'transfer_model'):
            image_path = "{0:s}/image_{1:d}".format(pipeline.continuum,
                                                    self_cal_iter_counter)
            crystalball_model = config['transfer_model']['model']
            mslist_out = ms_dict_tmodel[target]
            if crystalball_model == 'auto':
                crystalball_model = '{0:s}/{1:s}_{2:s}_{3:d}-sources.txt'.format(get_dir_path(image_path,
                                                                                              pipeline), prefix, field, self_cal_iter_counter)
            for i, msname in enumerate(mslist_out):
                step = 'transfer_model-field{0:d}-ms{1:d}'.format(target_iter, i)
                recipe.add('cab/crystalball', step,
                           {
                               "ms": msname,
                               "sky-model": crystalball_model + ':output',
                               "row-chunks": config['transfer_model']['row_chunks'],
                               "model-chunks": config['transfer_model']['model_chunks'],
                               "within": sdm.dismissable(config['transfer_model']['within'] or None),
                               "points-only": config['transfer_model']['points_only'],
                               "num-sources": sdm.dismissable(config['transfer_model']['num_sources']),
                               "num-workers": sdm.dismissable(config['transfer_model']['num_workers']),
                               "memory-fraction": config['transfer_model']['mem_frac'],
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Transfer model {2:s} to ms={1:s}'.format(step, msname, crystalball_model))

        target_iter += 1

    i = 0
    # Write and manage flag versions only if flagging tasks are being
    # executed on these .MS files
    if flag_main_ms:
        for i, m in enumerate(all_msfile):
            substep = 'save-{0:s}-ms{1:d}'.format(flags_after_worker, i)
            manflags.add_cflags(pipeline, recipe, flags_after_worker, m,
                                cab_name=substep, overwrite=config['overwrite_flagvers'])

    i += 1
    if pipeline.enable_task(config, 'transfer_apply_gains'):
        for j, m in enumerate(all_msfile_tgain):
            substep = 'save-{0:s}-ms{1:d}'.format(flags_after_worker, i + j)
            manflags.add_cflags(pipeline, recipe, flags_after_worker, m,
                                cab_name=substep, overwrite=config['overwrite_flagvers'])

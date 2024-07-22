# -*- coding: future_fstrings -*-
import sys
import os
import glob
import stimela.dismissable as sdm
import stimela.recipe
import shutil
import itertools
import json
import psutil
import re
import datetime
import numpy as np
import caracal
from caracal.dispatch_crew import utils, noisy
from caracal.workers.utils import manage_flagsets as manflags
from caracal import log
from caracal.workers.utils import remove_output_products
from caracal.workers.utils import image_contsub
from caracal.workers.utils import flag_Uzeros
from casacore.tables import table
from caracal.utils.requires import extras

NAME = 'Process and Image Line Data'
LABEL = 'line'


def get_relative_path(path, pipeline):
    """Returns e.g. cubes/<dir> given output/cubes/<dir>"""
    return os.path.relpath(path, pipeline.output)


def add_ms_label(msname, label="mst"):
    """Adds _label to end of MS name, before the extension"""
    msbase, ext = os.path.splitext(msname)
    return f"{msbase}_{label}{ext}"


@extras("astropy")
def freq_to_vel(filename, reverse):
    from astropy.io import fits
    C = 2.99792458e+8       # m/s
    HI = 1.4204057517667e+9  # Hz
    if not os.path.exists(filename):
        caracal.log.warn(
            'Skipping conversion for {0:s}. File does not exist.'.format(filename))
    else:
        with fits.open(filename, mode='update') as cube:
            headcube = cube[0].header
            if 'restfreq' in headcube:
                restfreq = float(headcube['restfreq'])
            else:
                restfreq = HI
                # add rest frequency to FITS header
                headcube['restfreq'] = restfreq

            # convert from frequency to radio velocity
            if (headcube['naxis'] > 2) and ('FREQ' in headcube['ctype3']) and not reverse:
                headcube['cdelt3'] = -C * float(headcube['cdelt3']) / restfreq
                headcube['crval3'] = C * \
                    (1 - float(headcube['crval3']) / restfreq)
                # FITS standard for radio velocity as per
                # https://fits.gsfc.nasa.gov/standard40/fits_standard40aa-le.pdf

                headcube['ctype3'] = 'VRAD'
                headcube['cunit3'] = 'm/s'
                # 3 lines below commented out because of issue 1209
                #if 'cunit3' in headcube:
                #    # delete cunit3 because we adopt the default units = m/s
                #    del headcube['cunit3']

            # convert from radio velocity to frequency
            elif (headcube['naxis'] > 2) and ('VRAD' in headcube['ctype3']) and (headcube['naxis'] > 2) and reverse:
                headcube['cdelt3'] = -restfreq * float(headcube['cdelt3']) / C
                headcube['crval3'] = restfreq * \
                    (1 - float(headcube['crval3']) / C)
                headcube['ctype3'] = 'FREQ'
                headcube['cunit3'] = 'Hz'
                # 3 lines below commented out because of issue 1209
                #if 'cunit3' in headcube:
                #    # delete cunit3 because we adopt the default units = Hz
                #    del headcube['cunit3']
            else:
                if not reverse:
                    caracal.log.warn(
                        'Skipping conversion for {0:s}. Input is not a cube or not in frequency.'.format(filename))
                else:
                    caracal.log.warn(
                        'Skipping conversion for {0:s}. Input is not a cube or not in velocity.'.format(filename))


@extras("astropy")
def remove_stokes_axis(filename):
    from astropy.io import fits
    if not os.path.exists(filename):
        caracal.log.warn(
            'Skipping Stokes axis removal for {0:s}. File does not exist.'.format(filename))
    else:
        with fits.open(filename, mode='update') as cube:
            headcube = cube[0].header
            if (headcube['naxis'] == 4) and (headcube['ctype4'] == 'STOKES'):
                caracal.log.info('Working on {}'.format(filename))
                cube[0].data = cube[0].data[0]
                del headcube['cdelt4']
                del headcube['crpix4']
                del headcube['crval4']
                del headcube['ctype4']
                if 'cunit4' in headcube:
                    del headcube['cunit4']
            else:
                caracal.log.warn(
                    'Skipping Stokes axis removal for {0:s}. Input cube has less than 4 axis or the 4th axis type is not "STOKES".'.format(filename))


@extras("astropy")
def fix_specsys_ra(filename, specframe):
    from astropy.io import fits
    # Reference frame codes below from from http://www.eso.org/~jagonzal/telcal/Juan-Ramon/SDMTables.pdf, Sec. 2.50 and
    # FITS header notation from
    # https://fits.gsfc.nasa.gov/standard40/fits_standard40aa-le.pdf
    specsys3 = {
        0: 'LSRD',
        1: 'LSRK',
        2: 'GALACTOC',
        3: 'BARYCENT',
        4: 'GEOCENTR',
        5: 'TOPOCENT'}[
        np.unique(
            np.array(specframe))[0]]
    if not os.path.exists(filename):
        caracal.log.warn(
            'Skipping SPECSYS fix for {0:s}. File does not exist.'.format(filename))
    else:
        with fits.open(filename, mode='update') as cube:
            headcube = cube[0].header
            if 'specsys' in headcube:
                del headcube['specsys']
            headcube['specsys3'] = specsys3
            if headcube['CRVAL1'] < 0:
                headcube['CRVAL1'] += 360.


@extras("astropy")
def make_pb_cube(filename, apply_corr, typ, dish_size, cutoff):
    from astropy.io import fits
    C = 2.99792458e+8       # m/s
    HI = 1.4204057517667e+9  # Hz

    if not os.path.exists(filename):
        caracal.log.warn(
            'Skipping primary beam cube for {0:s}. File does not exist.'.format(filename))
    else:
        with fits.open(filename) as cube:
            headcube = cube[0].header
            datacube = np.indices(
                (headcube['naxis2'], headcube['naxis1']), dtype=np.float32)
            datacube[0] -= (headcube['crpix2'] - 1)
            datacube[1] -= (headcube['crpix1'] - 1)
            datacube = np.sqrt((datacube**2).sum(axis=0))
            datacube.resize((1, datacube.shape[0], datacube.shape[1]))

            datacube = np.repeat(datacube,
                                 headcube['naxis3'],
                                 axis=0) * np.abs(headcube['cdelt1'])

            cdelt3 = float(headcube['cdelt3'])
            crval3 = float(headcube['crval3'])

            # Convert radio velocity to frequency if required
            if 'VRAD' in headcube['ctype3']:
                if 'restfreq' in headcube:
                    restfreq = float(headcube['restfreq'])
                else:
                    restfreq = HI
                cdelt3 = - restfreq * cdelt3 / C
                crval3 = restfreq * (1 - crval3 / C)

            freq = (crval3 + cdelt3 * (np.arange(headcube['naxis3'], dtype=np.float32) -
                                       headcube['crpix3'] + 1))

            if typ == 'gauss':
                sigma_pb = 17.52 / (freq / 1e+9) / dish_size / 2.355
                sigma_pb.resize((sigma_pb.shape[0], 1, 1))
                datacube = np.exp(-datacube**2 / 2 / sigma_pb**2)
            elif typ == 'mauch':
                FWHM_pb = (57.5 / 60) * (freq / 1.5e9)**-1
                FWHM_pb.resize((FWHM_pb.shape[0], 1, 1))
                datacube = (np.cos(1.189 * np.pi * (datacube / FWHM_pb)) / (
                            1 - 4 * (1.189 * datacube / FWHM_pb)**2))**2

            datacube[datacube < cutoff] = np.nan
            if headcube['naxis'] == 4:
                datacube = np.expand_dims(datacube, 0)
            fits.writeto(filename.replace('image.fits', 'pb.fits'),
                         datacube, header=headcube, overwrite=True)
            if apply_corr:
                fits.writeto(filename.replace('image.fits', 'pb_corr.fits'),
                             cube[0].data / datacube, header=headcube, overwrite=True)  # Applying the primary beam correction
            caracal.log.info('Created primary beam cube FITS {0:s}'.format(
                filename.replace('image.fits', 'pb.fits')))


@extras("astropy")
def calc_rms(filename, linemaskname):
    from astropy.io import fits
    if linemaskname is None:
        if not os.path.exists(filename):
            caracal.log.info(
                'Noise not determined in cube for {0:s}. File does not exist.'.format(filename))
        else:
            with fits.open(filename) as cube:
                datacube = cube[0].data
                y = datacube[~np.isnan(datacube)]
            return np.sqrt(np.sum(y * y, dtype=np.float64) / y.size)
    else:
        with fits.open(filename) as cube:
            datacube = cube[0].data
        with fits.open(linemaskname) as mask:
            datamask = mask[0].data
            # select channels
            selchans = datamask.sum(axis=(2, 3)) > 0
            newcube = datacube[selchans]
            newmask = datamask[selchans]
            y2 = newcube[newmask == 0]
        return np.sqrt(np.nansum(y2 * y2, dtype=np.float64) / y2.size)


@extras(packages="astropy")
def worker(pipeline, recipe, config):
    import astropy
    from astropy.io import fits
    # Modules useful to calculate common barycentric frequency grid
    from astropy.time import Time
    from astropy.coordinates import SkyCoord
    from astropy.coordinates import EarthLocation
    from astropy import constants
    import astropy.units as units

    wname = pipeline.CURRENT_WORKER
    flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
    flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
    flag_main_ms = (pipeline.enable_task(config, 'flag_u_zeros') or pipeline.enable_task(config, 'sunblocker')) and not config['sunblocker']['use_mstransform']
    flag_mst_ms = (pipeline.enable_task(config, 'sunblocker') and config['sunblocker']['use_mstransform']) or (pipeline.enable_task(config, 'flag_u_zeros') and config['flag_u_zeros']['use_mstransform']) or pipeline.enable_task(config, 'flag_mst_errors')
    rewind_main_ms = config['rewind_flags']["enable"] and (config['rewind_flags']['mode'] == 'reset_worker' or config['rewind_flags']["version"] != 'null')
    rewind_mst_ms = config['rewind_flags']["enable"] and (config['rewind_flags']['mode'] == 'reset_worker' or config['rewind_flags']["mstransform_version"] != 'null')
    label = config['label_in']
    line_name = config['line_name']
    if label != '':
        flabel = label
    else:
        flabel = label
    all_targets, all_msfiles, ms_dict = pipeline.get_target_mss(flabel)
    RA, Dec = [], []
    firstchanfreq_all, chanw_all, lastchanfreq_all = [], [], []
    restfreq = config['restfreq']

    # distributed deconvolution settings
    ncpu = config['ncpu']
    if ncpu == 0:
        ncpu = psutil.cpu_count()
    else:
        ncpu = min(ncpu, psutil.cpu_count())
    nrdeconvsubimg = ncpu if config['make_cube']['wscl_nrdeconvsubimg'] == 0 else config['make_cube']['wscl_nrdeconvsubimg']
    if nrdeconvsubimg == 1:
        wscl_parallel_deconv = None
    else:
        wscl_parallel_deconv = int(np.ceil(max(config['make_cube']['npix']) / np.sqrt(nrdeconvsubimg)))

    for i, msfile in enumerate(all_msfiles):
        # Update pipeline attributes (useful if, e.g., channel averaging was
        # performed by the split_data worker)
        msinfo = pipeline.get_msinfo(msfile)
        spw = msinfo['SPW']['NUM_CHAN']
        caracal.log.info('MS #{0:d}: {1:s}'.format(i, msfile))
        caracal.log.info('  {0:d} spectral windows, with NCHAN={1:s}'.format(len(spw), ','.join(map(str, spw))))

        # Get first chan, last chan, chan width
        chfr = msinfo['SPW']['CHAN_FREQ']
        # To be done: add user selected  spw
        firstchanfreq = [ss[0] for ss in chfr]
        lastchanfreq = [ss[-1] for ss in chfr]
        chanwidth = [(ss[-1] - ss[0]) / (len(ss) - 1) for ss in chfr]
        firstchanfreq_all.append(firstchanfreq), chanw_all.append(
            chanwidth), lastchanfreq_all.append(lastchanfreq)
        caracal.log.info('  CHAN_FREQ from {0:s} Hz to {1:s} Hz with average channel width of {2:s} Hz'.format(
            ','.join(map(str, firstchanfreq)), ','.join(map(str, lastchanfreq)), ','.join(map(str, chanwidth))))

        tinfo = msinfo['FIELD']
        targetpos = tinfo['REFERENCE_DIR']
        while len(targetpos) == 1:
            targetpos = targetpos[0]
        tRA = targetpos[0] / np.pi * 180.
        tDec = targetpos[1] / np.pi * 180.
        RA.append(tRA)
        Dec.append(tDec)
        caracal.log.info('  Target RA, Dec for Doppler correction: {0:.3f} deg, {1:.3f} deg'.format(RA[i], Dec[i]))

    # Find common barycentric frequency grid for all input .MS, or set it as
    # requested in the config file
    if pipeline.enable_task(config, 'mstransform') and pipeline.enable_task(config['mstransform'],
                                                                            'doppler') and config['mstransform']['doppler']['changrid'] == 'auto':
        firstchanfreq = list(itertools.chain.from_iterable(firstchanfreq_all))
        chanw = list(itertools.chain.from_iterable(chanw_all))
        lastchanfreq = list(itertools.chain.from_iterable(lastchanfreq_all))
        teldict = {
            'meerkat': [21.4430, -30.7130],
            'gmrt': [73.9723, 19.1174],
            'vla': [-107.6183633, 34.0783584],
            'wsrt': [52.908829698, 6.601997592],
            'atca': [-30.307665436, 149.550164466],
            'askap': [116.5333, -16.9833],
        }
        tellocation = teldict[config['mstransform']['doppler']['telescope']]
        telloc = EarthLocation.from_geodetic(tellocation[0], tellocation[1])
        firstchanfreq_dopp, chanw_dopp, lastchanfreq_dopp = firstchanfreq, chanw, lastchanfreq
        corr_order = False
        if len(chanw) > 1:
            if np.max(chanw) > 0 and np.min(chanw) < 0:
                corr_order = True

        for i, msfile in enumerate(all_msfiles):
            msinfo = '{0:s}/{1:s}-obsinfo.txt'.format(pipeline.msdir, os.path.splitext(msfile)[0])
            with open(msinfo, 'r') as searchfile:
                for longdatexp in searchfile:
                    if "Observed from" in longdatexp:
                        dates = longdatexp
                        matches = re.findall(
                            r'(\d{2}[- ](\d{2}|January|Jan|February|Feb|March|Mar|April|Apr|May|May|June|Jun|July|Jul|August|Aug|September|Sep|October|Oct|November|Nov|December|Dec)[\- ]\d{2,4})',
                            dates)
                        obsstartdate = str(matches[0][0])
                        obsdate = datetime.datetime.strptime(
                            obsstartdate, '%d-%b-%Y').strftime('%Y-%m-%d')
                        targetpos = SkyCoord(
                            RA[i], Dec[i], frame='icrs', unit='deg')
                        v = targetpos.radial_velocity_correction(
                            kind='barycentric', obstime=Time(obsdate), location=telloc).to('km/s')
                        corr = np.sqrt((constants.c - v) / (constants.c + v))
                        if corr_order:
                            if chanw_dopp[0] > 0.:
                                firstchanfreq_dopp[i], chanw_dopp[i], lastchanfreq_dopp[i] = lastchanfreq_dopp[i] * \
                                    corr, chanw_dopp[i] * corr, firstchanfreq_dopp[i] * corr
                            else:
                                firstchanfreq_dopp[i], chanw_dopp[i], lastchanfreq_dopp[i] = firstchanfreq_dopp[i] * \
                                    corr, chanw_dopp[i] * corr, lastchanfreq_dopp[i] * corr
                        else:
                            firstchanfreq_dopp[i], chanw_dopp[i], lastchanfreq_dopp[i] = firstchanfreq_dopp[i] * \
                                corr, chanw_dopp[i] * corr, lastchanfreq_dopp[i] * corr  # Hz, Hz, Hz

        # WARNING: the following line assumes a single SPW for the line data
        # being processed by this worker!
        if np.min(chanw_dopp) < 0:
            comfreq0, comfreql, comchanw = np.min(firstchanfreq_dopp), np.max(
                lastchanfreq_dopp), -1 * np.max(np.abs(chanw_dopp))
            # safety measure to avoid wrong Doppler settings due to change of
            # Doppler correction during a day
            comfreq0 += comchanw
            # safety measure to avoid wrong Doppler settings due to change of
            # Doppler correction during a day
            comfreql -= comchanw
        else:
            comfreq0, comfreql, comchanw = np.max(firstchanfreq_dopp), np.min(
                lastchanfreq_dopp), np.max(chanw_dopp)
            # safety measure to avoid wrong Doppler settings due to change of
            # Doppler correction during a day
            comfreq0 += comchanw
            # safety measure to avoid wrong Doppler settings due to change of
            # Doppler correction during a day
            comfreql -= comchanw
        nchan_dopp = int(np.floor(((comfreql - comfreq0) / comchanw))) + 1
        comfreq0 = '{0:.3f}Hz'.format(comfreq0)
        comchanw = '{0:.3f}Hz'.format(comchanw)
        caracal.log.info(
            'Calculated common Doppler-corrected channel grid for all input .MS: {0:d} channels starting at {1:s} and with channel width {2:s}.'.format(
                nchan_dopp, comfreq0, comchanw))
        if pipeline.enable_task(config, 'make_cube') and config['make_cube']['image_with'] == 'wsclean' and corr_order:
            caracal.log.error('wsclean requires a consistent ordering of the frequency axis across multiple MSs')
            caracal.log.error('(all increasing or all decreasing). Use casa_image if this is not the case.')
            raise caracal.BadDataError("inconsistent frequency axis ordering across MSs")

    elif pipeline.enable_task(config, 'mstransform') and pipeline.enable_task(config['mstransform'], 'doppler') and config['mstransform']['doppler']['changrid'] != 'auto':
        if len(config['mstransform']['doppler']['changrid'].split(',')) != 3:
            caracal.log.error(
                'Incorrect format for mstransform:doppler:changrid in the .yml config file.')
            caracal.log.error(
                'Current setting is mstransform:doppler:changrid:"{0:s}"'.format(
                    config['mstransform']['doppler']['changrid']))
            caracal.log.error(
                'Expected "nchan,chan0,chanw" (note the commas) where nchan is an integer, and chan0 and chanw must include units appropriate for the chosen mstransform:mode')
            raise caracal.ConfigurationError("can't parse mstransform:doppler:changrid setting")
        nchan_dopp, comfreq0, comchanw = config['mstransform']['doppler']['changrid'].split(
            ',')
        nchan_dopp = int(nchan_dopp)
        caracal.log.info(
            'Set requested Doppler-corrected channel grid for all input .MS: {0:d} channels starting at {1:s} and with channel width {2:s}.'.format(
                nchan_dopp, comfreq0, comchanw))

    elif pipeline.enable_task(config, 'mstransform'):
        nchan_dopp, comfreq0, comchanw = None, None, None

    for i, msname in enumerate(all_msfiles):

        # Write/rewind flag versions only if flagging tasks are being
        # executed on these .MS files, or if the user asks to rewind flags
        if flag_main_ms or rewind_main_ms:
            available_flagversions = manflags.get_flags(pipeline, msname)
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
                        manflags.conflict('rewind_too_little', pipeline, wname, msname, config, flags_before_worker, flags_after_worker)
                    substep = 'version-{0:s}-ms{1:d}'.format(version, i)
                    manflags.restore_cflags(pipeline, recipe, version, msname, cab_name=substep)
                    if version != available_flagversions[-1]:
                        substep = 'delete-flag_versions-after-{0:s}-ms{1:d}'.format(version, i)
                        manflags.delete_cflags(pipeline, recipe,
                                               available_flagversions[available_flagversions.index(version) + 1],
                                               msname, cab_name=substep)
                    if version != flags_before_worker and flag_main_ms:
                        substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                        manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                            msname, cab_name=substep, overwrite=config['overwrite_flagvers'])
                elif stop_if_missing:
                    manflags.conflict('rewind_to_non_existing', pipeline, wname, msname, config, flags_before_worker, flags_after_worker)
                elif flag_main_ms:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                        msname, cab_name=substep, overwrite=config['overwrite_flagvers'])
            else:
                if flags_before_worker in available_flagversions and not config['overwrite_flagvers']:
                    manflags.conflict('would_overwrite_bw', pipeline, wname, msname, config, flags_before_worker, flags_after_worker)
                else:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                        msname, cab_name=substep, overwrite=config['overwrite_flagvers'])

        if pipeline.enable_task(config, 'subtractmodelcol'):

            # Check if a model subtraction has already been done
            t = table('{0:s}/{1:s}'.format(pipeline.msdir, msname), readonly=False)
            caracal.log.info(f"Check the MS name: {msname}")
            try:
                nModelSub = t.getcolkeyword('CORRECTED_DATA', 'modelSub')
                caracal.log.info(f"NmodelSub here = {nModelSub}")
            except RuntimeError:
                nModelSub = 0

            if (nModelSub <= -1) & (config['subtractmodelcol']['force'] == False):
                caracal.log.error(f'The model has been subtracted {np.abs(nModelSub)} times.')
                caracal.log.error('Exiting CARACal.')
                raise caracal.PlayingWithFire("I am very confident you shouldn't be doing this. If you know better, use the 'force' option.")

            if (nModelSub <= -1) & (config['subtractmodelcol']['force']):
                caracal.log.warn(f'The model has been subtracted {np.abs(nModelSub)} times.')
                caracal.log.warn('You have chosen to force another model subtraction.')
                caracal.log.warn('God speed!')

            step = 'modelsub-ms{:d}'.format(i)
            recipe.add('cab/msutils', step,
                       {
                           "command": 'sumcols',
                           "msname": msname,
                           "subtract": True,
                           "col1": 'CORRECTED_DATA',
                           "col2": 'MODEL_DATA',
                           "column": 'CORRECTED_DATA'
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Subtract model column'.format(step))

            t.putcolkeyword('CORRECTED_DATA', 'modelSub', nModelSub - 1)
            t.close()

        if pipeline.enable_task(config, 'addmodelcol'):

            # Check if a model addition has already been done
            t = table('{0:s}/{1:s}'.format(pipeline.msdir, msname), readonly=False)
            caracal.log.info(f"Check the MS name: {msname}")
            try:
                nModelSub = t.getcolkeyword('CORRECTED_DATA', 'modelSub')
            except RuntimeError:
                nModelSub = 0

            if (nModelSub >= 0) & (config['addmodelcol']['force'] == False):
                caracal.log.error(f'The model has been added {np.abs(nModelSub)} times.')
                caracal.log.error('Exiting CARACal.')
                raise caracal.PlayingWithFire("I am very confident you shouldn't be doing this. If you know better, use the 'force' option.")

            if (nModelSub >= 0) & (config['addmodelcol']['force']):
                caracal.log.warn(f'The model has been added  {np.abs(nModelSub)} times.')
                caracal.log.warn('You have chosen to force another model addition.')
                caracal.log.warn('God speed!')

            step = 'modeladd-ms{:d}'.format(i)
            recipe.add('cab/msutils', step,
                       {
                           "command": 'sumcols',
                           "msname": msname,
                           "col1": 'CORRECTED_DATA',
                           "col2": 'MODEL_DATA',
                           "column": 'CORRECTED_DATA'
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Add model column'.format(step))

            t.putcolkeyword('CORRECTED_DATA', 'modelSub', nModelSub + 1)
            t.close()

        msname_mst = add_ms_label(msname, "mst")
        msname_mst_base = os.path.splitext(msname_mst)[0]
        flagv = msname_mst + ".flagversions"
        summary_file = f'{msname_mst_base}-summary.json'
        obsinfo_file = f'{msname_mst_base}-obsinfo.txt'

        if pipeline.enable_task(config, 'mstransform'):
            # Set UVLIN fit channel range
            if pipeline.enable_task(config['mstransform'], 'uvlin') and config['mstransform']['uvlin']['exclude_known_sources']:
                C = 2.99792458e+5       # km/s
                chanfreqs = np.arange(firstchanfreq_all[i][0], lastchanfreq_all[i][0] + chanw_all[i][0], chanw_all[i][0])
                chanids = np.arange(chanfreqs.shape[0])
                linechans = chanids < 0  # Array of False's used to build the fitspw settings
                line_id, line_ra, line_dec, line_vmin, line_vmax, line_flux = np.loadtxt('{0:s}/{1:s}'.format(pipeline.input, config['mstransform']['uvlin']['known_sources_cat']), dtype='str', unpack=True)
                line_ra = astropy.coordinates.Angle(line_ra, unit='hour').degree
                line_dec = astropy.coordinates.Angle(line_dec, unit='degree').degree
                line_flux = line_flux.astype(float)
                line_fmax = (units.Quantity(config['restfreq']) / ((line_vmin.astype(float) - config['mstransform']['uvlin']['known_sources_dv']) / C + 1)).to_value(units.hertz)
                line_fmin = (units.Quantity(config['restfreq']) / ((line_vmax.astype(float) + config['mstransform']['uvlin']['known_sources_dv']) / C + 1)).to_value(units.hertz)
                distance = 180 / np.pi * np.arccos(np.sin(Dec[i] / 180 * np.pi) * np.sin(line_dec / 180 * np.pi) +
                                                   np.cos(Dec[i] / 180 * np.pi) * np.cos(line_dec / 180 * np.pi) * np.cos((RA[i] - line_ra) / 180 * np.pi))
                # Select line sources:
                #     within the search radius;
                #     above the line flux threashold (not PB-corrected);
                #     and (at least partly) within the MS frequency range.
                line_selected = (distance < config['mstransform']['uvlin']['known_sources_radius']) *\
                                (line_flux >= config['mstransform']['uvlin']['known_sources_flux']) *\
                                ((line_fmin >= chanfreqs.min()) * (line_fmin <= chanfreqs.max()) +
                                 (line_fmax >= chanfreqs.min()) * (line_fmax <= chanfreqs.max()))
                line_id, line_fmin, line_fmax = line_id[line_selected], line_fmin[line_selected], line_fmax[line_selected]
                line_chanmin, line_chanmax = [], []
                caracal.log.info('Excluding the following line sources and channel intervals from the UVLIN fit:')
                for ll in range(line_id.shape[0]):
                    if line_fmin[ll] < chanfreqs[0]:
                        line_chanmin.append(chanids[0])
                    else:
                        line_chanmin.append(chanids[chanfreqs < line_fmin[ll]].max())
                    if line_fmax[ll] > chanfreqs[-1]:
                        line_chanmax.append(chanids[-1])
                    else:
                        line_chanmax.append(chanids[chanfreqs > line_fmax[ll]].min())
                    caracal.log.info('  {0:20s}:  {1:5d} - {2:5d}'.format(line_id[ll], line_chanmin[ll], line_chanmax[ll]))
                    linechans += (chanids >= line_chanmin[ll]) * (chanids <= line_chanmax[ll])
                autofitchans = ~linechans
                if config['mstransform']['uvlin']['fitspw']:
                    caracal.log.info('Combining the above channel intervals with the user input {0:s}'.format(config['mstransform']['uvlin']['fitspw']))
                    userfitchans = [qq.split(';') for qq in config['mstransform']['uvlin']['fitspw'].split(':')[1::2]]
                    while len(userfitchans) > 1:
                        userfitchans[0] = userfitchans[0] + userfitchans[1]
                        del (userfitchans[1])
                    userfitchans = [list(map(int, qq.split('~'))) for qq in userfitchans[0]]
                    userfitchans = np.array([(chanids >= qq[0]) * (chanids <= qq[1]) for qq in userfitchans]).sum(axis=0).astype('bool')
                    autofitchans *= userfitchans
                fitspw = '0~' if autofitchans[0] else ''
                for cc in chanids[1:]:
                    if not autofitchans[cc - 1] and autofitchans[cc] and (not fitspw or fitspw[-1] == ';'):
                        fitspw += '{0:d}~'.format(cc)
                    elif autofitchans[cc - 1] and not autofitchans[cc]:
                        fitspw += '{0:d};'.format(cc - 1)
                if not fitspw:
                    raise caracal.BadDataError('No channels available for UVLIN fit.')
                elif fitspw[-1] == '~':
                    fitspw += '{0:d}'.format(chanids[-1])
                elif fitspw[-1] == ';':
                    fitspw = fitspw[:-1]
                fitspw = '0:{0:s}'.format(fitspw)
                caracal.log.info('The UVLIN fit will be executed on the channels {0:s}'.format(fitspw))

            else:
                fitspw = config['mstransform']['uvlin']['fitspw']

            # If the output of this run of mstransform exists, delete it first
            remove_output_products((msname_mst, flagv, summary_file, obsinfo_file), directory=pipeline.msdir, log=log)

            col = config['mstransform']['col']
            step = 'mstransform-ms{:d}'.format(i)
            recipe.add('cab/casa_mstransform',
                       step,
                       {"msname": msname,
                        "outputvis": msname_mst,
                        "regridms": pipeline.enable_task(config['mstransform'], 'doppler'),
                        "mode": config['mstransform']['doppler']['mode'],
                        "nchan": sdm.dismissable(nchan_dopp),
                        "start": sdm.dismissable(comfreq0),
                        "width": sdm.dismissable(comchanw),
                        "interpolation": 'nearest',
                        "datacolumn": col,
                        "restfreq": restfreq,
                        "outframe": config['mstransform']['doppler']['frame'],
                        "veltype": config['mstransform']['doppler']['veltype'],
                        "douvcontsub": pipeline.enable_task(config['mstransform'], 'uvlin'),
                        "fitspw": sdm.dismissable(fitspw),
                        "fitorder": config['mstransform']['uvlin']['fitorder'],
                        },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Doppler tracking corrections'.format(step))

            substep = 'save-{0:s}-ms{1:d}'.format('caracal_legacy', i)
            manflags.add_cflags(pipeline, recipe, 'caracal_legacy', msname_mst,
                                cab_name=substep, overwrite=False)

            if config['mstransform']['obsinfo']:
                step = 'listobs-ms{:d}'.format(i)
                recipe.add('cab/casa_listobs',
                           step,
                           {"vis": msname_mst,
                            "listfile": '{0:s}-obsinfo.txt:msfile'.format(msname_mst_base),
                            "overwrite": True,
                            },
                           input=pipeline.input,
                           output=pipeline.obsinfo,
                           label='{0:s}:: Get observation information ms={1:s}'.format(step,
                                                                                       msname_mst))

                step = 'summary_json-ms{:d}'.format(i)
                recipe.add(
                    'cab/msutils',
                    step,
                    {
                        "msname": msname_mst,
                        "command": 'summary',
                        "display": False,
                        "outfile": '{0:s}-summary.json:msfile'.format(msname_mst_base),
                    },
                    input=pipeline.input,
                    output=pipeline.obsinfo,
                    label='{0:s}:: Get observation information as a json file ms={1:s}'.format(
                        step,
                        msname_mst))

        recipe.run()
        recipe.jobs = []

        if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, msname_mst)):
            mst_exist = True
        else:
            mst_exist = False

        # Write/rewind flag versions of the mst .MS files only if they have just
        # been created, their FLAG is being changed, or the user asks to rewind flags
        if mst_exist and (pipeline.enable_task(config, 'mstransform') or flag_mst_ms or rewind_mst_ms):
            available_flagversions = manflags.get_flags(pipeline, msname_mst)
            if rewind_mst_ms:
                if config['rewind_flags']['mode'] == 'reset_worker':
                    version = flags_before_worker
                    stop_if_missing = False
                elif config['rewind_flags']['mode'] == 'rewind_to_version':
                    version = config['rewind_flags']['mstransform_version']
                    if version == 'auto':
                        version = flags_before_worker
                    stop_if_missing = True
                if version in available_flagversions:
                    if flags_before_worker in available_flagversions and available_flagversions.index(flags_before_worker) < available_flagversions.index(version) and not config['overwrite_flagvers']:
                        manflags.conflict('rewind_too_little', pipeline, wname, msname_mst, config, flags_before_worker, flags_after_worker, read_version='mstransform_version')
                    substep = 'version_{0:s}_ms{1:d}'.format(version, i)
                    manflags.restore_cflags(pipeline, recipe, version, msname_mst, cab_name=substep)
                    if version != available_flagversions[-1]:
                        substep = 'delete-flag_versions-after-{0:s}-ms{1:d}'.format(version, i)
                        manflags.delete_cflags(pipeline, recipe,
                                               available_flagversions[available_flagversions.index(version) + 1],
                                               msname_mst, cab_name=substep)
                    if version != flags_before_worker and flag_mst_ms:
                        substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                        manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                            msname_mst, cab_name=substep, overwrite=config['overwrite_flagvers'])
                elif stop_if_missing:
                    manflags.conflict('rewind_to_non_existing', pipeline, wname, msname_mst, config, flags_before_worker, flags_after_worker, read_version='mstransform_version')
                elif flag_mst_ms:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                        msname_mst, cab_name=substep, overwrite=config['overwrite_flagvers'])
            else:
                if flags_before_worker in available_flagversions and not config['overwrite_flagvers']:
                    manflags.conflict('would_overwrite_bw', pipeline, wname, msname_mst, config, flags_before_worker, flags_after_worker, read_version='mstransform_version')
                else:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                        msname_mst, cab_name=substep, overwrite=config['overwrite_flagvers'])

        if pipeline.enable_task(config, 'flag_mst_errors'):
            step = 'flag_mst_errors-ms{0:d}'.format(i)
            recipe.add('cab/autoflagger',
                       step,
                       {"msname": msname_mst,
                        "column": 'DATA',
                        "strategy": config['flag_mst_errors']['strategy'],
                        "indirect-read": True if config['flag_mst_errors']['readmode'] == 'indirect' else False,
                        "memory-read": True if config['flag_mst_errors']['readmode'] == 'memory' else False,
                        "auto-read-mode": True if config['flag_mst_errors']['readmode'] == 'auto' else False,
                        },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: file ms={1:s}'.format(step, msname_mst))

        recipe.run()
        recipe.jobs = []

        if pipeline.enable_task(config, 'flag_u_zeros'):
            uZeros = flag_Uzeros.UzeroFlagger(config)

            if config['flag_u_zeros']['use_mstransform']:
                msname_Flag = msname_mst
            else:
                msname_Flag = msname

            uZeros.run_flagUzeros(pipeline, all_targets, msname_Flag)

        if pipeline.enable_task(config, 'sunblocker'):
            if config['sunblocker']['use_mstransform']:
                msnamesb = msname_mst
            else:
                msnamesb = msname
            step = 'sunblocker-ms{0:d}'.format(i)
            recipe.add("cab/sunblocker", step,
                       {
                           "command": "phazer",
                           "inset": msnamesb,
                           "outset": msnamesb,
                           "imsize": config['sunblocker']['imsize'],
                           "cell": config['sunblocker']['cell'],
                           "pol": 'i',
                           "threshmode": 'mad',
                           "threshold": config['sunblocker']['thr'],
                           "mode": 'all',
                           "radrange": 0,
                           "angle": 0,
                           "show": pipeline.prefix + '.sunblocker.svg',
                           "verb": True,
                           "dryrun": False,
                           "uvmax": config['sunblocker']['uvmax'],
                           "uvmin": config['sunblocker']['uvmin'],
                           "vampirisms": config['sunblocker']['vampirisms'],
                           "flagonlyday": config['sunblocker']['flagonlyday'],
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Block out sun'.format(step))

        if flag_main_ms:
            substep = 'save-{0:s}-ms{1:d}'.format(flags_after_worker, i)
            manflags.add_cflags(pipeline, recipe, flags_after_worker, msname,
                                cab_name=substep, overwrite=config['overwrite_flagvers'])

        if mst_exist and flag_mst_ms:
            substep = 'save-{0:s}-mst{1:d}'.format(flags_after_worker, i)
            manflags.add_cflags(pipeline, recipe, flags_after_worker, msname_mst,
                                cab_name=substep, overwrite=config['overwrite_flagvers'])

        recipe.run()
        recipe.jobs = []
        # Move the sunblocker plots to the diagnostic_plots
        if pipeline.enable_task(config, 'sunblocker'):
            sunblocker_plots = glob.glob(
                "{0:s}/*_{1:s}.sunblocker.svg".format(pipeline.output, pipeline.prefix))
            for plot in sunblocker_plots:
                shutil.copyfile(plot, '{0:s}/{1:s}'.format(pipeline.diagnostic_plots, os.path.basename(plot)))
                os.remove(plot)

    if pipeline.enable_task(config, 'predict_noise'):
        tsyseff = config['predict_noise']['tsyseff']
        diam = config['predict_noise']['diam']
        kB = 1380.6                                   # Boltzmann constant (Jy m^2 / K)
        Aant = np.pi * (diam / 2)**2                      # collecting area of 1 antenna (m^2)
        SEFD = 2 * kB * tsyseff / Aant                      # system equivalent flux density (Jy)
        caracal.log.info('Predicting natural noise of line cubes (Stokes I, single channel of MS file) for Tsys/eff = {0:.1f} K, diam = {1:.1f} m -> SEFD = {2:.1f} Jy'.format(tsyseff, diam, SEFD))
        for tt, target in enumerate(all_targets):
            if config['make_cube']['use_mstransform']:
                mslist = [add_ms_label(ms, "mst") for ms in ms_dict[target]]
            else:
                mslist = ms_dict[target]
            caracal.log.info('  Target #{0:d}: {1:}, files {2:}'.format(tt, target, mslist))
            noisy.PredictNoise(['{0:s}/{1:s}'.format(pipeline.msdir, mm) for mm in mslist], str(tsyseff), diam, target, verbose=2)

    if pipeline.enable_task(config, 'make_cube') and config['make_cube']['image_with'] == 'wsclean':
        nchans_all, specframe_all = [], []
        label = config['label_in']
        if label != '':
            flabel = label
        else:
            flabel = label

        caracal.log.info('Collecting spectral info on MS files being imaged')
        if config['make_cube']['use_mstransform']:
            for i, msfile in enumerate(all_msfiles):
                # Get channelisation of _mst.ms file
                msbase, ext = os.path.splitext(msfile)
                msinfo = pipeline.get_msinfo(f"{msbase}_mst{ext}")
                spw = msinfo['SPW']['NUM_CHAN']
                nchans = spw
                nchans_all.append(nchans)
                caracal.log.info('MS #{0:d}: {1:s}'.format(i, msfile.replace('.ms', '_mst.ms')))
                caracal.log.info('  {0:d} spectral windows, with NCHAN={1:s}'.format(
                    len(spw), ','.join(map(str, spw))))
                # Get first chan, last chan, chan width
                chfr = msinfo['SPW']['CHAN_FREQ']
                firstchanfreq = [ss[0] for ss in chfr]
                lastchanfreq = [ss[-1] for ss in chfr]
                chanwidth = [(ss[-1] - ss[0]) / (len(ss) - 1) for ss in chfr]
                caracal.log.info('  CHAN_FREQ from {0:s} Hz to {1:s} Hz with average channel width of {2:s} Hz'.format(
                    ','.join(map(str, firstchanfreq)), ','.join(map(str, lastchanfreq)), ','.join(map(str, chanwidth))))
                # Get spectral reference frame
                specframe = msinfo['SPW']['MEAS_FREQ_REF']
                specframe_all.append(specframe)
                caracal.log.info('  The spectral reference frame is {0:}'.format(specframe))

        else:
            for i, msfile in enumerate(all_msfiles):
                msinfo = pipeline.get_msinfo(msfile)
                spw = msinfo['SPW']['NUM_CHAN']
                nchans = spw
                nchans_all.append(nchans)
                caracal.log.info('MS #{0:d}: {1:s}'.format(i, msfile))
                caracal.log.info('  {0:d} spectral windows, with NCHAN={1:s}'.format(
                    len(spw), ','.join(map(str, spw))))
                specframe = msinfo['SPW']['MEAS_FREQ_REF']
                specframe_all.append(specframe)
                caracal.log.info(
                    '  The spectral reference frame is {0:}'.format(specframe))

        spwid = config['make_cube']['spwid']
        nchans = config['make_cube']['nchans']
        if nchans == 0:
            # Assuming user wants same spw for all msfiles and they have same
            # number of channels
            nchans = nchans_all[0][spwid]
        # Assuming user wants same spw for all msfiles and they have same
        # specframe
        specframe_all = [ss[spwid] for ss in specframe_all][0]
        firstchan = config['make_cube']['firstchan']
        binchans = config['make_cube']['binchans']
        channelrange = [firstchan, firstchan + nchans * binchans]
        npix = config['make_cube']['npix']
        if len(npix) == 1:
            npix = [npix[0], npix[0]]

        # Construct weight specification
        if config['make_cube']['weight'] == 'briggs':
            weight = 'briggs {0:.3f}'.format(
                config['make_cube']['robust'])
        else:
            weight = config['make_cube']['weight']
        wscl_niter = max(1,config['make_cube']['wscl_sofia_niter'])
        wscl_tol = config['make_cube']['wscl_sofia_converge']

        line_image_opts = {
            "weight": weight,
            "taper-gaussian": str(config['make_cube']['taper']),
            "pol": config['make_cube']['stokes'],
            "npix": npix,
            "padding": config['make_cube']['padding'],
            "scale": config['make_cube']['cell'],
            "channelsout": nchans,
            "channelrange": channelrange,
            "niter": config['make_cube']['niter'] if not config['make_cube']['wscl_onlypsf'] else 1,
            "gain": config['make_cube']['gain'],
            "mgain": config['make_cube']['wscl_mgain'],
            "auto-threshold": config['make_cube']['wscl_auto_thr'],
            "multiscale": config['make_cube']['wscl_multiscale'],
            "multiscale-scale-bias": config['make_cube']['wscl_multiscale_bias'],
            "parallel-deconvolution": sdm.dismissable(wscl_parallel_deconv),
            "no-update-model-required": config['make_cube']['wscl_noupdatemod'],
            "make-psf-only": config['make_cube']['wscl_onlypsf'],
        }
        if config['make_cube']['wscl_multiscale_scales']:
            line_image_opts.update({"multiscale-scales": list(map(int, config['make_cube']['wscl_multiscale_scales'].split(',')))})

        if config['make_cube']['wscl_beam'] != [0, 0, 0]:
            line_image_opts.update({"beamshape": config['make_cube']['wscl_beam']})

        for tt, target in enumerate(all_targets):
            caracal.log.info('Starting to make line cube for target {0:}'.format(target))
            if config['make_cube']['use_mstransform']:
                mslist = [add_ms_label(ms, "mst") for ms in ms_dict[target]]
            else:
                mslist = ms_dict[target]
            field = utils.filter_name(target)
            line_clean_mask_file = None
            rms_values = []
            if 'fitsmask' in line_image_opts:
                del (line_image_opts['fitsmask'])
            if 'auto-mask' in line_image_opts:
                del (line_image_opts['auto-mask'])
            for j in range(1, wscl_niter + 1):
                cube_path = "{0:s}/cube_{1:d}".format(
                    pipeline.cubes, j)
                if not os.path.exists(cube_path):
                    os.mkdir(cube_path)
                cube_dir = '{0:s}/cube_{1:d}'.format(
                    get_relative_path(pipeline.cubes, pipeline), j)

                line_image_opts.update({
                    "msname": mslist,
                    "prefix": '{0:s}/{1:s}_{2:s}_{3:s}_{4:d}'.format(
                        cube_dir, pipeline.prefix, field, line_name, j)
                })

                if j == 1:
                    own_line_clean_mask = config['make_cube']['wscl_user_clean_mask']
                    if own_line_clean_mask:
                        '''
                        MAKE HDR FILE FOR REGRIDDING THE USER SUPPLIED MASK
                        '''
                        doProj = False
                        doSpec = False
                        C = 2.99792458e+8  # m/s
                        femit = [r.strip() for r in re.split('([-+]?\d+\.\d+)|([-+]?\d+)', restfreq.strip()) if r is not None and r.strip() != '']
                        femit = (eval(femit[0]) * units.Unit(femit[1])).to(units.Hz).value  # Hz
                        t = mslist[0].replace('.ms', '-summary.json') # first file given to WSClean as input
                        with open('{}/{}'.format(pipeline.msdir, t)) as f:
                            obsDict = json.load(f)
                        raTarget = np.round(obsDict['FIELD']['REFERENCE_DIR'][0][0][0] / np.pi * 180, 5)
                        decTarget = np.round(obsDict['FIELD']['REFERENCE_DIR'][0][0][1] / np.pi * 180, 5)

                        cubeHeight = config['make_cube']['npix'][0]
                        cubeWidth = config['make_cube']['npix'][1] if len(config['make_cube']['npix']) == 2 else cubeHeight

                        preGridMask = own_line_clean_mask

                        caracal.log.info('+++++++++++++++++++++++++++++')
                        caracal.log.info('Checking Mask dimensions')
                        caracal.log.info('doProj = {}'.format(doProj))
                        caracal.log.info('RA = {}'.format(raTarget))
                        caracal.log.info('Dec = {}'.format(decTarget))
                        caracal.log.info('CubeHeight (px) = {}'.format(cubeHeight))
                        caracal.log.info('CubeWidht (px) = {}'.format(cubeWidth))

                        postGridMask = preGridMask.replace('.fits', '_{}_{}_regrid.fits'.format(pipeline.prefix, target))

                        with fits.open('{}/{}'.format(pipeline.masking, preGridMask)) as hdul:

                            if hdul[0].header["CRVAL1"] < 0:
                                hdul[0].header["CRVAL1"] += 360.
                            caracal.log.info('MaskRA = {}'.format(hdul[0].header["CRVAL1"]))
                            caracal.log.info('MaskDec = {}'.format(hdul[0].header["CRVAL2"]))

                            caracal.log.info('MaskWidth = {}'.format(hdul[0].header["NAXIS1"]))
                            caracal.log.info('MaskHeight = {}'.format(hdul[0].header["NAXIS2"]))

                            if hdul[0].header["NAXIS1"] != cubeWidth:
                                caracal.log.info('NAXIS1')
                                doProj = True
                            if hdul[0].header["NAXIS2"] != cubeHeight:
                                caracal.log.info('NAXIS2')
                                doProj = True
                            if np.round(hdul[0].header["CRVAL1"], 5) != np.round(raTarget, 5):
                                caracal.log.info('CRVAL1')
                                doProj = True
                            if np.round(hdul[0].header["CRVAL2"], 5) != np.round(decTarget, 5):
                                doProj = True
                                caracal.log.info('CRVAL2')

                            if int(hdul[0].header['NAXIS3']) > int(nchans):
                                doSpec = True
                            else:
                                dpSpec = None  # this should work in both a request for a subset, and if the cube is to be binned.

                            if 'FREQ' in hdul[0].header['CTYPE3']:
                                cdelt = round(hdul[0].header['CDELT3'], 2)
                            else:
                                cdelt = round(hdul[0].header['CDELT3'] * femit / (-C), 2)

                            if np.round(cdelt,1) > np.round(chanwidth[0],1):

                                doSpec = True
                            elif doProj:
                                pass
                            else:
                                doSpec = None  # likely will fail/produce incorrect result in the case that the ms file and mask were not created with the same original spectral grid.

                            if doProj:
                                ax3param = []
                                for key in ['NAXIS3', 'CTYPE3', 'CRPIX3', 'CRVAL3', 'CDELT3']:
                                    ax3param.append(hdul[0].header[key])

 
                        caracal.log.info('doSpecProj = {}'.format(doSpec))
                        caracal.log.info('+++++++++++++++++++++++++++++')

                        caracal.log.info('doSpaceProj = {}'.format(doProj))

                        caracal.log.info('+++++++++++++++++++++++++++++')

                        if doProj:
                            '''
                            MAKE HDR FILE FOR REGRIDDING THE USER SUPPLIED MASK AND REPROJECT
                            '''
                            with open('{}/tmp.hdr'.format(pipeline.masking), 'w') as file:
                                file.write('SIMPLE  =   T\n')
                                file.write('BITPIX  =   -64\n')
                                file.write('NAXIS   =   2\n')
                                file.write('NAXIS1  =   {}\n'.format(cubeWidth))
                                file.write('CTYPE1  =   \'RA---SIN\'\n')
                                file.write('CRVAL1  =   {}\n'.format(raTarget))
                                file.write('CRPIX1  =   {}\n'.format(cubeWidth / 2 + 1))
                                file.write('CDELT1  =   {}\n'.format(-1 * config['make_cube']['cell'] / 3600.))
                                file.write('NAXIS2  =   {}\n'.format(cubeHeight))
                                file.write('CTYPE2  =   \'DEC--SIN\'\n')
                                file.write('CRVAL2  =   {}\n'.format(decTarget))
                                file.write('CRPIX2  =   {}\n'.format(cubeHeight / 2 + 1))
                                file.write('CDELT2  =   {}\n'.format(config['make_cube']['cell'] / 3600.))
                                file.write('EXTEND  =   T\n')
                                file.write('EQUINOX =   2000.0\n')
                                file.write('END\n')

                            if os.path.exists('{}/{}'.format(pipeline.masking, postGridMask)):
                                os.remove('{}/{}'.format(pipeline.masking, postGridMask))

                            with fits.open('{}/{}'.format(pipeline.masking, preGridMask)) as hdul:
                                if np.amax(hdul[0].data) > 1:
                                    mask = np.where(hdul[0].data > 0)
                                    hdul[0].data[mask] = 1
                                    preGridMaskNew = preGridMask.replace('.fits', '_01.fits')
                                    hdul.writeto('{}/{}'.format(pipeline.masking, preGridMaskNew), overwrite=True)
                                    preGridMask = preGridMaskNew
                            caracal.log.info('Reprojecting mask {} to match the grid of the cube.'.format(preGridMask))

                            step = 'reprojectMask-field{}'.format(tt)
                            recipe.add('cab/mProjectCube', step,
                                       {
                                           "in.fits": preGridMask,
                                           "out.fits": postGridMask,
                                           "hdr.template": 'tmp.hdr',
                                       },
                                       input=pipeline.masking,
                                       output=pipeline.masking,
                                       label='{0:s}:: Reprojecting user input mask {1:s} to match the grid of the cube'.format(step, preGridMask))

                            # In order to make sure that we actually find stuff in the images we execute the rec ipe here
                            recipe.run()
                            # Empty job que after execution
                            recipe.jobs = []

                            if not os.path.exists('{}/{}'.format(pipeline.masking, postGridMask)):
                                raise IOError(
                                    "The regridded mask {0:s} does not exist. The original mask likely has no overlap with the cube.".format(postGridMask))

                            with fits.open('{}/{}'.format(pipeline.masking, postGridMask), mode='update') as hdul:
                                for i, key in enumerate(['NAXIS3', 'CTYPE3', 'CRPIX3', 'CRVAL3', 'CDELT3']):
                                    hdul[0].header[key] = ax3param[i]
                                axDict = {'1': [2, cubeWidth],
                                          '2': [1, cubeHeight]}
                                for i in ['1', '2']:
                                    cent, nax = hdul[0].header['CRPIX' + i], hdul[0].header['NAXIS' + i]
                                    if cent < axDict[i][1] / 2 + 1:
                                        delt = int(axDict[i][1] / 2 + 1 - cent)
                                        if i == '1':
                                            toAdd = np.zeros([hdul[0].header['NAXIS3'], hdul[0].data.shape[1], delt])
                                        else:
                                            toAdd = np.zeros([hdul[0].header['NAXIS3'], delt, hdul[0].data.shape[2]])
                                        hdul[0].data = np.concatenate([toAdd, hdul[0].data], axis=axDict[i][0])
                                        hdul[0].header['CRPIX' + i] = cent + delt
                                    if hdul[0].data.shape[axDict[i][0]] < axDict[i][1]:
                                        delt = int(axDict[i][1] - hdul[0].data.shape[axDict[i][0]])
                                        if i == '1':
                                            toAdd = np.zeros([hdul[0].header['NAXIS3'], hdul[0].data.shape[1], delt])
                                        else:
                                            toAdd = np.zeros([hdul[0].header['NAXIS3'], delt, hdul[0].data.shape[2]])
                                        hdul[0].data = np.concatenate([hdul[0].data, toAdd], axis=axDict[i][0])
                                    if hdul[0].data.shape[axDict[i][0]] > axDict[i][1]:
                                        delt = int(hdul[0].data.shape[axDict[i][0]] - axDict[i][1])
                                        hdul[0].data = hdul[0].data[:, :, -delt] if i == '1' else hdul[0].data[:, -delt, :]
                                        if cent > axDict[i][1] / 2 + 1:
                                            hdul[0].header['CRPIX' + i] = hdul[0].data.shape[axDict[i][0]] / 2 + 1

                                hdul[0].data = np.around(hdul[0].data.astype(np.float32)).astype(np.int16)
                                try:
                                    del hdul[0].header['EN']
                                except KeyError:
                                    pass
                                hdul.flush()

                            line_image_opts.update({"fitsmask": '{0:s}/{1:s}:output'.format(
                                get_relative_path(pipeline.masking, pipeline), postGridMask.split('/')[-1])})

                        else:
                            if not doSpec:
                                line_image_opts.update({"fitsmask": '{0:s}/{1:s}:output'.format(
                                    get_relative_path(pipeline.masking, pipeline), preGridMask.split('/')[-1])})
                            else:
                                pass

                        if doSpec:
                            gridMask = postGridMask if doProj else preGridMask
                            caracal.log.info('Reprojecting mask {} to match the spectral axis of the cube.'.format(gridMask))
                            if doProj:
                                hdul = fits.open('{}/{}'.format(pipeline.masking, postGridMask), mode='update')
                            else:
                                hdul = fits.open('{}/{}'.format(pipeline.masking, preGridMask))

                                if os.path.exists('{}/{}'.format(pipeline.masking, postGridMask)):
                                    os.remove('{}/{}'.format(pipeline.masking, postGridMask))

                            if 'FREQ' in hdul[0].header['CTYPE3']:
                                # all in Hz
                                crval = firstchanfreq[0] + chanwidth[0] * firstchan
                                cdeltm = hdul[0].header['CDELT3']
                                cdelte = crval + nchans * binchans * chanwidth[0]
                            else:
                                # all in m/s
                                crval = C * (femit - (firstchanfreq[0] + chanwidth[0] * firstchan)) / femit
                                cdeltm = hdul[0].header['CDELT3'] * femit / (-C)
                                crvale = C * (femit - (firstchanfreq[0] + chanwidth[0] * firstchan + nchans * binchans * chanwidth[0])) / femit

                            cdelt = chanwidth[0] * binchans  # in Hz
                            hdr = hdul[0].header
                            ax3 = np.arange(hdr['CRVAL3'] - hdr['CDELT3'] * (hdr['CRPIX3'] - 1), hdr['CRVAL3'] + hdr['CDELT3'] * (hdr['NAXIS3'] - hdr['CRPIX3'] + 1), hdr['CDELT3'])
                            if (np.max([crval, crvale]) <= np.max([ax3[0], ax3[-1]])) & (np.min([crval, crvale]) >= np.min([ax3[0], ax3[-1]])):
                                caracal.log.info("Requested channels are contained in mask {}.".format(gridMask))

                                idx = np.argmin(abs(ax3 - crval))
                                ide = np.argmin(abs(ax3 - crvale))

                                if cdelt > cdeltm:
                                    hdul[0].data = hdul[0].data[idx:ide]
                                    hdul[0].header['CRPIX3'] = 1
                                    hdul[0].header['CRVAL3'] = crval
                                    hdul[0].header['NAXIS3'] = nchans
                                    hdul[0].header['CDELT3'] = hdul[0].header['CDELT3'] * binchans
                                    if binchans > 1:
                                        if (nchans % binchans) > 0:
                                            rdata = (hdul[0].data[:-(nchans % binchans)]).reshape((nchans - 1 * (nchans % binchans), binchans, hdul[0].header['NAXIS1'], hdul[0].header['NAXIS2']))
                                            rdata = np.nansum(rdata, axis=1)
                                            rdata = np.concatenate((rdata, np.nansum(hdul[0].data[-(nchans % binchans):])), axis=0)
                                        else:
                                            rdata = (hdul[0].data).reshape(nchans, binchans, hdul[0].header['NAXIS1'], hdul[0].header['NAXIS2'])
                                            rdata = np.nansum(rdata, axis=1)
                                        rdata[rdata > 0] = 1
                                        hdul[0].data = rdata
                                    else:
                                        pass

                                else:

                                    rdata = np.zeros((nchans, hdr['NAXIS1'], hdr['NAXIS2']))
                                    rr = int(cdeltm / cdelt)
                                    for nn in range(nchans):
                                        rdata[nn] = hdul[0].data[idx + nn // rr]

                                    hdul[0].header['NAXIS3'] = nchans
                                    hdul[0].header['CRPIX3'] = 1
                                    hdul[0].header['CRVAL3'] = crval
                                    hdul[0].header['CDELT3'] = hdul[0].header['CDELT3'] / rr
                                    hdul[0].data = rdata

                                hdul[0].data = np.around(hdul[0].data.astype(np.float32)).astype(np.int16)
                                if doProj:
                                    hdul.flush()
                                else:
                                    hdul.writeto('{}/{}'.format(pipeline.masking, postGridMask))

                                line_image_opts.update({"fitsmask": '{0:s}/{1:s}:output'.format(
                                    get_relative_path(pipeline.masking, pipeline), gridMask.split('/')[-1])})
                            else:
                                raise IOError("Requested channels are not contained in mask {}.".format(gridMask))

                        else:
                            if not doProj:
                                line_image_opts.update({"fitsmask": '{0:s}/{1:s}:output'.format(
                                    get_relative_path(pipeline.masking, pipeline), preGridMask.split('/')[-1])})
                            else:
                                pass

                        step = 'make_cube-{0:s}-field{1:d}-iter{2:d}-with_user_mask'.format(line_name, tt, j)
                    else:
                        line_image_opts.update({"auto-mask": config['make_cube']['wscl_auto_mask']})
                        step = 'make_cube-{0:s}-field{1:d}-iter{2:d}-with_automasking'.format(line_name, tt, j)
                else:
                    step = 'make_sofia_mask-field{0:d}-iter{1:d}'.format(tt, j - 1)
                    line_clean_mask = '{0:s}_{1:s}_{2:s}_{3:d}.image_clean_mask.fits:output'.format(
                        pipeline.prefix, field, line_name, j)
                    line_clean_mask_file = '{0:s}/{1:s}_{2:s}_{3:s}_{4:d}.image_clean_mask.fits'.format(
                        cube_path, pipeline.prefix, field, line_name, j)
                    cubename = '{0:s}_{1:s}_{2:s}_{3:d}.image.fits:input'.format(
                        pipeline.prefix, field, line_name, j - 1)
                    cubename_file = '{0:s}/{1:s}_{2:s}_{3:s}_{4:d}.image.fits'.format(
                        cube_path, pipeline.prefix, field, line_name, j - 1)
                    outmask = '{0:s}_{1:s}_{2:s}_{3:d}.image_clean'.format(
                        pipeline.prefix, field, line_name, j)
                    recipe.add('cab/sofia', step,
                               {
                                   "import.inFile": cubename,
                                   "steps.doFlag": False,
                                   "steps.doScaleNoise": True,
                                   "steps.doSCfind": True,
                                   "steps.doMerge": True,
                                   "steps.doReliability": False,
                                   "steps.doParameterise": False,
                                   "steps.doWriteMask": True,
                                   "steps.doMom0": False,
                                   "steps.doMom1": False,
                                   "steps.doWriteCat": False,
                                   "flag.regions": [],
                                   "scaleNoise.statistic": 'mad',
                                   "SCfind.threshold": 4,
                                   "SCfind.rmsMode": 'mad',
                                   "merge.radiusX": 3,
                                   "merge.radiusY": 3,
                                   "merge.radiusZ": 3,
                                   "merge.minSizeX": 2,
                                   "merge.minSizeY": 2,
                                   "merge.minSizeZ": 2,
                                   "writeCat.basename": outmask,
                               },
                               input=pipeline.cubes + '/cube_' + str(j - 1),
                               output=pipeline.output + '/' + cube_dir,
                               label='{0:s}:: Make SoFiA mask'.format(step))

                    recipe.run()
                    recipe.jobs = []

                    if not os.path.exists(line_clean_mask_file):
                        caracal.log.info(
                            'Sofia mask_' + str(j - 1) + ' was not found. Exiting and saving the cube')
                        j -= 1
                        break

                    step = 'make_cube-{0:s}-field{1:d}-iter{2:d}-with_SoFiA_mask'.format(line_name, tt, j)
                    line_image_opts.update({"fitsmask": '{0:s}/{1:s}'.format(cube_dir, line_clean_mask)})
                    if 'auto-mask' in line_image_opts:
                        del (line_image_opts['auto-mask'])

                recipe.add('cab/wsclean',
                           step, line_image_opts,
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Image Line'.format(step))
                recipe.run()
                recipe.jobs = []

                # delete line "MFS" images made by WSclean by averaging all channels
                for mfs in glob.glob('{0:s}/{1:s}/{2:s}_{3:s}_{4:s}_{5:d}-MFS*fits'.format(
                        pipeline.output, cube_dir, pipeline.prefix, field, line_name, j)):
                    os.remove(mfs)

                # Stack channels together into cubes and fix spectral frame
                if config['make_cube']['wscl_make_cube']:
                    if config['make_cube']['wscl_onlypsf']:
                        imagetype = ['psf',]
                    elif not config['make_cube']['niter']:
                        imagetype = ['dirty', 'image']
                    else:
                        imagetype = ['dirty', 'image', 'psf', 'residual', 'model']
                        if config['make_cube']['wscl_mgain'] < 1.0:
                            imagetype.append('first-residual')
                    for mm in imagetype:
                        step = '{0:s}-cubestack-field{1:d}-iter{2:d}'.format(
                            mm.replace('-', '_'), tt, j)
                        if not os.path.exists('{6:s}/{0:s}/{1:s}_{2:s}_{3:s}_{4:d}-0000-{5:s}.fits'.format(
                                cube_dir, pipeline.prefix, field, line_name, j, mm, pipeline.output)):
                            caracal.log.warn('Skipping container {0:s}. Single channels do not exist.'.format(step))
                        else:
                            stacked_cube = '{0:s}/{1:s}_{2:s}_{3:s}_{4:d}.{5:s}.fits'.format(cube_dir,
                                                                                             pipeline.prefix, field, line_name, j, mm)
                            recipe.add(
                                'cab/fitstool',
                                step,
                                {
                                    "file_pattern": '{0:s}/{1:s}_{2:s}_{3:s}_{4:d}-*-{5:s}.fits:output'.format(
                                        cube_dir, pipeline.prefix, field, line_name,
                                        j, mm),
                                    "output": stacked_cube,
                                    "stack": True,
                                    "delete-files": True,
                                    "fits-axis": 'FREQ',
                                },
                                input=pipeline.input,
                                output=pipeline.output,
                                label='{0:s}:: Make {1:s} cube from wsclean {1:s} channels'.format(
                                    step,
                                    mm.replace('-', '_')))

                            recipe.run()
                            recipe.jobs = []

                            # Replace channels that are single-valued (usually zero-ed) in the dirty cube with blanks
                            #   in all cubes assuming that channels run along numpy axis 1 (axis 0 is for Stokes)
                            if not config['make_cube']['wscl_onlypsf']:
                                with fits.open('{0:s}/{1:s}'.format(pipeline.output, stacked_cube)) as stck:
                                    cubedata = stck[0].data
                                    cubehead = stck[0].header
                                    if mm == 'dirty':
                                        tobeblanked = (cubedata == np.nanmean(cubedata, axis=(0, 2, 3)).reshape((
                                            1, cubedata.shape[1], 1, 1))).all(axis=(0, 2, 3))
                                    cubedata[:, tobeblanked] = np.nan
                                    fits.writeto('{0:s}/{1:s}'.format(pipeline.output, stacked_cube), cubedata, header=cubehead, overwrite=True)

                    caracal.log.info('Fixing the spectral system of all cubes for target {0:d}, iteration {1:d}'.format(tt, j))
                    for ss in ['dirty', 'psf', 'first-residual', 'residual', 'model', 'image']:
                        cubename = '{6:s}/{0:s}/{1:s}_{2:s}_{3:s}_{4:d}.{5:s}.fits'.format(
                            cube_dir, pipeline.prefix, field, line_name, j, ss, pipeline.output)
                        recipe.add(fix_specsys_ra,
                                   'fixspecsysra-{0:s}-cube-field{1:d}-iter{2:d}'.format(ss.replace("_", "-"), tt, j),
                                   {'filename': cubename,
                                    'specframe': specframe_all, },
                                   input=pipeline.input,
                                   output=pipeline.output,
                                   label='Fix spectral reference frame for cube {0:s}'.format(cubename))

                    recipe.run()
                    recipe.jobs = []

                if not config['make_cube']['wscl_onlypsf']:
                    cubename_file = '{0:s}/cube_{1:d}/{2:s}_{3:s}_{4:s}_{1:d}.image.fits'.format(
                        pipeline.cubes, j, pipeline.prefix, field, line_name)
                    rms_values.append(calc_rms(cubename_file, line_clean_mask_file))
                    caracal.log.info('RMS = {0:.3e} Jy/beam for {1:s}'.format(rms_values[-1], cubename_file))

                # if the RMS has decreased by a factor < wscl_tol compared to the previous cube then cleaning is no longer improving the cube and we can stop
                if len(rms_values) > 1 and wscl_tol and rms_values[-2] / rms_values[-1] <= wscl_tol:
                    caracal.log.info('The cube RMS noise has decreased by a factor <= {0:.3f} compared to the previous WSclean iteration. Noise convergence achieved.'.format(wscl_tol))
                    break

                # If the RMS has decreased by a factor > wscl_tol compared to the previous cube then cleaning is still improving the cube and it's worth continuing with a new SoFiA + WSclean iteration
                elif len(rms_values) > 1 and wscl_tol and rms_values[-2] / rms_values[-1] > wscl_tol:
                    # rms_old = rms_new
                    caracal.log.info('The cube RMS noise has decreased by a factor > {0:.3f} compared to the previous WSclean iteration. The noise has not converged yet and we should continue iterating SoFiA + WSclean.'.format(wscl_tol))
                    if j == wscl_niter:
                        caracal.log.info('Stopping anyway. Maximum number of SoFiA + WSclean iterations reached.')
                    else:
                        caracal.log.info('Starting a new SoFiA + WSclean iteration.')

            # Out of SoFiA + WSclean loop -- prepare final data products
            for ss in ['dirty', 'psf', 'first-residual', 'residual', 'model', 'image']:
                if 'dirty' in ss:
                    caracal.log.info('Preparing final cubes.')
                cubename = '{0:s}/{1:s}_{2:s}_{3:s}_{4:d}.{5:s}.fits'.format(
                    cube_path, pipeline.prefix, field, line_name, j, ss)
                finalcubename = '{0:s}/{1:s}_{2:s}_{3:s}.{4:s}.fits'.format(
                    cube_path, pipeline.prefix, field, line_name, ss)
                line_clean_mask_file = '{0:s}/{1:s}_{2:s}_{3:s}_{4:d}.image_clean_mask.fits'.format(
                    cube_path, pipeline.prefix, field, line_name, j)
                final_line_clean_mask_file = '{0:s}/{1:s}_{2:s}_{3:s}.image_clean_mask.fits'.format(
                    cube_path, pipeline.prefix, field, line_name)
                MFScubename = '{0:s}/{1:s}_{2:s}_{3:s}_{4:d}-MFS-{5:s}.fits'.format(
                    cube_path, pipeline.prefix, field, line_name, j, ss)
                finalMFScubename = '{0:s}/{1:s}_{2:s}_{3:s}-MFS-{4:s}.fits'.format(
                    cube_path, pipeline.prefix, field, line_name, ss)
                if os.path.exists(cubename):
                    os.rename(cubename, finalcubename)
                if os.path.exists(line_clean_mask_file):
                    os.rename(line_clean_mask_file, final_line_clean_mask_file)
                if os.path.exists(MFScubename):
                    os.rename(MFScubename, finalMFScubename)

            for j in range(1, wscl_niter):
                if config['make_cube']['wscl_removeintermediate']:
                    for ss in ['dirty', 'psf', 'first-residual', 'residual', 'model', 'image']:
                        cubename = '{0:s}/{1:s}_{2:s}_{3:s}_{4:d}.{5:s}.fits'.format(
                            pipeline.cubes, pipeline.prefix, field, line_name, j, ss)
                        line_clean_mask_file = '{0:s}/{1:s}_{2:s}_{3:s}_{4:d}.image_clean_mask.fits'.format(
                            pipeline.cubes, pipeline.prefix, field, line_name, j)
                        MFScubename = '{0:s}/{1:s}_{2:s}_{3:s}_{4:d}-MFS-{5:s}.fits'.format(
                            pipeline.cubes, pipeline.prefix, field, line_name, j, ss)
                        if os.path.exists(cubename):
                            os.remove(cubename)
                        if os.path.exists(line_clean_mask_file):
                            os.remove(line_clean_mask_file)
                        if os.path.exists(MFScubename):
                            os.remove(MFScubename)

    if pipeline.enable_task(config, 'make_cube') and config['make_cube']['image_with'] == 'casa':
        cube_dir = get_relative_path(pipeline.cubes, pipeline)
        nchans_all, specframe_all = [], []
        label = config['label_in']
        if label != '':
            flabel = '_' + label
        else:
            flabel = label

        caracal.log.info('Collecting spectral info on MS files being imaged')
        if config['make_cube']['use_mstransform']:
            for i, msfile in enumerate(all_msfiles):
                if not pipeline.enable_task(config, 'mstransform'):
                    msinfo = pipeline.get_msinfo(msfile)
                    spw = msinfo['SPW']['NUM_CHAN']
                    nchans = spw
                    nchans_all.append(nchans)
                    caracal.log.info('MS #{0:d}: {1:s}'.format(i, msfile))
                    caracal.log.info('  {0:d} spectral windows, with NCHAN={1:s}'.format(
                        len(spw), ','.join(map(str, spw))))

                    # Get first chan, last chan, chan width
                    chfr = msinfo['SPW']['CHAN_FREQ']
                    firstchanfreq = [ss[0] for ss in chfr]
                    lastchanfreq = [ss[-1] for ss in chfr]
                    chanwidth = [(ss[-1] - ss[0]) / (len(ss) - 1)
                                 for ss in chfr]
                    caracal.log.info('  CHAN_FREQ from {0:s} Hz to {1:s} Hz with average channel width of {2:s} Hz'.format(
                        ','.join(map(str, firstchanfreq)), ','.join(map(str, lastchanfreq)), ','.join(map(str, chanwidth))))

                    specframe = msinfo['SPW']['MEAS_FREQ_REF']
                    specframe_all.append(specframe)
                    caracal.log.info(
                        '  The spectral reference frame is {0:}'.format(specframe))

                elif pipeline.enable_task(config['mstransform'], 'doppler'):
                    nchans_all[i] = [nchan_dopp for kk in chanw_all[i]]
                    specframe_all.append([{'lsrd': 0, 'lsrk': 1, 'galacto': 2, 'bary': 3, 'geo': 4, 'topo': 5}[
                                         config['mstransform']['doppler']['frame']] for kk in chanw_all[i]])
        else:
            for i, msfile in enumerate(all_msfiles):
                msinfo = pipeline.get_msinfo(msfile)
                spw = msinfo['SPW']['NUM_CHAN']
                nchans = spw
                nchans_all.append(nchans)
                caracal.log.info('MS {0:d}: {1:s}'.format(i, msfile))
                caracal.log.info('  {0:d} spectral windows, with NCHAN={1:s}'.format(
                    len(spw), ','.join(map(str, spw))))
                specframe = msinfo['SPW']['MEAS_FREQ_REF']
                specframe_all.append(specframe)
                caracal.log.info(
                    '  The spectral reference frame is {0:}'.format(specframe))

        spwid = config['make_cube']['spwid']
        nchans = config['make_cube']['nchans']
        if nchans == 0:
            # Assuming user wants same spw for all msfiles and they have same
            # number of channels
            nchans = nchans_all[0][spwid]
        # Assuming user wants same spw for all msfiles and they have same
        # specframe
        specframe_all = [ss[spwid] for ss in specframe_all][0]
        firstchan = config['make_cube']['firstchan']
        binchans = config['make_cube']['binchans']
        channelrange = [firstchan, firstchan + nchans * binchans]
        # Construct weight specification
        if config['make_cube']['weight'] == 'briggs':
            weight = 'briggs {0:.3f}'.format(
                config['make_cube']['robust'])
        else:
            weight = config['make_cube']['weight']

        for tt, target in enumerate(all_targets):
            if config['make_cube']['use_mstransform']:
                mslist = [add_ms_label(ms, "mst") for ms in ms_dict[target]]
            else:
                mslist = ms_dict[target]
            field = utils.filter_name(target)

            step = 'make_line_cube-field{0:d}'.format(tt)
            image_opts = {
                "msname": mslist,
                "prefix": '{0:s}/{1:s}_{2:s}_{3:s}'.format(cube_dir, pipeline.prefix, field, line_name),
                "mode": 'channel',
                "nchan": nchans,
                "start": config['make_cube']['firstchan'],
                "interpolation": 'nearest',
                "niter": config['make_cube']['niter'],
                "gain": config['make_cube']['gain'],
                "psfmode": 'hogbom',
                "threshold": config['make_cube']['casa_thr'],
                "npix": config['make_cube']['npix'],
                "cellsize": config['make_cube']['cell'],
                "weight": config['make_cube']['weight'],
                "robust": config['make_cube']['robust'],
                "stokes": config['make_cube']['stokes'],
                "port2fits": config['make_cube']['casa_port2fits'],
                "restfreq": restfreq,
            }
            if config['make_cube']['taper'] != '':
                image_opts.update({
                    "uvtaper": True,
                    "outertaper": config['make_cube']['taper'],
                })
            recipe.add('cab/casa_clean', step, image_opts,
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Image Line'.format(step))

    recipe.run()
    recipe.jobs = []

    # This prevents multiple running of imcontsub if the
    # targets are specified explicitly
    rancsonce = False

    # Once all cubes have been made fix the headers etc.
    # Search cubes and cubes/cubes_*/ for cubes whose header should be fixed
    cube_dir = get_relative_path(pipeline.cubes, pipeline)
    for tt, target in enumerate(all_targets):
        field = utils.filter_name(target)

        casa_cube_list = glob.glob('{0:s}/{1:s}/{2:s}_{3:s}_{4:s}*.fits'.format(
            pipeline.output, cube_dir, pipeline.prefix, field, line_name))
        wscl_cube_list = glob.glob('{0:s}/{1:s}/cube_*/{2:s}_{3:s}_{4:s}*.fits'.format(
            pipeline.output, cube_dir, pipeline.prefix, field, line_name))
        cube_list = casa_cube_list + wscl_cube_list
        image_cube_list = [cc for cc in cube_list if 'image.fits' in cc]
        dirty_cube_list = [cc for cc in cube_list if 'dirty.fits' in cc]

        image_mask_list = [cc for cc in cube_list if 'image_mask.fits' in cc]
        image_clean_mask_list = [cc for cc in cube_list if 'image_clean_mask.fits' in cc]

        if pipeline.enable_task(config, 'pb_cube'):
            caracal.log.info('Will create primary beam cube for target {0:d}'.format(tt))
            for uu in range(len(image_cube_list)):
                recipe.add(make_pb_cube,
                           'make pb_cube-{0:d}'.format(uu),
                           {'filename': image_cube_list[uu],
                            'apply_corr': config['pb_cube']['apply_pb'],
                            'typ': config['pb_cube']['pb_type'],
                            'dish_size': config['pb_cube']['dish_size'],
                            'cutoff': config['pb_cube']['cutoff'],
                            },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Make primary beam cube for {0:s}'.format(image_cube_list[uu]))
                cube_list.append(image_cube_list[uu].replace('image.fits', 'pb.fits'))
                if config['pb_cube']['apply_pb']:
                    cube_list.append(image_cube_list[uu].replace('image.fits', 'pb_corr.fits'))

        if pipeline.enable_task(config, 'remove_stokes_axis'):
            caracal.log.info('Will remove Stokes axis of all cubes/images of target {0:d}'.format(tt))
            for uu in range(len(cube_list)):
                recipe.add(remove_stokes_axis,
                           'remove_cube_stokes_axis-{0:d}'.format(uu),
                           {'filename': cube_list[uu], },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Remove Stokes axis for cube {0:s}'.format(cube_list[uu]))

        if pipeline.enable_task(config, 'freq_to_vel'):
            if not config['freq_to_vel']['reverse']:
                caracal.log.info(
                    'Will convert spectral axis of all cubes from frequency to radio velocity for target {0:d}'.format(tt))
            else:
                caracal.log.info(
                    'Will convert spectral axis of all cubes from radio velocity to frequency for target {0:d}'.format(tt))
            for uu in range(len(cube_list)):
                recipe.add(freq_to_vel,
                           'convert-spectral_header-cube{0:d}'.format(uu),
                           {'filename': cube_list[uu],
                            'reverse': config['freq_to_vel']['reverse'], },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Convert spectral axis from frequency to radio velocity for cube {0:s}'.format(cube_list[uu]))

        recipe.run()
        recipe.jobs = []

        if pipeline.enable_task(config, 'sofia'):
            if config['sofia']['imcontsub']:
                simage_cube_list = []
                for uu in range(len(image_cube_list)):
                    icsname = image_cube_list[uu].replace(
                        '.image.fits', '.imcontsub.fits')
                    if len(glob.glob(icsname)) > 0:
                        simage_cube_list.append(icsname)
                    else:
                        simage_cube_list.append(image_cube_list[uu])
            else:
                simage_cube_list = image_cube_list

            for uu in range(len(image_cube_list)):
                step = 'sofia-source_finding-{0:d}'.format(uu)
                recipe.add(
                    'cab/sofia',
                    step,
                    {
                        "import.inFile": simage_cube_list[uu].split('/')[-1] + ':input',
                        "steps.doFlag": config['sofia']['flag'],
                        "steps.doScaleNoise": True,
                        "steps.doSCfind": True,
                        "steps.doMerge": config['sofia']['merge'],
                        "steps.doReliability": False,
                        "steps.doParameterise": False,
                        "steps.doWriteMask": True,
                        "steps.doMom0": config['sofia']['mom0'],
                        "steps.doMom1": config['sofia']['mom1'],
                        "steps.doCubelets": config['sofia']['cubelets'],
                        "steps.doWriteCat": False,
                        "flag.regions": config['sofia']['flagregion'],
                        "scaleNoise.statistic": config['sofia']['rmsMode'],
                        "SCfind.threshold": config['sofia']['thr'],
                        "SCfind.rmsMode": config['sofia']['rmsMode'],
                        "merge.radiusX": config['sofia']['mergeX'],
                        "merge.radiusY": config['sofia']['mergeY'],
                        "merge.radiusZ": config['sofia']['mergeZ'],
                        "merge.minSizeX": config['sofia']['minSizeX'],
                        "merge.minSizeY": config['sofia']['minSizeY'],
                        "merge.minSizeZ": config['sofia']['minSizeZ'],
                    },
                    input='/'.join(simage_cube_list[uu].split('/')[:-1]),
                    output='/'.join(simage_cube_list[uu].split('/')[:-1]),
                    label='{0:s}:: Make SoFiA mask and images for cube {1:s}'.format(step, simage_cube_list[uu]))
        # Again, in some cases this should run once
        if rancsonce:
            pass
        else:
            if pipeline.enable_task(config, 'imcontsub'):

                caracal.log.info(
                    'Subtracting continuum in the image domain for target {0:d}'.format(tt))

                # Using highest cube directory
                dirlist = glob.glob('{0:s}/{1:s}/cube_*'.format(pipeline.output, cube_dir))
                poopoo = max([int(gi[-1]) for gi in dirlist])
                if config['imcontsub']['lastiter']:
                    wscl_cube_list = glob.glob(
                        '{0:s}/{1:s}/cube_{2:d}/{3:s}_{4:s}_{5:s}*.fits'.format(
                            pipeline.output, cube_dir, poopoo,
                            pipeline.prefix, field, line_name))
                else:
                    wscl_cube_list = glob.glob(
                        '{0:s}/{1:s}/cube_*/{2:s}_{3:s}_{4:s}*.fits'.format(
                            pipeline.output, cube_dir,
                            pipeline.prefix, field, line_name))

                # Hoping that the order is the same for all suffixes
                wimage_cube_list = [cc for cc in wscl_cube_list if 'image.fits' in cc]
                wdirty_cube_list = [cc for cc in wscl_cube_list if 'dirty.fits' in cc]
                wimage_mask_list = [cc for cc in wscl_cube_list if 'image_mask.fits' in cc]
                wimage_clean_mask_list = [cc for cc in wscl_cube_list if 'image_clean_mask.fits' in cc]

                # See comment below
                runonce = False
                if len(config['imcontsub']['incubus']) == 0 or len(config['imcontsub']['incubus'][0]) == 0:
                    if len(wimage_cube_list):
                        contsincubelist = wimage_cube_list
                        rsuffix = '.image.fits'
                    else:
                        contsincubelist = wdirty_cube_list
                        rsuffix = '.dirty.fits'
                else:
                    # Run only once if the cubes are specified explicitly
                    # Otherwise we'd do the same thing number of targers times
                    runonce = True
                    contsincubelist = config['imcontsub']['incubus']
                    rsuffix = '.fits'
                outputlist = [i.replace(rsuffix, '.imcontsub.fits') for i in contsincubelist]

                if config['imcontsub']['mask'] == '':
                    if len(config['imcontsub']['masculin']) == 0 or len(config['imcontsub']['masculin'][0]) == 0:
                        maskimc = []
                    else:
                        maskimc = config['imcontsub']['masculin']
                elif config['imcontsub']['mask'] == 'clean':
                    maskimc = wimage_clean_mask_list
                elif config['imcontsub']['mask'] == 'sofia':
                    maskimc = wimage_mask_list
                else:
                    maskimc = []

                if len(maskimc) == 0:
                    maskimc = [None for i in contsincubelist]
                    caracal.log.info(
                        'Not using mask for image subtraction of target {0:d}'.format(tt))

                if config['imcontsub']['outfit']:
                    outfitlist = [i.replace(rsuffix, '.contsfit.fits') for i in contsincubelist]
                else:
                    outfitlist = [None for i in contsincubelist]

                if config['imcontsub']['outfitcon']:
                    outconlist = [i.replace(rsuffix, '.contsfitcon.fits') for i in contsincubelist]
                else:
                    outconlist = [None for i in contsincubelist]

                # outconlist = [i.replace('dirty.fits', 'imcontsub.fits') for i in dirty_cube_list]

                for uu in range(len(contsincubelist)):
                    image_contsub.imcontsub(
                        incubus=contsincubelist[uu], outcubus=outputlist[uu],
                        fitmode=config['imcontsub']['fitmode'],
                        polyorder=config['imcontsub']['polyorder'],
                        length=config['imcontsub']['length'],
                        mask=maskimc[uu],
                        sgiters=config['imcontsub']['sgiters'],
                        kertyp=config['imcontsub']['kertyp'],
                        kersiz=config['imcontsub']['kersiz'],
                        fitted=outfitlist[uu],
                        confit=outconlist[uu],
                        clobber=True,
                    )
                    if runonce:
                        rancsonce = True
                        break

        if pipeline.enable_task(config, 'sharpener'):
            for uu in range(len(image_cube_list)):
                step = 'continuum-spectral_extraction-{0:d}'.format(uu)

                params = {"enable_spec_ex": True,
                          "enable_source_catalog": True,
                          "enable_abs_plot": True,
                          "enable_source_finder": False,
                          "cubename": image_cube_list[uu] + ':output',
                          "channels_per_plot": config['sharpener']['chans_per_plot'],
                          "workdir": '{0:s}/'.format(stimela.recipe.CONT_IO["output"]),
                          "label": config['sharpener']['label'],
                          }

                runsharp = False
                if config['sharpener']['catalog'] == 'PYBDSF':
                    catalogs = []
                    nimages = glob.glob("{0:s}/image_*".format(pipeline.continuum))

                    for ii in range(0, len(nimages)):
                        catalog = glob.glob("{0:s}/image_{1:d}/{2:s}_{3:s}_*.lsm.html".format(
                            pipeline.continuum, ii + 1, pipeline.prefix, field))
                        catalogs.append(catalog)

                    catalogs = sorted(catalogs)
                    catalogs = [cat for catalogs in catalogs for cat in catalogs]
                    # Right now, this is the last catalog made
                    if len(catalogs):
                        catalog_file = catalogs[-1].split('output/')[-1]
                        params["catalog_file"] = '{0:s}:output'.format(catalog_file)
                    else:
                        catalog_file = []

                    if len(catalog_file) > 0:
                        runsharp = True
                        params["catalog"] = "PYBDSF"
                        recipe.add('cab/sharpener',
                                   step,
                                   params,
                                   input='/'.join('{0:s}/{1:s}'.format(pipeline.output, image_cube_list[uu]).split('/')[:-1]),
                                   output=pipeline.output,
                                   label='{0:s}:: Continuum Spectral Extraction'.format(step))
                    else:
                        caracal.log.warn(
                            'No PyBDSM catalogs found. Skipping continuum spectral extraction.')

                elif config['sharpener']['catalog'] == 'NVSS':
                    runsharp = True
                    params["thr"] = config['sharpener']['thr']
                    params["width"] = config['sharpener']['width']
                    params["catalog"] = "NVSS"
                    recipe.add('cab/sharpener',
                               step,
                               params,
                               input='/'.join('{0:s}/{1:s}'.format(pipeline.output, image_cube_list[uu]).split('/')[:-1]),
                               output=pipeline.output,
                               label='{0:s}:: Continuum Spectral Extraction'.format(step))

                recipe.run()
                recipe.jobs = []

                # Move the sharpener output to diagnostic_plots
                if runsharp:
                    sharpOut = '{0:s}/{1:s}'.format(pipeline.output, 'sharpOut')
                    finalsharpOut = '{0:s}/{1:s}_{2:s}_{3:s}'.format(
                        pipeline.diagnostic_plots, pipeline.prefix, field, 'sharpOut')
                    if os.path.exists(finalsharpOut):
                        shutil.rmtree(finalsharpOut)
                    shutil.move(sharpOut, finalsharpOut)

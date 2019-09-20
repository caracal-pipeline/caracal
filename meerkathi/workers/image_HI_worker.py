import sys
import os
import glob
import warnings
import stimela.dismissable as sdm
import stimela.recipe as stimela
import astropy
import shutil
from astropy.io import fits
import meerkathi
# Modules useful to calculate common barycentric frequency grid
from astropy.time import Time
from astropy.coordinates import SkyCoord
from astropy.coordinates import EarthLocation
from astropy import constants
import astropy.units as asunits
import re
import datetime
import numpy as np
import yaml
from meerkathi.dispatch_crew import utils
import itertools

# To split out cubes/<dir> from output/cubes/dir


def get_dir_path(string, pipeline): return string.split(pipeline.output)[1][1:]


def target_to_msfiles(targets, msnames, label, doppler=False):
    target_ls, target_msfiles, target_ms_ls, all_target = [], [], [], []

    for t in targets:  # list all targets per input ms and make a unique list of all target fields
        tmp = t.split(',')
        target_ls.append(tmp)
        for tt in tmp:
            all_target.append(tt)
    all_target = list(set(all_target))

    for i, ms in enumerate(
            msnames):  # make a list of all input ms file names for each target field
        for t in target_ls[i]:
            tmp = utils.filter_name(t)
            if doppler:
                target_ms_ls.append(
                    '{0:s}-{1:s}{2:s}_mst.ms'.format(ms[:-3], tmp, label))
            else:
                target_ms_ls.append(
                    '{0:s}-{1:s}{2:s}.ms'.format(ms[:-3], tmp, label))

    for t in all_target:  # group ms files by target field name
        tmp = []
        for m in target_ms_ls:
            if m.find(utils.filter_name(t)) > -1:
                tmp.append(m)
        target_msfiles.append(tmp)

    return all_target, target_ms_ls, dict(zip(all_target, target_msfiles))


def freq_to_vel(filename, reverse):
    C = 2.99792458e+8       # m/s
    HI = 1.4204057517667e+9  # Hz
    filename = filename.split(':')
    filename = '{0:s}/{1:s}'.format(filename[1], filename[0])
    if not os.path.exists(filename):
        meerkathi.log.info(
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
            if 'FREQ' in headcube['ctype3'] and not reverse:
                headcube['cdelt3'] = -C * float(headcube['cdelt3']) / restfreq
                headcube['crval3'] = C * \
                    (1 - float(headcube['crval3']) / restfreq)
                # FITS standard for radio velocity as per
                # https://fits.gsfc.nasa.gov/standard40/fits_standard40aa-le.pdf
                headcube['ctype3'] = 'VRAD'
                if 'cunit3' in headcube:
                    # delete cunit3 because we adopt the default units = m/s
                    del headcube['cunit3']

            # convert from radio velocity to frequency
            elif 'VRAD' in headcube['ctype3'] and reverse:
                headcube['cdelt3'] = -restfreq * float(headcube['cdelt3']) / C
                headcube['crval3'] = restfreq * \
                    (1 - float(headcube['crval3']) / C)
                headcube['ctype3'] = 'FREQ'
                if 'cunit3' in headcube:
                    # delete cunit3 because we adopt the default units = Hz
                    del headcube['cunit3']
            else:
                if not reverse:
                    meerkathi.log.info(
                        'Skipping conversion for {0:s}. Input cube not in frequency.'.format(filename))
                else:
                    meerkathi.log.info(
                        'Skipping conversion for {0:s}. Input cube not in velocity.'.format(filename))


def remove_stokes_axis(filename):
    filename = filename.split(':')
    filename = '{0:s}/{1:s}'.format(filename[1], filename[0])
    if not os.path.exists(filename):
        meerkathi.log.info(
            'Skipping Stokes axis removal for {0:s}. File does not exist.'.format(filename))
    else:
        with fits.open(filename, mode='update') as cube:
            headcube = cube[0].header
            if headcube['naxis'] == 4 and headcube['ctype4'] == 'STOKES':
                cube[0].data = cube[0].data[0]
                del headcube['cdelt4']
                del headcube['crpix4']
                del headcube['crval4']
                del headcube['ctype4']
                if 'cunit4' in headcube:
                    del headcube['cunit4']
            else:
                meerkathi.log.info(
                    'Skipping Stokes axis removal for {0:s}. Input cube has less than 4 axis or the 4th axis type is not "STOKES".'.format(filename))


def fix_specsys(filename, specframe):
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
    filename = filename.split(':')
    filename = '{0:s}/{1:s}'.format(filename[1], filename[0])
    if not os.path.exists(filename):
        meerkathi.log.info(
            'Skipping SPECSYS fix for {0:s}. File does not exist.'.format(filename))
    else:
        with fits.open(filename, mode='update') as cube:
            headcube = cube[0].header
            if 'specsys' in headcube:
                del headcube['specsys']
            headcube['specsys3'] = specsys3


def make_pb_cube(filename, apply_corr):
    filename = filename.split(':')
    filename = '{0:s}/{1:s}'.format(filename[1], filename[0])
    if not os.path.exists(filename):
        meerkathi.log.info(
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
            sigma_pb = 17.52 / (headcube['crval3'] + headcube['cdelt3'] * (
                np.arange(headcube['naxis3']) - headcube['crpix3'] + 1)) * 1e+9 / 13.5 / 2.355
            # sigma_pb=headcube['crval3']+headcube['cdelt3']*(np.arange(headcube['naxis3'])-headcube['crpix3']+1)
            sigma_pb.resize((sigma_pb.shape[0], 1, 1))
            #print sigma_pb
            datacube = np.exp(-datacube**2 / 2 / sigma_pb**2)
            fits.writeto(
                filename.replace(
                    'image.fits',
                    'pb.fits'),
                datacube,
                header=headcube,
                overwrite=True)
            if apply_corr:
                fits.writeto(
                    filename.replace(
                        'image.fits',
                        'pb_corr.fits'),
                    cube[0].data / datacube,
                    header=headcube,
                    overwrite=True)  # Applying the primary beam correction
            meerkathi.log.info(
                'Created primary beam cube FITS {0:s}'.format(
                    filename.replace(
                        'image.fits', 'pb.fits')))


def calc_rms(filename, HImaskname):
    if HImaskname is None:
        if not os.path.exists(filename):
            meerkathi.log.info(
                'Noise not determined in cube for {0:s}. File does not exist.'.format(filename))
        else:
            with fits.open(filename) as cube:
                datacube = cube[0].data
                y = datacube[~np.isnan(datacube)]
            return np.sqrt(np.sum(y * y, dtype=np.float64) / y.size)
    else:
        with fits.open(filename) as cube:
            datacube = cube[0].data
        with fits.open(HImaskname) as mask:
            datamask = mask[0].data

            # select channels
            selchans = datamask.sum(axis=(2, 3)) > 0
            newcube = datacube[selchans]
            newmask = datamask[selchans]
            y2 = newcube[newmask == 0]
            #print newcube.shape, newmask.shape, y.shape
            #y = datacube[(~np.isnan(datacube)) & (np.sum(datamask,axis=(1,2))>0) & (npdatamask[:,0,0]==0)]
        return np.sqrt(np.nansum(y2 * y2, dtype=np.float64) / y2.size)


NAME = 'Make HI Cube'


def worker(pipeline, recipe, config):
    label = config['label']
    if label != '':
        flabel = '_' + label
    else:
        flabel = label
    all_targets, all_msfiles, ms_dict = target_to_msfiles(
        pipeline.target, pipeline.msnames, flabel, False)
    RA, Dec = [], []
    firstchanfreq_all, chanw_all, lastchanfreq_all = [], [], []
    mslist = ['{0:s}_{1:s}.ms'.format(did, config['label'])
              for did in pipeline.dataid]
    pipeline.prefixes = [
        '{2:s}-{0:s}-{1:s}'.format(
            did,
            config['label'],
            pipeline.prefix) for did in pipeline.dataid]
    prefixes = pipeline.prefixes
    restfreq = config.get('restfreq')
    npix = config.get('npix')
    if len(npix) == 1:
        npix = [npix[0], npix[0]]
    cell = config.get('cell')
    weight = config.get('weight')
    robust = config.get('robust')

    for i, msfile in enumerate(all_msfiles):
        # Upate pipeline attributes (useful if, e.g., channel averaging was
        # performed by the split_data worker)
        msinfo = '{0:s}/{1:s}-{2:s}-obsinfo.json'.format(
            pipeline.output, pipeline.prefix, msfile[:-3])
        meerkathi.log.info('Updating info from {0:s}'.format(msinfo))
        with open(msinfo, 'r') as stdr:
            spw = yaml.load(stdr)['SPW']['NUM_CHAN']
        meerkathi.log.info('MS has {0:d} spectral windows, with NCHAN={1:s}'.format(
            len(spw), ','.join(map(str, spw))))

        # Get first chan, last chan, chan width
        with open(msinfo, 'r') as stdr:
            chfr = yaml.load(stdr)['SPW']['CHAN_FREQ']
            # To be done: add user selected  spw
            firstchanfreq = [ss[0] for ss in chfr]
            lastchanfreq = [ss[-1] for ss in chfr]
            chanwidth = [(ss[-1] - ss[0]) / (len(ss) - 1) for ss in chfr]
            firstchanfreq_all.append(firstchanfreq), chanw_all.append(
                chanwidth), lastchanfreq_all.append(lastchanfreq)
        meerkathi.log.info('CHAN_FREQ from {0:s} Hz to {1:s} Hz with average channel width of {2:s} Hz'.format(
            ','.join(map(str, firstchanfreq)), ','.join(map(str, lastchanfreq)), ','.join(map(str, chanwidth))))

        with open(msinfo, 'r') as stdr:
            tinfo = yaml.safe_load(stdr)['FIELD']
            targetpos = tinfo['REFERENCE_DIR']
            while len(targetpos) == 1:
                targetpos = targetpos[0]
            tRA = targetpos[0] / np.pi * 180.  # original
            tDec = targetpos[1] / np.pi * 180.  # original
           # tRA  = targetpos[0][0][0]/np.pi*180. # modified
           # tDec = targetpos[0][0][1]/np.pi*180. # modified
            RA.append(tRA)
            Dec.append(tDec)
        meerkathi.log.info(
            'Target RA, Dec for Doppler correction: {0:.3f} deg, {1:.3f} deg'.format(
                RA[i], Dec[i]))

    # Find common barycentric frequency grid for all input .MS, or set it as
    # requested in the config file
    if pipeline.enable_task(config, 'mstransform') and config['mstransform'].get(
            'doppler') and config['mstransform'].get('outchangrid') == 'auto':
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
        tellocation = teldict[config["mstransform"].get('telescope')]
        telloc = EarthLocation.from_geodetic(tellocation[0], tellocation[1])
        firstchanfreq_dopp, chanw_dopp, lastchanfreq_dopp = firstchanfreq, chanw, lastchanfreq
        corr_order = False
        if len(chanw) > 1:
            if np.max(chanw) > 0 and np.min(chanw) < 0:
                corr_order = True

        for i, msfile in enumerate(all_msfiles):
            msinfo = '{0:s}/{1:s}-{2:s}-obsinfo.txt'.format(
                pipeline.output, pipeline.prefix, msfile[:-3])
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

        # WARNING: the following line assumes a single SPW for the HI line data
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
        meerkathi.log.info(
            'Calculated common Doppler-corrected channel grid for all input .MS: {0:d} channels starting at {1:s} and with channel width {2:s}.'.format(
                nchan_dopp,
                comfreq0,
                comchanw))
        if pipeline.enable_task(config, 'wsclean_image') and corr_order:
            meerkathi.log.info(
                'wsclean will not work when the input measurement sets are ordered in different directions. Use casa_image')
            sys.exit(1)

    elif pipeline.enable_task(config, 'mstransform') and config['mstransform'].get('doppler') and config['mstransform'].get('outchangrid') != 'auto':
        if len(config['mstransform']['outchangrid'].split(',')) != 3:
            meerkathi.log.error(
                'Wrong format for mstransform:outchangrid in the .yml config file.')
            meerkathi.log.error(
                'Current setting is mstransform:outchangrid:"{0:s}"'.format(
                    config['mstransform']['outchangrid']))
            meerkathi.log.error(
                'It must be "nchan,chan0,chanw" (note the commas) where nchan is an integer, and chan0 and chanw must include units appropriate for the chosen mstransform:mode')
            sys.exit(1)
        nchan_dopp, comfreq0, comchanw = config['mstransform']['outchangrid'].split(
            ',')
        nchan_dopp = int(nchan_dopp)
        meerkathi.log.info(
            'Set requested Doppler-corrected channel grid for all input .MS: {0:d} channels starting at {1:s} and with channel width {2:s}.'.format(
                nchan_dopp,
                comfreq0,
                comchanw))

    elif pipeline.enable_task(config, 'mstransform'):
        nchan_dopp, comfreq0, comchanw = None, None, None

    for i, msname in enumerate(all_msfiles):
        msname_mst = msname.replace('.ms', '_mst.ms')
        if pipeline.enable_task(config, 'subtractmodelcol'):
            step = 'modelsub_{:d}'.format(i)
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

        if pipeline.enable_task(config, 'mstransform'):
            if os.path.exists(
                    '{1:s}/{0:s}'.format(msname_mst, pipeline.msdir)):
                os.system(
                    'rm -r {1:s}/{0:s}'.format(msname_mst, pipeline.msdir))
            col = config['mstransform'].get('column')
            step = 'mstransform_{:d}'.format(i)
            recipe.add('cab/casa_mstransform',
                       step,
                       {"msname": msname,
                        "outputvis": msname_mst,
                        "regridms": config['mstransform'].get('doppler'),
                        "mode": config['mstransform'].get('mode'),
                           "nchan": sdm.dismissable(nchan_dopp),
                           "start": sdm.dismissable(comfreq0),
                           "width": sdm.dismissable(comchanw),
                           "interpolation": 'nearest',
                           "datacolumn": col,
                           "restfreq": restfreq,
                           "outframe": config['mstransform'].get('outframe'),
                           "veltype": config['mstransform'].get('veltype'),
                           "douvcontsub": config['mstransform'].get('uvlin'),
                           "fitspw": sdm.dismissable(config['mstransform'].get('fitspw')),
                           "fitorder": config['mstransform'].get('fitorder'),
                        },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Doppler tracking corrections'.format(step))

            if config['mstransform'].get('obsinfo', True):
                step = 'listobs_{:d}'.format(i)
                recipe.add('cab/casa_listobs',
                           step,
                           {"vis": msname_mst,
                            "listfile": '{0:s}-{1:s}-obsinfo.txt'.format(pipeline.prefix,
                                                                         msname_mst.replace('.ms',
                                                                                            '')),
                            "overwrite": True,
                            },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Get observation information ms={1:s}'.format(step,
                                                                                       msname_mst))

                step = 'summary_json_{:d}'.format(i)
                recipe.add(
                    'cab/msutils',
                    step,
                    {
                        "msname": msname_mst,
                        "command": 'summary',
                        "outfile": '{0:s}-{1:s}-obsinfo.json'.format(
                            pipeline.prefix,
                            msname_mst.replace(
                                '.ms',
                                '')),
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Get observation information as a json file ms={1:s}'.format(
                        step,
                        msname_mst))

        if pipeline.enable_task(config, 'sunblocker'):
            if config['sunblocker'].get('use_mstransform', True):
                msnamesb = msname_mst
            else:
                msnamesb = msname
            step = 'sunblocker_{0:d}'.format(i)
            recipe.add("cab/sunblocker",
                       step,
                       {"command": "phazer",
                        "inset": msnamesb,
                        "outset": msnamesb,
                        "imsize": config['sunblocker'].get('imsize',
                                                           max(npix)),
                           "cell": config['sunblocker'].get('cell',
                                                            cell),
                           "pol": 'i',
                           "threshmode": 'fit',
                           "threshold": config['sunblocker'].get('threshold'),
                           "mode": 'all',
                           "radrange": 0,
                           "angle": 0,
                           "show": '{0:s}.sunblocker.svg'.format(pipeline.prefix),
                           "verb": True,
                           "dryrun": False,
                           "uvmax": config['sunblocker'].get('uvmax'),
                           "uvmin": config['sunblocker'].get('uvmin'),
                           "vampirisms": config['sunblocker'].get('vampirisms'),
                        },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Block out sun'.format(step))

        recipe.run()
        recipe.jobs = []
        # Move the sunblocker plots to the diagnostic_plots
        if pipeline.enable_task(config, 'sunblocker'):
            sunblocker_plots = glob.glob(
                "{0:s}/{1:s}".format(pipeline.output, '*.svg'))
            for plot in sunblocker_plots:
                shutil.copy(plot, pipeline.diagnostic_plots)
                os.remove(plot)

    if pipeline.enable_task(config, 'wsclean_image'):
        nchans_all, specframe_all = [], []
        label = config['label']

        if label != '':
            flabel = '_' + label
        else:
            flabel = label
        if config['wsclean_image'].get('use_mstransform'):

            all_targets, all_msfiles, ms_dict = target_to_msfiles(
                pipeline.target, pipeline.msnames, flabel, True)

            for i, msfile in enumerate(all_msfiles):
                    # If channelisation changed during a previous pipeline run
                    # as stored in the obsinfo.json file
                if not pipeline.enable_task(config, 'mstransform'):
                    msinfo = '{0:s}/{1:s}-{2:s}-obsinfo.json'.format(
                        pipeline.output, pipeline.prefix, msfile[:-3])
                    meerkathi.log.info(
                        'Updating info from {0:s}'.format(msinfo))
                    with open(msinfo, 'r') as stdr:
                        spw = yaml.load(stdr)['SPW']['NUM_CHAN']
                        nchans = spw
                        nchans_all.append(nchans)
                    meerkathi.log.info('MS has {0:d} spectral windows, with NCHAN={1:s}'.format(
                        len(spw), ','.join(map(str, spw))))

                    # Get first chan, last chan, chan width
                    with open(msinfo, 'r') as stdr:
                        chfr = yaml.load(stdr)['SPW']['CHAN_FREQ']
                        firstchanfreq = [ss[0] for ss in chfr]
                        lastchanfreq = [ss[-1] for ss in chfr]
                        chanwidth = [(ss[-1] - ss[0]) / (len(ss) - 1)
                                     for ss in chfr]
                    meerkathi.log.info('CHAN_FREQ from {0:s} Hz to {1:s} Hz with average channel width of {2:s} Hz'.format(
                        ','.join(map(str, firstchanfreq)), ','.join(map(str, lastchanfreq)), ','.join(map(str, chanwidth))))

                    with open(msinfo, 'r') as stdr:
                        specframe = yaml.load(stdr)['SPW']['MEAS_FREQ_REF']
                        specframe_all.append(specframe)
                    meerkathi.log.info(
                        'The spectral reference frame is {0:}'.format(specframe))

                elif config['mstransform'].get('doppler'):
                    nchans_all.append([nchan_dopp for kk in chanw_all[i]])
                    specframe_all.append([{'lsrd': 0, 'lsrk': 1, 'galacto': 2, 'bary': 3, 'geo': 4, 'topo': 5}[
                                         config['mstransform'].get('outframe')] for kk in chanw_all[i]])
        else:
            #all_targets, all_msfiles, ms_dict = target_to_msfiles(pipeline.target,pipeline.msnames,flabel,False)
            msinfo = '{0:s}/{1:s}-{2:s}-obsinfo.json'.format(
                pipeline.output, pipeline.prefix, msfile[:-3])
            with open(msinfo, 'r') as stdr:
                spw = yaml.load(stdr)['SPW']['NUM_CHAN']
                nchans = spw
                nchans_all.append(nchans)
            meerkathi.log.info('MS has {0:d} spectral windows, with NCHAN={1:s}'.format(
                len(spw), ','.join(map(str, spw))))
            with open(msinfo, 'r') as stdr:
                specframe = yaml.load(stdr)['SPW']['MEAS_FREQ_REF']
                specframe_all.append(specframe)
            meerkathi.log.info(
                'The spectral reference frame is {0:}'.format(specframe))

        spwid = config['wsclean_image'].get('spwid')
        nchans = config['wsclean_image'].get('nchans')
        if nchans == 0 or nchans == 'all':
            # Assuming user wants same spw for all msfiles and they have same
            # number of channels
            nchans = nchans_all[0][spwid]
        # Assuming user wants same spw for all msfiles and they have same
        # specframe
        specframe_all = [ss[spwid] for ss in specframe_all][0]
        firstchan = config['wsclean_image'].get('firstchan')
        binchans = config['wsclean_image'].get('binchans')
        if nchans != 'all':
            nchans = int(nchans)
        channelrange = [firstchan, firstchan + nchans * binchans]
        # Construct weight specification
        if config['wsclean_image'].get('weight') == 'briggs':
            weight = 'briggs {0:.3f}'.format(
                config['wsclean_image'].get(
                    'robust', robust))
        else:
            weight = config['wsclean_image'].get('weight', weight)
        wscl_niter = config['wsclean_image'].get('wscl_niter')
        tol = config['wsclean_image'].get('tol')

        for target in (all_targets):
            mslist = ms_dict[target]
            field = utils.filter_name(target)
            for j in range(1, wscl_niter + 1):

                image_path = "{0:s}/image_{1:d}".format(
                    pipeline.cubes, j)
                if not os.path.exists(image_path):
                    os.mkdir(image_path)

                img_dir = '{0:s}/image_{1:d}'.format(
                    get_dir_path(pipeline.cubes, pipeline), j)

                if j == 1:
                    step = 'wsclean_image_HI_with_automasking'
                    ownHIclean_mask = config['wsclean_image'].get(
                        'ownfitsmask')
                    HI_image_opts = {
                        "msname": mslist,
                        "prefix": '{0:s}/{1:s}_{2:s}_HI_{3:s}'.format(img_dir,
                                                                      pipeline.prefix, field, str(j)),
                        "weight": weight,
                        "taper-gaussian": str(config['wsclean_image'].get('taper')),
                        "pol": config['wsclean_image'].get('pol'),
                        "npix": config['wsclean_image'].get('npix', npix),
                        "padding": config['wsclean_image'].get('padding'),
                        "scale": config['wsclean_image'].get('cell', cell),
                        "channelsout": nchans,
                        "channelrange": channelrange,
                        "niter": config['wsclean_image'].get('niter'),
                        "mgain": config['wsclean_image'].get('mgain'),
                        "auto-threshold": config['wsclean_image'].get('auto_threshold'),
                        "auto-mask": config['wsclean_image'].get('auto_mask'),
                        "multiscale": config['wsclean_image'].get('multi_scale'),
                        "multiscale-scales": sdm.dismissable(config['wsclean_image'].get('multi_scale_scales')),
                        "no-update-model-required": config['wsclean_image'].get('no_update_mod')
                    }

                    if ownHIclean_mask:
                        HI_image_opts.update({"fitsmask": '{0:s}/{1:s}:output'.format(
                            get_dir_path(pipeline.masking, pipeline), ownHIclean_mask)})

                    recipe.add('cab/wsclean', step, HI_image_opts,
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{:s}:: Image HI'.format(step))

                    if config['wsclean_image']['make_cube']:
                        if not config['wsclean_image'].get('niter'):
                            imagetype = ['image', 'dirty']
                        else:
                            imagetype = [
                                'image', 'dirty', 'psf', 'residual', 'model']
                            if config['wsclean_image'].get('mgain') < 1.0:
                                imagetype.append('first-residual')
                        for mm in imagetype:
                            step = 'make_{0:s}_cube'.format(
                                mm.replace('-', '_'))
                            recipe.add(
                                'cab/fitstool',
                                step,
                                {
                                    "image": [
                                        '{0:s}/{1:s}_{2:s}_HI_{3:d}-{4:04d}-{5:s}.fits:output'.format(
                                            img_dir,
                                            pipeline.prefix,
                                            field,
                                            j,
                                            d,
                                            mm)for d in xrange(nchans)],
                                    "output": '{0:s}/{1:s}_{2:s}_HI_{3:d}.{4:s}.fits'.format(
                                        img_dir,
                                        pipeline.prefix,
                                        field,
                                        j,
                                        mm),
                                    "stack": True,
                                    "delete-files": True,
                                    "fits-axis": 'FREQ',
                                },
                                input=pipeline.input,
                                output=pipeline.output,
                                label='{0:s}:: Make {1:s} cube from wsclean {1:s} channels'.format(
                                    step,
                                    mm.replace(
                                        '-',
                                        '_')))

                    recipe.run()
                    recipe.jobs = []
                    cubename = '{0:s}/{1:s}_{2:s}_HI_{3:d}.image.fits'.format(
                        image_path, pipeline.prefix, field, j)
                    rms_ref = calc_rms(cubename, None)
                    meerkathi.log.info(
                        'Initial rms = ' + str("{0:.7f}".format(rms_ref)) + ' Jy/beam')

                else:
                    step = 'make_sofia_mask_' + str(j - 1)
                    HIclean_mask = '{0:s}_{1:s}_HI_{2:d}.image_clean_mask.fits:output'.format(
                        pipeline.prefix, field, j)
                    HIclean_mask_file = '{0:s}/{1:s}_{2:s}_HI_{3:d}.image_clean_mask.fits'.format(
                        image_path, pipeline.prefix, field, j)
                    cubename = '{0:s}_{1:s}_HI_{2:d}.image.fits:input'.format(
                        pipeline.prefix, field, j - 1)
                    cubename_file = '{0:s}/{1:s}_{2:s}_HI_{3:d}.image.fits'.format(
                        image_path, pipeline.prefix, field, j - 1)
                    outmask = '{0:s}_{1:s}_HI_{2:d}.image_clean'.format(
                        pipeline.prefix, field, j)
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
                               input=pipeline.cubes + '/image_' + str(j - 1),
                               output=pipeline.output + '/' + img_dir,
                               label='{0:s}:: Make SoFiA mask'.format(step))

                    recipe.run()
                    recipe.jobs = []

                    if not os.path.exists(HIclean_mask_file):
                        meerkathi.log.info(
                            'Sofia mask_' + str(j - 1) + ' was not found. Exiting and saving the cube')

                        for ss in [
                            'dirty',
                            'psf',
                            'first-residual',
                            'residual',
                            'model',
                                'image']:
                            cubename = '{0:s}/{1:s}_{2:s}_HI_{3:s}.{4:s}.fits:output'.format(
                                img_dir, pipeline.prefix, field, str(j), ss)
                            MFScubename = '{0:s}/{1:s}_{2:s}_HI_{3:s}-MFS-{4:s}.fits:output'.format(
                                img_dir, pipeline.prefix, field, str(j), ss)
                            recipe.add(
                                fix_specsys,
                                'fix_specsys_{0:s}_cube'.format(ss),
                                {
                                    "filename": cubename,
                                    "specframe": specframe_all,
                                },
                                input=pipeline.input,
                                output=pipeline.output,
                                label='Fix spectral reference frame for cube {0:s}'.format(cubename))

                        if pipeline.enable_task(config, 'freq_to_vel'):
                            for ss in [
                                'dirty',
                                'psf',
                                'first-residual',
                                'residual',
                                'model',
                                    'image']:
                                cubename = '{0:s}/{1:s}_{2:s}_HI_{3:s}.{4:s}.fits:output'.format(
                                    img_dir, pipeline.prefix, field, str(j), ss)
                                MFScubename = '{0:s}/{1:s}_{2:s}_HI_{3:s}-MFS-{4:s}.fits'.format(
                                    img_dir, pipeline.prefix, field, str(j), ss)
                                recipe.add(
                                    freq_to_vel,
                                    'spectral_header_to_vel_radio_{0:s}_cube'.format(ss),
                                    {
                                        'filename': cubename,
                                        'reverse': config['freq_to_vel'].get('reverse')},
                                    input=pipeline.input,
                                    output=pipeline.output,
                                    label='Convert spectral axis from frequency to radio velocity for cube {0:s}'.format(cubename))
                        j -= 1
                        break

                    step = 'Sofia_mask_found_running_wsclean_image_HI_' + \
                        str(j)
                    recipe.add('cab/wsclean',
                               step,
                               {"msname": mslist,
                                "prefix": '{0:s}/{1:s}_{2:s}_HI_{3:s}'.format(img_dir,
                                                                              pipeline.prefix,
                                                                              field,
                                                                              str(j)),
                                "weight": weight,
                                "taper-gaussian": str(config['wsclean_image'].get('taper')),
                                   "pol": config['wsclean_image'].get('pol'),
                                   "npix": config['wsclean_image'].get('npix',
                                                                       npix),
                                   "padding": config['wsclean_image'].get('padding'),
                                   "scale": config['wsclean_image'].get('cell',
                                                                        cell),
                                   "channelsout": nchans,
                                   "channelrange": channelrange,
                                   "fitsmask": '{0:s}/{1:s}'.format(img_dir,
                                                                    HIclean_mask),
                                   "niter": config['wsclean_image'].get('niter'),
                                   "mgain": config['wsclean_image'].get('mgain'),
                                   "auto-threshold": config['wsclean_image'].get('auto_threshold'),
                                   "multiscale": config['wsclean_image'].get('multi_scale'),
                                   "multiscale-scales": sdm.dismissable(config['wsclean_image'].get('multi_scale_scales')),
                                   "no-update-model-required": config['wsclean_image'].get('no_update_mod')},
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{:s}:: re-Image HI_' + str(j).format(step))

                    if config['wsclean_image']['make_cube']:
                        if not config['wsclean_image'].get('niter'):
                            imagetype = ['image', 'dirty']
                        else:
                            imagetype = [
                                'image', 'dirty', 'psf', 'residual', 'model']
                            if config['wsclean_image'].get('mgain') < 1.0:
                                imagetype.append('first-residual')
                        for mm in imagetype:
                            step = 'make_{0:s}_cube'.format(
                                mm.replace('-', '_'))
                            recipe.add(
                                'cab/fitstool',
                                step,
                                {
                                    "image": [
                                        '{0:s}/{1:s}_{2:s}_HI_{3:s}-{4:04d}-{5:s}.fits:output'.format(
                                            img_dir,
                                            pipeline.prefix,
                                            field,
                                            str(j),
                                            d,
                                            mm)for d in xrange(nchans)],
                                    "output": '{0:s}/{1:s}_{2:s}_HI_{3:s}.{4:s}.fits'.format(
                                        img_dir,
                                        pipeline.prefix,
                                        field,
                                        str(j),
                                        mm),
                                    "stack": True,
                                    "delete-files": True,
                                    "fits-axis": 'FREQ',
                                },
                                input=pipeline.input,
                                output=pipeline.output,
                                label='{0:s}:: Make {1:s} cube from wsclean {1:s} channels'.format(
                                    step,
                                    mm.replace(
                                        '-',
                                        '_')))

                    recipe.run()
                    recipe.jobs = []
                    HIclean_mask_file = '{0:s}/{1:s}_{2:s}_HI_{3:d}.image_clean_mask.fits'.format(
                        image_path, pipeline.prefix, field, j)
                    cubename_file = '{0:s}/image_{1:d}/{2:s}_{3:s}_HI_{1:d}.image.fits'.format(
                        pipeline.cubes, j - 1, pipeline.prefix, field)
                    rms2 = calc_rms(cubename_file, HIclean_mask_file)
                    meerkathi.log.info(
                        'The updated rms = ' + str("{0:.7f}".format(rms2)) + ' Jy/beam')

                    if rms2 <= (
                            1.0 - tol) * rms_ref:  # if the noise changes by less than 10% keep_going
                        rms_ref = rms2
                        if j == wscl_niter:
                            meerkathi.log.info('The relative noise change is larger than ' + str("{0:.3f}".format(
                                (100.0 - (rms2 / rms_ref) * 100.0)) + '%. Noise convergence not achieved.'))
                            meerkathi.log.info(
                                'Maximum number of wsclean iterations reached.')
                        if j < wscl_niter:
                            meerkathi.log.info('The relative noise change is larger than ' + str("{0:.3f}".format(
                                (100.0 - (rms2 / rms_ref) * 100.0)) + '%. Noise convergence not achieved.'))

                        for ss in [
                            'dirty',
                            'psf',
                            'first-residual',
                            'residual',
                            'model',
                                'image']:
                            cubename = '{0:s}/{1:s}_{2:s}_HI_{3:s}.{4:s}.fits:output'.format(
                                img_dir, pipeline.prefix, field, str(j), ss)
                            MFScubename = '{0:s}/{1:s}_{2:s}_HI_{3:s}-MFS-{4:s}.fits:output'.format(
                                img_dir, pipeline.prefix, field, str(j), ss)

                            recipe.add(
                                fix_specsys,
                                'fix_specsys_{0:s}_cube'.format(ss),
                                {
                                    "filename": cubename,
                                    "specframe": specframe_all,
                                },
                                input=pipeline.input,
                                output=pipeline.output,
                                label='Fix spectral reference frame for cube {0:s}'.format(cubename))

                        if pipeline.enable_task(config, 'freq_to_vel'):
                            for ss in [
                                'dirty',
                                'psf',
                                'first-residual',
                                'residual',
                                'model',
                                    'image']:
                                cubename = '{0:s}/{1:s}_{2:s}_HI_{3:s}.{4:s}.fits:output'.format(
                                    img_dir, pipeline.prefix, field, str(j), ss)
                                MFScubename = '{0:s}/{1:s}_{2:s}_HI_{3:s}-MFS-{4:s}.fits:output'.format(
                                    img_dir, pipeline.prefix, field, str(j), ss)
                                recipe.add(
                                    freq_to_vel,
                                    'spectral_header_to_vel_radio_{0:s}_cube'.format(ss),
                                    {
                                        "filename": cubename,
                                        "reverse": config['freq_to_vel'].get('reverse')},
                                    input=pipeline.input,
                                    output=pipeline.output,
                                    label='Convert spectral axis from frequency to radio velocity for cube {0:s}'.format(cubename))
                        continue
                    else:
                        if rms2 >= (1.0 - tol) * rms_ref:
                            meerkathi.log.info('The relative noise change is less than ' + str("{0:.3f}".format(
                                (100.0 - (rms2 / rms_ref) * 100.0)) + '%. Noise convergence achieved.'))
                        break

            for ss in [
                'dirty',
                'psf',
                'first-residual',
                'residual',
                'model',
                    'image']:
                if 'dirty' in ss:
                    meerkathi.log.info('Preparing final cubes.')
                cubename = '{0:s}/{1:s}_{2:s}_HI_{3:d}.{4:s}.fits'.format(
                    image_path, pipeline.prefix, field, j, ss)
                finalcubename = '{0:s}/{1:s}_{2:s}_HI.{3:s}.fits'.format(
                    image_path, pipeline.prefix, field, ss)
                HIclean_mask_file = '{0:s}/{1:s}_{2:s}_HI_{3:d}.image_clean_mask.fits'.format(
                    image_path, pipeline.prefix, field, j)
                finalHIclean_mask_file = '{0:s}/{1:s}_{2:s}_HI.image_clean_mask.fits'.format(
                    image_path, pipeline.prefix, field)
                MFScubename = '{0:s}/{1:s}_{2:s}_HI_{3:d}-MFS-{4:s}.fits'.format(
                    image_path, pipeline.prefix, field, j, ss)
                finalMFScubename = '{0:s}/{1:s}_{2:s}_HI-MFS-{3:s}.fits'.format(
                    image_path, pipeline.prefix, field, ss)
                if os.path.exists(cubename):
                    os.rename(cubename, finalcubename)
                if os.path.exists(HIclean_mask_file):
                    os.rename(HIclean_mask_file, finalHIclean_mask_file)
                if os.path.exists(MFScubename):
                    os.rename(MFScubename, finalMFScubename)

            for j in range(1, wscl_niter):
                if config['wsclean_image'].get('rm_intcubes'):
                    for ss in [
                        'dirty',
                        'psf',
                        'first-residual',
                        'residual',
                        'model',
                            'image']:
                        cubename = '{0:s}/{1:s}_{2:s}_HI_{3:s}.{4:s}.fits'.format(
                            pipeline.cubes, pipeline.prefix, field, str(j), ss)
                        HIclean_mask_file = '{0:s}/{1:s}_{2:s}_HI_{3:s}.image_clean_mask.fits'.format(
                            pipeline.cubes, pipeline.prefix, field, str(j))
                        MFScubename = '{0:s}/{1:s}_{2:s}_HI_{3:s}-MFS-{4:s}.fits'.format(
                            pipeline.cubes, pipeline.prefix, field, str(j), ss)
                        if os.path.exists(cubename):
                            os.remove(cubename)
                        if os.path.exists(HIclean_mask_file):
                            os.remove(HIclean_mask_file)
                        if os.path.exists(MFScubename):
                            os.remove(MFScubename)

    if pipeline.enable_task(config, 'casa_image'):
        img_dir = get_dir_path(pipeline.cubes, pipeline)
        nchans_all, specframe_all = [], []
        label = config['label']
        if label != '':
            flabel = '_' + label
        else:
            flabel = label
        if config['wsclean_image'].get('use_mstransform'):
            all_targets, all_msfiles, ms_dict = target_to_msfiles(
                pipeline.target, pipeline.msnames, flabel, True)
            for i, msfile in enumerate(all_msfiles):
                if not pipeline.enable_task(config, 'mstransform'):
                    msinfo = '{0:s}/{1:s}-{2:s}-obsinfo.json'.format(
                        pipeline.output, pipeline.prefix, msfile[:-3])
                    meerkathi.log.info(
                        'Updating info from {0:s}'.format(msinfo))
                    with open(msinfo, 'r') as stdr:
                        spw = yaml.load(stdr)['SPW']['NUM_CHAN']
                        nchans = spw
                        nchans_all.append(nchans)
                    meerkathi.log.info('MS has {0:d} spectral windows, with NCHAN={1:s}'.format(
                        len(spw), ','.join(map(str, spw))))

                    # Get first chan, last chan, chan width
                    with open(msinfo, 'r') as stdr:
                        chfr = yaml.load(stdr)['SPW']['CHAN_FREQ']
                        firstchanfreq = [ss[0] for ss in chfr]
                        lastchanfreq = [ss[-1] for ss in chfr]
                        chanwidth = [(ss[-1] - ss[0]) / (len(ss) - 1)
                                     for ss in chfr]
                    meerkathi.log.info('CHAN_FREQ from {0:s} Hz to {1:s} Hz with average channel width of {2:s} Hz'.format(
                        ','.join(map(str, firstchanfreq)), ','.join(map(str, lastchanfreq)), ','.join(map(str, chanwidth))))

                    with open(msinfo, 'r') as stdr:
                        specframe = yaml.load(stdr)['SPW']['MEAS_FREQ_REF']
                        specframe_all.append(specframe)
                    meerkathi.log.info(
                        'The spectral reference frame is {0:}'.format(specframe))

                elif config['mstransform'].get('doppler'):
                    nchans_all[i] = [nchan_dopp for kk in chanw_all[i]]
                    specframe_all.append([{'lsrd': 0, 'lsrk': 1, 'galacto': 2, 'bary': 3, 'geo': 4, 'topo': 5}[
                                         config['mstransform'].get('outframe', 'bary')] for kk in chanw_all[i]])
        else:
            msinfo = '{0:s}/{1:s}-{2:s}-obsinfo.json'.format(
                pipeline.output, pipeline.prefix, msfile[:-3])
            with open(msinfo, 'r') as stdr:
                spw = yaml.load(stdr)['SPW']['NUM_CHAN']
                nchans = spw
                nchans_all.append(nchans)
            meerkathi.log.info('MS has {0:d} spectral windows, with NCHAN={1:s}'.format(
                len(spw), ','.join(map(str, spw))))
            with open(msinfo, 'r') as stdr:
                specframe = yaml.load(stdr)['SPW']['MEAS_FREQ_REF']
                specframe_all.append(specframe)
            meerkathi.log.info(
                'The spectral reference frame is {0:}'.format(specframe))

        spwid = config['casa_image'].get('spwid')
        nchans = config['casa_image'].get('nchans')
        if nchans == 0 or nchans == 'all':
            # Assuming user wants same spw for all msfiles and they have same
            # number of channels
            nchans = nchans_all[0][spwid]
        # Assuming user wants same spw for all msfiles and they have same
        # specframe
        specframe_all = [ss[spwid] for ss in specframe_all][0]
        firstchan = config['casa_image'].get('firstchan')
        binchans = config['casa_image'].get('binchans')
        if nchans != 'all':
            nchans = int(nchans)
        channelrange = [firstchan, firstchan + nchans * binchans]
        # Construct weight specification
        if config['casa_image'].get('weight') == 'briggs':
            weight = 'briggs {0:.3f}'.format(
                config['casa_image'].get('robust', robust))
        else:
            weight = config['casa_image'].get('weight', weight)

        for target in (all_targets):
            mslist = ms_dict[target]
            field = utils.filter_name(target)

            step = 'casa_image_HI'
            image_opts = {
                "msname": mslist,
                "prefix": '{0:s}/{1:s}_{2:s}_HI'.format(img_dir, pipeline.prefix, field),
                #                 "field"          :    target,
                "mode": 'channel',
                "nchan": nchans,
                "start": config['casa_image'].get('firstchan'),
                "interpolation": 'nearest',
                "niter": config['casa_image'].get('niter'),
                "psfmode": 'hogbom',
                "threshold": config['casa_image'].get('threshold'),
                "npix": config['casa_image'].get('npix'),
                "cellsize": config['casa_image'].get('cell'),
                "weight": config['casa_image'].get('weight'),
                "robust": config['casa_image'].get('robust'),
                "stokes": config['casa_image'].get('pol'),
                #                 "wprojplanes"    :    1,
                # was hardcoded to true
                "port2fits": config['casa_image'].get('port2fits'),
                "restfreq": restfreq,
            }
            if config['casa_image'].get('taper') != '':
                image_opts.update({
                    "uvtaper": True,
                    "outertaper": config['casa_image'].get('taper'),
                })
            recipe.add('cab/casa_clean', step, image_opts,
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{:s}:: Image HI'.format(step))

    for target in all_targets:
        mslist = ms_dict[target]
        field = utils.filter_name(target)

        if pipeline.enable_task(config, 'casa_image'):
            img_dir = get_dir_path(pipeline.cubes, pipeline)
        else:
            img_dir = finalcubename.split(
                '/{0:s}_{1:s}_HI.image.fits'.format(pipeline.prefix, field))[0]
            img_dir = img_dir.split('output/')[-1]

        if pipeline.enable_task(config, 'remove_stokes_axis'):
            for ss in [
                'dirty',
                'psf',
                'first-residual',
                'residual',
                'model',
                'image',
                    'flux']:
                cubename = '{0:s}/{1:s}_{2:s}_HI.{3:s}.fits:output'.format(
                    img_dir, pipeline.prefix, field, ss)
                recipe.add(remove_stokes_axis,
                           'remove_stokes_axis_{0:s}_cube'.format(ss),
                           {'filename': cubename,
                            },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Remove Stokes axis for cube {0:s}'.format(cubename))

        if pipeline.enable_task(config, 'pb_cube'):
            cubename = '{0:s}/{1:s}_{2:s}_HI.image.fits:output'.format(
                img_dir, pipeline.prefix, field)

            recipe.add(make_pb_cube,
                       'pb_cube',
                       {'filename': cubename,
                        'apply_corr': config['pb_cube'].get('apply_pb')},
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Make primary beam cube for {0:s}'.format(cubename))

        for ss in [
            'dirty',
            'psf',
            'first-residual',
            'residual',
            'model',
            'image',
            'pb',
            'pb_corr',
                'flux']:
            cubename = '{0:s}/{1:s}_{2:s}_HI.{3:s}.fits:output'.format(
                img_dir, pipeline.prefix, field, ss)
            recipe.add(fix_specsys,
                       'fix_specsys_{0:s}_cube'.format(ss),
                       {'filename': cubename,
                           'specframe': specframe_all,
                        },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Fix spectral reference frame for cube {0:s}'.format(cubename))

        if pipeline.enable_task(config, 'freq_to_vel'):
            if not config['freq_to_vel'].get('reverse'):
                meerkathi.log.info(
                    'Converting spectral axis of cubes from frequency to radio velocity')
            else:
                meerkathi.log.info(
                    'Converting spectral axis of cubes from radio velocity to frequency')
            for ss in [
                'dirty',
                'psf',
                'first-residual',
                'residual',
                'model',
                'image',
                'pb',
                'pb_corr',
                    'flux']:
                cubename = '{0:s}/{1:s}_{2:s}_HI.{3:s}.fits:output'.format(
                    img_dir, pipeline.prefix, field, ss)
                recipe.add(
                    freq_to_vel,
                    'spectral_header_to_vel_radio_{0:s}_cube'.format(ss),
                    {
                        'filename': cubename,
                        'reverse': config['freq_to_vel'].get('reverse')},
                    input=pipeline.input,
                    output=pipeline.output,
                    label='Convert spectral axis from frequency to radio velocity for cube {0:s}'.format(cubename))

        if pipeline.enable_task(config, 'sofia'):
            if pipeline.enable_task(config, 'casa_image'):
                out_dir = pipeline.cubes
            else:
                out_dir = finalcubename.split(
                    '/{0:s}_{1:s}_HI.image.fits'.format(pipeline.prefix, field))[0]

            step = 'sofia_sources'
            recipe.add(
                'cab/sofia',
                step,
                {
                    "import.inFile": '{0:s}_{1:s}_HI.image.fits:output'.format(
                        pipeline.prefix,
                        field),
                    "steps.doFlag": config['sofia'].get('flag'),
                    "steps.doScaleNoise": True,
                    "steps.doSCfind": True,
                    "steps.doMerge": config['sofia'].get('merge'),
                    "steps.doReliability": False,
                    "steps.doParameterise": False,
                    "steps.doWriteMask": True,
                    "steps.doMom0": config['sofia'].get('do_mom0'),
                    "steps.doMom1": config['sofia'].get('do_mom1'),
                    "steps.doCubelets": config['sofia'].get('do_cubelets'),
                    "steps.doWriteCat": False,
                    "flag.regions": config['sofia'].get('flagregion'),
                    "scaleNoise.statistic": config['sofia'].get('rmsMode'),
                    "SCfind.threshold": config['sofia'].get('threshold'),
                    "SCfind.rmsMode": config['sofia'].get('rmsMode'),
                    "merge.radiusX": config['sofia'].get('mergeX'),
                    "merge.radiusY": config['sofia'].get('mergeY'),
                    "merge.radiusZ": config['sofia'].get('mergeZ'),
                    "merge.minSizeX": config['sofia'].get('minSizeX'),
                    "merge.minSizeY": config['sofia'].get('minSizeY'),
                    "merge.minSizeZ": config['sofia'].get('minSizeZ'),
                },
                input=pipeline.input,
                output=out_dir,
                label='{0:s}:: Make SoFiA mask and images'.format(step))

    if pipeline.enable_task(config, 'sharpener'):
        step = 'continuum_spectral_extraction'

        if pipeline.enable_task(config, 'casa_image'):
            img_dir = get_dir_path(pipeline.cubes, pipeline)
        else:
            img_dir = finalcubename.split(
                '/{0:s}_{1:s}_HI.image.fits'.format(pipeline.prefix, field))[0]
            img_dir = img_dir.split('output/')[-1]

        params = {"enable_spec_ex": True,
                  "enable_source_catalog": True,
                  "enable_abs_plot": True,
                  "enable_source_finder": False,
                  "cubename": '{0:s}/{1:s}_{2:s}_HI.image.fits:output'.format(img_dir,
                                                                              pipeline.prefix, field),
                  "channels_per_plot": config['sharpener'].get('channels_per_plot'),
                  "workdir": '{0:s}/'.format(stimela.CONT_IO[recipe.JOB_TYPE]["output"]),
                  "label": config['sharpener'].get('label', pipeline.prefix)
                  }
        if config['sharpener'].get('catalog') == 'PYBDSF':
            catalogs = []
            nimages = glob.glob("{0:s}/image_*".format(pipeline.continuum))

            for ii in range(0, len(nimages)):
                catalog = glob.glob(
                    "{0:s}/image_{1:d}/{2:s}_{3:s}_*.lsm.html".format(
                        pipeline.continuum, ii + 1, pipeline.prefix, field))
                catalogs.append(catalog)

            catalogs = sorted(catalogs)
            catalogs = [cat for catalogs in catalogs for cat in catalogs]
            # Right now, this is the last catalog made
            catalog_file = catalogs[-1].split('output/')[-1]
            params["catalog_file"] = '{0:s}:output'.format(catalog_file)

            if len(catalog_file) > 0:

                params["catalog"] = "PYBDSF"
                recipe.add(
                    'cab/sharpener',
                    step,
                    params,
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Continuum Spectral Extraction'.format(step))
            else:
                # or should we force it onto NVSS?
                meerkathi.log.info(
                    'No PyBDSM catalogs found. Skipping continuum spectral extraction.')
        elif config['sharpener'].get('catalog') == 'NVSS':
            params["thresh"] = config['sharpener'].get('thresh')
            params["width"] = config['sharpener'].get('width')
            params["catalog"] = "NVSS"
            recipe.add(
                'cab/sharpener',
                step,
                params,
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Continuum Spectral Extraction'.format(step))

            recipe.run()
            recipe.jobs = []

    if pipeline.enable_task(config, 'sharpener'):
        # Move the sharpener output to diagnostic_plots
        sharpOut = '{0:s}/{1:s}'.format(pipeline.output, 'sharpOut')
        finalsharpOut = '{0:s}/{1:s}_{2:s}_{3:s}'.format(
            pipeline.diagnostic_plots, pipeline.prefix, field, 'sharpOut')
        if os.path.exists(finalsharpOut):
            shutil.rmtree(finalsharpOut)
        shutil.move(sharpOut, finalsharpOut)

    if pipeline.enable_task(config, 'flagging_summary'):
        for i, msname in enumerate(all_msfiles):
            step = 'flagging_summary_image_HI_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata',
                       step,
                       {"vis": msname,
                        "mode": 'summary',
                        },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Flagging summary  ms={1:s}'.format(step,
                                                                         msname))

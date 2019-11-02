import meerkathi.dispatch_crew.utils as utils
import yaml
import meerkathi
import sys
import numpy as np
from os import path

NAME = 'Automatically catergorize observed fields'


def worker(pipeline, recipe, config):
    if pipeline.virtconcat:
        msnames = [pipeline.vmsname]
        prefixes = [pipeline.prefix]
        nobs = 1
    else:
        msnames = pipeline.msnames
        prefixes = pipeline.prefixes
        nobs = pipeline.nobs

    for i in range(nobs):
        prefix = prefixes[i]
        msname = msnames[i]
        msroot = msname[:-3]

        if pipeline.enable_task(config, 'obsinfo'):
            if config['obsinfo'].get('listobs'):
                step = 'listobs_{:d}'.format(i)
                recipe.add('cab/casa_listobs', step,
                           {
                               "vis": msname,
                               "listfile": '{0:s}-obsinfo.txt'.format(msroot),
                               "overwrite": True,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Get observation information ms={1:s}'.format(step, msname))

            if config['obsinfo'].get('summary_json'):
                step = 'summary_json_{:d}'.format(i)
                recipe.add('cab/msutils', step,
                           {
                               "msname": msname,
                               "command": 'summary',
                               "display": False,
                               "outfile": '{0:s}-obsinfo.json'.format(msroot),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Get observation information as a json file ms={1:s}'.format(step, msname))

        if config['obsinfo'].get('vampirisms'):
            step = 'vampirisms_{0:d}'.format(i)
            recipe.add('cab/sunblocker', step,
                       {
                           "command": 'vampirisms',
                           "inset": msname,
                           "dryrun": True,
                           "nononsoleil": True,
                           "verb": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Note sunrise and sunset'.format(step))

        recipe.run()
        recipe.jobs = []

    # initialse things
    for item in 'xcal fcal bpcal gcal target reference_antenna'.split():
        val = config.get(item)
        if val and not isinstance(val, list):
            setattr(pipeline, item, [val]*pipeline.nobs)
        elif isinstance(val, list):
            setattr(pipeline, item, val)
        else:
            setattr(pipeline, item, [None]*pipeline.nobs)

    setattr(pipeline, 'TRA', [None]*pipeline.nobs)
    setattr(pipeline, 'TDec', [None]*pipeline.nobs)
    setattr(pipeline, 'nchans', [None]*pipeline.nobs)
    setattr(pipeline, 'firstchanfreq', [None]*pipeline.nobs)
    setattr(pipeline, 'lastchanfreq', [None]*pipeline.nobs)
    setattr(pipeline, 'chanwidth', [None]*pipeline.nobs)
    setattr(pipeline, 'specframe', [None]*pipeline.nobs)

    # Set antenna properties
    pipeline.Tsys_eta = config.get('Tsys_eta')
    pipeline.dish_diameter = config.get('dish_diameter')

    for item in 'xcal fcal bpcal gcal target'.split():
        setattr(pipeline, item + "_id", [])

    for i, prefix in enumerate(prefixes):
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, pipeline.dataid[i])
        meerkathi.log.info('Extracting info from {0:s}/{1:s}-obsinfo.json (if present) and {2:s}'.format(
            pipeline.data_path, pipeline.dataid[i], msinfo))

        # get reference antenna
        if config.get('reference_antenna') == 'auto':
            msmeta = '{0:s}/{1:s}-obsinfo.json'.format(
                pipeline.data_path, pipeline.dataid[i])
            if path.exists(msmeta):
                pipeline.reference_antenna[i] = utils.meerkat_refant(msmeta)
                meerkathi.log.info('Auto selecting reference antenna as {:s}'.format(
                    pipeline.reference_antenna[i]))
            else:
                meerkathi.log.error(
                    'Cannot auto select reference antenna because the file {0:s} does not exist.'.format(msmeta))
                meerkathi.log.error(
                    'Please set the reference antenna manually in the config file and try again.')
                sys.exit(1)

        # Get channels in MS
        with open(msinfo, 'r') as stdr:
            spw = yaml.safe_load(stdr)['SPW']['NUM_CHAN']
            pipeline.nchans[i] = spw
        meerkathi.log.info('MS has {0:d} spectral windows, with NCHAN={1:s}'.format(
            len(spw), ','.join(map(str, spw))))

        # Get first chan, last chan, chan width
        with open(msinfo, 'r') as stdr:
            chfr = yaml.safe_load(stdr)['SPW']['CHAN_FREQ']
            firstchanfreq = [ss[0] for ss in chfr]
            lastchanfreq = [ss[-1] for ss in chfr]
            chanwidth = [(ss[-1]-ss[0])/(len(ss)-1) for ss in chfr]
            pipeline.firstchanfreq[i] = firstchanfreq
            pipeline.lastchanfreq[i] = lastchanfreq
            pipeline.chanwidth[i] = chanwidth
            meerkathi.log.info('CHAN_FREQ from {0:s} Hz to {1:s} Hz with average channel width of {2:s} Hz'.format(
                ','.join(map(str, firstchanfreq)), ','.join(map(str, lastchanfreq)), ','.join(map(str, chanwidth))))
        if i == len(prefixes)-1 and np.max(pipeline.chanwidth) > 0 and np.min(pipeline.chanwidth) < 0:
            meerkathi.log.info(
                'Some datasets have positive channel increment, some others negative. This will lead to errors. Exiting')
            sys.exit(1)
        # Get spectral frame
        with open(msinfo, 'r') as stdr:
            pipeline.specframe[i] = yaml.safe_load(
                stdr)['SPW']['MEAS_FREQ_REF']

        # Auto select some/all fields if user didn't manually override all of them
        if 'auto' in [config[item] for item in 'fcal bpcal gcal target xcal'.split()]:
            intents = utils.categorize_fields(msinfo)
            # Get fields and their purposes
            fcals = intents['fcal'][-1]
            gcals = intents['gcal'][-1]
            bpcals = intents['bpcal'][-1]
            targets = intents['target'][-1]
            xcals = []
            # Set crosshand angle calibrator
            if config.get('xcal') == 'auto':
                if len(intents['xcal']) > 0:
                    # last on the list if auto
                    pipeline.xcal[i] = intents['xcal'][-1]
                else:
                    # empty string to be picked up as no cross phase calibrator
                    pipeline.xcal[i] = ""
                    meerkathi.log.warn(
                        "No crosshand angle reference calibrator specified. This calibration will not be performed.")
            else:
                pipeline.xcal[i] = config['xcal']  # user specified
            # Set gain calibrator
            if config['gcal'] == 'auto':
                pipeline.gcal[i] = utils.select_gcal(
                    msinfo, targets, gcals, mode='nearest')
                meerkathi.log.info(
                    'Auto selecting gain calibrator as {:s}'.format(pipeline.gcal[i]))
            else:
                pipeline.gcal[i] = config['gcal']
            tobs = utils.field_observation_length(
                msinfo, pipeline.gcal[i])/60.0
            meerkathi.log.info('Gain calibrator field "{0:s}" was observed for {1:.2f} minutes'.format(
                pipeline.gcal[i], tobs))

            # Set flux calibrator
            if config['fcal'] == 'auto':
                while len(fcals) > 0:
                    fcal = utils.observed_longest(msinfo, fcals)
                    if utils.find_in_casa_calibrators(msinfo, fcal) or utils.find_in_native_calibrators(msinfo, fcal):
                        pipeline.fcal[i] = fcal
                        break
                    fcals.remove(fcal)
                meerkathi.log.info(
                    'Auto selecting flux calibrator as {:s}'.format(pipeline.fcal[i]))
            else:
                pipeline.fcal[i] = config['fcal']
            tobs = utils.field_observation_length(
                msinfo, pipeline.fcal[i])/60.0
            meerkathi.log.info('Flux calibrator field "{0:s}" was observed for {1:.2f} minutes'.format(
                pipeline.fcal[i], tobs))

            # Set bandpass calibrator
            if config['bpcal'] == 'auto':
                pipeline.bpcal[i] = utils.observed_longest(msinfo, bpcals)
                meerkathi.log.info(
                    'Auto selecting bandpass calibrator field as {:s}'.format(pipeline.bpcal[i]))
            else:
                pipeline.bpcal[i] = config['bpcal']
            tobs = utils.field_observation_length(
                msinfo, pipeline.bpcal[i])/60.0
            meerkathi.log.info('Bandpass calibrator field "{0:s}" was observed for {1:.2f} minutes'.format(
                pipeline.bpcal[i], tobs))

            # Select target field(s)
            if config['target'] == 'auto':
                pipeline.target[i] = ','.join(targets)
                meerkathi.log.info(
                    'Auto selecting target field as {:s}'.format(pipeline.target[i]))
            else:
                targets = config['target'].split(',')
                pipeline.target[i] = ','.join(targets)
            meerkathi.log.info('Found {0:d} target fields {1:s}'.format(
                len(targets), pipeline.target[i]))
            for target in targets:
                tobs = utils.field_observation_length(msinfo, target)/60.0
                meerkathi.log.info(
                    'Target field "{0:s}" was observed for {1:.2f} minutes'.format(target, tobs))

        # Get the target RA and Dec
        with open(msinfo, 'r') as stdr:
            # WARNING: this sets a single RA,Dec value even in case of multiple targets (e.g., in a mosaic obs; in this case it takes the RA,Dec of the first target in the targets list).
            # A similar approach is taken by the split_target worker, which is hardcoded to split pipeline.target[i].split(',')[0] only
            targetinfo = yaml.safe_load(stdr)['FIELD']
            targetpos = targetinfo['REFERENCE_DIR'][targetinfo['NAME'].index(
                pipeline.target[i].split(',')[0])][0]
            pipeline.TRA[i] = targetpos[0]/np.pi*180.
            pipeline.TDec[i] = targetpos[1]/np.pi*180.
            meerkathi.log.info('Target RA, Dec for Doppler correction: {0:.3f} deg, {1:.3f} deg'.format(
                pipeline.TRA[i], pipeline.TDec[i]))

        # update ids for all fields now that auto fields were selected
        for item in 'xcal fcal bpcal gcal target'.split():
                setattr(pipeline, item+"_id", utils.get_field_id(msinfo, 
                        getattr(pipeline, item)[i]))
        else:
            flds = getattr(pipeline, 'target')[i].split(',') \
                if isinstance(getattr(pipeline, 'target')[i], str) else getattr(pipeline, 'target')[i]
            getattr(pipeline, "target_id").append(
                ','.join([str(utils.get_field_id(msinfo, f)) for f in flds]))

    if pipeline.enable_task(config, 'primary_beam'):
        meerkathi.log.info('Generating primary beam')
        recipe.add('cab/eidos', 'primary_beam',
                   {
                       "diameter": config['primary_beam'].get('diameter'),
                       "pixels": config['primary_beam'].get('pixels'),
                       "freq": config['primary_beam'].get('freq'),
                       "coeff": config['primary_beam'].get('coefficients', 'me'),
                       "prefix": pipeline.prefix,
                       "output-eight": True,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label="generate_primary_beam:: Generate primary beam")

        pipeline.primary_beam = pipeline.prefix + "-$\(xy\)_$\(reim).fits"
        pipeline.primary_beam_l_axis = "X"
        pipeline.primary_beam_m_axis = "Y"
        meerkathi.log.info('Primary beam registered as : \\ Pattern - {0:s}\
                                                         \\ l-axis  - {1:s}\
                                                         \\ m-axis  - {2:s}'.format(pipeline.primary_beam,
                                                                                    pipeline.primary_beam_l_axis,
                                                                                    pipeline.primary_beam_m_axis))

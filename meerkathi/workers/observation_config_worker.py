import meerkathi.dispatch_crew.utils as utils
import yaml
import meerkathi
import sys

NAME = 'Automatically catergorize observed fields'

def worker(pipeline, recipe, config):

    if pipeline.enable_task(config, 'obsinfo'):
        for i in range(pipeline.nobs):
            msname = pipeline.msnames[i]
            prefix = pipeline.prefixes[i]
            if config['obsinfo'].get('listobs', True):
                step = 'listobs_{:d}'.format(i)
                recipe.add('cab/casa_listobs', step,
                    {
                      "vis"         : msname,
                      "listfile"    : prefix+'-obsinfo.txt' ,
                      "overwrite"   : True,
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Get observation information ms={1:s}'.format(step, msname))
        
            if config['obsinfo'].get('summary_json', True):
                 step = 'summary_json_{:d}'.format(i)
                 recipe.add('cab/msutils', step,
                    {
                      "msname"      : msname,
                      "command"     : 'summary',
                      "outfile"     : prefix+'-obsinfo.json',
                    },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Get observation information as a json file ms={1:s}'.format(step, msname))

            if config['obsinfo'].get('vampirisms', False):
                step = 'vampirisms_{0:d}'.format(i)
                recipe.add('cab/sunblocker', step,
                    {
                        "command"     : 'vampirisms',
                        "inset"       : msname,
                        "dryrun"      : True,
                        "nononsoleil" : True,
                        "verb"        : True,
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                label='{0:s}:: Note sunrise and sunset'.format(step))

        recipe.run()
        recipe.jobs = []

    # itnitialse things
    for item in 'fcal bpcal gcal target reference_antenna nchans'.split():
        val = config.get(item, 'auto')
        if val and not isinstance(config[item], list):
            setattr(pipeline, item, [config[item]]*pipeline.nobs)
        elif isinstance(config[item], list):
            setattr(pipeline, item, config[item])
        else:
            setattr(pipeline, item, [None]*pipeline.nobs)

    # Set antenna properties
    pipeline.Tsys_eta = config.get('Tsys_eta', 22.0)
    pipeline.dish_diameter = config.get('dish_diameter', 13.5)
   
    for item in 'fcal bpcal gcal target'.split():
        setattr(pipeline, item + "_id", [])

    for i, prefix in enumerate(pipeline.prefixes):
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, prefix)

        # get reference antenna
        if config.get('reference_antenna', 'auto') == 'auto':
            msmeta = '{0:s}/{1:s}.json'.format(pipeline.data_path, pipeline.dataid[i])
            pipeline.reference_antenna[i] = utils.meerkat_refant(msmeta)
            meerkathi.log.info('Auto selecting reference antenna as {:s}'.format(pipeline.reference_antenna[i]))

        # Get channels in MS
        if config['nchans'] == 'auto':
            with open(msinfo, 'r') as stdr:
                spw = yaml.load(stdr)['SPW']['NUM_CHAN']
                pipeline.nchans[i] = spw
            meerkathi.log.info('MS has {0:d} spectral windows, with NCHAN={1:s}'.format(len(spw), ','.join(map(str, spw))))

        #Auto select some/all fields if user didn't manually override all of them
        if 'auto' in [config[item] for item in 'fcal bpcal gcal target'.split()]:
            intents = utils.categorize_fields(msinfo)
            # Get fields and their purposes
            fcals = intents['fcal'][-1]
            gcals = intents['gcal'][-1]
            bpcals = intents['bpcal'][-1]
            targets = intents['target'][-1]

            # Set gain calibrator
            if config['gcal'] == 'auto':
                pipeline.gcal[i] = utils.select_gcal(msinfo, targets, gcals, mode='nearest')
                meerkathi.log.info('Auto selecting gain calibrator as {:s}'.format(pipeline.gcal[i]))
            else:
                pipeline.gcal[i] = config['gcal']
            tobs = utils.field_observation_length(msinfo, pipeline.gcal[i])/60.0
            meerkathi.log.info('Gain calibrator field "{0:s}" was observed for {1:.2f} minutes'.format(pipeline.gcal[i], tobs))

            # Set flux calibrator
            if config['fcal'] == 'auto':
                while len(fcals)>0:
                    fcal = utils.observed_longest(msinfo, fcals)
                    if utils.find_in_casa_calibrators(msinfo, fcal) or utils.find_in_native_calibrators(msinfo, fcal):
                        pipeline.fcal[i] = fcal
                        break
                    fcals.remove(fcal)
                meerkathi.log.info('Auto selecting flux calibrator as {:s}'.format(pipeline.fcal[i]))
            else:
                pipeline.fcal[i] = config['fcal']
            tobs = utils.field_observation_length(msinfo, pipeline.fcal[i])/60.0
            meerkathi.log.info('Flux calibrator field "{0:s}" was observed for {1:.2f} minutes'.format(pipeline.fcal[i], tobs))

            # Set bandpass calibrator
            if config['bpcal'] == 'auto':
                pipeline.bpcal[i] = utils.observed_longest(msinfo, bpcals)
                meerkathi.log.info('Auto selecting bandpass calibrator field as {:s}'.format(pipeline.bpcal[i]))
            else:
                pipeline.bpcal[i] = config['bpcal']
            tobs = utils.field_observation_length(msinfo, pipeline.bpcal[i])/60.0
            meerkathi.log.info('Bandpass calibrator field "{0:s}" was observed for {1:.2f} minutes'.format(pipeline.bpcal[i], tobs))

            # Select target field(s)
            if config['target'] == 'auto':
                pipeline.target[i] = ','.join(targets)
                meerkathi.log.info('Auto selecting target field as {:s}'.format(pipeline.target[i]))
            else:
                targets = config['target'].split(',')
                pipeline.target[i] = ','.join(targets)
            meerkathi.log.info('Found {0:d} target fields {1:s}'.format(len(targets), pipeline.target[i]))
            for target in targets:
                tobs = utils.field_observation_length(msinfo, target)/60.0
                meerkathi.log.info('Target field "{0:s}" was observed for {1:.2f} minutes'.format(target, tobs))

        # update ids for all fields now that auto fields were selected
        for item in 'fcal bpcal gcal target'.split():
            flds =  getattr(pipeline, item)[i].split(',') \
                        if isinstance(getattr(pipeline, item)[i], str) else getattr(pipeline, item)[i]
            getattr(pipeline, item + "_id").append(','.join([str(utils.get_field_id(msinfo, f)) for f in flds]))

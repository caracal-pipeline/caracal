import meerkathi.dispatch_crew.utils as utils
import yaml
import meerkathi
import sys

NAME = 'Automatically catergorize observed fields'

def worker(pipeline, recipe, config):
    """Requires *-obsinfo.json file from get_data worker"""

    # itnitialse things
    for item in 'fcal bpcal gcal target reference_antenna'.split():
        val = config[item]
        if val and not isinstance(config[item], list):
            setattr(pipeline, item, [config[item]]*pipeline.nobs)
        elif isinstance(config[item], list):
            setattr(pipeline, item, config[item])
        else:
            setattr(pipeline, item, [None]*pipeline.nobs)
   
    pipeline.nchans = [None]*pipeline.nobs
            
    for i, prefix in enumerate(pipeline.prefixes):
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, prefix)
        intents = utils.categorize_fields(msinfo)

        # Get fields and their purposes
        fcals = intents['fcal'][-1]
        gcals = intents['gcal'][-1]
        bpcals = intents['bpcal'][-1]
        targets = intents['target'][-1]

        # Get channels in MS
        with open(msinfo, 'r') as stdr:
            pipeline.nchans[i] = yaml.load(stdr)['SPW']['NUM_CHAN'][0]
        
        # Set gain calibrator
        if config['gcal'] == 'auto':
            pipeline.gcal[i] = utils.select_gcal(msinfo, targets, gcals, mode='nearest')
            meerkathi.log.info('Auto selecting gain calibrator as {:s}'.format(pipeline.gcal[i]))
        else:
            pipeline.gcal[i] = config['gcal']
        tobs = utils.field_observation_length(msinfo, pipeline.gcal[i])/60.0
        meerkathi.log.info('Field "{0:s}" was observed for {1:.2f} minutes'.format(pipeline.gcal[i], tobs))
  
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
        meerkathi.log.info('Field "{0:s}" was observed for {1:.2f} minutes'.format(pipeline.fcal[i], tobs))
        
        # Set bandpass calibrator
        if config['bpcal'] == 'auto':
            pipeline.bpcal[i] = utils.observed_longest(msinfo, bpcals)
            meerkathi.log.info('Auto selecting bandpass calibrator field as {:s}'.format(pipeline.bpcal[i]))
        else:
            pipeline.bpcal[i] = onfig['bpcal']
        tobs = utils.field_observation_length(msinfo, pipeline.bpcal[i])/60.0
        meerkathi.log.info('Field "{0:s}" was observed for {1:.2f} minutes'.format(pipeline.bpcal[i], tobs))

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
            meerkathi.log.info('Targer field "{0:s}" was observed for {1:.2f} minutes'.format(target, tobs))


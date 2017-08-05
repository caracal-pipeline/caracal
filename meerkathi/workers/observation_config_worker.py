import meerkathi.dispatch_crew.utils as utils
import yaml
import meerkathi
import sys

NAME = 'Automatically catergorize observed fields'
####TODO(Sphe): 
# Log how long the fields were observed
# Revamp set_model in crosscal_worker to check fcal in native in CASA database, then 
# decide on best way to set model.
def worker(pipeline, recipe, config):
    "Requires *-obsinfo.json file from get_data worker"

    # itnitialse things
    pipeline.fcal = [None] * pipeline.nobs
    pipeline.bpcal = [None] * pipeline.nobs
    pipeline.gcal = [None] * pipeline.nobs
    pipeline.target = [None] * pipeline.nobs
    pipeline.reference_antenna = [None] * pipeline.nobs

    for i, prefix in enumerate(pipeline.prefixes):
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, prefix)
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
#       meerkathi.log.info('Field "{0:s}" was observed for {1:.2f} minutes'.format(pipeline.gcal[i], tobs))
  
        # Set flux calibrator
        if config['fcal'] == 'auto':
            pipeline.fcal[i] = utils.observed_longest(msinfo, fcals)
            meerkathi.log.info('Auto selecting flux calibrator as {:s}'.format(pipeline.fcal[i]))
        else:
            pipeline.fcal[i] = config['fcal']
        meerkathi.log.info('Checking if flux calibrator is in our database or in the CASA NRAO database')
        found = False
        if 
        
        # Set bandpass calibrator
        if config['bpcal'] == 'auto':
            pipeline.bpcal[i] = utils.observed_longest(msinfo, bpcals)
            meerkathi.log.info('Auto selecting bandpass calibrator field as {:s}'.format(pipeline.bpcal[i]))

        if config['target'] == 'auto':
            pipeline.target[i] = ','.join(targets)
            meerkathi.log.info('Auto selecting target field as {:s}'.format(pipeline.target[i]))
        else:
            pipeline.target[i] = config['target']


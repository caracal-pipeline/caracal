from  argparse import ArgumentParser
import sys
import os
import stimela
import yaml
import glob

pckgdir = os.path.dirname(os.path.abspath(__file__))


class MeerKATHI(object):
    def __init__(self, config, workers_directory, 
            stimela_build=None, prefix=None, 
            add_all_first=False):
        
        with open(config) as _conf:
            self.config = yaml.load(_conf)

        self.add_all_first = add_all_first

        self.msdir = self.config['general']['msdir']
        self.input = self.config['general']['input']
        self.output = self.config['general']['output']
        self.data_url = self.config['general']['data_url']
        self.data_path = self.config['general']['data_path']

        self.workers_directory = workers_directory
        # Add workers to packages
        sys.path.append(self.workers_directory)
        self.workers = []

        for i, (name,opts) in enumerate(self.config.iteritems()):
            if name.find('general')>=0:
                continue
            order = opts.get('order', i+1)

            if name.find('-'):
                worker = name.split('-')[0] + '_worker'
            else: 
                worker = name + '_worker'

            self.workers.append((name, worker, order))

        self.workers = sorted(self.workers, key=lambda a: a[2])
        
        self.dataid = self.config['general']['dataid']
        self.nobs = len(self.dataid)

        self.fcal = self.config['general']['fcal']
        self.bpcal = self.config['general']['bpcal']
        self.gcal = self.config['general']['gcal']
        self.target = self.config['general']['target']
        self.refant = self.config['general']['reference_antenna']
        self.nchans = self.config['general']['nchans']

        for item in 'fcal bpcal gcal target refant'.split():
            value = getattr(self, item, None)
            # First ensure that value is set is a list
            if value is None:
                raise RuntimeError('Parameter \'{:s}\' under general section has not been set'.format(item))
            elif not hasattr(value, '__iter__'):
                value = [value]
            # Duplicate value if its not a list
            if value and len(value)==1:
                setattr(self, item, value*self.nobs)

        self.prefix = prefix or self.config['general']['prefix']
        self.stimela_build = stimela_build
        self.recipes = {}
        # Workers to skip
        self.skip = [] 

        self._init_names()

    def _init_names(self):
        self.h5files = ['{:s}.h5'.format(dataid) for dataid in self.dataid]
        self.msnames = ['{:s}.ms'.format(os.path.basename(dataid)) for dataid in self.dataid]
        self.split_msnames = ['{:s}_split.ms'.format(os.path.basename(dataid)) for dataid in self.dataid]
        self.cal_msnames = ['{:s}_cal.ms'.format(os.path.basename(dataid)) for dataid in self.dataid]
        self.prefixes = ['meerkathi-{:s}'.format(os.path.basename(dataid)) for dataid in self.dataid]

    def enable_task(self, config, task):
        a = config.get(task, False)
        if a:
            return a['enable']
        else:
            False

    def run_workers(self):
        for _name, _worker, i in self.workers:
            try:
                worker = __import__(_worker)
            except ImportError:
                raise ImportError('Worker "{0:s}" could not be found at {1:s}'.format(_worker, self.workers_directory)) 

            config = self.config[_worker.split('_worker')[0]]
            if config['enable'] is False:
                self.skip.append(_worker)
                continue
            # Define stimela recipe instance for worker
            # Also change logger name to avoid duplication of logging info
            recipe = stimela.Recipe(worker.NAME, ms_dir=self.msdir, 
                               loggername='STIMELA-{:d}'.format(i), 
                               build_label=self.stimela_build)
            # Get recipe steps
            # 1st get correct section of config file
            worker.worker(self, recipe, config)
            # Save worker recipes for later execution
            # execute each worker after adding its steps
            if self.add_all_first:
                self.recipes[_worker] = recipe
            else:
                recipe.run()

        # Execute all workers if they saved for later execution
        if self.add_all_first:
            for worker in self.workers:
                if worker not in self.skip:
                    self.recipes[worker[1]].run()


def main(argv):
    parser = ArgumentParser(description='MeerKAT HI pipeline : https://github.com/sphemakh/meerkathi \n \
Options set on the command line will overwrite options in the --pipeline-configuration file')
    add = parser.add_argument

    add('-gd', '--get-default',
        help='Name file where the configuration should be saved')

    add('-pc', '--pipeline-configuration', 
        help='Pipeline configuarion file (YAML/JSON format)')
    
    add('-aaf', '--add-all-first', action='store_true',
        help='Add steps from all workers to pipeline before exucting. Default is execute each workers as they are encountered.')

    add('-id', '--input', 
        help='Pipeline input directory')

    add('-od', '--output', 
        help='Pipeline output directory')

    add('-md', '--msdir',
        help='Pipeline MS directory. All MSs, for a given pipeline run, should/will be placed here')

    add('-bl', '--stimela-build', 
        help='Label of stimela build to use')

    add('-wd', '--workers-directory', default='{:s}/workers'.format(pckgdir),
        help='Directory where pipeline workers can be found. These are stimela recipes describing the pipeline')

    add('-dp', '--data-path', action='append',
        help='Path where data can be found. This is where the file <dataid>.h5 should be located. Can be specified multiple times if --dataid(s) have different locations')

    add('-du', '--data-url', action='append',
        help='URL where data can be found. This is where the file <dataid>.h5 should be located. Can be specified multiple times if --dataid(s) have different locations')

    add('-di', '--dataid', action='append',
        help='Data ID of hdf5 file to be reduced. May be specified muliple times. Must be used in combination with --data-path')

    add('-p', '--prefix',
        help='Prefix for pipeline output products')

    add('-ra', '--reference-antenna', action='append',
        help='Reference antenna. Can be specified multiple times if reference antenna is different for different --dataid(s)')

    add('-fc', '--fcal', action='append', type=int,
        help='Field ID of Flux calibrator source/field. Can be specified multiple times if different for different --dataid(s)')

    add('-bc', '--bpcal', action='append', type=int,
        help='Field ID of Bandpass calibrator source/field. Can be specified multiple times if different for different --dataid(s)')
    
    add('-gc', '--gcal', action='append', type=int,
        help='Field ID of gain calibrator source/field. Can be specified multiple times if different for different --dataid(s)')
 
    add('-t', '--target', action='append', type=int,
        help='Field ID of target field. Can be specified multiple times if different for different --dataid(s)')

    args = parser.parse_args(argv)

    if args.get_default:
        os.system('cp {0:s}/default-config.yml {1:s}'.format(pckgdir, args.get_default))
        return

    pipeline = MeerKATHI(args.pipeline_configuration,
                  args.workers_directory, stimela_build=args.stimela_build, 
                  add_all_first=args.add_all_first, prefix=args.prefix)

    for item in 'input msdir output'.split():
        value = getattr(args, item, None)
        if value:
            setattr(pipeline, item, value)

    dataids = args.dataid
    if dataids is None:
        with open(args.pipeline_configuration) as _conf:
            dataids = yaml.load(_conf)['general']['dataid']
    else:
        pipeline.dataid = dataids

    nobs = len(dataids)
    for item in 'data_path data_url reference_antenna fcal bpcal gcal target'.split():
        value = getattr(args, item, None)
        if value and len(value)==1:
            value = value*nobs
            setattr(pipeline, item, value)
    
    pipeline._init_names()
    pipeline.run_workers()

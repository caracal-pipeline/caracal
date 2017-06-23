from  argparse import ArgumentParser
import sys
import os
import stimela
import yaml
import glob

pckgdir = os.path.dirname(os.path.abspath(__file__))


class MeerKATHI(object):
    def __init__(self, config, workers_directory, stimela_build=None):
        
        with open(config) as _conf:
            self.config = yaml.load(_conf)
        
        self.msdir = self.config['general']['msdir']
        self.input = self.config['general']['input']
        self.output = self.config['general']['output']
        self.data_path = self.config['general']['data_paths']

        self.workers_directory = workers_directory
        # Add workers to packages
        sys.path.append(self.workers_directory)
        workers = glob.glob('{:s}/*_worker.py'.format(self.workers_directory))
        workers = [ os.path.basename(a).split('.py')[0] for a in workers]
        # Order workers
        self.workers = range(len(workers))
        for worker in workers:
            j = self.config[worker.split('_worker')[0]]['order']
            self.workers[j-1] = worker
        
        self.dataids = self.config['general']['dataids']
        self.nobs = len(self.dataids)

        self.bpcal = self.config['general']['bpcal']
        self.gcal = self.config['general']['gcal']
        self.target = self.config['general']['target']
        self.refant = self.config['general']['reference_antenna']

        for item in 'bpcal gcal target refant'.split():
            value = getattr(self, item, None)
            # First ensure that value is set is a list
            if value is None:
                raise RuntimeError('Parameter \'{:s}\' under general section has not been set'.format(item))
            elif hasattr(value, '__iter__'):
                value = [value]
            # Duplicate value if its not a list
            if value and len(value)==1:
                setattr(self, item, value*self.nobs)

        self.prefix = self.config['general']['prefix']
        self.dataids = self.config['general']['dataids']

        self.h5files = ['{:s}.h5'.format(dataid) for dataid in self.dataids]
        self.msnames = ['{:s}.ms'.format(os.path.basename(dataid)) for dataid in self.dataids]
        self.split_msnames = ['{:s}_split.ms'.format(os.path.basename(dataid)) for dataid in self.dataids]
        self.cal_msnames = ['{:s}_cal.ms'.format(os.path.basename(dataid)) for dataid in self.dataids]
        self.prefixes = ['meerkathi-{:s}'.format(os.path.basename(dataid)) for dataid in self.dataids]
        
        self.stimela_build = stimela_build
        self.recipes = {}
        # Workers to skip
        self.skip = [] 

    def define_workers(self):
        for i, _worker in enumerate(self.workers):
            worker = __import__(_worker)
            name = worker.NAME
            config = self.config[_worker.split('_worker')[0]]
            if config['enable'] is False:
                self.skip.append(_worker)
                continue
            # Define stimela recipe instance for worker
            # Also change logger name to avoid duplication of logging info
            recipe = stimela.Recipe(name, ms_dir=self.msdir, 
                               loggername='STIMELA-{:d}'.format(i), 
                               build_label=self.stimela_build)
            # Get recipe steps
            # 1st get correct section of config file
            steps = worker.worker(self, recipe, config)
            self.recipes[_worker] = (recipe, steps)

    def run_workers(self):
        for worker in self.workers:
            if worker not in self.skip:
                self.recipes[worker][0].run(self.recipes[worker][1])


def main(argv):
    parser = ArgumentParser(description='MeerKAT HI pipeline : https://github.com/sphemakh/meerkathi \n \
Options set on the command line will overwrite options in the --pipeline-configuration file')
    add = parser.add_argument

    add('-pc', '--pipeline-configuration', 
        help='Pipeline configuarion file (YAML/JSON format)')

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

    add('-di', '--dataid', action='append',
        help='Data ID of hdf5 file to be reduced. May be specified muliple times. Must be used in combination with --data-path')

    add('-p', '--prefix', default='meerkathi-pipeline',
        help='Prefix for pipeline output products')

    add('-ra', '--reference-antenna', action='append',
        help='Reference antenna. Can be specified multiple times if reference antenna is different for different --dataid(s)')

    add('-bc', '--bandpass-cal', action='append', type=int,
        help='Field ID of Bandpass calibrator source/field. Can be specified multiple times if different for different --dataid(s)')
    
    add('-gc', '--gain-cal', action='append', type=int,
        help='Field ID of gain calibrator source/field. Can be specified multiple times if different for different --dataid(s)')
 
    add('-t', '--target', action='append', type=int,
        help='Field ID of target field. Can be specified multiple times if different for different --dataid(s)')

    args = parser.parse_args(argv)

    pipeline = MeerKATHI(args.pipeline_configuration,
                  args.workers_directory, stimela_build=args.stimela_build)

    for item in 'input msdir output'.split():
        value = getattr(args, item, None)
        if value:
            setattr(pipeline, item, value)

    dataids = args.dataid
    if dataids is None:
        with open(args.pipeline_configuration) as _conf:
            dataids = yaml.load(_conf)['general']['dataids']

    nobs = len(dataids)
    for item in 'data_path prefix reference_antenna bandpass_cal gain_cal target'.split():
        value = getattr(args, item, None)
        if value and len(value)==1:
            value = value*nobs
            setattr(pipeline, item, value)

    pipeline.define_workers()
    pipeline.run_workers()

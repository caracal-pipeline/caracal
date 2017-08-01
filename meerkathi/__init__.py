# -*- coding: utf-8 -*-

import sys
import os

pckgdir = os.path.dirname(os.path.abspath(__file__))

import stimela
import glob
from meerkathi.dispatch_crew.config_parser import config_parser as cp
import meerkathi.__version__ as __version__
import logging
import traceback
import meerkathi.dispatch_crew.caltables as mkct

MEERKATHI_LOG = os.path.join(os.getcwd(), "meerkathi.log")

def create_logger():
    """ Create a console logger """
    log = logging.getLogger(__name__)
    cfmt = logging.Formatter(('%(name)s - %(asctime)s %(levelname)s - %(message)s'))
    log.setLevel(logging.DEBUG)
    filehandler = logging.FileHandler(MEERKATHI_LOG)
    filehandler.setFormatter(cfmt)
    log.addHandler(filehandler)
    log.setLevel(logging.INFO)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(cfmt)

    log.addHandler(console)

    return log

# Create the log object
log = create_logger()

class MeerKATHI(object):
    def __init__(self, config, workers_directory, 
            stimela_build=None, prefix=None, 
            add_all_first=False):

        self.config = config

        self.add_all_first = add_all_first

        self.msdir = self.config['general']['msdir']
        self.input = self.config['general']['input']
        self.output = self.config['general']['output']
        self.data_url = self.config['general']['data_url']
        self.data_path = self.config['general']['data_path']
        self.download_mode = self.config['general']['download_mode']
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
        """ Runs the  workers """
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
                log.info("Adding worker %s before running" % _worker)
                self.recipes[_worker] = recipe
            else:
                log.info("Running worker %s" % _worker)
                recipe.run()

        # Execute all workers if they saved for later execution
        if self.add_all_first:
            for worker in self.workers:
                if worker not in self.skip:
                    self.recipes[worker[1]].run()


def main(argv):
    log.info("")

    log.info("███╗   ███╗███████╗███████╗██████╗ ██╗  ██╗ █████╗ ████████╗██╗  ██╗██╗")
    log.info("████╗ ████║██╔════╝██╔════╝██╔══██╗██║ ██╔╝██╔══██╗╚══██╔══╝██║  ██║██║")
    log.info("██╔████╔██║█████╗  █████╗  ██████╔╝█████╔╝ ███████║   ██║   ███████║██║")
    log.info("██║╚██╔╝██║██╔══╝  ██╔══╝  ██╔══██╗██╔═██╗ ██╔══██║   ██║   ██╔══██║██║")
    log.info("██║ ╚═╝ ██║███████╗███████╗██║  ██║██║  ██╗██║  ██║   ██║   ██║  ██║██║")
    log.info("╚═╝     ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═╝")
    log.info("")
    # parse config file and set up command line argument override parser
    log.info("Module installed at: %s (version %s)" % (pckgdir, str(__version__.version)))
    log.info("A logfile will be dumped here: %s" % MEERKATHI_LOG)
    log.info("")
    args = cp(argv).args
    arg_groups = cp(argv).arg_groups
    # User requests default config => dump and exit
    if args.get_default:
        log.info("Dumping default configuration to %s as requested. Goodbye!" % args.get_default)
        os.system('cp {0:s}/default-config.yml {1:s}'.format(pckgdir, args.get_default))
        return

    # Very good idea to print user options into the log before running:
    cp().log_options()

    # Obtain some divine knowledge
    mkct.calibrator_database()

    try:
        pipeline = MeerKATHI(arg_groups,
                             args.workers_directory, stimela_build=args.stimela_build,
                             add_all_first=args.add_all_first, prefix=args.general_prefix)

        for item in 'input msdir output'.split():
            value = getattr(arg_groups["general"], item, None)
            if value:
                setattr(pipeline, item, value)

        dataids = args.general_dataid
        if dataids is None:
            dataids = arg_groups['general']['dataid']
        else:
            pipeline.dataid = dataids

        nobs = len(dataids)
        for item in 'data_path data_url reference_antenna fcal bpcal gcal target'.split():
            value = getattr(arg_groups["general"], item, None)
            if value and len(value)==1:
                value = value*nobs
                setattr(pipeline, item, value)

        pipeline._init_names()
        pipeline.run_workers()
    except:
        log.error("Whoops... there has explosion - you sent pipes flying all over the show! Time to call in the monkeywrenchers.")
        log.error("Your logfile is here: %s. You are running version: %s" % (MEERKATHI_LOG, str(__version__.version)))
        tb = traceback.format_exc()
        log.error(tb)

import sys, os, string
import traceback
from datetime import datetime
import stimela
import glob
from meerkathi.dispatch_crew.config_parser import config_parser as cp
import logging
import traceback
import meerkathi.dispatch_crew.caltables as mkct
from SimpleHTTPServer import SimpleHTTPRequestHandler
from BaseHTTPServer import HTTPServer
from multiprocessing import Process
import webbrowser
import base64
from urllib import urlencode
import ruamel.yaml
from ruamel.yaml.comments import CommentedMap, CommentedKeySeq
assert ruamel.yaml.version_info >= (0, 12, 14)
import json
import subprocess
from meerkathi.dispatch_crew import worker_help

from meerkathi import log, pckgdir

try:
    import meerkathi.scripts as scripts
    from meerkathi.scripts import reporter as mrr
    REPORTS = True
except ImportError:
    log.warning("Modules for creating pipeline disgnostic reports are not installed. Please install \"meerkathi[extra_diagnostics]\" if you want these reports")
    REPORTS = False



class worker_administrator(object):
    def __init__(self, config, workers_directory,
            stimela_build=None, prefix=None, configFileName=None,
            add_all_first=False, singularity_image_dir=None,
            container_tech='docker'):

        self.config = config
        self.add_all_first = add_all_first
        self.singularity_image_dir = singularity_image_dir
        self.container_tech = container_tech
        self.msdir = self.config['general']['msdir']
        self.input = self.config['general']['input']
        self.output = self.config['general']['output']
        self.logs = self.config['general']['output'] + '/logs'
        self.reports = self.config['general']['output'] + '/reports'
        self.diagnostic_plots = self.config['general']['output'] + '/diagnostic_plots'
        self.configFolder = self.config['general']['output'] + '/cfgFiles'
        self.caltables = self.config['general']['output'] + '/caltables'
        self.masking = self.config['general']['output'] + '/masking'
        self.continuum = self.config['general']['output'] + '/continuum'
        self.cubes = self.config['general']['output'] + '/cubes'

        print self.config['general']['msdir']

        sys.exit(0)
        
        if not self.config['general']['data_path']:
            self.config['general']['data_path'] = os.getcwd()
            self.data_path = self.config['general']['data_path']
        else:
            self.data_path = self.config['general']['data_path']

        self.virtconcat = False
        self.workers_directory = workers_directory
        # Add workers to packages
        sys.path.append(self.workers_directory)
        self.workers = []

        for i, (name,opts) in enumerate(self.config.iteritems()):
            if name.find('general')>=0:
                continue
            order = opts.get('order', i+1)

            if name.find('__')>=0:
                worker = name.split('__')[0] + '_worker'
            else:
                worker = name + '_worker'

            self.workers.append((name, worker, i))

        self.workers = sorted(self.workers, key=lambda a: a[2])

        self.prefix = prefix or self.config['general']['prefix']
        self.stimela_build = stimela_build

        # Get possible flagsets for reduction
        self.flagsets = {"legacy" : ["legacy"]}
        for _name, _worker, i in self.workers:
            try:
                wkr = __import__(_worker)
            except ImportError:
                traceback.print_exc()
                raise ImportError('Worker "{0:s}" could not be found at {1:s}'.format(_worker, self.workers_directory))

            if hasattr(wkr, "FLAGSETS_SUFFIX"):
                self.flagsets[_name] = ["_".join([_name, suffix]) if suffix else _name for suffix in wkr.FLAGSETS_SUFFIX]

        self.recipes = {}
        # Workers to skip
        self.skip = []
        # Initialize empty lists for ddids, leave this up to get data worker to define
        self.init_names([])
        if config["general"].get("init_pipeline"):
            self.init_pipeline()
            
        # save configuration file
        timeNow = datetime.now()
        configFileName = string.split(str(configFileName),'.')[0]
        outConfigName = '{:s}_{:%Y%m%d-%H%M}.yml'.format(configFileName,timeNow)

        with open(self.configFolder+'/'+outConfigName, 'w') as outfile:
            ruamel.yaml.dump(self.config, outfile, Dumper=ruamel.yaml.RoundTripDumper)

    def init_names(self, dataid):
        """ iniitalize names to be used throughout the pipeline and associated
            general fields that must be propagated
        """
        self.dataid = dataid
        self.nobs = nobs = len(self.dataid)
        self.h5files = ['{:s}.h5'.format(dataid) for dataid in self.dataid]
        self.msnames = ['{:s}.ms'.format(os.path.basename(dataid)) for dataid in self.dataid]
        self.split_msnames = ['{:s}_split.ms'.format(os.path.basename(dataid)) for dataid in self.dataid]
        self.cal_msnames = ['{:s}_cal.ms'.format(os.path.basename(dataid)) for dataid in self.dataid]
        self.hires_msnames = ['{:s}_hires.ms'.format(os.path.basename(dataid)) for dataid in self.dataid]
        self.prefixes = ['{0:s}-{1:s}'.format(self.prefix,os.path.basename(dataid)) for dataid in self.dataid]

        for item in 'input msdir output'.split():
            value = getattr(self, item, None)
            if value:
                setattr(self, item, value)

        for item in 'data_path reference_antenna fcal bpcal gcal target xcal'.split():
            value = getattr(self, item, None)
            if value and len(value)==1:
                value = value*nobs
                setattr(self, item, value)

    def set_cal_msnames(self, label):
        if self.virtconcat:
            self.cal_msnames = ['{0:s}{1:s}.ms'.format(msname[:-3].split("SUBMSS/")[-1], "-"+label if label else "") for msname in self.msnames]
        else:
            self.cal_msnames = ['{0:s}{1:s}.ms'.format(msname[:-3], "-"+label if label else "") for msname in self.msnames]

    def set_hires_msnames(self, label):
        if self.virtconcat:
            self.hires_msnames = ['{0:s}{1:s}.ms'.format(msname[:-3].split("SUBMSS/")[-1], "-"+label if label else "") for msname in self.msnames]
        else:
            self.hires_msnames = ['{0:s}{1:s}.ms'.format(msname[:-3], "-"+label if label else "") for msname in self.msnames]

    def init_pipeline(self):
        # First create input folders if they don't exist
        if not os.path.exists(self.input):
            os.mkdir(self.input)
        if not os.path.exists(self.output):
            os.mkdir(self.output)
        if not os.path.exists(self.data_path):
            os.mkdir(self.data_path)
        if not os.path.exists(self.logs):
            os.mkdir(self.logs)
        if not os.path.exists(self.reports):
            os.mkdir(self.reports)
        if not os.path.exists(self.diagnostic_plots):
            os.mkdir(self.diagnostic_plots)
        if not os.path.exists(self.configFolder):
            os.mkdir(self.configFolder)
        if not os.path.exists(self.caltables):
            os.mkdir(self.caltables)
        if not os.path.exists(self.masking):
            os.mkdir(self.masking)
        if not os.path.exists(self.continuum):
            os.mkdir(self.continuum)
        if not os.path.exists(self.cubes):
            os.mkdir(self.cubes)

        # Copy input data files into pipeline input folder
        log.info("Copying meerkat input files into input folder")
        for _f in os.listdir("{0:s}/data/meerkat_files".format(pckgdir)):
            f = "{0:s}/data/meerkat_files/{1:s}".format(pckgdir, _f)
            if not os.path.exists("{0:}/{1:s}".format(self.input, _f)):
                subprocess.check_call(["cp", "-r", f, self.input])

        #Copy fields for masking in input/fields/.
        log.info("Copying fields for masking into input folder")
        for _f in os.listdir("{0:s}/data/meerkat_files/".format(pckgdir)):
            f = "{0:s}/data/meerkat_files/{1:s}".format(pckgdir, _f)
            if not os.path.exists("{0:}/{1:s}".format(self.input, _f)):
                subprocess.check_call(["cp", "-r", f, self.input])

    def enable_task(self, config, task):
        a = config.get(task)
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
                traceback.print_exc()
                raise ImportError('Worker "{0:s}" could not be found at {1:s}'.format(_worker, self.workers_directory))

            config = self.config[_name]
            if config.get('enable') is False:
                self.skip.append(_worker)
                continue
            # Define stimela recipe instance for worker
            # Also change logger name to avoid duplication of logging info
            recipe = stimela.Recipe(worker.NAME, ms_dir=self.msdir,
                               loggername='STIMELA-{:d}'.format(i),
                               build_label=self.stimela_build,
                               singularity_image_dir=self.singularity_image_dir)

            recipe.JOB_TYPE = self.container_tech
            self.CURRENT_WORKER = _name
            # Don't allow pipeline-wide resume
            # functionality
            os.system('rm -f {}'.format(recipe.resume_file))
            # Get recipe steps
            # 1st get correct section of config file
            worker.worker(self, recipe, config)
            # Save worker recipes for later execution
            # execute each worker after adding its steps

            if self.add_all_first:
                log.info("Adding worker {0:s} before running".format(_worker))
                self.recipes[_worker] = recipe
            else:
                log.info("Running worker {0:s}".format(_worker))
                recipe.run()

        # Execute all workers if they saved for later execution
        try:
            if self.add_all_first:
                for worker in self.workers:
                    if worker not in self.skip:
                        self.recipes[worker[1]].run()
        finally: # write reports even if the pipeline only runs partially
            if REPORTS:
                reporter = mrr(self)
                reporter.generate_reports()

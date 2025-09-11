import caracal
import sys
import os
import traceback
import shutil
from datetime import datetime
import stimela
from functools import partial
from caracal import log, pckgdir, notebooks
import glob
from caracal import log
from caracal.utils import ObjDict
import re
from caracal.utils.basetypes import (
    File,
    Directory,
    MS,
)
from typing import (
    Dict,
    Callable,
    List,
)
from dataclasses import dataclass
from caracal.workers import PIPELINE_MIN_REQUIRES
from caracal.workers.utils import (
    make_symlink,
)
from caracal.workers import pipeline_class_functions

import ruamel.yaml
assert ruamel.yaml.version_info >= (0, 12, 14)

REPORTS = True

@dataclass
class Pipeline:
    """
    CARCal pipeline instance for one dataid. Having a Pipeline per dataid makes it easier to distribute pipeline runs across mutliptle nodes.
    """
    config_dict: Dict
    obsid: int 
    workers_directory: Directory = None
    generate_reports: bool = True
    container_tech: str = "singularity"
    container_image_dir: Directory = None
    
    def __post_init__(self):
        """
        Initialize CARACal Pipeline instance. Effectively, this runs the general, getdata and obsconf workers, which do the setup for all other workers.

        Raises:
            caracal.ConfigurationError: Error in the Pipeline configuration file
        """
        # Add worker scripts/recipes to the Python context 
        if self.workers_directory in [None, ""]:
            self.workers_directory = os.path.dirname(__file__)
        sys.path.append(self.workers_directory)
       
        self.config = config = ObjDict.from_dict(self.config_dict)
        
        self.prep_workspace = config.general.prep_workspace
        self.input = config.general.input
        self.output = config.general.output
        self.msdir = config.general.msdir
        self.rawdatadir = config.general.rawdatadir
        self.prefix = config.general.prefix
        
        if self.rawdatadir in [None, ""]:
            self.rawdatadir = self.msdir
        self.timeNow = '{:%Y%m%d-%H%M%S}'.format(datetime.now())
        
        # add pipeline functions from the pipeline_class_functions.py script
        # doing it this way to keep the this class definition short
        for _attr_str in dir(pipeline_class_functions):
            _attr = getattr(pipeline_class_functions, _attr_str)
            if isinstance(_attr, Callable) and not _attr_str.startswith('__'):
                setattr(self, _attr_str, partial(_attr, self) )
                
        self.cabspecs = self.parse_cabspec_dict(self.config_dict["general"]["cabs"])
            
        self.logs_symlink = f'{self.output}/logs'
        self.logs = f"{self.logs_symlink}-{self.timeNow}"
        
        self.obsinfo = f'{self.output}/obsinfo'
        self.reports = f'{self.output}/reports'
        self.diagnostic_plots = f'{self.output}/diagnostic_plots'
        self.configFolder = f'{self.output}/cfgFiles'
        self.caltables = f'{self.output}/caltables'
        self.masking = f'{self.output}/masking'
        self.continuum = f'{self.output}/continuum'
        self.crosscal_continuum = f'{self.output}/continuum/crosscal'
        self.cubes = f'{self.output}/cubes'
        self.mosaics = f'{self.output}/mosaics'
        
        # create required input folders if they don't exist
        if not os.path.exists(self.input):
            os.mkdir(self.input)
        if not os.path.exists(self.output):
            os.mkdir(self.output)
        if not os.path.exists(self.rawdatadir):
            raise caracal.ConfigurationError(f"{self.rawdatadir} does not exist, check your general config section")
        if not os.path.exists(self.obsinfo):
            os.mkdir(self.obsinfo)
        if not os.path.exists(self.logs):
            os.mkdir(self.logs)
        log.info("output directory for logs is {}".format(self.logs))
        make_symlink(self.logs_symlink, os.path.basename(self.logs))
        if not os.path.exists(self.reports):
            os.mkdir(self.reports)
        if not os.path.exists(self.diagnostic_plots):
            os.mkdir(self.diagnostic_plots)
        if not os.path.exists(self.configFolder):
            os.mkdir(self.configFolder)
            
        CARACAL_LOG_BASENAME = 'log-caracal.txt'
        caracal.CARACAL_LOG = os.path.join(self.logs, CARACAL_LOG_BASENAME)
        caracal.log_filehandler.setFilename(caracal.CARACAL_LOG, delay=False)

        # placing a symlink into logs to appease Josh
        make_symlink(os.path.join(self.output, CARACAL_LOG_BASENAME),
                    os.path.join(os.path.basename(self.logs), CARACAL_LOG_BASENAME))

        # Copy input data files into pipeline input folder
        if self.prep_workspace:
            log.info("Copying MeerKAT input files into input folder")
            datadir = "{0:s}/data/meerkat_files".format(pckgdir)
            for filename in os.listdir(datadir):
                src = os.path.join(datadir, filename)
                dest = os.path.join(self.input, filename)
                if not os.path.exists(dest):
                    if os.path.isdir(src):
                        shutil.copytree(src, dest)
                    else:
                        shutil.copy2(src, dest, follow_symlinks=False)

        # Copy standard notebooks
        self._init_notebooks = self.config.general.init_notebooks
        self._report_notebooks = self.config.general.report_notebooks
        all_nbs = set(self._init_notebooks) | set(self._report_notebooks)
        if all_nbs:
            notebooks.setup_default_notebooks(all_nbs, output_dir=self.output, prefix=self.prefix, config=self.config_dict)
       
        self.__getdata__()
        self.add_worker("obsconf")
        self.run_worker("obsconf")

    def __getdata__(self):
        self.ms_extension = self.config.getdata.extension
        self.ignore_missing = self.config.getdata.ignore_missing
        self._msinfo_cache = {}
        
        self.dataid = dataid = self.config.getdata.dataid[self.obsid]
        if not dataid:
            raise caracal.ConfigurationError(f"Empty 'getdata: dataid' entry")
        
        msname = f"{dataid}.{self.ms_extension}"
        mspath = os.path.join(self.rawdatadir, msname)

        if not os.path.exists(mspath):
            if self.ignore_missing:
                log.warning(f"'{mspath}' did not match any files, but getdata: ignore_missing is set, proceeding anyway")
            else:
                raise caracal.ConfigurationError(f"'{msname}' did not match any files under {self.rawdatadir}. Check your "
                                                "'general: msdir/rawdatadir' and/or 'getdata: dataid/extension' settings, or "
                                                "set 'getdata: ignore_missing: true'")
        msbase = os.path.splitext(msname)[0]
        self.msname = msname
        self.msbasename = msbase
        self.prefix_msbase = f"{self.prefix}-{msbase}"
            
        self.CURRENT_WORKER = "getdata"
        
        self.add_worker("getdata")
        self.run_worker("getdata")
    
    def add_worker(self, name:str):
        """
        Add worker configuration to Pipeline

        Args:
            name (str): Worker name
            config (Dict): Worker runtime configuration
        """
       
        # check if repeat call of a worker 
        re_match = re.match(r'([^_]+)__\d+', name)
        if re_match:
            module = __import__(f"{re_match.groups()[0]}_worker" )
        else:
            module = __import__(f"{name}_worker" )
        
        
        worker_obj = getattr(self.config, name)
        
        setattr(worker_obj, "worker_module",  module)
        # create directories required by the worker
        for dirlabel in getattr(module, "DIRECTORIES", []):
            dirname = getattr(self, dirlabel, None)
            if dirname is None:
                raise caracal.ConfigurationError("Requested worker directory label, '{dirlabel}', is not known.")
            elif os.path.exists(dirname):
                os.mkdir(dirname)
            
    def run_worker(self, name):
        worker = getattr(self.config, name)
        
        self.CURRENT_WORKER = name
        
        missing_requires = []
        worker_requires = getattr(worker.worker_module, "PIPELINE_REQUIRES", PIPELINE_MIN_REQUIRES)
        
        for worker_req in worker_requires:
            if not hasattr(self, worker_req):
                missing_requires.append(worker_req)
        if missing_requires:
            raise caracal.ConfigurationError(f"Worker cannot be added to the pipeline. "
                                             f"Missing required attribute(s): {missing_requires}")

        worker_cabspecs = getattr(worker, "cabs", {})
        cabspecs = dict(self.cabspecs)
        cabspecs.update(worker_cabspecs)
        
        recipe = stimela.Recipe(name,
                                JOB_TYPE=self.container_tech,
                                singularity_image_dir=self.container_image_dir,
                                log_dir=self.logs,
                                cabspecs=cabspecs,
                                logfile=False,  # no logfiles for recipes
                                logfile_task=f'{self.logs}/log-{name}-{{task}}-{self.timeNow}.txt')
        
        worker.worker_module.worker(self, recipe, self.config_dict[name])
         
        if recipe.jobs:
            if name in ["getdata", "obsconf"]:
                recipe.run()
            recipe.close()
            
        del self.CURRENT_WORKER


class WorkerAdministrator(object):
    def __init__(self, config:Dict, workers_directory,
                prefix=None, configFileName=None,
                add_all_first=False, singularity_image_dir=None,
                start_worker=None, end_worker=None,
                container_tech='docker', generate_reports=True):

        self.config = ObjDict.from_dict(config)
        self.config_file = configFileName
        self.singularity_image_dir = singularity_image_dir
        self.container_tech = container_tech
        for key in "msdir input output".split():
            if not self.config['general'].get(key):
                raise caracal.ConfigurationError(f"'general: {key}' must be specified")
            
        pipeline = Pipeline(
            input=self.config.general.input,
            output=self.config.general.output,
            msdir=self.config.general.msdir,
            rawdatadir=self.config.general.rawdatadir,
            prefix=prefix,
            workers_directory=workers_directory,
            generate_reports=generate_reports,
        )

        self._msinfo_cache = {}
        self.virtconcat = False
        self.workers = []
        
        last_mandatory = 2  # index of last mendatory worker
        # general, getdata and obsconf are all mendatory.
        # That's why the lowest starting index is 2 (third element)
        start_idx = last_mandatory
        end_idx = len(self.config.keys())
        workers = []

        if start_worker and start_worker not in self.config.keys():
            raise RuntimeError("Requested --start-worker '{0:s}' is unknown. Please check your options".format(start_worker))
        if end_worker and end_worker not in self.config.keys():
            raise RuntimeError("Requested --end-worker '{0:s}' is unknown. Please check your options".format(end_worker))
        for i,name in enumerate(self.config):
            if name.find('general') >= 0 or name == "schema_version":
                continue
            if name.find('__') >= 0:
                worker = name.split('__')[0] + '_worker'
            else:
                worker = name + '_worker'
            if name == start_worker and name == end_worker:
                start_idx = len(workers)
                end_idx = len(workers)
            elif name == start_worker:
                start_idx = len(workers)
            elif name == end_worker:
                end_idx = len(workers)
            workers.append((name, worker, i))

        if end_worker in list(self.config.keys())[:last_mandatory + 1]:
            # no need for +1 this time since 'general' was removed from
            # this list
            self.workers = workers[:last_mandatory]
        else:
            start_idx = max(start_idx, last_mandatory)
            end_idx = max(end_idx, last_mandatory)
            self.workers = workers[:last_mandatory] + workers[start_idx:end_idx + 1]

        # Get possible flagsets for reduction
        self.flags = {"legacy": ["legacy"]}
        for _name, _worker, i in self.workers:
            try:
                wkr = __import__(_worker)
            except ImportError:
                traceback.print_exc()
                raise ImportError('Worker "{0:s}" could not be found at {1:s}'.format(
                    _worker, self.workers_directory))

            if hasattr(wkr, "FLAG_NAMES"):
                self.flags[_name] = ["_".join(
                    [_name, suffix]) if suffix else _name for suffix in wkr.FLAG_NAMES]

        self.recipes = {}
        # Workers to skip
        self.skip = []
        # Initialize empty lists for ddids, leave this up to getdata worker to define
        self.dataid = []
        # names of all MSs
        self.msnames = []
        # basenames of all MSs (sans extension)
        self.msbasenames = []
        # filename prefixes for outputs (formed up as prefix-msbase)
        self.prefix_msbases = []

        config_base = os.path.splitext(os.path.basename(configFileName))[0]
        outConfigOrigName = f'{self.configFolder}/{config_base}-{self.timeNow}.orig.yml'
        outConfigName = f'{self.configFolder}/{config_base}-{self.timeNow}.yml'

        log.info(f"Saving original configuration file as {outConfigOrigName}")
        shutil.copyfile(configFileName, outConfigOrigName)  # original config

        log.info(f"Saving full configuration as {outConfigName}")
        with open(outConfigName, 'w') as outfile:           # config+command line
            ruamel.yaml.dump(self.config, outfile, Dumper=ruamel.yaml.RoundTripDumper)


    def run_workers(self):
        """ Runs the  workers """
        report_updated = False

        for _name, _worker, i in self.workers:
            try:
                worker = __import__(_worker)
            except ImportError:
                traceback.print_exc()
                raise ImportError('Worker "{0:s}" could not be found at {1:s}'.format(
                    _worker, self.workers_directory))

        if self.config["general"]["cabs"]:
            log.info("Configuring cab specification overrides")
            cabspecs_general = self.parse_cabspec_dict(self.config["general"]["cabs"])
        else:
            cabspecs_general = {}
        active_workers = []
        # first, check that workers import, and check their configs
        for _name, _worker, i in self.workers:
            config = self.config[_name]
            if 'enable' in config and not config['enable']:
                self.skip.append(_worker)
                continue
            log.info("Configuring worker {}".format(_name))
            try:
                worker = __import__(_worker)
            except ImportError:
                log.error('Error importing worker "{0:s}" from {1:s}'.format(_worker, self.workers_directory))
                raise
            if hasattr(worker, 'check_config'):
                worker.check_config(config, name=_name)
            # check for cab specs
            cabspecs = cabspecs_general
            if config["cabs"]:
                cabspecs = cabspecs.copy()
                cabspecs.update(self.parse_cabspec_dict(config["cabs"]))
            active_workers.append((_name, worker, config, cabspecs))

        # now run the actual pipeline
        # for _name, _worker, i in self.workers:
        for _name, worker, config, cabspecs in active_workers:
            # Define stimela recipe instance for worker
            # Also change logger name to avoid duplication of logging info
            label = getattr(worker, 'LABEL', None)
            if label is None:
                # if label is not set, take filename, and split off _worker.py
                label = os.path.basename(worker.__file__).rsplit("_", 1)[0]
            # if worker name has a __suffix, add that to label
            if "__" in _name:
                label += "__" + _name.split("__", 1)[1]

            recipe = stimela.Recipe(label,
                                    ms_dir=self.msdir,
                                    singularity_image_dir=self.singularity_image_dir,
                                    log_dir=self.logs,
                                    cabspecs=cabspecs,
                                    logfile=False,  # no logfiles for recipes
                                    logfile_task=f'{self.logs}/log-{label}-{{task}}-{self.timeNow}.txt')

            recipe.JOB_TYPE = self.container_tech
            self.CURRENT_WORKER = _name
            # Don't allow pipeline-wide resume
            # functionality
            os.system('rm -f {}'.format(recipe.resume_file))
            # Get recipe steps
            # 1st get correct section of config file
            log_label = "" if _name == label or _name.startswith(label + "__") else f" ({label})"
            log.info(f"{_name}{log_label}: initializing", extra=dict(color="GREEN"))
            worker.worker(self, recipe, config)
            log.info(f"{_name}{log_label}: running")
            recipe.run()
            log.info(f"{_name}{log_label}: finished")

            # this should be in the cab cleanup code, no?

            casa_last = glob.glob(self.output + '/*.last')
            for file_ in casa_last:
                os.remove(file_)

            # update report at end of worker if so configured
            if self.generate_reports and config["report"]:
                self.regenerate_reports()
                report_updated = True
            else:
                report_updated = False

        # generate final report
        if self.config["general"]["final_report"] and self.generate_reports and not report_updated:
            self.regenerate_reports()

        log.info("pipeline run complete")

    def regenerate_reports(self):
        notebooks.generate_report_notebooks(self._report_notebooks, self.output, self.prefix, self.container_tech)

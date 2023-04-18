# -*- coding: future_fstrings -*-
from caracal.dispatch_crew import utils
from collections import OrderedDict
import caracal
from caracal import log, pckgdir, notebooks
import sys
import os
from datetime import datetime
import stimela
import glob
import shutil
import traceback
import itertools

import ruamel.yaml
assert ruamel.yaml.version_info >= (0, 12, 14)


REPORTS = True


class WorkerAdministrator(object):
    def __init__(self, config, workers_directory,
                 prefix=None, configFileName=None,
                 add_all_first=False, singularity_image_dir=None,
                 start_worker=None, end_worker=None,
                 container_tech='docker', generate_reports=True):

        self.config = config
        self.config_file = configFileName
        self.singularity_image_dir = singularity_image_dir
        self.container_tech = container_tech
        for key in "msdir input output".split():
            if not self.config['general'].get(key):
                raise caracal.ConfigurationError(f"'general: {key}' must be specified")

        self.msdir = self.config['general']['msdir']
        self.input = self.config['general']['input']
        self.output = self.config['general']['output']
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
        self.generate_reports = generate_reports
        self.timeNow = '{:%Y%m%d-%H%M%S}'.format(datetime.now())
        self.ms_extension = self.config["getdata"]["extension"]
        self.ignore_missing = self.config["getdata"]["ignore_missing"]

        self._msinfo_cache = {}

        self.logs_symlink = f'{self.output}/logs'
        self.logs = "{}-{}".format(self.logs_symlink, self.timeNow)

        self.rawdatadir = self.config['general']['rawdatadir']
        if not self.rawdatadir:
            self.rawdatadir = self.config['general']['rawdatadir'] = self.msdir

        self.virtconcat = False
        self.workers_directory = workers_directory
        # Add workers to packages
        if workers_directory:
            sys.path.append(self.workers_directory)
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
        for i, (name, opts) in enumerate(self.config.items()):
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

        self.prefix = prefix or self.config['general']['prefix']

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

        # OMS skipping this here, leave it to the getdata
        # self.init_names([], allow_empty=True)
        self.init_pipeline(prep_input=config["general"]["prep_workspace"])
        # save configuration files

        config_base = os.path.splitext(os.path.basename(configFileName))[0]
        outConfigOrigName = f'{self.configFolder}/{config_base}-{self.timeNow}.orig.yml'
        outConfigName = f'{self.configFolder}/{config_base}-{self.timeNow}.yml'

        log.info(f"Saving original configuration file as {outConfigOrigName}")
        shutil.copyfile(configFileName, outConfigOrigName)  # original config

        log.info(f"Saving full configuration as {outConfigName}")
        with open(outConfigName, 'w') as outfile:           # config+command line
            ruamel.yaml.dump(self.config, outfile, Dumper=ruamel.yaml.RoundTripDumper)

    def init_names(self, dataids):
        """ iniitalize names to be used throughout the pipeline and associated
            general fields that must be propagated
        """
        # OMS: this was a very elaborate no-op, away with it
        # for item in 'rawdatadir input msdir output'.split():
        #     value = getattr(self, item, None)
        #     if value:
        #         setattr(self, item, value)

        self.dataid = list(filter(bool, dataids))
        if not self.dataid:
            raise caracal.ConfigurationError(f"Empty 'getdata: dataid' entry")
        patterns = [f"{dataid}.{self.ms_extension}" for dataid in self.dataid]

        for pattern in patterns:
            msnames = [os.path.basename(ms) for ms in glob.glob(os.path.join(self.rawdatadir, pattern))]
            if not msnames:
                if self.ignore_missing:
                    log.warning(f"'{pattern}' did not match any files, but getdata: ignore_missing is set, proceeding anyway")
                else:
                    raise caracal.ConfigurationError(f"'{pattern}' did not match any files under {self.rawdatadir}. Check your "
                                                     "'general: msdir/rawdatadir' and/or 'getdata: dataid/extension' settings, or "
                                                     "set 'getdata: ignore_missing: true'")
            msbases = [os.path.splitext(ms)[0] for ms in msnames]
            self.msnames += msnames
            self.msbasenames += msbases
            self.prefix_msbases += [f"{self.prefix}-{x}" for x in msbases]
        self.nobs = len(self.msnames)

        if not self.nobs:
            raise caracal.ConfigurationError(f"No matching input data found in {self.rawdatadir} for {','.join(patterns)}. Check your "
                                             " 'general: msdir/rawdatadir' and/or 'getdata: dataid/extension' settings.")

        for item in 'refant fcal bpcal gcal target xcal'.split():
            value = getattr(self, item, None)
            if value and len(value) == 1:
                value = value * self.nobs
                setattr(self, item, value)

    def get_msinfo(self, msname):
        """Returns info dict corresponding to an MS. Caches and reloads as needed"""
        msinfo_file = os.path.splitext(msname)[0] + "-summary.json"
        msinfo_path = os.path.join(self.msdir, msinfo_file)
        msdict, mtime_cache = self._msinfo_cache.get(msname, (None, 0))
        if not os.path.exists(msinfo_path):
            raise RuntimeError(f"MS summary file {msinfo_file} not found at expected location. This is a bug or "
                               "a misconfiguration. Was the MS transformed properly?")
        # reload cached dict if file on disk is newer
        mtime = os.path.getmtime(msinfo_path)
        if msdict is None or mtime > mtime_cache:
            with open(msinfo_path, 'r') as f:
                msdict = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader)
            self._msinfo_cache[msname] = msdict, mtime
        return msdict

    # The following three methods provide MS naming services for workers

    def form_msname(self, msbase, label=None, field=None):
        """
        Given a base MS name, an optional label, and an optional field name, return the full MS name
        """
        label = '' if not label else '-' + label
        field = '' if not field else '-' + utils.filter_name(field)
        return f'{msbase}{field}{label}.{self.ms_extension}'

    def get_mslist(self, iobs, label="", target=False):
        """
        Given an MS number (0...nobs-1), and an optional label, returns list of corresponding MSs.
        If target is True, this will be one MS per each (split-out) target.
        If target is False, the list will contain just the single MS.
        Applies label in both cases.
        """
        msbase = self.msbasenames[iobs]
        if target:
            return [self.form_msname(msbase, label, targ) for targ in self.target[iobs]]
        else:
            return [self.form_msname(msbase, label)]

    def get_target_mss(self, label=None):
        """
        Given an MS label, returns a tuple of unique_targets, all_mss, mss_per_target
        Where all_mss is a list of all MSs to be processed for all targets, and mss_per_target maps target field
        to associated list of MSs
        """
        target_msfiles = OrderedDict()
        # self.target is a list of lists of targets, per each MS
        for msbase, targets in zip(self.msbasenames, self.target):
            for targ in targets:
                target_msfiles.setdefault(targ, []).append(self.form_msname(msbase, label, targ))
        # collect into flat list of MSs
        target_ms_ls = list(itertools.chain(*target_msfiles.values()))
        return list(target_msfiles.keys()), target_ms_ls, target_msfiles

    def get_callib_name(self, name, ext="yml", extra_label=None):
        """Makes a callib name with the given extension. Replaces extension if needed. Adds callib- if needed."""
        name, _ = os.path.splitext(name)
        if not name.startswith("callib-"):
            name = f"callib-{name}"
        if extra_label:
            name = f"{name}-{extra_label}"
        return os.path.join(self.caltables, f"{name}.{ext}")

    def load_callib(self, name):
        """Loads calibration library specified by name"""
        filename = self.get_callib_name(name)
        if not os.path.exists(filename):
            raise IOError(f"Calibration library {filename} doesn't exist")
        with open(filename, 'r') as f:
            return ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader)

    def save_callib(self, callib, name):
        """Dumps caldict to calibration library specified by name"""
        with open(self.get_callib_name(name), 'w') as f:
            ruamel.yaml.dump(callib, f, ruamel.yaml.RoundTripDumper)

    def parse_cabspec_dict(self, cabspec_seq):
        """Turns sequence of cabspecs into a Stimela cabspec dict"""
        cabspecs = OrderedDict()
        speclists = OrderedDict()
        # collect all specs encountered, sort them by cab
        for spec in cabspec_seq:
            name, version, tag = spec["name"], spec.get("version") or None, spec.get("tag") or None
            if not version and not tag:
                log.warning(f"Neither version nor tag specified for cabspec {name}, ignoring")
                continue
            speclists.setdefault(name, []).append((version, tag))
        # now process each cab's list of specs.
        for name, speclist in speclists.items():
            if len(speclist) == 1:
                version, tag = speclist[0]
                if version is None:
                    log.info(f"  {name}: forcing tag {tag} for all invocations")
                    cabspecs[name] = dict(tag=tag, force=True)
                    continue
                elif tag is None:
                    log.info(f"  {name}: forcing version {version} for all invocations")
                    cabspecs[name] = dict(version=version)
                    continue
            # else make dict of version: tag pairs
            cabspecs[name] = dict(version={version: tag for version, tag in speclist}, force=True)
            for version, tag in speclist:
                log.info(f"  {name}: using tag {tag} for version {version}")
        return cabspecs

    def init_pipeline(self, prep_input=True):
        def make_symlink(link, target):
            if os.path.lexists(link):
                if os.path.islink(link):
                    os.unlink(link)  # old symlink can go
                else:
                    log.warning("{} already exists and is not a symlink, can't relink".format(link))
                    return False
            if not os.path.lexists(link):
                os.symlink(target, link)
                log.info("{} links to {}".format(link, target))

        # First create input folders if they don't exist
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
        if not os.path.exists(self.caltables):
            os.mkdir(self.caltables)
        if not os.path.exists(self.masking):
            os.mkdir(self.masking)
        if not os.path.exists(self.continuum):
            os.mkdir(self.continuum)
        if not os.path.exists(self.cubes):
            os.mkdir(self.cubes)
        # create proper logfile and start flushing
        # NB (Oleg): placing this into output rather than output/logs to make the reporting notebooks easier
        CARACAL_LOG_BASENAME = 'log-caracal.txt'
        caracal.CARACAL_LOG = os.path.join(self.logs, CARACAL_LOG_BASENAME)
        caracal.log_filehandler.setFilename(caracal.CARACAL_LOG, delay=False)

        # placing a symlink into logs to appease Josh
        make_symlink(os.path.join(self.output, CARACAL_LOG_BASENAME),
                     os.path.join(os.path.basename(self.logs), CARACAL_LOG_BASENAME))

        # Copy input data files into pipeline input folder
        if prep_input:
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
        self._init_notebooks = self.config['general']['init_notebooks']
        self._report_notebooks = self.config['general']['report_notebooks']
        all_nbs = set(self._init_notebooks) | set(self._report_notebooks)
        if all_nbs:
            notebooks.setup_default_notebooks(all_nbs, output_dir=self.output, prefix=self.prefix, config=self.config)

    def enable_task(self, config, task):
        return task in config and config[task].get("enable")

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

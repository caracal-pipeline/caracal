# -*- coding: future_fstrings -*-

import pkg_resources
import os
import subprocess
import logging
from time import gmtime, strftime
import stimela
import stimela.utils

##############################################################################
# Globals
##############################################################################

class CaracalException(RuntimeError):
    """Base class for pipeline logic errors"""
    pass

class UserInputError(CaracalException):
    """Something wrong with user input"""
    pass

class ConfigurationError(CaracalException):
    """Something wrong with the configuration"""
    pass

class BadDataError(CaracalException):
    """Something wrong with the data"""
    pass

def report_version():
    # Distutils standard  way to do version numbering
    try:
        __version__ = pkg_resources.require("caracal")[0].version
    except pkg_resources.DistributionNotFound:
        __version__ = "dev"
    # perhaps we are in a github with tags; in that case return describe
    path = os.path.dirname(os.path.abspath(__file__))
    try:
        # work round possible unavailability of git -C
        result = subprocess.check_output(
            'cd %s; git describe --tags' % path, shell=True, stderr=subprocess.STDOUT).rstrip().decode()
    except subprocess.CalledProcessError:
        result = None
    if result != None and 'fatal' not in result:
        # will succeed if tags exist
        return result
    else:
        # perhaps we are in a github without tags? Cook something up if so
        try:
            result = subprocess.check_output(
                'cd %s; git rev-parse --short HEAD' % path, shell=True, stderr=subprocess.STDOUT).rstrip().decode()
        except subprocess.CalledProcessError:
            result = None
        if result != None and 'fatal' not in result:
            return __version__+'-'+result
        else:
            # we are probably in an installed version
            return __version__


__version__ = report_version()

# global settings
pckgdir = os.path.dirname(os.path.abspath(__file__))
# this gets renamed once the config is read in
CARACAL_LOG = "log-caracal.txt"

DEFAULT_CONFIG = os.path.join(
    pckgdir, "sample_configurations", "minimalConfig.yml")
SCHEMA = os.path.join(
    pckgdir, "schema", "schema-{0:s}.yml".format(__version__))

################################################################################
# Logging
################################################################################

import logging.handlers

class DelayedFileHandler(logging.handlers.MemoryHandler):
    """A DelayedFileHandler is a variation on the MemoryHandler. It will buffer up log
    entries until told to stop delaying, then dumps everything into the target file
    and from then on logs continuously. This allows the log file to be switched at startup."""
    def __init__(self, filename=None, delay=True):
        logging.handlers.MemoryHandler.__init__(self, 100000, target=filename and logging.FileHandler(filename, delay=True))
        self._delay = delay

    def shouldFlush(self, record):
        return not self._delay

    def setFilename(self, filename, delay=False):
        self._delay = delay
        target = logging.FileHandler(filename)
        target.setFormatter(self.formatter)
        for filt in self.filters:
            target.addFilter(filt)
        target.setLevel(self.level)
        self.setTarget(target)
        if not delay:
            self.flush()


LOGGER_NAME = "CARACal"
STIMELA_LOGGER_NAME = "CARACal.Stimela"
DEBUG = 0

log = logging.getLogger(LOGGER_NAME)

# these will be set up by init_logger
log_filehandler = log_console_handler = log_console_formatter = None

def create_logger():
    """ Creates logger and associated objects. Called upon import"""
    global log, log_filehandler

    log.setLevel(logging.DEBUG)
    log.propagate = False

    # init stimela logger as a sublogger
    if stimela.is_logger_initialized():
        raise RuntimeError("Stimela logger already initialized. This is a bug: you must have an incompatible version of Stimela.")

    stimela.logger(STIMELA_LOGGER_NAME, propagate=True, console=False)

    log_filehandler = DelayedFileHandler()

    log_filehandler.setFormatter(stimela.log_boring_formatter)
    log_filehandler.setLevel(logging.INFO)

    log.addHandler(log_filehandler)


def init_console_logging(boring=False, debug=False):
    """Sets up console logging"""
    global log_console_handler, log_console_formatter, log_filehandler, DEBUG

    DEBUG = debug
    log_filehandler.setLevel(logging.DEBUG if debug else logging.INFO)

    log_console_formatter = stimela.log_boring_formatter if boring else stimela.log_colourful_formatter

    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_console_formatter)

    # add filter to console handler:
    # (the logfile still gets all the messages)
    if not debug:
        def _console_filter(rec):
            # traceback dumps don't go to cosnole
            if getattr(rec, 'traceback_report', None) or getattr(rec, 'logfile_only', None):
                return False
            # for Stimela messages at level <=INFO, only allow through subprocess  output and job state
            if rec.name.startswith(STIMELA_LOGGER_NAME) and rec.levelno <= logging.INFO:
                if hasattr(rec, 'stimela_subprocess_output') and rec.stimela_subprocess_output[1] != 'start':
                    return True
                elif hasattr(rec, 'stimela_job_state'):
                    return True
                return False
            return True
        log_console_handler.addFilter(_console_filter)

    log.addHandler(log_console_handler)


def remove_log_handler(hndl):
    log.removeHandler(hndl)

def add_log_handler(hndl):
    log.addHandler(hndl)

create_logger()



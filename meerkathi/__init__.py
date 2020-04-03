# -*- coding: utf-8 -*-

import pkg_resources
import os
import subprocess
import logging
from time import gmtime, strftime
import stimela

##############################################################################
# Globals
##############################################################################


def report_version():
    # Distutils standard  way to do version numbering
    try:
        __version__ = pkg_resources.require("meerkathi")[0].version
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
BASE_MEERKATHI_LOG = "log-meerkathi.txt"
MEERKATHI_LOG = os.path.join(os.getcwd(), BASE_MEERKATHI_LOG)
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
    def __init__(self, filename, delay=True):
        logging.handlers.MemoryHandler.__init__(self, 100000, target=logging.FileHandler(filename))
        self._delay = delay

    def shouldFlush(self, record):
        return not self._delay

    def setFilename(self, filename, delay=False):
        self._delay = delay
        self.setTarget(logging.FileHandler(filename))
        if not delay:
            self.flush()


def create_logger():
    """ Create a console logger """
    log = logging.getLogger("CARACAL")
    cfmt = logging.Formatter(fmt="{asctime} {name} {levelname}: {message}", datefmt="%Y-%m-%d %H:%M:%S", style="{")
    log.setLevel(logging.DEBUG)
    log.propagate = False

    # init stimela logger as a sublogger
    stimela.logger("CARACAL.STIMELA", propagate=True, console=False)

    filehandler = DelayedFileHandler(MEERKATHI_LOG)
    filehandler.setFormatter(cfmt)

    log.addHandler(filehandler)
    log.setLevel(logging.INFO)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(cfmt)

    log.addHandler(console)

    return log, filehandler, console, cfmt


def remove_log_handler(hndl):
    log.removeHandler(hndl)


def add_log_handler(hndl):
    log.addHandler(hndl)


log, log_filehandler, log_console_handler, log_formatter = create_logger()

# -*- coding: utf-8 -*-

import pkg_resources
import os
import subprocess
import logging

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
        result = subprocess.check_output('cd %s; git describe --tags' % path, shell=True, stderr=subprocess.STDOUT).rstrip().decode()
    except subprocess.CalledProcessError:
        result = None
    if result != None and 'fatal' not in result:
        # will succeed if tags exist
        return result
    else:
        # perhaps we are in a github without tags? Cook something up if so
        try:
            result = subprocess.check_output('cd %s; git rev-parse --short HEAD' % path, shell=True, stderr=subprocess.STDOUT).rstrip().decode()
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
MEERKATHI_LOG = os.path.join(os.getcwd(), "log-meerkathi.txt")
DEFAULT_CONFIG = os.path.join(pckgdir,"sample_configurations","minimalConfig.yml")
SCHEMA = os.path.join(pckgdir, "schema", "schema-{0:s}.yml".format(__version__))

################################################################################
# Logging
################################################################################
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

    return log, filehandler, console, cfmt

def remove_log_handler(hndl):
    log.removeHandler(hndl)

def add_log_handler(hndl):
    log.addHandler(hndl)


log, log_filehandler, log_console_handler, log_formatter = create_logger()

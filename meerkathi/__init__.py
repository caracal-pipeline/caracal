# -*- coding: utf-8 -*-

#############################################################################
#  Standard imports
#############################################################################
import pkg_resources
import os
import sys
import ruamel.yaml
from SimpleHTTPServer import SimpleHTTPRequestHandler
from BaseHTTPServer import HTTPServer

import webbrowser
import subprocess
import traceback
import pdb
import logging
import StringIO
import exceptions

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
        result = subprocess.check_output('cd %s; git describe --tags' % path, shell=True, stderr=subprocess.STDOUT).rstrip()
    except subprocess.CalledProcessError:
        result = None
    if result is not None and 'fatal' not in result:
        # will succeed if tags exist
        return result
    else:
        # perhaps we are in a github without tags? Cook something up if so
        try:
            result = subprocess.check_output('cd %s; git rev-parse --short HEAD' % path, shell=True, stderr=subprocess.STDOUT).rstrip()
        except subprocess.CalledProcessError:
            result = None
        if result is not None and 'fatal' not in result:
            return __version__+'-'+result
        else:
            # we are probably in an installed version
            return __version__

__version__ = report_version()

# global settings
pckgdir = os.path.dirname(os.path.abspath(__file__))
MEERKATHI_LOG = os.path.join(os.getcwd(), "meerkathi.log")
DEFAULT_CONFIG = os.path.join(pckgdir, "default-config.yml")
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

# Create the log object
log, log_filehandler, log_console_handler, log_formatter = create_logger()
####################################################################
# MeerKATHI imports
####################################################################
from meerkathi.dispatch_crew.config_parser import config_parser as cp
from meerkathi.dispatch_crew import worker_help

import meerkathi.dispatch_crew.caltables as mkct

from meerkathi.workers.worker_administrator import worker_administrator as mwa


from meerkathi.view_controllers import event_loop

from meerkathi.dispatch_crew.interruptable_process import interruptable_process
from meerkathi.dispatch_crew.stream_director import stream_director

####################################################################
# Runtime routines
####################################################################
def print_worker_help(args, schema_version):
    """
    worker help
    """
    schema = os.path.join(pckgdir, "schema",
            "{0:s}_schema-{1:s}.yml".format(args.worker_help, schema_version))
    with open(schema, "r") as f:
        worker_dict = cfg_txt = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1))

    helper = worker_help.worker_options(args.worker_help, worker_dict["mapping"][args.worker_help])
    helper.print_worker()

def get_default(to):
    """
    Get default parset copy
    """
    log.info("Dumping default configuration to {0:s} as requested. Goodbye!".format(to))
    os.system('cp {0:s}/default-config.yml {1:s}'.format(pckgdir, to))

def reconstruct_defaults(filename):
    cp().reconstruct_defaults(filename)

def start_viewer(args, timeout=None, open_webbrowser=True):
    """
    Starts HTTP service and opens default system webpager
    for report viewing
    """
    port = args.interactive_port
    web_dir = os.path.abspath(os.path.join(args.general_output, 'reports'))

    log.info("Entering interactive mode as requested: MEERKATHI report viewer")
    log.info("Starting HTTP web server, listening on port {} and hosting directory {}".format(port, web_dir))
    log.info("Press Ctrl-C to exit")

    def __host():
        with open(os.devnull, "w") as the_void:
            stdout_bak = sys.stdout
            stderr_bak = sys.stderr
            sys.stdout = the_void
            sys.stderr = the_void
            httpd = HTTPServer(("", port), hndl)
            os.chdir(web_dir)
            try:
                httpd.serve_forever()
            except KeyboardInterrupt, SystemExit:
                httpd.shutdown()
            finally:
                sys.stdout = stdout_bak
                sys.stderr = stderr_bak

    if not os.path.exists(web_dir):
        raise RuntimeError("Reports directory {} does not yet exist. Has the pipeline been run here?".format(web_dir))

    hndl = SimpleHTTPRequestHandler

    wt = interruptable_process(target = __host)
    try:
        wt.start()
        if open_webbrowser:
            # suppress webbrowser output
            savout = os.dup(1)
            os.close(1)
            os.open(os.devnull, os.O_RDWR)
            try:
                webbrowser.open('http://localhost:{}'.format(port))
            finally:
                os.dup2(savout, 1)

        wt.join(timeout=timeout)
    except KeyboardInterrupt, SystemExit:
        log.info("Interrupt received - shutting down web server. Goodbye!")
    return wt

def log_logo():
    """ Some nicities """
    log.info("")

    log.info("███╗   ███╗███████╗███████╗██████╗ ██╗  ██╗ █████╗ ████████╗██╗  ██╗██╗")
    log.info("████╗ ████║██╔════╝██╔════╝██╔══██╗██║ ██╔╝██╔══██╗╚══██╔══╝██║  ██║██║")
    log.info("██╔████╔██║█████╗  █████╗  ██████╔╝█████╔╝ ███████║   ██║   ███████║██║")
    log.info("██║╚██╔╝██║██╔══╝  ██╔══╝  ██╔══██╗██╔═██╗ ██╔══██║   ██║   ██╔══██║██║")
    log.info("██║ ╚═╝ ██║███████╗███████╗██║  ██║██║  ██╗██║  ██║   ██║   ██║  ██║██║")
    log.info("╚═╝     ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═╝")
    log.info("")
    # parse config file and set up command line argument override parser
    log.info("Module installed at: {0:s} (version {1:s})".format(pckgdir, str(__version__)))
    log.info("A logfile will be dumped here: {0:s}".format(MEERKATHI_LOG))
    log.info("")

def execute_pipeline(args, arg_groups, block):
    # setup piping infractructure to send messages to the parent
    def __run():
        """ Executes pipeline """
        with stream_director(log) as director: #stdout and stderr needs to go to the log as well            
            try:
                log_logo()
                # Very good idea to print user options into the log before running:
                cp().log_options()

                # Obtain some divine knowledge
                cdb = mkct.calibrator_database()

                pipeline = mwa(arg_groups,
                            args.workers_directory, stimela_build=args.stimela_build,
                            add_all_first=args.add_all_first, prefix=args.general_prefix,
                            singularity_image_dir=args.singularity_image_dir, 
                            container_tech=args.container_tech)

                pipeline.run_workers()
            except exceptions.SystemExit as e:
                if e.code != 0:
                    log.error("One or more pipeline workers enacted E.M.E.R.G.E.N.C.Y protocol {0:} shutdown. This is likely a bug, please report.".format(e.code))
                    log.error("Your logfile is here: {0:s}. You are running version: {1:s}".format(MEERKATHI_LOG, str(__version__)))
                    sys.exit(1) #indicate failure
                else:
                    log.info("One or more pipeline workers requested graceful shutdown. Goodbye!")
            except KeyboardInterrupt:
                log.info("Interrupt request received from user - gracefully shutting down. Goodbye!")
            except Exception as e:
                log.error("An unhandled exeption occured. If you think this is a bug please report it.")
                log.error("Your logfile is here: {0:s}.".format(MEERKATHI_LOG))
                log.error("You are running version: {0:s}".format(str(__version__)))
                log.error(traceback.format_exc())
                sys.exit(1) #indicate failure
    
    # now fork and block or continue depending on whether interaction is wanted
    try:
        wt = interruptable_process(target=__run)
        wt.start()
        wt.join(None if block else 0)
    except KeyboardInterrupt:
        wt.interrupt()
    return wt
        
############################################################################
# Driver entrypoint
############################################################################
def main(argv):
    
    args = cp(argv).args
    arg_groups = cp(argv).arg_groups

    if args.schema:
        schema = {}
        for item in args.schema:
            _name, _schema = item.split(",")
            schema[_name] = _schema
        args.schema = schema
    else:
        args.schema = {}
    with open(args.config, 'r') as f:
        tmp = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1))
        schema_version = tmp["schema_version"]

    if args.worker_help:
        print_worker_help(args, schema_version)
        return

    if not args.no_interactive and args.report_viewer:
        raise ValueError("Incompatible options: --no-interactive and --report-viewer")
    # User requests default config => dump and exit
    if args.get_default:
        log_logo()
        get_default(args.get_default)
        return
    
    # standalone report hosting
    if args.report_viewer:
        log_logo()
        start_viewer(args)
        return

    if args.print_calibrator_standard:
        cdb = mkct.calibrator_database()
        log.info("Found the following reference calibrators (in CASA format):")
        log.info(cdb)
        return
    if args.reconstruct_defaults_from_schema:
        reconstruct_defaults("./DefaultParset.yaml")
        log.info("Default parset reconstructed as best possible and dumped to ./DefaultParset.yaml. Please fill any missing values by inspection.")
        return
    if not args.no_interactive and \
       args.config == DEFAULT_CONFIG and \
       not args.get_default and \
       not args.report_viewer:
       # Run interactively
        remove_log_handler(log_console_handler) 
        try:
            event_loop().run()
        except KeyboardInterrupt:
            return
    else:
       # Run non-interactively
       p = execute_pipeline(args, arg_groups, block=True)
       log.info("PIPELINER EXITS WITH RETURN CODE {}".format(p.exitcode))
       sys.exit(p.exitcode) # must return exit code when non-interactive
    
    

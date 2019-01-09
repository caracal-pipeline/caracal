# -*- coding: utf-8 -*-

# Standard imports
import pkg_resources
import os
import sys
import ruamel.yaml
from SimpleHTTPServer import SimpleHTTPRequestHandler
from BaseHTTPServer import HTTPServer
from multiprocessing import Process
import webbrowser
import traceback
import logging

# Distutils standard  way to do version numbering
try:
    __version__ = pkg_resources.require("meerkathi")[0].version
except pkg_resources.DistributionNotFound:
    __version__ = "dev"

# global settings
pckgdir = os.path.dirname(os.path.abspath(__file__))
MEERKATHI_LOG = os.path.join(os.getcwd(), "meerkathi.log")
DEFAULT_CONFIG = os.path.join(pckgdir, "default-config.yml")
SCHEMA = os.path.join(pckgdir, "schema", "schema-{0:s}.yml".format(__version__))

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

# MeerKATHI imports
from meerkathi.dispatch_crew.config_parser import config_parser as cp
from meerkathi.dispatch_crew import worker_help
import meerkathi.dispatch_crew.caltables as mkct
from meerkathi.workers.worker_administrator import worker_administrator as mwa
from meerkathi.view_controllers import event_loop

def print_worker_help(args, schema_version):
    schema = os.path.join(pckgdir, "schema",
            "{0:s}_schema-{1:s}.yml".format(args.worker_help, schema_version))
    with open(schema, "r") as f:
        worker_dict = cfg_txt = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1))

    helper = worker_help.worker_options(args.worker_help, worker_dict["mapping"][args.worker_help])
    helper.print_worker()

def get_default(to):
    log.info("Dumping default configuration to {0:s} as requested. Goodbye!".format(to))
    os.system('cp {0:s}/default-config.yml {1:s}'.format(pckgdir, to))

def start_viewer(args):
    log.info("Entering interactive mode as requested: MEERKATHI report viewer")
    port = args.interactive_port
    web_dir = os.path.abspath(os.path.join(args.general_output, 'reports'))

    def __host():
        httpd = HTTPServer(("", port), hndl)
        os.chdir(web_dir)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt, SystemExit:
            httpd.shutdown()

    if not os.path.exists(web_dir):
        log.error("Reports directory '%s' does not yet exist. Has the pipeline been run here?" % web_dir)
        return
    log.info("Starting HTTP web server, listening on port %d and hosting directory %s" %
                (port, web_dir))
    log.info("Press Ctrl-C to exit")
    hndl = SimpleHTTPRequestHandler

    wt = Process(target = __host)
    try:
        wt.start()
        webbrowser.open("http://localhost:%d/" % port)
        wt.join()
    except KeyboardInterrupt, SystemExit:
        log.info("Interrupt received - shutting down web server. Goodbye!")
    return

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

    if not args.no_interactive:
        event_loop().run()

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

    # User requests default config => dump and exit
    if args.get_default:
        get_default(args.get_default)
    elif args.report_viewer:
        start_viewer(args)
   
    # Very good idea to print user options into the log before running:
    cp().log_options()

    # Obtain some divine knowledge
    cdb = mkct.calibrator_database()

    if args.print_calibrator_standard:
        log.info("Found the following reference calibrators (in CASA format):")
        log.info(cdb)
        return

    import exceptions
    try:
        pipeline = mwa(arg_groups,
                       args.workers_directory, stimela_build=args.stimela_build,
                       add_all_first=args.add_all_first, prefix=args.general_prefix,
                       singularity_image_dir=args.singularity_image_dir)

        pipeline.run_workers()
    except exceptions.SystemExit as e:
        if e.code != 0:
            log.error("One or more pipeline workers enacted E.M.E.R.G.E.N.C.Y protocol {0:} shutdown. This is likely a bug, please report.".format(e.code))
            log.error("Your logfile is here: {0:s}. You are running version: {1:s}".format(MEERKATHI_LOG, str(__version__)))
            sys.exit(1) #indicate failure
        else:
            log.info("One or more pipeline workers requested graceful shutdown. Goodbye!")
    except:
        log.error("Whoops... big explosion - you sent pipes flying all over the show! Time to call in the monkeywrenchers.")
        log.error("Your logfile is here: {0:s}. You are running version: {1:s}".format(MEERKATHI_LOG, str(__version__)))
        tb = traceback.format_exc()
        log.error(tb)
        sys.exit(1) #indicate failure

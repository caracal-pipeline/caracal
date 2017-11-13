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
import meerkathi.scripts as scripts
from SimpleHTTPServer import SimpleHTTPRequestHandler
from BaseHTTPServer import HTTPServer
from multiprocessing import Process
import webbrowser
import base64
from urllib import urlencode
import ruamel.yaml
import json
from meerkathi.dispatch_crew.reporter import reporter as mrr

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
        self.data_path = self.config['general']['data_path']
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

            self.workers.append((name, worker, order))

        self.workers = sorted(self.workers, key=lambda a: a[2])

        self.prefix = prefix or self.config['general']['prefix']
        self.stimela_build = stimela_build
        self.recipes = {}
        # Workers to skip
        self.skip = []
        # Initialize empty lists for ddids, leave this up to get data worker to define
        self.init_names([])

    def init_names(self, dataid):
        """ iniitalize names to be used throughout the pipeline and associated 
            general fields that must be propagated
        """
        self.dataid = dataid
        self.nobs = len(self.dataid)
        self.h5files = ['{:s}.h5'.format(dataid) for dataid in self.dataid]
        self.msnames = ['{:s}.ms'.format(os.path.basename(dataid)) for dataid in self.dataid]
        self.split_msnames = ['{:s}_split.ms'.format(os.path.basename(dataid)) for dataid in self.dataid]
        self.cal_msnames = ['{:s}_cal.ms'.format(os.path.basename(dataid)) for dataid in self.dataid]
        self.prefixes = ['meerkathi-{:s}'.format(os.path.basename(dataid)) for dataid in self.dataid]

        for item in 'input msdir output'.split():
            value = getattr(self, item, None)
            if value:
                setattr(self, item, value)

        for item in 'data_path reference_antenna fcal bpcal gcal target'.split():
            value = getattr(self, item, None)
            if value and len(value)==1:
                value = value*nobs
                setattr(self, item, value)

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

            config = self.config[_name]
            if config.get('enable', True) is False:
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
        try:
            if self.add_all_first:
                for worker in self.workers:
                    if worker not in self.skip:
                        self.recipes[worker[1]].run()
        finally: # write reports even if the pipeline only runs partially
            reporter = mrr(self)
            reporter.generate_reports()



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
    log.info("Module installed at: %s (version %s)" % (pckgdir, str(__version__.__version__)))
    log.info("A logfile will be dumped here: %s" % MEERKATHI_LOG)
    log.info("")
    args = cp(argv).args
    arg_groups = cp(argv).arg_groups
    def __host():
        httpd = HTTPServer(("", port), hndl)
        os.chdir(web_dir)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt, SystemExit:
            httpd.shutdown()

    # User requests default config => dump and exit
    if args.get_default:
        log.info("Dumping default configuration to %s as requested. Goodbye!" % args.get_default)
        os.system('cp {0:s}/default-config.yml {1:s}'.format(pckgdir, args.get_default))
        return
    elif args.config_editor:
        log.info("Entering interactive mode as requested: MeerKATHI configuration editor")
        port = args.interactive_port
        file_abs = args.config
        with file(file_abs, 'r') as f:
            cfg_txt = json.dumps(ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1)))
        web_dir = os.path.join(os.path.dirname(scripts.__file__), 'conf_helper')
        log.info("Starting HTTP web server, listening on port %d and hosting directory %s" %
                 (port, web_dir))
        log.info("Press Ctrl-C to exit")
        hndl = SimpleHTTPRequestHandler

        wt = Process(target = __host)
        try:
            wt.start()
            webbrowser.open("http://localhost:%d/index.html?%s" % (port, urlencode({"filetxt":cfg_txt})))
            wt.join()
        except KeyboardInterrupt, SystemExit:
            log.info("Interrupt received - shutting down web server. Goodbye!")
        return

    # Very good idea to print user options into the log before running:
    cp().log_options()

    # Obtain some divine knowledge
    cdb = mkct.calibrator_database()

    if args.print_calibrator_standard:
        meerkathi.log.info("Found the following reference calibrators (in CASA format):")
        meerkathi.log.info(cdb)

    import exceptions
    try:
        pipeline = MeerKATHI(arg_groups,
                             args.workers_directory, stimela_build=args.stimela_build,
                             add_all_first=args.add_all_first, prefix=args.general_prefix)

        pipeline.run_workers()
    except exceptions.SystemExit as e:
        if e.code != 0:
            log.error("One or more pipeline workers enacted E.M.E.R.G.E.N.C.Y protocol %d shutdown. This is likely a bug, please report." % e.code)
            log.error("Your logfile is here: %s. You are running version: %s" % (MEERKATHI_LOG, str(__version__.__version__)))
            sys.exit(1) #indicate failure
        else:
            log.info("One or more pipeline workers requested graceful shutdown. Goodbye!")
    except:
        log.error("Whoops... big explosion - you sent pipes flying all over the show! Time to call in the monkeywrenchers.")
        log.error("Your logfile is here: %s. You are running version: %s" % (MEERKATHI_LOG, str(__version__.__version__)))
        tb = traceback.format_exc()
        log.error(tb)
        sys.exit(1) #indicate failure

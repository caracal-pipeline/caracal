# -*- coding: future_fstrings -*-

import caracal
import pkg_resources
import os
import sys
import pdb
import ruamel.yaml
from http.server import SimpleHTTPRequestHandler
from http.server import HTTPServer
import webbrowser
import subprocess
import traceback
import pdb
import logging
import io
from caracal.dispatch_crew import config_parser
from caracal.dispatch_crew import worker_help
import caracal.dispatch_crew.caltables as mkct
from caracal.workers.worker_administrator import worker_administrator
import stimela

__version__ = caracal.__version__
pckgdir = caracal.pckgdir
DEFAULT_CONFIG = caracal.DEFAULT_CONFIG
SAMPLE_CONFIGS = caracal.SAMPLE_CONFIGS = {
        "minimal" : "minimalConfig.yml",
        "meerkat" : "meerkat-defaults.yml",
        "carate" : "carateConfig.yml",
        "meerkat_continuum" : "meerkat-continuum-defaults.yml",
        "mosaic_basic" : "mosaic_basic_config.yml",
        }
SCHEMA = caracal.SCHEMA

# Create the log object
from caracal import log

####################################################################
# CARACal imports
####################################################################

####################################################################
# Runtime routines
####################################################################


def print_worker_help(args, schema_version=None):
    """
    worker help
    """
    schema = os.path.join(pckgdir, "schema",
                          "{0:s}_schema.yml".format(args.worker_help))
    with open(schema, "r") as f:
        worker_dict = cfg_txt = ruamel.yaml.load(
            f, ruamel.yaml.RoundTripLoader, version=(1, 1))

    helper = worker_help.worker_options(
        args.worker_help, worker_dict["mapping"][args.worker_help])
    helper.print_worker()


def get_default(sample, to):
    """
    Get default parset copy
    """
    log.info(
        "Dumping default configuration to {0:s} as requested. Goodbye!".format(to))
    sample_config = os.path.join(pckgdir, "sample_configurations",
            SAMPLE_CONFIGS[sample])
    os.system('cp {0:s} {1:s}'.format(sample_config, to))


def start_viewer(args, timeout=None, open_webbrowser=True):
    """
    Starts HTTP service and opens default system webpager
    for report viewing
    """
    port = args.interactive_port
    web_dir = os.path.abspath(os.path.join(args.general_output, 'reports'))

    log.info("Entering interactive mode as requested: CARACal report viewer")
    log.info("Starting HTTP web server, listening on port {} and hosting directory {}".format(
        port, web_dir))
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
            except KeyboardInterrupt as SystemExit:
                httpd.shutdown()
            finally:
                sys.stdout = stdout_bak
                sys.stderr = stderr_bak

    if not os.path.exists(web_dir):
        raise RuntimeError(
            "Reports directory {} does not yet exist. Has the pipeline been run here?".format(web_dir))

    hndl = SimpleHTTPRequestHandler

    wt = interruptable_process(target=__host)
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
    except KeyboardInterrupt as SystemExit:
        log.info("Interrupt received - shutting down web server. Goodbye!")
    return wt


def log_logo():
    # print("WWWWWWWWMMWMMWWMMWWWWWMWWWWMMMMWWWWWWWWWWWWWWWWWWWWMMMMWWWWWWWWWWWWWWWWWWWWWWWWWMMMWWWWWMWWWWWWWWWWWWWNNNNWWMMWWMWWWWWWW")
    # print("WWWWWWWMMWWWWWWMMWWWWWWWWWWMMWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWMMMMMMMWWWWWWWWWWWWWWWWWWWWWWWWMWWWWXkO0KWWWWWWWWWWWWW")
    # print("WWMWWWWWWWWWWWKOxdollcok00KKXWWWWMMMMWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWNXKKXXXXXNWWWWWWWN0d::ckWMWWMMWWWWWWW")
    # print("WMWWMWWWWWN0dc;'....',cxOOkkkO0XWMWWMMWWWWWX0xolllccc::::cllooddxxxxdddoooooooollcc;,,,;;;;;:cldxO0Ol,'.'lKWWWWMWWWWWWWW")
    # print("MWWWWWWWXkc'.''...,ckKNWWWNXKOkk0XWWWWWWKxc,.....................................................',''''.,xNWWWWWWWWWWWWW")
    # print("MWWWWWNk:'',;,'.,l0NWWWWWWMWWNKOkkKWWKxc'........................................................'''''''',dXWWMMWWWWWWWW")
    # print("WWWWW0c'',;:;'.:kNWWWMWWWWWMWWWN0kdoc'....,ll'.....................................................''''''',kWWWMMWWWWWWW")
    # print("MWWWO;.';cc,''cKWWWWWMMMMWWWWWWXx:.....:dkkx:...................................................';::;'.''''oNWWWWWWWWWWW")
    # print("WWWO;.';cc;'.cKWWWWMMWWWWWWWWW0;...'..':l;.....................................................,oKNNKOd:,,:xXWWWWWWWWWWW")
    # print("WWKc.';cc:'.;0WWWWWMMMWMMWWWWW0odOOc..........................',,'...................'.........cKWWWWWWN0KNWWWWWWWWWWWWW")
    # print("MWx'.,:cc,.'dWMWWWWWWWMWMMMWMMWWWWk'....................;cdkO0KXXOxdoc,''''',;;:lokOOkc........,cxXWWWWWWWMWWWMMWWWWWWWW")
    # print("MNl.';cc:'.;OWWWWWMWWWWWMWWMWWWWWO;...''.............:dOXWWMMWWWWMMWWNX0KK00KXXNWWWWWWXkl:,'.....':x0NWWWWMMWWMMMWWWWWWW")
    # print("WKc.';cc:'.:KWWWWWWWMWWMWWMWWWWWO;...,x0kdoloolc:cox0NMWWWWMMWMMWWWMWWMWWWWMMWWWWWWWWWWWWNKOxoc:,'..,lONWWWWWMMMWWWWWWWW")
    # print("WKc.';cc:'.:KWWMWWWWMMMWMMWWWMXo'....l0NWWWWWMWWWWMWWWMWMMWMMMMWWWWWMMWWMWWWWWWMWWWWWWMWWWWWWWWNk;.''.,xNWWWMMWWWWWWWWWW")
    # print("WXl.';cc:'.;0MWWWWWWWWWWWMMWWWx.....:x0NWWWMMMWWWWMWWMWWWWMWWWMMMWWWMWWMMMWWMWWWWMWWMMMWWWWMWMWWNk:'''.;OWWWWWWMMWWWWWWW")
    # print("WWd'.,ccc,.'xWMWWWWWWWWMWWMWWNo..;loxkKWWWMMWWMMMWWWMWWMWWWWWWWMMWWWWWMMMWWWWMWWMMWWMMWWMWWMWMMWMWX00OdxXWWWWMWWWWWWWWWW")
    # print("WW0:.';cc;'.cKMWWWWWWWWWWWWWWWNOdxxdxOXWWMXOkkkkkkOKNWWMMWWMMN0kkOXWMWMMMWWN0kxxxk0XWMWWMWN0kkONWWWWMNOkkKWWWWWWWWWWWWWW")
    # print("WWWk,.':cc,''oXWWWWWWWWWWWWWWWXo'..'cOWMWWO;..''''',:dKWWWWNNx,..'lXMWMMWKd:'.''''';dXMWMNd'..'oNMWWW0;..lNWWMMWWWWWWWWW")
    # print("MWWNx,.';c:,.'oXWWMWWWMWWMMWWNd'.,,.'dNWWWO;..cO0Od,.'oNMWWkc,.,;''oXMMW0:..;dO00kooONWWNx,.,;.'dNMWW0;..lNWWWWMWWMWWWWW")
    # print("WWWWNk;.',::,.'lKWWWWMWWWWWWNx,.,ol,.,xWWWO;..lXNXk;..cXWWO;..,xO:.'dNMNo..,kWWWWWWWWWWWk,.;kk;.,xWWW0;..lNWWWWWWMMMWWMW")
    # print("WWWWWWKo,.';;,'.;xXWWWWMMWWWk,.';ll;'.;kWMO;..,:::,.'c0WW0:...:dkc'.,dNNl..;OWWWWWWWWWWk;.':xxc'.,kWW0;..lNWWWWWMWWWWMMM")
    # print("WMWWMWWNOl,.',''.':d0NWWWWWO;.'',,,,,'.;OWO;..;ol;..:0WWKc.'',,,,,,'.,xNO:.';d0K0kod0NO;.',,,,,,'.;OW0;..cO0000KNMWWWWWW")
    # print("WMWWMWWWWWKxc,'.....,cdOK0x;.'cOKKKKOc..:0O;..oNW0c'':OKl..:dOKKKK0o'.,kNKd:'.',''';dk:..l0KKKK0c'.;O0;..'''''':0MWWWWWW")
    # print("WWWWWWWMWWWWNKkdlc:;;,:dOOxdxOXWWWWWWKkkkKXOkkKWWWXOkk0KOkkKWWWWWWWXOkk0NMWX0kxxxk0XWKkkOXWWWWWWXOkkKXOkkkkkkkkONMWWWMMW")
    # print("WWWWWWWMMWWWWWWWWWNXKKXNNWWWMWWWWMWWWWWMWWWWMMWWWWWWWWMMMWMMWWWWWWWWMMWWWWWWMMWWWWWWMMMWWMWWMWWWWMMMMWMMMMWWMWWMMWWWWWWW")
    # print("WWWWWWWMMWWMWWWWMMWWWWWWWWWWWWWWMWWWWWWWWMWWWWMWWWMWWWWMWWWMMWWWMWWWWMMWWWWWWWMMMMWWMMMWMWWMWWMWMMWWMWWMMWMWWMWWWWMMMWWW")

    print("""
........................................................................................................................
..........................................................................................................Z.~...........
...........................................................................................................Z.O..........
..................,8OOOOOZ==++,...........................................................................ZZOZ..........
...............?OZOOOOOOOO+======..................~=$ZOOO8OOZ~ ............~~....~7ZZOZOZZZOZZOO$.....,ZZZZZ=..........
.............OOOOOOOOOO$.....~=====...........$88888OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOZZZZZZZZZZZZZZZZZZZZZZZ7...........
...........OOOOO$OOOO7..........====,.......88888888OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOZZZZZZZZZZZZZZZZZZZZZZZZ...........
.........~ZOOO77OOOO.............:===~...Z8888888888OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOZZZZZZZZZZZZZZZZZZZZZZZZZ..........
........$OOOZIIOOOO................====88888887Z8888OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOZZZZZZZZZZZZZZZZZZZZZZZZZZ.........
.......OOOOIIIOOOO..................?888888O++.OO888OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOZZZZZZZZZZZZZZZZZZZZZZZZZZ.........
......+OOO7IIOOOO.................:O8888O8,..Z888888OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOZZZZZZZZZZZZZZ?.,ZZZZZZZZ=........
....,.OOOII7ZOOO.................8888D78888888888888OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOZZZZZZZZZZZZZO:......+OZ$Z.........
.....OOOO77IOOO,..................8,.:I8888888888888OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOZZZZZZZZZZZZO.....................
.....OOOI7I7OOO.......................88888888888888OOOOOOOOOOO~....II8OOOOOOOOOOOOOOO$$,ZZZZZZZZZZZ....................
....$OOO77IOOOZ......................888888888888888OOOOOOO?............?IZI$Z$=?I........ZZZZZZZZZZOZ..................
....OOO$II7OOO,.....................?8888O888888888888888...................................ZZZZZZZZZZZ?................
....OOO7I7IOOO.....................,88887+.,,Z8ID88888..........................................IOZZZZZZZ=..............
....OOO7IIIOOO....................OO8888==..........................................................ZZZZZZI.............
....OOO7IIIOOO...................+888888==..........................................................OZZZZZZ.............
....OOO$II7OOO+..................88888O+==............................................................ZZZZZZ............
....?OOOII7$OOZ..................D888:+=+:................................................................O.............
.....OOO7777OOZ.......................==+...............................................................................
.....7OOOI77OOOZ..................$OOOO=+.....OOOOOOZOO?........$OOOO.........IOOOOOOO?.......OOOO=......OOOO...........
......OOOZI77OOZ..................OOOOOO......OOOOOOOOOOO~......OOOOOO......IOOOOOOOOOOO.....ZOOOOO......OOOO...........
.......OOO$7IZOOO................OOOZZOO......OOOO....ZOOO.....OOO8OOO.....?OOOO.....OZ.....=OOOOOOZ.....OOOO...........
........OOOOI7OOOO..............IOOO+ZOOO.....OOOO....OOOO....7OOO.8OOO....OOOO.............OOZ=.OOO~....OOOO...........
.........OOOOI7OOOO.............OOOI==OOOO....OOOO++IOZOOO...:OOO~..OOOO...OOOO............OOOZ..$OOO....OOOO...........
..........8OOOOIOOOO$..........OOOOOOOOOOO+...OOOOOOOOOOZ....OOOOOOOOOOO,..OOOO...........ZOOOOOOOOOOO...OOOO...........
...........,8OOOO$OOOZ........OOOOOOOOOOOOO...OOOO::OOOO....OOOOOOOOOOOOO..:OOOO=....OO...OOOOOOOOOOOO$..OOOO...........
..............OOOOOOOOOOO===++OOO7,.....OOOO..OOOO...OOOO..~OOO=......OOOO...ZOOOOOOOOOO.OOOO......ZOOO..OOOOOOOOOOO....
................~OOOOOOOOO+=+OOOO.......?OOO$.OOOO....ZOOZ.ZOOO.......$ZOO?....ZOOOOO7..ZOOO,.......OOOO.OOOOOOOOOOO....
......................+I7~=~............................................................................................
........................................................................................................................
........................................................................................................................
""")

#     print("""
# ................................................................................
# ........................................................................?.......
# ..............:~~:,....................................................Z$.......
# ..........ZOOOOOO+==+=...........+7O88OOOI,......,+7~+?OOZZOZZZZZ=...ZZZZ.......
# ........OOOOOOO.....,==+.....:88888OOOOOOOOOOOOOOOOOOOOOZZZZZZZZZZZZZZZZ........
# ......OOOIOOO.........+=+..88888888OOOOOOOOOOOOOOOOOOOOOZZZZZZZZZZZZZZZZZ,......
# .....ZOOIOOO...........==88888+I888OOOOOOOOOOOOOOOOOOOOOZZZZZZZZZZZZZZZZZZ......
# ....OOZI$OO............8888I.$DO888OOOOOOOOOOOOOOOOOOOOOOZZZZZZZZZZ..ZZZZZ .....
# ...$OO7IOO............88.+888888888OOOOOOOOOOOOOOOOOOOOOOZZZZZZZZO..............
# ...OO7I$OO...............8888888888OOOOO8....I,OOOOOOOOO?=.OZZZZZZO.............
# ...OOIIOO=..............D888888888OOOO......................,ZZZZZZZ:...........
# ...OOIIOO..............I888+..:...Z..............................OZZZZ:.........
# ...OOIIOO.............8888==.......................................ZZZZ.........
# ...OOIIOO7............8888==........................................ZZZZ........
# ...OOII7OO...............+=.....................................................
# ....OOIIOO.............OOO=...$OOOOOO+.....OOO.....~OOOOOO....8OOZ....OOO.......
# ....ZOZIIOO...........OOOOO...$OO...OOI...OOOOO...ZOO,..=O...,OOOO....OOO.......
# .....OOOI$OO.........OOO=OO:..$OO...OOZ..OOO.OO,..OO~........OO.:OO...OOO.......
# ......=OOO7OO?......:OOOOOOO..$OOOOOOO..=OOOOZOO..OOZ.......OOOOOOOO..OOO.......
# ........ZOOOOOO+....OOZ$$$OOO.$OZ.,OO7..OO$$$$OOO.,OOOZOOO~$OO$$$$OO=.OOZOOOO...
# ...........OOOOOO+=OOO.....OO$$OO...OOOZOO.....ZO?..:OOOO..OO,....~OO.OOOOOOO...
# ................................................................................
# ................................................................................
#     """)

    log.info("Version {1:s} installed at {0:s}".format(pckgdir, str(__version__)))

def execute_pipeline(args, arg_groups, block):
    # setup piping infractructure to send messages to the parent
    def __run(debug=False):
        """ Executes pipeline """
#        with stream_director(log) as director:  # stdout and stderr needs to go to the log as well -- nah

        try:
            log_logo()
            # Very good idea to print user options into the log before running:
            config_parser.config_parser().log_options(args.config)

            # Obtain some divine knowledge
            cdb = mkct.calibrator_database()

            pipeline = worker_administrator(arg_groups,
                                            args.workers_directory, stimela_build=args.stimela_build,
                                            add_all_first=False, prefix=args.general_prefix,
                                            configFileName=args.config, singularity_image_dir=args.singularity_image_dir,
                                            container_tech=args.container_tech, start_worker=args.start_worker,
                                            end_worker=args.end_worker, generate_reports=not args.no_reports)

            pipeline.run_workers()
        except SystemExit as e:
            # if e.code != 0:
            log.error("A pipeline worker initiated sys.exit({0:}). This is likely a bug, please report.".format(e.code))
            log.info("  More information can be found in the logfile at {0:s}".format(caracal.CARACAL_LOG))
            log.info("  You are running version {0:s}".format(str(__version__)), extra=dict(logfile_only=True))
            if debug:
                log.warning("you are running with -debug enabled, dropping you into pdb. Use Ctrl+D to exit.")
                pdb.post_mortem(sys.exc_info()[2])
            sys.exit(1)  # indicate failure

        except KeyboardInterrupt:
            log.error("Ctrl+C received from user, shutting down. Goodbye!")
        except Exception as exc:
            log.error("{} [{}]".format(exc, type(exc).__name__), extra=dict(boldface=True))
            log.info("  More information can be found in the logfile at {0:s}".format(caracal.CARACAL_LOG))
            log.info("  You are running version {0:s}".format(str(__version__)), extra=dict(logfile_only=True))
            for line in traceback.format_exc().splitlines():
                log.error(line, extra=dict(traceback_report=True))
            if debug:
                log.warning("you are running with -debug enabled, dropping you into pdb. Use Ctrl+D to exit.")
                pdb.post_mortem(sys.exc_info()[2])
            log.info("exiting with error code 1")
            sys.exit(1)  # indicate failure

    return __run(debug=args.debug)

############################################################################
# Driver entrypoint
############################################################################


def main(argv):
    # parse initial arguments to init basic switches
    parser = config_parser.primary_parser(argv)
    args, _ = parser.parse_known_args(argv)

    caracal.init_console_logging(boring=args.boring, debug=args.debug)
    stimela.logger().setLevel(logging.DEBUG if args.debug else logging.INFO)

    try:
        parser = config_parser.config_parser(argv)
    except config_parser.ConfigErrors as exc:
        log.error("{}, list of errors follows:".format(exc))
        for section, errors in exc.errors.items():
            print("  {}:".format(section))
            for err in errors:
                print("    - {}".format(err))
        sys.exit(1)  # indicate failure
    except Exception as exc:
        traceback.print_exc()
        log.error("Error parsing arguments or configuration: {}".format(exc))
        if args.debug:
            log.warning("you are running with -debug enabled, dropping you into pdb. Use Ctrl+D to exit.")
            pdb.post_mortem(sys.exc_info()[2])
        sys.exit(1)  # indicate failure

    args = parser.args
    arg_groups = parser.arg_groups

    if args.schema:
        schema = {}
        for item in args.schema:
            _name, _schema = item.split(",")
            schema[_name] = _schema
        args.schema = schema
    else:
        args.schema = {}

    with open(args.config, 'r') as f:
        tmp = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1, 1))
        arg_groups["schema_version"] = schema_version = tmp["schema_version"]

    if args.worker_help:
        print_worker_help(args)
        return

    # User requests default config => dump and exit
    if args.get_default:
        log_logo()
        get_default(args.get_default_template, args.get_default)
        return

    if args.print_calibrator_standard:
        cdb = mkct.calibrator_database()
        log.info("Found the following reference calibrators (in CASA format):")
        log.info(cdb)
        return

    if args.config is caracal.DEFAULT_CONFIG:
        config_parser.primary_parser().print_help()
        sys.exit(1)
    execute_pipeline(args, arg_groups, block=True)

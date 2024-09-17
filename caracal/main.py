# -*- coding: future_fstrings -*-

from caracal import log
import caracal
import os
import sys
import ruamel.yaml
import pdb
import traceback
import logging
import shutil
from caracal.dispatch_crew import config_parser
from caracal.dispatch_crew import worker_help
import caracal.dispatch_crew.caltables as mkct
from caracal.workers.worker_administrator import WorkerAdministrator
import stimela
from caracal.schema import SCHEMA_VERSION

__version__ = caracal.__version__
pckgdir = caracal.PCKGDIR
DEFAULT_CONFIG = caracal.DEFAULT_CONFIG
SAMPLE_CONFIGS = caracal.SAMPLE_CONFIGS = {
    "minimal": "minimalConfig.yml",
    "meerkat": "meerkat-defaults.yml",
    "carate": "carateConfig.yml",
    "meerkat_continuum": "meerkat-continuum-defaults.yml",
    "mosaic_basic": "mosaic_basic_config.yml",
}
SCHEMA = caracal.SCHEMA

# Create the log object

####################################################################
# CARACal imports
####################################################################

####################################################################
# Runtime routines
####################################################################


def print_worker_help(worker):
    """
    worker help
    """
    schema = os.path.join(pckgdir, "schema", "{0:s}_schema.yml".format(worker))
    if not os.path.exists(schema):
        return None

    with open(schema, "r") as f:
        worker_dict = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1, 1))

    helper = worker_help.worker_options(worker, worker_dict["mapping"][worker])

    helper.print_worker()
    return True


def get_default(sample, to):
    """
    Get default parset copy
    """
    log.info(
        "Dumping default configuration to {0:s} as requested. Goodbye!".format(to))
    sample_config = os.path.join(pckgdir, "sample_configurations",
                                 SAMPLE_CONFIGS[sample])
    os.system('cp {0:s} {1:s}'.format(sample_config, to))


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


def execute_pipeline(options, config, block):
    # setup piping infractructure to send messages to the parent
    workers_directory = os.path.join(caracal.pckgdir, "workers")
    backend = config['general']['backend']
    if options.container_tech and options.container_tech != 'default':
        backend = options.container_tech

    def __run(debug=False):
        """ Executes pipeline """
#        with stream_director(log) as director:  # stdout and stderr needs to go to the log as well -- nah

        try:
            pipeline = WorkerAdministrator(config,
                                           workers_directory,
                                           add_all_first=False, prefix=options.general_prefix,
                                           configFileName=options.config, singularity_image_dir=options.singularity_image_dir,
                                           container_tech=backend, start_worker=options.start_worker,
                                           end_worker=options.end_worker, generate_reports=not options.no_reports)

            if options.report:
                pipeline.regenerate_reports()
            else:
                # OMS: I don't think this is necessary, as it is not used here directly, and loaded on-demand
                # # Obtain some divine knowledge
                # cdb = mkct.calibrator_database()
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

    return __run(debug=options.debug)

############################################################################
# Driver entrypoint
############################################################################


def driver():
    main(sys.argv[1:])


def main(argv):
    # parse initial arguments to init basic switches and modes
    parser = config_parser.basic_parser(argv)
    options, _ = parser.parse_known_args(argv)

    caracal.init_console_logging(boring=options.boring, debug=options.debug)
    stimela.logger().setLevel(logging.DEBUG if options.debug else logging.INFO)

    # user requests worker help
    if options.worker_help:
        if not print_worker_help(options.worker_help):
            parser.error("unknown worker '{}'".format(options.worker_help))
        return

    caracal.log.info(f"Invoked as {' '.join(sys.argv)}")

    # User requests default config => dump and exit
    if options.get_default:
        sample_config = SAMPLE_CONFIGS.get(options.get_default_template)
        if sample_config is None:
            parser.error("unknown default template '{}'".format(options.get_default_template))
        sample_config_path = os.path.join(pckgdir, "sample_configurations", sample_config)
        if not os.path.exists(sample_config_path):
            raise RuntimeError("Missing sample config file {}. This is a bug, please report".format(sample_config))
        # validate the file
        try:
            parser = config_parser.config_parser()
            _, version = parser.validate_config(sample_config_path)
            if version != SCHEMA_VERSION:
                log.warning("Sample config file {} version is {}, current CARACal version is {}.".format(sample_config,
                                                                                                         version,
                                                                                                         SCHEMA_VERSION))
                log.warning("Proceeding anyway, but please notify the CARACal team to ship a newer sample config!")
        except config_parser.ConfigErrors as exc:
            log.error("{}, list of errors follows:".format(exc))
            for section, errors in exc.errors.items():
                print("  {}:".format(section))
                for err in errors:
                    print("    - {}".format(err))
            sys.exit(1)  # indicate failure
        log.info("Initializing {1} from config template '{0}' (schema version {2})".format(options.get_default_template,
                                                                                           options.get_default, version))
        shutil.copyfile(sample_config_path, options.get_default)
        return

    if options.print_calibrator_standard:
        cdb = mkct.calibrator_database()
        log.info("Found the following reference calibrators (in CASA format):")
        log.info(cdb)
        return

    # if config was not specified (i.e. stayed default), print help and exit
    config_file = options.config
    if config_file == caracal.DEFAULT_CONFIG:
        parser.print_help()
        sys.exit(1)

    try:
        parser = config_parser.config_parser()
        config, version = parser.validate_config(config_file)
        if version != SCHEMA_VERSION:
            log.warning("Config file {} schema version is {}, current CARACal version is {}".format(config_file,
                                                                                                    version, SCHEMA_VERSION))
            log.warning("Will try to proceed anyway, but please be advised that configuration options may have changed.")
        # populate parser with items from config
        parser.populate_parser(config)
        # reparse arguments
        caracal.log.info("Loading pipeline configuration from {}".format(config_file), extra=dict(color="GREEN"))
        options, config = parser.update_config_from_args(config, argv)
        # raise warning on schema version
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
        if options.debug:
            log.warning("you are running with -debug enabled, dropping you into pdb. Use Ctrl+D to exit.")
            pdb.post_mortem(sys.exc_info()[2])
        sys.exit(1)  # indicate failure

    if options.report and options.no_reports:
        log.error("-report contradicts --no-reports")
        sys.exit(1)

    log_logo()
    # Very good idea to print user options into the log before running:
    parser.log_options(config)

    execute_pipeline(options, config, block=True)

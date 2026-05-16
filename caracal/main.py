import logging
import os
import pdb  # noqa: T100
import shutil
import sys
import traceback
import warnings

import stimela

import caracal
import caracal.dispatch_crew.caltables as mkct
from caracal import log, utils
from caracal.dispatch_crew import config_parser, worker_help
from caracal.schema import SCHEMA_VERSION
from caracal.workers.worker_administrator import WorkerAdministrator

warnings.filterwarnings("ignore", category=SyntaxWarning)

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
    schema = os.path.join(pckgdir, "schema", f"{worker:s}_schema.yml")
    if not os.path.exists(schema):
        return None

    worker_dict = utils.load_yaml(schema)

    helper = worker_help.worker_options(worker, worker_dict["mapping"][worker])

    helper.print_worker()
    return True


def get_default(sample, to):
    """
    Get default parset copy
    """
    log.info(f"Dumping default configuration to {to:s} as requested. Goodbye!")
    sample_config = os.path.join(pckgdir, "sample_configurations", SAMPLE_CONFIGS[sample])
    shutil.copyfile(sample_config, to)


def log_logo():
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

    log.info(f"Version {__version__!s:s} installed at {pckgdir:s}")


def execute_pipeline(options, config):
    # setup piping infractructure to send messages to the parent
    workers_directory = os.path.join(caracal.pckgdir, "workers")
    backend = config["general"]["backend"]
    if options.container_tech and options.container_tech != "default":
        backend = options.container_tech

    def __run(debug=False):
        """Executes pipeline"""
        #        with stream_director(log) as director:  # stdout and stderr needs to go to the log as well -- nah

        try:
            pipeline = WorkerAdministrator(
                config,
                workers_directory,
                prefix=options.general_prefix,
                configFileName=options.config,
                singularity_image_dir=options.singularity_image_dir,
                container_tech=backend,
                start_worker=options.start_worker,
                end_worker=options.end_worker,
                generate_reports=not options.no_reports,
            )

            if options.report:
                pipeline.regenerate_reports()
            else:
                pipeline.run_workers()
        except SystemExit as e:
            # if e.code != 0:
            log.error(f"A pipeline worker initiated sys.exit({e.code}). This is likely a bug, please report.")
            log.info(f"  More information can be found in the logfile at {caracal.CARACAL_LOG:s}")
            log.info(f"  You are running version {__version__!s:s}", extra={"logfile_only": True})
            if debug:
                log.warning("you are running with -debug enabled, dropping you into pdb. Use Ctrl+D to exit.")
                pdb.post_mortem(sys.exc_info()[2])
            sys.exit(1)  # indicate failure

        except KeyboardInterrupt:
            log.error("Ctrl+C received from user, shutting down. Goodbye!")
        except Exception as exc:  # noqa: BLE001
            log.error(f"{exc} [{type(exc).__name__}]", extra={"boldface": True})
            log.info(f"  More information can be found in the logfile at {caracal.CARACAL_LOG:s}")
            log.info(f"  You are running version {__version__!s:s}", extra={"logfile_only": True})
            for line in traceback.format_exc().splitlines():
                log.error(line, extra={"traceback_report": True})
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
            parser.error(f"unknown worker '{options.worker_help}'")
        return

    caracal.log.info(f"Invoked as {' '.join(sys.argv)}")

    # User requests default config => dump and exit
    if options.get_default:
        sample_config = SAMPLE_CONFIGS.get(options.get_default_template)
        if sample_config is None:
            parser.error(f"unknown default template '{options.get_default_template}'")
        sample_config_path = os.path.join(pckgdir, "sample_configurations", sample_config)
        if not os.path.exists(sample_config_path):
            raise RuntimeError(f"Missing sample config file {sample_config}. This is a bug, please report")
        # validate the file
        try:
            parser = config_parser.config_parser()
            _, version = parser.validate_config(sample_config_path)
            if version != SCHEMA_VERSION:
                log.warning(f"Sample config file {sample_config} version is {SCHEMA_VERSION}, current CARACal version is {{version}}.")
                log.warning("Proceeding anyway, but please notify the CARACal team to ship a newer sample config!")
        except config_parser.ConfigErrors as exc:
            log.error(f"{exc}, list of errors follows:")
            for section, errors in exc.errors.items():
                print(f"  {section}:")
                for err in errors:
                    print(f"    - {err}")
            sys.exit(1)  # indicate failure
        log.info(f"Initializing {options.get_default} from config template '{options.get_default_template}' (schema version {{version}})")
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
            log.warning(f"Config file {config_file} schema version is {SCHEMA_VERSION}, current CARACal version is {version}")
            log.warning("Will try to proceed anyway, but please be advised that configuration options may have changed.")
        # populate parser with items from config
        parser.populate_parser(config)
        # reparse arguments
        caracal.log.info(f"Loading pipeline configuration from {config_file}", extra={"color": "GREEN"})
        options, config = parser.update_config_from_args(config, argv)
        # raise warning on schema version
    except config_parser.ConfigErrors as exc:
        log.error(f"{exc}, list of errors follows:")
        for section, errors in exc.errors.items():
            print(f"  {section}:")
            for err in errors:
                print(f"    - {err}")
        sys.exit(1)  # indicate failure
    except Exception as exc:  # noqa BLE001
        traceback.print_exc()
        log.error(f"Error parsing arguments or configuration: {exc}")
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

    execute_pipeline(options, config)

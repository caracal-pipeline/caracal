import argparse
import yaml
import meerkathi
import os
import copy
import ruamel.yaml
import itertools
from collections import OrderedDict

DEFAULT_CONFIG = os.path.join(meerkathi.pckgdir, 'default-config.yml')

def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file '%s' does not exist!" % arg)

    return arg


class config_parser:
    __ARGS = None
    __GROUPS = None

    @classmethod
    def __store_args(cls, args, arg_groups):
        """ Store arguments for later retrieval """

        if cls.__ARGS is not None:
            meerkathi.log.warn("Replacing existing stored arguments '{}'"
                               "with '{}'.", __ARGS, args)

        cls.__ARGS = args
        cls.__GROUPS = arg_groups

    @property
    def arg_groups(self):
        """ Retrieve groups """
        cls = self.__class__
        if cls.__GROUPS is None:
            raise ValueError("No arguments were stored. "
                             "Please call store_args first.")

        return copy.deepcopy(cls.__GROUPS)

    @property
    def args(self):
        """ Retrieve stored arguments """
        cls = self.__class__
        if cls.__ARGS is None:
            raise ValueError("No arguments were stored. "
                             "Please call store_args first.")

        return copy.deepcopy(cls.__ARGS)

    @classmethod
    def __primary_parser(cls, add_help=False):
        parser = argparse.ArgumentParser("MeerKATHI HI and Continuum Imaging Pipeline.\n"
                                         "(C) RARG, SKA-SA 2016-2017.\n"
                                         "All rights reserved.",
                                         add_help=add_help)
        add = parser.add_argument
        add("-v","--version", action='version',version='{0:s} version {1:s}'.format(parser.prog, meerkathi.__version__))
        add('-c', '--config',
            type=lambda a: is_valid_file(parser, a),
            default=DEFAULT_CONFIG,
            help='Pipeline configuration file (YAML/JSON format)')

        add('-gd', '--get-default',
            help='Name file where the configuration should be saved')

        add('-aaf', '--add-all-first', action='store_true',
            help='Add steps from all workers to pipeline before exucting. Default is execute each workers as they are encountered.')

        add('-bl', '--stimela-build',
            help='Label of stimela build to use',
            default=None)

        add('-pcs', '--print-calibrator-standard',
            help='Prints auxilary calibrator standard into the log',
            action='store_true')

        add('-wd', '--workers-directory', default='{:s}/workers'.format(meerkathi.pckgdir),
            help='Directory where pipeline workers can be found. These are stimela recipes describing the pipeline')

        add('-ce', '--config-editor', action='store_true',
            help='Start the interactive configuration editor (requires X session with decent [ie. firefox] webbrowser installed).')

        add('-rv', '--report-viewer', action='store_true',
            help='Start the interactive report viewer (requires X session with decent [ie. firefox] webbrowser installed).')

        add('--interactive-port', type=int, default=8888,
            help='Port on which to listen when an interactive mode is selected (e.g the configuration editor)')

        return parser

    __HAS_BEEN_INIT = False

    def __init__(self, args=None):
        """ Configuration parser. Sets up command line interface for MeerKATHI
            This is a singleton class, and should only be initialized once
        """
        cls = self.__class__
        if cls.__HAS_BEEN_INIT:
            return

        cls.__HAS_BEEN_INIT = True

        """ Extract """

        #=========================================================
        # Handle the configuration file argument first,
        # if one is supplied use that for defaulting arguments
        # created further down the line, otherwise use the
        # default configuration file
        #=========================================================
        # Create parser object
        parser  = cls.__primary_parser()

        # Lambda for transforming sections and options
        xformer = lambda s: s.replace('-', '_')
        def _str2bool(v):
            if v.upper() in ("YES","TRUE"):
                return True
            elif v.upper() in ("NO","FALSE"):
                return False
            else:
                raise argparse.ArgumentTypeError("Failed to convert argument. Must be one of "
                                                 "'yes', 'true', 'no' or 'false'.")

        def _nonetype(v):
            if v.upper() in ("NONE","NULL"):
                return None
            else:
                return str(v)

        def _subparser_tree(sections,
                            base_section="",
                            update_only = False,
                            args = None,
                            parser = None):
            """ Recursively creates subparser tree for the config """
            if sections is None:
                return

            sec_defaults = {xformer(k): v for k,v in sections.iteritems() }

            # Transform keys
            # Add subsection / update when necessary
            groups = OrderedDict()
            for opt, default in sec_defaults.iteritems():
                if opt == "__helpstr": continue
                option_name = base_section + "_" + opt if base_section != "" else opt
                if isinstance(default, dict):
                    groups[opt] = _subparser_tree(default,
                                                  base_section=option_name,
                                                  update_only=update_only,
                                                  args=args,
                                                  parser=parser)
                else:
                    if update_only:
                        parser.set_defaults(**{option_name: default})
                        groups[opt] = getattr(args, option_name)
                    else:
                        if isinstance(default, list):
                            parser.add_argument("--%s" % option_name,
                                                type=str,
                                                default=default,
                                                nargs="?",
                                                action='append')
                        elif isinstance(default, bool):
                            parser.add_argument("--%s" % option_name,
                                                type=_str2bool,
                                                default=default,
                                                nargs="?",
                                                const=True)
                        elif default is None:
                            parser.add_argument("--%s" % option_name,
                                                type=_nonetype,
                                                default=default,
                                                nargs="?")
                        else:
                            parser.add_argument("--%s" % option_name,
                                                type=type(default),
                                                default=default,
                                                nargs="?")

                        groups[opt] = default
            return groups

        # Parse user commandline options, loading defaults either from the default pipeline or user-supplied pipeline
        args_bak = copy.deepcopy(args)
        args, remainder = parser.parse_known_args(args_bak)
        if args.config:
            meerkathi.log.info("Loading defaults from user configuration '{}'".format(args.config))
            with open(args.config, 'r') as f:
                file_config = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1))
        else:
            meerkathi.log.info("Loading defaults from installation configuration '{}'".format(DEFAULT_CONFIG))
            with open(DEFAULT_CONFIG, 'r') as f:
                file_config = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1))
        parser = cls.__primary_parser(add_help=True)
        groups = _subparser_tree(file_config, parser=parser)
        args, remainder = parser.parse_known_args(args_bak)
        if len(remainder) > 0:
            raise RuntimeError("The following arguments were not parsed: %s" ",".join(remainder))
        groups = _subparser_tree(file_config, update_only=True, args=args, parser=parser)

        cls.__store_args(args, groups)

    @classmethod
    def log_options(cls):
        """ Prints argument tree to the logger for prosterity to behold """
        meerkathi.log.info("".join(["".ljust(25,"#"), " PIPELINE CONFIGURATION ", "".ljust(25,"#")]))
        def _tree_print(branch, indent="\t"):
            dicts = OrderedDict( [(k, v) for k, v in branch.iteritems() if isinstance(v, dict)] )
            other = OrderedDict( [(k, v) for k, v in branch.iteritems() if not isinstance(v, dict)] )

            def _printval(k, v):
                if isinstance(v, dict):
                    (indent=="\t") and meerkathi.log.info(indent.ljust(60,"#"))
                    meerkathi.log.info(indent + "Subsection %s:" % k)
                    (indent=="\t") and meerkathi.log.info(indent.ljust(60,"#"))
                    (indent!="\t") and meerkathi.log.info(indent.ljust(60,"-"))
                    _tree_print(v, indent=indent+"\t")
                else:
                    meerkathi.log.info("%s%s= %s" %(indent,
                                                   k.ljust(30),
                                                   v))

            for k, v in other.iteritems(): _printval(k, v)
            for k, v in dicts.iteritems(): _printval(k, v)
        ordered_groups = OrderedDict(sorted(cls.__GROUPS.items(),
                                             key=lambda p: p[1].get("order",0)))
        _tree_print(ordered_groups)
        meerkathi.log.info("".join(["".ljust(25,"#"), " END OF CONFIGURATION ", "".ljust(25,"#")]))

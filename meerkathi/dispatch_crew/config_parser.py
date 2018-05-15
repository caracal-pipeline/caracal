import argparse
import yaml
import meerkathi
import os
import copy
import ruamel.yaml
from pykwalify.core import Core
import itertools
from collections import OrderedDict

DEFAULT_CONFIG = meerkathi.DEFAULT_CONFIG

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

        add('-s', '--schema', action='append', metavar='[WORKER_NAME,PATH_TO_SCHEMA]',
            help='Path to custom schema for worker(s). Can be specified multiple times')

        add('-wh', '--worker-help', metavar="WORKER_NAME",
            help='Get help for a worker')

        add('-pcs', '--print-calibrator-standard',
            help='Prints auxilary calibrator standard into the log',
            action='store_true')

        add('-wd', '--workers-directory', default='{:s}/workers'.format(meerkathi.pckgdir),
            help='Directory where pipeline workers can be found. These are stimela recipes describing the pipeline')

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

        def _nonetype(v, opt_type="str"):
            type_map = { "str" : str, "int": int, "float": float, "bool": _str2bool, "text": str }
            _opt_type = type_map[opt_type]
            if v.upper() in ("NONE","NULL"):
                return None
            else:
                return _opt_type(v)

        def _option_factory(opt_type,
                            is_list,
                            opt_name,
                            opt_required,
                            opt_desc,
                            opt_valid_opts,
                            opt_default,
                            parser_instance):
            opt_desc = opt_desc.replace("%", "%%").encode('utf-8').strip()
            if opt_type == "int" or opt_type == "float" or opt_type == "str" or opt_type == "bool" or opt_type == "text":
                meta = opt_type
                parser_instance.add_argument("--%s" % opt_name,
                                             choices=opt_valid_opts,
                                             default=opt_default,
                                             nargs=("+" if opt_required else "*") if is_list else "?",
                                             metavar=meta,
                                             type=_nonetype,
                                             help=opt_desc + " [%s default: %s]" % ("list:%s" % opt_type if is_list else opt_type, str(opt_default)))
            else:
                raise ValueError("opt_type %s not understood for %s" % (opt_type, opt_name))



        def _subparser_tree(sections,
                            schema_sections,
                            base_section="",
                            update_only = False,
                            args = None,
                            parser = None):
            """ Recursively creates subparser tree for the config """
            groups = OrderedDict()

            if sections is None:
                return groups

            sec_defaults = {xformer(k): v for k,v in sections.iteritems()}

            # Transform keys
            # Add subsection / update when necessary
            assert isinstance(schema_sections, dict)
            assert schema_sections["type"] == "map"
            assert isinstance(schema_sections["mapping"], dict)
            for opt, default in sec_defaults.iteritems():
                option_name = base_section + "_" + opt if base_section != "" else opt
                assert opt in schema_sections["mapping"], "%s does not define a type in schema" % opt
                if isinstance(default, dict):
                    groups[opt] = _subparser_tree(default,
                                                  schema_sections["mapping"][opt],
                                                  base_section=option_name,
                                                  update_only=update_only,
                                                  args=args,
                                                  parser=parser)
                else:
                    assert (("seq" in schema_sections["mapping"][opt]) and ("type" in schema_sections["mapping"][opt]["seq"][0])) or \
                            "type" in schema_sections["mapping"][opt], "Option %s missing type in schema" % option_name

                    if update_only:
                        parser.set_defaults(**{option_name: default})
                        groups[opt] = getattr(args, option_name)
                    else:
                        _option_factory(schema_sections["mapping"][opt]["type"] if "seq" not in schema_sections["mapping"][opt] else \
                                                                schema_sections["mapping"][opt]["seq"][0]["type"],
                                        "seq" in schema_sections["mapping"][opt],
                                        option_name,
                                        schema_sections["mapping"][opt].get("required", False),
                                        schema_sections["mapping"][opt].get("desc", "!!! option %s missing schema description. Please file this bug !!!" % option_name),
                                        schema_sections["mapping"][opt].get("enum", None),
                                        default,
                                        parser)
                        groups[opt] = default
            return groups

        # Parse user commandline options, loading defaults either from the default pipeline or user-supplied pipeline
        args_bak = copy.deepcopy(args)
        args, remainder = parser.parse_known_args(args_bak)
        file_config = {}
        if args.schema:
            _schema = {}
            for item in args.schema:
                _name, __schema = item.split(",")
                _schema[_name] = __schema
            args.schema = _schema
        else:
            args.schema = {}


        config_file = args.config if args.config else DEFAULT_CONFIG

        with open(args.config, 'r') as f:
            tmp = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1))
            schema_version = tmp["schema_version"]

        # Validate each worker section against the schema and
        # parse schema to extract types and set up cmd argument parser
        parser = cls.__primary_parser(add_help=True)
        for key,worker in tmp.iteritems():
            if key=="schema_version":
                continue
            elif worker.get("enable", True) is False:
                continue
            _key = key.split("__")[0]
            schema_fn = os.path.join(meerkathi.pckgdir,
                                     "schema", "{0:s}_schema-{1:s}.yml".format(_key,
                                                                               schema_version))
            source_data = {
                            _key : worker,
                            "schema_version" : schema_version,
            }
            c = Core(source_data=source_data, schema_files=[schema_fn])
            file_config[key] = c.validate(raise_exception=True)[_key]
            with open(schema_fn, 'r') as f:
                schema = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1))
            _subparser_tree(file_config[key],
                            schema["mapping"][_key],
                            base_section=key,
                            parser=parser)

        # finally parse remaining args and update parameter tree with user-supplied commandline arguments
        args, remainder = parser.parse_known_args(args_bak)
        if len(remainder) > 0:
            raise RuntimeError("The following arguments were not parsed: %s" ",".join(remainder))

        groups = OrderedDict()
        for key,worker in tmp.iteritems():
            if key=="schema_version":
                continue
            elif worker.get("enable", True) is False:
                continue

            _key = key.split("__")[0]
            schema_fn = os.path.join(meerkathi.pckgdir,
                                      "schema", "{0:s}_schema-{1:s}.yml".format(_key,
                                                                                schema_version))
            with open(schema_fn, 'r') as f:
                schema = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1))
            groups[key] = _subparser_tree(file_config[key],
                                          schema["mapping"][_key],
                                          base_section=key,
                                          update_only=True,
                                          args=args,
                                          parser=parser)
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
                    if not v.get("enable", True): return
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

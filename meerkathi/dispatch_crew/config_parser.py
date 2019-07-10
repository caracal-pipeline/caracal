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
                               "with '{}'.".format(cls.__ARGS, args))

        cls.__ARGS = args
        cls.__GROUPS = arg_groups

    @classmethod
    def __store_global_schema(cls, schema):
        """ Store arguments for later retrieval """
        cls.__GLOBAL_SCHEMA = schema

    @property
    def arg_groups(self):
        """ Retrieve groups """
        cls = self.__class__
        if cls.__GROUPS is None:
            raise ValueError("No arguments were stored. "
                             "Please call store_args first.")

        return copy.deepcopy(cls.__GROUPS)

    def update_key(self, chain, new_value):
        """ Update a single value given a chain of keys """
        cls = self.__class__
        if cls.__GROUPS is None:
            raise ValueError("Please call store_args first.")
        def __walk_down_set(groups, chain, new_value):
            if len(chain) > 1:
                k = chain[0]
                if k not in groups:
                    raise KeyError("{} not a valid key for update rule".format(k))
                __walk_down_set(groups[k], chain[1:], new_value)
            else:
                if chain[0] == "enable" and chain[0] not in groups:
                    raise ValueError("This is a compulsory section and cannot be switched off")
                elif chain[0] not in groups:
                    raise KeyError("{} not a valid key for update rule".format(chain[0]))
                groups[chain[0]] = new_value
        __walk_down_set(self.__GROUPS, chain, new_value)
        self.update_args_key(chain, new_value)

    def update_args_key(self, chain, new_value):
        cls = self.__class__
        if cls.__GROUPS is None:
            raise ValueError("Please call store_args first.")
        setattr(self.__ARGS, "_".join(chain), new_value)
        

    def get_key(self, chain):
        """ Get value given a chain of keys """
        cls = self.__class__
        if cls.__GROUPS is None:
            raise ValueError("Please call store_args first.")
        def __walk_down_get(groups, chain):
            if len(chain) > 1:
                k = chain[0]
                if k not in groups:
                    raise KeyError("{} not a valid key for lookup rule".format(k))
                return __walk_down_get(groups[k], chain[1:])
            else:
                if chain[0] == "enable" and chain[0] not in groups:
                    return True
                elif chain[0] not in groups:
                    raise KeyError("{} not a valid key for lookup rule".format(chain[0]))
                return groups[chain[0]]

        return __walk_down_get(cls.__GROUPS, chain)
    
    def __get_schema_attr(self, chain, attr="desc"):
        """ Get schema attribute given a chain of keys """
        cls = self.__class__
        if cls.__GROUPS is None:
            raise ValueError("Please call store_args first.")
        def __walk_down_get(schema, chain):
            if len(chain) > 1:
                k = chain[0]
                if k not in schema and not ("mapping" in schema and k in schema["mapping"]):
                    raise KeyError("{} not a valid key for lookup rule".format(k))
                child = schema[k] if k in schema else schema["mapping"][k]
                return __walk_down_get(child, chain[1:])
            else:
                k = chain[0]
                if k == "enable" and k not in schema and not ("mapping" in schema and k in schema["mapping"]):
                    if attr == "desc":
                        return "Section enabled or not"
                    elif attr == "type":
                        return "bool"
                    elif attr == "required":
                        return True
                    elif attr == "mapping":
                        return None
                    elif attr == "enum":
                        return [True, False]
                    elif attr == "seq":
                        return None
                elif k not in schema and not ("mapping" in schema and k in schema["mapping"]):
                    raise KeyError("{} not a valid key for lookup rule".format(k))
                child = schema[k] if k in schema else schema["mapping"][k]
                return child.get(attr, None)

        return __walk_down_get(cls.__GLOBAL_SCHEMA, chain)

    def get_schema_help(self, chain):
        """ Get schema help string """
        return self.__get_schema_attr(chain, attr="desc")

    def get_schema_type(self, chain):
        """ Get schema type """
        return self.__get_schema_attr(chain, attr="type")

    def get_schema_required(self, chain):
        """ Get schema type """
        return self.__get_schema_attr(chain, attr="required")

    def is_schema_endnode(self, chain):
        """ checks if key has children """
        return self.__get_schema_attr(chain, attr="mapping") is not None
        
    def get_schema_enum(self, chain):
        """ get enum of schema key if exists otherwise None """
        return self.__get_schema_attr(chain, attr="enum")

    def get_schema_seq(self, chain):
        """ get enum of schema key if exists otherwise None """
        is_seq = self.__get_schema_attr(chain, attr="seq") is not None
        if is_seq:
            return self.__get_schema_attr(chain, attr="seq")[0]["type"]
        else:
            return None
    
    @property
    def args(self):
        """ Retrieve stored arguments """
        cls = self.__class__
        if cls.__ARGS is None:
            raise ValueError("No arguments were stored. "
                             "Please call store_args first.")

        return copy.deepcopy(cls.__ARGS)

    @property
    def global_schema(self):
        cls = self.__class__
        if cls.__GLOBAL_SCHEMA is None:
            raise ValueError("No schemas were parsed. "
                             "Please call store_global_schama first.")
        return copy.deepcopy(cls.__GLOBAL_SCHEMA)

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

        add('-sid', '--singularity-image-dir',
            help='Directory where stimela singularity images are stored')

        add('-gd', '--get-default',
            help='Name file where the configuration should be saved')

        add('-aaf', '--add-all-first', action='store_true',
            help='Add steps from all workers to pipeline before execucting. Default is execute each workers as they are encountered.')

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

        add('-ct', '--container-tech', choices=["docker", "udocker", "singularity"], default="docker",
            help='Container technology to use')

        add('--no-interactive',
            help='Disable interactivity',
            action='store_true')

        add('-wd', '--workers-directory', default='{:s}/workers'.format(meerkathi.pckgdir),
            help='Directory where pipeline workers can be found. These are stimela recipes describing the pipeline')

        add('-rv', '--report-viewer', action='store_true',
            help='Start the interactive report viewer (requires X session with decent [ie. firefox] webbrowser installed).')

        add('--interactive-port', type=int, default=8888,
            help='Port on which to listen when an interactive mode is selected (e.g the configuration editor)')

        add('--reconstruct-defaults-from-schema', help="Developer option to reconstruct default parset from schema",
            action='store_true')

        add("-la", '--log-append', help="Append to existing log-meerkathi.txt file instead of replacing it",
            action='store_true')
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

        # Parse user commandline options, loading defaults either from the default pipeline or user-supplied pipeline
        args_bak = copy.deepcopy(args)
        args, remainder = parser.parse_known_args(args_bak)
        cls.__validated_schema = {}
        if args.schema:
            _schema = {}
            for item in args.schema:
                _name, __schema = item.split(",")
                _schema[_name] = __schema
            args.schema = _schema
        else:
            args.schema = {}

        with open(args.config, 'r') as f:
            tmp = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1))
            schema_version = tmp["schema_version"]

        # Validate each worker section against the schema and
        # parse schema to extract types and set up cmd argument parser
        parser = cls.__primary_parser(add_help=True)
        for key,worker in tmp.iteritems():
            if key=="schema_version":
                continue
            #elif worker.get("enable", True) is False:
            #    continue
            _key = key.split("__")[0]
            schema_fn = os.path.join(meerkathi.pckgdir,
                                     "schema", "{0:s}_schema-{1:s}.yml".format(_key,
                                                                               schema_version))
            source_data = {
                            _key : worker,
                            "schema_version" : schema_version,
            }
            c = Core(source_data=source_data, schema_files=[schema_fn])
            cls.__validated_schema[key] = c.validate(raise_exception=True)[_key]
            with open(schema_fn, 'r') as f:
                schema = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1))
            cls._subparser_tree(self.__validated_schema[key],
                                schema["mapping"][_key],
                                base_section=key,
                                parser=parser)

        # finally parse remaining args and update parameter tree with user-supplied commandline arguments
        args, remainder = parser.parse_known_args(args_bak)
        if len(remainder) > 0:
            raise RuntimeError("The following arguments were not parsed: %s" ",".join(remainder))

        self.update_config(args)


    @classmethod
    def _subparser_tree(cls,
                        sections,
                        schema_sections,
                        base_section="",
                        update_only = False,
                        args = None,
                        parser = None):
        """ Recursively creates subparser tree for the config """
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
                                             type=lambda x: _nonetype(x, opt_type),
                                             help=opt_desc + " [%s default: %s]" % ("list:%s" % opt_type if is_list else opt_type, str(opt_default)))
            else:
                raise ValueError("opt_type %s not understood for %s" % (opt_type, opt_name))


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
                groups[opt] = cls._subparser_tree(default,
                                                  schema_sections["mapping"][opt],
                                                  base_section=option_name,
                                                  update_only=update_only,
                                                  args=args,
                                                  parser=parser)
            else:
                assert (("seq" in schema_sections["mapping"][opt]) and ("type" in schema_sections["mapping"][opt]["seq"][0])) or \
                        "type" in schema_sections["mapping"][opt], "Option %s missing type in schema" % option_name

                if update_only == "defaults and args":
                    parser.set_defaults(**{option_name: default})
                    
                    setattr(args, option_name, default)
                    groups[opt] = default
                elif update_only == "defaults":
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

    @classmethod
    def update_config(cls, args = None, update_mode="defaults"):
        """ Updates argument parser with values from config file """
        args = cls.__ARGS if not args else args
        parser = cls.__primary_parser()
        with open(args.config, 'r') as f:
            tmp = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1))
            schema_version = tmp["schema_version"]

        groups = OrderedDict()
        global_schema = OrderedDict()
        if not cls.__validated_schema:
            raise RuntimeError("Must init singleton before running this method")

        for key,worker in tmp.iteritems():
            if key=="schema_version":
                continue
            #elif worker.get("enable", True) is False:
            #    continue

            _key = key.split("__")[0]
            schema_fn = os.path.join(meerkathi.pckgdir,
                                      "schema", "{0:s}_schema-{1:s}.yml".format(_key,
                                                                                schema_version))
            if update_mode == "defaults and args": # new parset, re-validate
                source_data = {
                            _key : worker,
                            "schema_version" : schema_version,
                }
                c = Core(source_data=source_data, schema_files=[schema_fn])
                cls.__validated_schema[key] = c.validate(raise_exception=True)[_key]

            with open(schema_fn, 'r') as f:
                schema = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1))
            groups[key] = cls._subparser_tree(cls.__validated_schema[key],
                                              schema["mapping"][_key],
                                              base_section=key,
                                              update_only=update_mode,
                                              args=args,
                                              parser=parser)
            global_schema[key] = schema["mapping"][_key]
        cls.__store_args(args, groups)
        cls.__store_global_schema(global_schema)

    @classmethod
    def save_options(cls, filename):
        """ Save configuration options to yaml """
        if not cls.__GROUPS:
            raise RuntimeError("Singleton must be initialized before this method is called")
        dictovals = copy.deepcopy(cls.__GROUPS)
        dictovals["schema_version"] = "0.1.0"

        with open(filename, 'w') as f:
            f.write(yaml.dump(dictovals, Dumper=ruamel.yaml.RoundTripDumper))

    @classmethod
    def reconstruct_defaults(cls, filename, schema_version="0.1.0"):
        groups = OrderedDict()
        global_schema = OrderedDict()
        if not cls.__validated_schema:
            raise RuntimeError("Must init singleton before running this method")

        with open(DEFAULT_CONFIG, 'r') as f:
            parset = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1))

        dictovals = {}
        import glob
        detected_workers = []
        for worker in glob.glob(os.path.join(meerkathi.pckgdir,
                                             "schema", "*_schema-{0:s}.yml".format(schema_version))):
            _key = key=os.path.basename(worker).replace("_schema-{0:s}.yml".format(schema_version), "")
            schema_fn = os.path.join(meerkathi.pckgdir,
                                      "schema", "{0:s}_schema-{1:s}.yml".format(key,
                                                                                schema_version))
            meerkathi.log.info("Processing {0:s}".format(_key))
            detected_workers += [_key]
            with open(schema_fn, 'r') as f:
                schema = ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader, version=(1,1))

            def __recursive_reconstruct(schema, parset, dictovals):
                for key in schema["mapping"]:
                    if hasattr("__dict__", key):
                        dictovals[key] = {}
                        __recursive_reconstruct(key, parset[key], dictovals[key])
                    else:
                        default = parset.get(key, "!!UNDEFINED!!")
                        dictovals[key] = default
            dictovals[_key] = {}
            __recursive_reconstruct(schema["mapping"][_key], parset[_key], dictovals[_key])
        with open(filename, 'w') as f:
            sorted_keys = sorted(dictovals, key=lambda k: dictovals[k].get("order", 0) if hasattr(dictovals[k], "get") else 0)
            o = OrderedDict({k: dictovals[k] for k in sorted_keys})
            o["schema_version"] = "0.1.0"
            f.write(yaml.dump(o,
                              Dumper=ruamel.yaml.RoundTripDumper))

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

import argparse
import yaml
import caracal
import os
import copy
import ruamel.yaml
from collections import OrderedDict

# shut this guy up
import logging
pykwalify_logger = logging.getLogger('pykwalify.core')
pykwalify_logger.propagate = False
pykwalify_logger.setLevel(logging.CRITICAL)

from pykwalify.core import Core


DEFAULT_CONFIG = caracal.DEFAULT_CONFIG

class ConfigErrors(RuntimeError):
    def __init__(self, config_file, error_dict):
        RuntimeError.__init__(self, "configuration file {} fails to validate".format(config_file))
        self.config_file = config_file
        self.errors = error_dict



def primary_parser(add_help=True):
    """Returns ArgumentParser for basic command-line options"""

    parser = argparse.ArgumentParser(description="""
Welcome to CARACal (https://github.com/caracal-pipeline), a containerized data reduction pipeline for radio 
interferometry.""",
        usage="%(prog)s [-options] -c config_file",
        epilog="""
You can override configuration file settings using additional "--worker-option value" arguments. Use
"-wh worker" to get help on a particular worker.

To get started, run e.g. "%(prog)s -gdt meerkat -gd config.yml" to make yourself an initial configuration file, 
then edit the file to suit your needs.
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=add_help)
    add = parser.add_argument
    add("-v", "--version", action='version',
        version='{0:s} version {1:s}'.format(parser.prog, caracal.__version__))

    add('-c', '--config',
        type=lambda a: is_valid_file(parser, a),
        default=DEFAULT_CONFIG,
        help='pipeline configuration file. This is a mandatory argument.')

    add('-b', '--boring',
        help='enable boring mode, i.e. suppress colours in console output',
        action='store_true')

    add('-sid', '--singularity-image-dir', metavar="DIR",
        help='directory where stimela singularity images are stored')

    add('-gdt', '--get-default-template', # metavar="TEMPLATE",
            choices=caracal.SAMPLE_CONFIGS.keys(),
            default="minimal",
            help='init a configuration file from a default template')

    add('-gd', '--get-default', metavar="FILE",
        help='name of file where the template should be saved (use in conjunction with -gdt)')

    add('-sw', '--start-worker', metavar="WORKER",
        help='start pipeline at this worker')

    add('-ew', '--end-worker', metavar="WORKER",
        help='stop pipeline after this worker')

    # add('-aaf', '--add-all-first', action='store_true',
    #     help='add steps from all workers to pipeline before executing (default is execute in turn)')

    add('-bl', '--stimela-build',
        help='label of custom stimela build to use',
        default=None)

    add('-ct', '--container-tech', choices=["docker", "udocker", "singularity", "podman"],
        default="docker",
        help='Container technology to use')

    add('-wh', '--worker-help', metavar="WORKER",
        help='prints help for a particular worker, then exits')

    add('-pcs', '--print-calibrator-standard',
        help='prints list of auxiliary calibrator standards, then exits',
        action='store_true')

    # add('--no-interactive',
    #     help='Disable interactivity',
    #     action='store_true')

    add('-debug',
        help='enable debugging mode',
        action='store_true')

    add('-nr','--no-reports',
        help='disable generation of report about the pipeline run',
        action='store_true')

    add('-wd', '--workers-directory', default='{:s}/workers'.format(caracal.pckgdir),
        help='(ninja option) directory where custom pipeline workers can be found')
    add('-s', '--schema', action='append', metavar='[WORKER_NAME,PATH_TO_SCHEMA]',
        help='(ninja option) path to custom schema for worker(s), can be specified multiple times')


    # add('-rv', '--report-viewer', action='store_true',
    #     help='Start the interactive report viewer (requires X session with decent [ie. firefox] webbrowser installed).')
    #
    # add('--interactive-port', type=int, default=8888,
    #     help='Port on which to listen when an interactive mode is selected (e.g the configuration editor)')

    # add("-la", '--log-append', help="Append to existing log-caracal.txt file instead of replacing it",
    #     action='store_true')

    return parser


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
            caracal.log.warn("Replacing existing stored arguments '{}'"
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
                    raise KeyError(
                        "{} not a valid key for update rule".format(k))
                __walk_down_set(groups[k], chain[1:], new_value)
            else:
                if chain[0] == "enable" and chain[0] not in groups:
                    raise ValueError(
                        "This is a compulsory section and cannot be switched off")
                elif chain[0] not in groups:
                    raise KeyError(
                        "{} not a valid key for update rule".format(chain[0]))
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
                    raise KeyError(
                        "{} not a valid key for lookup rule".format(k))
                return __walk_down_get(groups[k], chain[1:])
            else:
                if chain[0] == "enable" and chain[0] not in groups:
                    return True
                elif chain[0] not in groups:
                    raise KeyError(
                        "{} not a valid key for lookup rule".format(chain[0]))
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
                    raise KeyError(
                        "{} not a valid key for lookup rule".format(k))
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
                    raise KeyError(
                        "{} not a valid key for lookup rule".format(k))
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

    __HAS_BEEN_INIT = False
    _parser = None

    def __init__(self, args=None):
        """ Configuration parser. Sets up command line interface for CARACal
            This is a singleton class, and should only be initialized once
        """
        cls = self.__class__
        if cls.__HAS_BEEN_INIT:
            return

        cls.__HAS_BEEN_INIT = True

        """ Extract """

        # =========================================================
        # Handle the configuration file argument first,
        # if one is supplied use that for defaulting arguments
        # created further down the line, otherwise use the
        # default configuration file
        # =========================================================
        # Create parser object
        config_parser._parser = parser = primary_parser()

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
            tmp = ruamel.yaml.load(
                f, ruamel.yaml.RoundTripLoader, version=(1, 1))
            self.schema_version = schema_version = tmp["schema_version"]

        # Validate each worker section against the schema and
        # parse schema to extract types and set up cmd argument parser

        #self._parser = parser = cls.__primary_parser(add_help=True)
        groups = OrderedDict()

        errors = OrderedDict()

        for worker, variables in tmp.items():
            if worker == "schema_version":
                continue
            _worker = worker.split("__")[0]

            schema_fn = os.path.join(caracal.pckgdir,
                                     "schema", "{0:s}_schema.yml".format(_worker))

            if not os.path.exists(schema_fn):
                errors[worker] = ["this is not a recognized worker name, or its schema file is missing"]
                continue

            # SCHEMA VALIDATION automatically check if variables of cfg file are given with appropriate syntax
            source_data = {
                _worker: variables,
                "schema_version": schema_version,
            }
            c = Core(source_data=source_data, schema_files=[schema_fn])

            cls.__validated_schema[worker] = c.validate(raise_exception=False)[_worker]

            # check for errors
            if c.validation_errors:
                errs = errors[worker] = []
                for message in c.validation_errors:
                    # crude hack: we're already fooling the schema by using "flagging" for the worker name
                    # when the name is e.g. "flagger__2", so the message is misleading. Substitute the hack back.
                    message = message.replace("'/{}'".format(_worker), "'/{}'".format(worker))
                    errs.append(message)
                continue

            with open(schema_fn, 'r') as f:
                schema = ruamel.yaml.load(
                    f, ruamel.yaml.RoundTripLoader, version=(1, 1))

            groups[worker] = cls._subparser_tree(variables,
                                                 schema["mapping"][_worker],
                                                 base_section=worker,
                                                 args=args,
                                                 parser=parser)
        # raise errors
        if errors:
            raise ConfigErrors(args.config, errors)

        # finally parse remaining args and update parameter tree with user-supplied commandline arguments

        args, remainder = parser.parse_known_args(args_bak)
        if len(remainder) > 0:
            raise RuntimeError(
                "The following arguments were not parsed: %s" ",".join(remainder))

        # store keywords as ordereddDict and namespace
        #cls.__store_args(args, groups)
        self.update_config(args, update_mode="defaults and args")

    @classmethod
    def _subparser_tree(cls,  # class for storage
                        cfgVars,  # config file variables
                        schema_section,  # section of the schema
                        base_section="",  # base of the tree-section of the schema
                        update_only=False,
                        args=None,  # base args
                        parser=None):  # parser
        '''
        This function recursively goes through the schema file, loaded as a nested orderedDict: subVars.
        If the variable of the schema is a map, the function goes to the inner nest of the dictionary.
        Finally, the default values to run the pipeline, stored in the schema as seq, bool, str/numbers,
        are loaded in groups (orderedDict) and in the parser (namespace).

        For each variable in the schema, the module checks if the same variable is present in the user cfg file: cfgVars
        If yes, groups[key] is overwritten.

        '''
        def _empty(alist):
            '''
            this recursive function checks if the elements in the array are empty (needed for the variables of the config file)
            '''
            try:
                for a in alist:
                    if not _empty(a):
                        return False
            except:
                # we will reach here if alist is not a iterator/list
                return False

            return True

        """ Recursively creates subparser tree for the config """
        def xformer(s): return s.replace('-', '_')
        groups = OrderedDict()
        # make schema section loopable
        sec_defaults = {xformer(k): v for k,
                        v in schema_section["mapping"].items()}

        # loop over each key of the variables in the schema
        # the key may contain a set of subkeys, being the schema a nested dictionary
        for key, subVars in sec_defaults.items():

            # store the total name of the key given the workerName(base_section) and key (which may be nested)
            option_name = base_section + "_" + key if base_section != "" else key

            def typecast(typ, val, string=False):
                if isinstance(val, list):
                    return val
                if typ.__name__ == "bool" and string:
                    return str(val).lower()
                elif typ.__name__ == "bool":
                    return val in "true yes 1".split()
                else:
                    return typ(val)

            # need to go in the nested variable
            if "mapping" in subVars:
                if key in list(cfgVars.keys()):  # check if enabled in config file
                    sub_vars = cfgVars[key]
                else:
                    sub_vars = dict.fromkeys(list(cfgVars.keys()), {})

                # recall the function with the set of variables of the nest
                groups[key] = cls._subparser_tree(sub_vars,
                                                  subVars,
                                                  base_section=option_name,
                                                  args=args,
                                                  update_only=update_only,
                                                  parser=parser)
                continue

            elif "seq" in subVars:
                # for lists
                dtype = __builtins__[subVars['seq'][0]['type']]
                subVars["example"] = str.split(
                    subVars['example'].replace(' ', ''), ',')
                if subVars['seq'][0]['type'] == 'bool':
                    default_value=[]
                    for i in range(0,len(subVars['example'])):
                        default_value.append(typecast(dtype, subVars['example'][i],string=False))
                else:
                    default_value = list(map(dtype, subVars["example"]))
            else:
                # for int, float, bool, str
                dtype = __builtins__[subVars['type']]
                default_value = subVars["example"]
                default_value = typecast(dtype, default_value, string=True)

            # update default if set in user config
            if key in list(cfgVars.keys()) and not _empty(list(cfgVars.values())):
                default_value = cfgVars[key]

            if update_only == "defaults and args":
                option_value = getattr(args, option_name, default_value)
                parser.set_defaults(**{option_name: option_value})
                groups[key] = typecast(dtype, option_value)

            elif update_only == "defaults":
                parser.set_defaults(**{option_name: default_value})
                groups[key] = typecast(dtype, default_value)

            else:
                default_value = typecast(dtype, default_value, string=True)
                if dtype.__name__ == "bool":
                    parser.add_argument("--" + option_name, help=argparse.SUPPRESS,
                                        choices="true yes 1 false no 0".split(), default=default_value)
                elif isinstance(default_value, (list, tuple)):
                    parser.add_argument("--" + option_name, help=argparse.SUPPRESS,
                                        type=dtype, nargs="+", default=default_value)
                else:
                    parser.add_argument("--" + option_name, help=argparse.SUPPRESS,
                                        type=dtype, default=default_value)

                groups[key] = default_value

        return groups

    @classmethod
    def update_config(cls, args=None, update_mode="defaults"):
        """ Updates argument parser with values from config file """
        args = cls.__ARGS if not args else args
        parser = cls._parser
        with open(args.config, 'r') as f:
            tmp = ruamel.yaml.load(
                f, ruamel.yaml.RoundTripLoader, version=(1, 1))
            schema_version = tmp["schema_version"]

        groups = OrderedDict()
        global_schema = OrderedDict()
        if not cls.__validated_schema:
            raise RuntimeError(
                "Must init singleton before running this method")

        for key, worker in tmp.items():
            if key == "schema_version":
                continue

            _key = key.split("__")[0]
            schema_fn = os.path.join(caracal.pckgdir,
                                     "schema", "{0:s}_schema.yml".format(_key))
            if update_mode == "defaults and args":  # new parset, re-validate
                source_data = {
                    _key: worker,
                    "schema_version": schema_version,
                }
                c = Core(source_data=source_data, schema_files=[schema_fn])
                cls.__validated_schema[key] = c.validate(
                    raise_exception=True)[_key]

            with open(schema_fn, 'r') as f:
                schema = ruamel.yaml.load(
                    f, ruamel.yaml.RoundTripLoader, version=(1, 1))
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
            raise RuntimeError(
                "Singleton must be initialized before this method is called")
        dictovals = copy.deepcopy(cls.__GROUPS)
        dictovals["schema_version"] = self.schema_version

        with open(filename, 'w') as f:
            f.write(yaml.dump(dictovals, Dumper=ruamel.yaml.RoundTripDumper))

    @classmethod
    def log_options(cls, config_file):
        """ Prints argument tree to the logger for prosterity to behold """

        caracal.log.info("Loaded pipeline configuration from {}".format(config_file), extra=dict(color="GREEN"))

        #caracal.log.info(
        #   "".join(["".ljust(25, "#"), " PIPELINE CONFIGURATION ", "".ljust(25, "#")]))
        indent0 = "  "

        def _tree_print(branch, indent=indent0):
            dicts = OrderedDict(
                [(k, v) for k, v in branch.items() if isinstance(v, dict)])
            other = OrderedDict(
                [(k, v) for k, v in branch.items() if not isinstance(v, dict)])

            def _printval(k, v):
                if isinstance(v, dict):
                    if not v.get("enable", True):
                        return
                    if indent == indent0:
                        caracal.log.info("")
                        extra = dict(color="GREEN")
                    else:
                        extra = {}
                    # (indent == "\t") and caracal.log.info(
                    #     indent.ljust(60, "#"))
                    caracal.log.info("{}{}:".format(indent, k), extra=extra)
                    # (indent == "\t") and caracal.log.info(
                    #     indent.ljust(60, "#"))
                    # (indent != "\t") and caracal.log.info(
                    #     indent.ljust(60, "-"))
                    _tree_print(v, indent=indent+indent0)
                else:
                    caracal.log.info("{}{:30}{}".format(indent, k+":", v))

            for k, v in other.items():
                _printval(k, v)
            for k, v in dicts.items():
                _printval(k, v)
        ordered_groups = OrderedDict(sorted(list(cls.__GROUPS.items()),
                                            key=lambda p: p[1].get("order", 0)))
        _tree_print(ordered_groups)
        # caracal.log.info(
        #     "".join(["".ljust(25, "#"), " END OF CONFIGURATION ", "".ljust(25, "#")]))

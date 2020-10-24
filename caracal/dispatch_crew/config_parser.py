# -*- coding: future_fstrings -*-

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



def basic_parser(add_help=True):
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

    add('-gdt', '--get-default-template',
            choices=caracal.SAMPLE_CONFIGS.keys(),
            default="minimal",
            help='init a configuration file from a default template')

    add('-gd', '--get-default', metavar="FILE",
        help='name of file where the template should be saved (use in conjunction with -gdt)')

    add('-sw', '--start-worker', metavar="WORKER",
        help='start pipeline at this worker')

    add('-ew', '--end-worker', metavar="WORKER",
        help='stop pipeline after this worker')

    add('-ct', '--container-tech', choices=["docker", "udocker", "singularity", "podman"],
        default="docker",
        help='Container technology to use')

    add('-wh', '--worker-help', metavar="WORKER",
        help='prints help for a particular worker, then exits')

    add('-pcs', '--print-calibrator-standard',
        help='prints list of auxiliary calibrator standards, then exits',
        action='store_true')

    add('-report',
        help='(re)generates a final HTML report, if configured, then exits',
        action='store_true')


    add('-debug',
        help='enable debugging mode',
        action='store_true')



    add('-nr','--no-reports',
        help='disable generation of HTML reports throughout the pipeline',
        action='store_true')

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


class config_parser(object):
    def __init__(self):
        """ Configuration parser. Sets up command line interface for CARACal
        """

        # =========================================================
        # Handle the configuration file argument first,
        # if one is supplied use that for defaulting arguments
        # created further down the line, otherwise use the
        # default configuration file
        # =========================================================
        # Create parser object
        self._parser = basic_parser()

        self._schemas = {}

    def validate_config(self, config_file):
        """Validates configuration file.
        Returns tuple of content, version, where content is validated config dict.
        Else raises ConfigErrors.
        """
        with open(config_file, 'r') as file:
            try:
                config_content = ruamel.yaml.load(file, ruamel.yaml.RoundTripLoader, version=(1, 1))
            except BaseException as exc:
                raise ConfigErrors(config_file, {'at top level': [str(exc)]})


        version = None
        # Validate each worker section against the schema and
        # parse schema to extract types and set up cmd argument parser

        #self._parser = parser = cls.__primary_parser(add_help=True)
        validated_content = OrderedDict()

        errors = OrderedDict()

        for worker, variables in config_content.items():
            # schema_version specifies config version
            if worker == "schema_version":
                version = variables
                continue
            _worker = worker.split("__")[0]

            if worker in self._schemas:
                schema_fn, _ = self._schemas[worker]
            elif _worker in self._schemas:
                schema_fn, _ = self._schemas[worker] = self._schemas[_worker]
            else:
                schema_fn = os.path.join(caracal.pckgdir,"schema", "{0:s}_schema.yml".format(_worker))

                if _worker == "worker" or not os.path.exists(schema_fn):
                    errors[worker] = ["this is not a recognized worker name, or its schema file is missing"]
                    continue

                with open(schema_fn, 'r') as file:
                    full_schema = ruamel.yaml.load(file, ruamel.yaml.RoundTripLoader, version=(1, 1))

                schema = full_schema["mapping"][_worker]
                self._schemas[worker] = self._schemas[_worker] = schema_fn, schema

            # validate worker config
            core = Core(source_data={_worker: variables}, schema_files=[schema_fn])

            validated_content[worker] = core.validate(raise_exception=False)[_worker]

            # check for errors
            if core.validation_errors:
                errs = errors[worker] = []
                for message in core.validation_errors:
                    # crude hack: we're already fooling the schema by using "flag" for the worker name
                    # when the name is e.g. "flag__2", so the message is misleading. Substitute the hack back.
                    message = message.replace("'/{}'".format(_worker), "'/{}'".format(worker))
                    errs.append(message)

        if errors:
            raise ConfigErrors(config_file, errors)

        return validated_content, version

    def populate_parser(self, config_content):
        """Takes config file content (as returned by validate_config), and
        populates the parser with corresponding options"""
        for worker, variables in config_content.items():
            self._process_subparser_tree(variables, self._schemas[worker][1], base_section=worker)


    def update_config_from_args(self, config_content, args):
        """ Updates argument parser with values from config file """
        options, remainder = self._parser.parse_known_args(args)
        if len(remainder) > 0:
            raise RuntimeError("The following arguments were not parsed: %s" ",".join(remainder))

        config = OrderedDict()

        for worker, variables in config_content.items():
            config[worker] = self._process_subparser_tree(variables, self._schemas[worker][1],
                                                          base_section=worker,
                                                          options=options)
        return options, config

    def _process_subparser_tree(self,  # class for storage
                                cfgVars,  # config file variables
                                schema_section,  # section of the schema
                                base_section="",  # base of the tree-section of the schema
                                options=None):     # if supplied, values of arguments will be propagated out into config
        '''
        This function recursively goes through the schema file, loaded as a nested orderedDict: subVars.
        If the variable of the schema is a map, the function goes to the inner nest of the dictionary.

        If options is None, the default values to run the pipeline, stored in the schema as seq, bool, str/numbers,
        are mapped to arguments in self._parser.

        If options is set, it must be a namespace returned by ArgumentParser.
        The content of the config is overwritten by the specified options.
        '''
        def _empty(alist):
            "recursive function checks if the elements in the array are empty (needed for the variables of the config file)"
            if type(alist) not in (list, tuple, dict):
                return False
            for a in alist:
                if not _empty(a):
                    return False
            return True

        groups = OrderedDict()
        # make schema section loopable
        sec_defaults = {k.replace('-', '_'): v for k, v in schema_section["mapping"].items()}

        # loop over each key of the variables in the schema
        # the key may contain a set of subkeys, being the schema a nested dictionary
        for key, subVars in sec_defaults.items():
            # store the total name of the key given the workerName(base_section) and key (which may be nested)
            # This has '-; for separators
            option_name = base_section + "-" + key if base_section != "" else key
            # corresponding attribute name
            attr_name = option_name.replace("-", "_")

            # For subsection, recurse into the nested variable
            if "mapping" in subVars:
                if key in cfgVars:  # check if enabled in config file
                    sub_vars = cfgVars[key]
                else:
                    sub_vars = {key: {} for key in cfgVars.keys()}
                # recurse with the set of variables of the nest
                groups[key] = self._process_subparser_tree(sub_vars, subVars, base_section=option_name, options=options)
                continue

            # True if variable is a list
            is_list = "seq" in subVars
            # type of variable (of list element, if dealing with lists)
            dtype = None

            def typecast(val):
                """Helper function to cast value to expected type.
                If string=True, bools are cast to string bools, so value is suitable for command-line parsing"""
                if is_list and isinstance(val, list):
                    return [typecast(x) for x in val]
                if dtype is bool and type(val) is str:
                    return val.lower() in {"true", "yes", "1"}
                return dtype(val)

            def value2str(val):
                """Converts value to string representation. Bools get special lowercase treatment."""
                return str(bool(val)).lower() if dtype is bool else str(val)

            def str2list(val):
                """Converts lists in string representation, e.g. "[a, b]" and "a,b", to lists of strings"""
                return list(val.lstrip("[").rstrip("]").replace(", ",",").split(","))

            # update default if set in user config
            default_value = None
            # NB: not sure why the check for _empty() is needed, some archaic carryover
            if not _empty(list(cfgVars.values())):
                default_value = cfgVars.get(key)

            # for sequences, do some type fiddling
            if is_list:
                dtype = __builtins__[subVars['seq'][0]['type']]
                if dtype is map:
                    dtype = dict
                if default_value is None:
                    if dtype is dict:
                        default_value = []
                    else:
                        default_value = subVars["example"]
                        if type(default_value) is str:
                            default_value = str2list(default_value)
                        if type(default_value) is not list:
                            raise TypeError(f"{option_name} default value is not configured correctly. This is a bug, please report!")
            else:
                # for int, float, bool, str
                dtype = __builtins__[subVars['type']]
                if default_value is None:
                    default_value = subVars["example"]

            # convert default value to expected type
            groups[key] = default_value = typecast(default_value)

            # if an options object is passed in, look if its value overrides our setting
            if options is not None:
                if hasattr(options, attr_name):
                    optval = getattr(options, attr_name)
                    # optval is always a string, so...
                    # ...parse lists or dicts as yaml objects
                    if is_list or dtype is dict:
                        optval = yaml.safe_load(optval)
                    # ...and typecast to expected type
                    option_value = typecast(optval)
                    if option_value != default_value:
                        caracal.log.info("  command line sets --{} = {}".format(option_name, option_value))
                    groups[key] = option_value
            # else populate parser with default value
            else:
                # lists and dicts expressed via yaml
                if is_list or dtype is dict:
                    self._parser.add_argument("--" + option_name, help=argparse.SUPPRESS, type=str,
                                                default=yaml.safe_dump(default_value))
                # booleans have a choice
                elif dtype is bool:
                    self._parser.add_argument("--" + option_name, help=argparse.SUPPRESS,
                                              choices="true yes 1 false no 0".split(),
                                              default=value2str(bool(default_value)))
                # all others passed with native dtype
                else:
                    self._parser.add_argument("--" + option_name, help=argparse.SUPPRESS,
                                              type=dtype, default=default_value)

        return groups

    def save_options(self, config, filename):
        """ Save configuration options to yaml """
        dictovals = copy.deepcopy(config)

        with open(filename, 'w') as f:
            f.write(yaml.dump(dictovals, Dumper=ruamel.yaml.RoundTripDumper))

    def log_options(self, config):
        """ Prints argument tree to the logger for posterity to behold """

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
                    caracal.log.info(f"{indent}{k}:", extra=extra)
                    # (indent == "\t") and caracal.log.info(
                    #     indent.ljust(60, "#"))
                    # (indent != "\t") and caracal.log.info(
                    #     indent.ljust(60, "-"))
                    _tree_print(v, indent=indent+indent0)
                else:
                    # totally ugly -- I promise I'll fix it when we have a better qualifier
                    if k == "cabs" and not v:
                        return
                    if type(v) in (list, tuple):
                        vstr = ', '.join([str(x) or '""' for x in v])
                        if len(v) < 2:
                            vstr = f"[{vstr}]"
                    else:
                        vstr = str(v) or '""'
                    k += ":"
                    caracal.log.info(f"{indent}{k:30}{vstr}")

            for k, v in other.items():
                _printval(k, v)
            for k, v in dicts.items():
                _printval(k, v)
        ordered_groups = OrderedDict(sorted(list(config.items()),
                                            key=lambda p: p[1].get("order", 0)))
        _tree_print(ordered_groups)
        # caracal.log.info(
        #     "".join(["".ljust(25, "#"), " END OF CONFIGURATION ", "".ljust(25, "#")]))

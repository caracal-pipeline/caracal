import os
import sys
from collections.abc import Mapping, Sequence
from collections import OrderedDict, namedtuple
import itertools
import yaml
from stimela.dismissable import dismissable as sdm
from caracal import log, ConfigurationError
import caracal.dispatch_crew.utils as utils
import numpy as np
import json


def check_config(config, name):
    shadems_cfg = config["shadems"]
    # dummy-process each plots sequence to catch any config errors
    basesubst = dict(msbase="", all_fields="", all_corrs="", bpcal="", gcal="", fcal="", xcal="")
    for plot_cat in "plots", "plot_by_field", "plots_by_corr":
        _process_shadems_plot_list([], basesubst, shadems_cfg.get(plot_cat, []), {}, plot_cat)


# Miscellaneous functions
def l2d(ins):
    """
    Convert some list of command line arguments and values to a dictionary
    from a list
    Parameters
    ----------
    ins: :obj:`list`
        List containing the command line arguemnts
    """
    if not isinstance(ins, list):
        ins = ins.split()

    keys = [(i, _) for i, _ in enumerate(ins)
            if _.startswith("-") and not _.lstrip("-").isdigit()]
    test = {}

    for i, (kidx, key) in enumerate(keys, start=1):
        if i < len(keys):
            test[key] = " ".join(ins[slice(kidx + 1, keys[i][0])])
        else:
            test[key] = " ".join(ins[slice(kidx + 1, None)])
    return test


def ms_exists(msdir, ms):
    return os.path.exists(os.path.join(msdir, ms))


# Input parameter related functions
def check_params(params):
    """
    Remove items from params dictionary containing value None, "", " " and
    convert python lists to comma separated string lists

    Parameters
    ----------
    params: :obj:`dict`
        Dictionary from which to remove params with value as None

    Returns
    params: :obj:`dict`
        'Cleaned' dictionary

    """
    params = {k: v for k, v in params.items() if v not in (None, "", " ")}

    # if any values are python lists, convert them to comma separated list
    params = {k: (",".join(v) if isinstance(v, list) else v)
              for k, v in params.items()}

    return params


def create_param_group(subgrp_name, subgrp_items, large_grp):
    """
    Divide a large group of parameters into smaller subgroups

    Parameters
    ----------
    subgrp_name: :obj:`str`
        Name of a subgroup
    subgrp_items: :obj:`list`
        List containing names of items to be placed in the subgroup. These
        items should be keys in the large_grp dictionary
    large_grp: :obj:`dict`
        Dictionary containing the larger group to be subdivided.
    """

    # get data from larger group
    group = {}
    for _ in subgrp_items:
        group[_] = large_grp[_]

    group = make_namespace(subgrp_name, group)
    return group


def group_configs(configs):
    """
    Group inputs from worker's configuration file for easier access

    Parameters
    ----------
    configs: :obj:`OrderedDict`
        Dictionary containing inputs from configuration file

    Returns
    -------
    Tuple containing named tuples containing grouped data:
        general: contains the general pipeline configuration settings
        plot_type: contains the types of plots available and their settings
        plot_params: contains the plotting tools' parameters
    """
    general = ["enable", "label_in", "label_plot", "dirname",
               "standard_plotter"]
    general = create_param_group("general", general, configs)

    plot_type = ["amp_ant", "amp_chan", "amp_phase", "amp_scan", "amp_uvwave",
                 "phase_chan", "phase_uvwave", "real_imag"]
    plot_type = create_param_group("plot_type", plot_type, configs)

    plot_params = ["field", "correlation", "mem_limit", "num_cores",
                   "uvrange"]
    plot_params = create_param_group("plot_params", plot_params, configs)

    return (general, plot_type, plot_params)


def make_namespace(name, items):
    """
    Create a named tuples

    Parameters
    ----------
    name: :obj:`str`
        Name of the namespace object
    items: :obj:`dict`
        Dictionary containing desired variable name as key and its
        corresponding value

    Returns
    -------
    tuple with the namespace provided
    """
    Name = namedtuple(name, " ".join(list(items.keys())))

    out = Name(**items)

    return out


# Axes, fields and correlation matching functions
def check_data(col):
    """Change pseudo data column name to actual column name if necessary"""
    cols = {
        "corrected": "CORRECTED_DATA",
        "data": "DATA",
        "model": "MODEL_DATA",
        "scan": "SCAN_NUMBER",
        "antenna1": "ANTENNA1"
    }
    return cols.get(col, col)


def get_xy(plot_name):
    """ Return x and y axis names given a plot name e.g amp_scan"""

    basic = {
        "amp_ant": {
            "xaxis": "antenna1",
            "yaxis": "amp"
        },
        "amp_chan": {
            "xaxis": "chan",
            "yaxis": "amp"
        },
        "amp_phase": {
            "xaxis": "phase",
            "yaxis": "amp"
        },
        "amp_scan": {
            "xaxis": "scan",
            "yaxis": "amp"
        },
        "amp_uvwave": {
            "xaxis": "uvwave",
            "yaxis": "amp",
            "colour": "scan"
        },
        "phase_chan": {
            "xaxis": "chan",
            "yaxis": "phase"
        },
        "phase_uvwave": {
            "xaxis": "uvwave",
            "yaxis": "phase",
            "colour": "scan"
        },
        "real_imag": {
            "xaxis": "imag",
            "yaxis": "real",
            "colour": "scan"
        }
    }

    return basic[plot_name]


def get_cfg_fields(pipeline, iobs, cfg_field, label_in):
    """
    Convert field representative names (e.g bpcal etc) to actual field names
    and ids

    Parameters
    ----------
    pipeline:
        caracal pipeline object
    iobs: :obj:`int`
        Item number in observation list
    cfg_field: :obj:`str`
        A string from the field section configuration file containing the
        representative field names
    label_in: str
        Label associated with input MS

    Returns
    -------
    fields: obj:`dict`
        A dictionary of form field_name: (repr_name, field_id) or None if the
        selected fields were invalid/not available
    """
    cases = {
        "calibrators": ['bpcal', 'gcal', 'fcal', 'xcal'],
        "target": ["target"]
    }

    f_types = cases.get(cfg_field, None)

    if f_types is None:
        # meaning the field specified were comma separated
        cfg_field = set(cfg_field.split())
        f_types = (cfg_field if cfg_field.issubset(cases["calibrators"])
                   else [])
    # convert field types to field names and field IDs
    fields = {}
    for f_type in f_types:
        fnames = getattr(pipeline, f_type)[iobs]
        for _fname in fnames:
            fields.setdefault(_fname, []).append(f_type)

    # return none if no items in field dict
    return fields or None


def get_cfg_corrs(cfg_corr, ms_corrs):
    """ Convert correlations specified to actual corr labels"""
    if cfg_corr in ['auto', 'all']:
        cfg_corr = ','.join(ms_corrs)
    elif cfg_corr in ['diag', 'parallel']:
        # the corr list has the order XX,XY,YX,YY or RR,RL,LR,LL
        # collect first and last regardless if list is size 2 or 4
        cfg_corr = ",".join([ms_corrs[0], ms_corrs[-1]])
    return cfg_corr


# Recipe functions
def plotms(pipeline, recipe, basic, extras=None):
    """
    Add the plotms recipe to stimela

    Parameters
    ----------
    pipeline:
        A caracal pipeline object containing the general pipeline details
    recipe:
        Stimela recipe object onto which we add recipes
    basic: :obj:`dict`
        Dictionary containing all the basic parameters to be passed on to the
        plotter a.k.a stimela cab. It also contains some information to be
        removed and passed to the pipeline.
    extras: :obj`dict` (optional)
        Can contain additional keyword arguments to be passed directly on to
        the plotter i.e. all the other arguments that are not available by
        default on the config file. These arguments will be passed as they
        are and thus attention must be paid.
    """
    step = basic.pop("step")
    label = basic.pop("label")
    output_dir = basic.pop("output_dir")

    basic["data"] = basic["data"].split("_")[0].lower()

    plotms_keys = {
        "xaxis": basic["xaxis"],
        "yaxis": basic["yaxis"],
        "vis": basic["ms"],
        "xdatacolumn": basic["data"],
        "ydatacolumn": basic["data"],
        "correlation": basic["corr"],
        "field": basic["field"],
        "iteraxis": basic["iterate"],
        "coloraxis": basic.get("colour", None),
        "plotfile": basic["output"],
        "expformat": 'png',
        "exprange": 'all',
        "overwrite": True,
        "showgui": False
    }

    if extras:
        plotms_keys.update(extras)

    # remove any empties or none
    plotms_keys = check_params(plotms_keys)

    recipe.add("cab/casa_plotms", step, plotms_keys,
               input=pipeline.input, output=output_dir,
               label=label, memory_limit=None, cpus=None)


def _process_shadems_plot_list(plot_args, basesubst, plotlist, defaults, description, extras=None):
    """Processes a list of plots, recusing into dicts"""
    for entry in plotlist:
        if not entry:
            continue
        # if plot is specified as a dict, its keys will override category defaults
        if isinstance(entry, Mapping):
            entry = entry.copy()
            desc = entry.pop("desc", "")
            comment = entry.pop("comment", "")
            enable = entry.pop("enable", True)
            plots = entry.pop("plots", None)
            if not isinstance(plots, Sequence):
                raise ConfigurationError(f"{description}: expecting a 'plots' sequence")
            # skip enable=False entries
            if not enable:
                log.info(f"shadems plot section '{desc}' is explicitly disabled in the config file")
                continue
            # all other keys go into new defaults (with substitutions done)
            new_defaults = defaults.copy()
            new_defaults.update(**{"--" + key.replace("_", "-"): val.format(**basesubst) if isinstance(val, str) else val
                                   for key, val in entry.items()})
            # and ecurse into new plot list
            _process_shadems_plot_list(plot_args, basesubst, plots, new_defaults, f"{description}: {desc or comment}", extras=extras)
        elif isinstance(entry, str):
            # add user-defined substitutions
            plot = entry.format(**basesubst)
            # convert argument list to dictionary for easy update
            args = l2d(plot)
            # add in defaults
            for key, value in defaults.items():
                args.setdefault(key, value)
            # add in extras, if any
            if extras:
                args.update(extras)
            # convert to list of arguments and add to plotlist
            # arg values of None and True and "" represent command-line arguments without a value
            cmdline_args = []
            for option, value in args.items():
                cmdline_args.append(option)
                if value not in (None, True, ""):
                    cmdline_args.append(str(value))
            plot_args.append(" ".join(cmdline_args))
        else:
            raise ConfigurationError(f"{description}: unexpected 'plots' entry of type {type(entry)}")


def direct_shadems(pipeline, recipe, shade_cfg, extras=None):
    """
    Create recipes for the new shade-ms plots
    """
    iobs = shade_cfg.pop("iobs")
    step = f"plot-shadems-ms{iobs}"
    msbase = shade_cfg.pop("ms_base")
    label = shade_cfg.pop("label")

    fields = shade_cfg["fields"]

    # some user facing substitutions for fields, corrs, and base MS name
    basesubst = dict(
        msbase=msbase,
        all_fields=",".join(fields.keys()),
        all_corrs=shade_cfg["corrs"]
    )
    for _f in fields.keys():
        for _ft in fields[_f]:
            basesubst[_ft] = _f

    # groups of plots available
    plot_cats = {
        "plots_by_field": {"--iter-field": "",
                           "--field": basesubst["all_fields"]},
        "plots_by_corr": {"--iter-corr": ""},
        "plots": {}
    }

    # remove the keys enable and ignore_errors
    bares = {k: v for k, v in shade_cfg.items()
             if k in ("plots_by_field", "plots_by_corr", "plots")}

    # # remove plot categories that have not been specified
    # bares = {k: v for k, v in bares.items() if len(v) > 1 or (v and v[0])}
    # I just skip them below as that's easier with the new logic

    plot_args = []

    # for each plot category i.e. plots, plot-by-field, plots-by-corr
    for plot_cat, plotlist in bares.items():
        # make dict of default arguments for this plot type
        category_defaults = {
            "--title": "'{ms} {_field}{_Spw}{_Scan}{_Ant}{_title}{_Alphatitle}{_Colortitle}'",
            "--col": shade_cfg["default_column"],
            "--png": f"{label}-{msbase}-{{field}}{{_Spw}}{{_Scan}}{{_Ant}}-{{label}}{{_alphalabel}}{{_colorlabel}}{{_suffix}}.png",
            "--corr": shade_cfg["corrs"],
            ** plot_cats[plot_cat]
        }
        _process_shadems_plot_list(plot_args, basesubst, plotlist, category_defaults, plot_cat)

    if len(plot_args) == 0:
        log.warning(
            "The shadems section doesn't contain any enabled 'plot_by_field' or 'plot_by_corr' or 'plots' entries.")
    else:
        recipe.add("cab/shadems_direct", step,
                   dict(ms=shade_cfg["ms"],
                        args=plot_args,
                        ignore_errors=shade_cfg["ignore_errors"]),
                   input=pipeline.input, output=shade_cfg["output_dir"],
                   label=f"{step}:: Plotting", memory_limit=None, cpus=None)


def shadems(pipeline, recipe, basic, extras=None):
    """
    Add the shadems recipe to stimela
    See docstring of :func:`plotms` for parameter descriptions
    """
    step = basic.pop("step")
    label = basic.pop("label")
    output_dir = basic.pop("output_dir")

    # contains the var names to be used as the suffix in case of iteration
    iter_axes = {"field": "{_field}",
                 "spw": "{_Spw}",
                 "scan": "{_Scan}",
                 "baseline": "{_Baseline}",
                 "ant": "{_Ant}"}

    col_names = {
        "antenna1": "ANTENNA1", "scan": "SCAN_NUMBER",
        "chan": "CHAN", "freq": "FREQ",
        "amp": "amp", "phase": "phase",
        "real": "real", "imag": " imag",
        "uvwave": "UV", "baseline": "UV"
    }

    # get a name conforming to those allowed in shadems
    def shade_cols(_c, names): return names.get(_c, _c.upper())

    # get the correlation names for the args
    corrs = basic["corr"].split(",")

    # iterate over correlation because of shadems naming issues
    for _corr in corrs:
        shadems_keys = {}
        shadems_keys = {
            "col": basic["data"],
            "xaxis": shade_cols(basic["xaxis"], col_names),
            "yaxis": shade_cols(basic["yaxis"], col_names),
            "ms": basic["ms"],
            "corr": _corr,
            "field": basic["field"],
            "colour-by": shade_cols(basic.get("colour", None), col_names),
            "png": f"{basic['output']}-corr-{_corr}.png",
            # "mem_limit": basic["mem_limit"],
            "num-parallel": basic["num_cores"]
        }

        iterate = basic["iterate"]

        if iterate and (iterate in iter_axes):
            shadems_keys.update(
                {f"iter-{iterate}": True,
                 "png": f"{basic['output']}-corr-{_corr}{iter_axes[iterate]}.png"})

        if shadems_keys["colour-by"] == "baseline":
            shadems_keys["colour-by"] = "UV"

        if extras:
            shadems_keys.update(extras)

        # remove any empties or none
        shadems_keys = check_params(shadems_keys)

        recipe.add("cab/shadems", step, shadems_keys,
                   input=pipeline.input, output=output_dir,
                   label=label, memory_limit=None, cpus=None)


def ragavi_vis(pipeline, recipe, basic, extras=None):
    """
    Add the ragavi_vis recipe to stimela
    See docstring of :func:`plotms` for parameter descriptions
    """
    step = basic.pop("step")
    label = basic.pop("label")
    output_dir = basic.pop("output_dir")

    ragavi_keys = {
        "data-column": basic["data"],
        "xaxis": basic["xaxis"],
        "yaxis": basic["yaxis"],
        "ms": basic["ms"],
        "corr": basic["corr"],
        "field": basic["field"],
        "iter-axis": basic["iterate"],
        "colour-axis": basic.get("colour", None),
        "htmlname": f"{basic['output']}.html",
        # "cbin": basic["avgchan"],
        # "tbin": basic["avgtime"],
        "canvas-width": 1080,
        "canvas-height": 720,
        "mem-limit": basic["mem_limit"],
        "num-cores": basic["num_cores"]
    }

    if extras:
        ragavi_keys.update(extras)

    # remove any empties or none
    ragavi_keys = check_params(ragavi_keys)

    recipe.add("cab/ragavi_vis", step, ragavi_keys,
               input=pipeline.input, output=output_dir,
               label=label, memory_limit=None, cpus=None)


# main function
def worker(pipeline, recipe, config):
    """
    Inspect worker driver function

    1. Parses inputs from the worker's configuration file
    2. Iterate over observations
        - Iterate over mss for this observation
            - Iterate over the plots available
                - Iterate over the required fields
                    - Form the plotter's arguments
                    - Call plotter's function to add to stimela recipe
    """

    gen_params, plot_axes, plotter_params = group_configs(config)
    plot_axes = plot_axes._asdict()

    # general pipeline setup
    nobs = pipeline.nobs

    subdir = gen_params.dirname
    label_in = gen_params.label_in
    label = gen_params.label_plot
    plotter = gen_params.standard_plotter

    # use default output dir if no explict output dir was specified
    if subdir:
        output_dir = os.path.join(pipeline.diagnostic_plots, subdir)
    else:
        output_dir = pipeline.diagnostic_plots

    for iobs in range(nobs):
        mslist = pipeline.get_mslist(iobs, label_in,
                                     target=(config['field'] == 'target'))

        for ms in mslist:
            if not ms_exists(pipeline.msdir, ms):
                raise IOError(f"MS {ms} does not exist. Please check that is where it should be.")

            log.info(f"Plotting MS: {ms}")

            ms_base = os.path.splitext(ms)[0]

            ms_info_dict = pipeline.get_msinfo(ms)
            # get corr types for MS
            ms_corrs = ms_info_dict["CORR"]["CORR_TYPE"]

            corrs = get_cfg_corrs(plotter_params.correlation, ms_corrs)

            fields = get_cfg_fields(pipeline, iobs, plotter_params.field,
                                    label_in)

            if fields is None:
                raise ValueError(f"""
                    Eligible values for 'field': 'target', \
                    'calibrators', 'fcal', 'bpcal', 'xcal' or 'gcal'. \
                    User selected {",".join(fields)}""")

            # for the newer plots to shadems
            if pipeline.enable_task(config, "shadems"):
                shade_cfg = config["shadems"]
                shade_cfg.update({
                    "ms": ms,
                    "iobs": iobs,
                    "label": label,
                    "corrs": corrs,
                    "fields": fields,
                    "ms_base": ms_base,
                    "output_dir": output_dir})
                direct_shadems(pipeline, recipe, shade_cfg)

            # the older plots
            if plotter and plotter != "none":
                for axes in plot_axes:
                    if pipeline.enable_task(config, axes):
                        del plot_axes[axes]["enable"]
                    else:
                        continue

                    plot_args = get_xy(axes)

                    for fname, ftype in fields.items():
                        plot_args.update({
                            "ms": ms,
                            "data": check_data(plot_axes[axes].get("col")),
                            "corr": corrs,
                            "iterate": "corr",
                            # "colour": "scan",
                            "num_cores": plotter_params.num_cores,
                            "mem_limit": plotter_params.mem_limit,
                            "uvrange": plotter_params.uvrange,
                            "field": fname,
                            "output": f"{label}-{ms_base}-{ftype[0]}-{fname}-{axes}",
                            "output_dir": output_dir,
                            "step": f"plot-{axes}-{iobs}-{ftype[0]}",
                            "label": label,
                            **plot_axes[axes]})

                        globals()[plotter](pipeline, recipe, plot_args, extras=None)

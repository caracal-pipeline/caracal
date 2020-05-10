# -*- coding: future_fstrings -*-
import os
import sys
from collections import OrderedDict
import itertools
import yaml
from stimela.dismissable import dismissable as sdm
from caracal import log
import caracal.dispatch_crew.utils as utils

NAME = 'Inspect Data'
LABEL = "inspect"

# E.g. to split out continuum/<dir> from output/continuum/dir


def get_dir_path(string, pipeline):
    return string.split(pipeline.output)[1][1:]


def plotms(pipeline, recipe, config, plotname, msname, field, iobs, label, prefix, opts, ftype, fid, output_dir, corr_label=None):
    step = 'plot-{0:s}-{1:d}-{2:d}'.format(plotname, iobs, fid)
    colouraxis = opts.get("colouraxis", None)
    recipe.add("cab/casa_plotms", step, {
        "vis": msname,
        "field": field,
        "correlation": opts['corr'],
        "timerange": '',
        "antenna": '',
        "xaxis": opts['xaxis'],
        "xdatacolumn": config[plotname]['col'],
        "yaxis": opts['yaxis'],
        "ydatacolumn": config[plotname]['col'],
        "avgtime": config[plotname]['avgtime'],
        "avgchannel": config[plotname]['avgchan'],
        "coloraxis": sdm(colouraxis),
        "iteraxis": sdm(opts.get('iteraxis', None)),
        "plotfile": '{0:s}-{1:s}-{2:s}-{3:s}-{4:s}.png'.format(prefix, label, field, plotname, ftype),
        "expformat": 'png',
        "exprange": 'all',
        "overwrite": True,
        "showgui": False,
        "uvrange": config["uvrange"],
    },
        input=pipeline.input,
        output=output_dir,
        label="{0:s}:: Plotting corrected {1:s}".format(step, plotname))


def shadems(pipeline, recipe, config, plotname, msname, field, iobs, label, prefix, opts, ftype, fid, output_dir, corr_label=None):
    step = 'plot-{0:s}-{1:d}-{2:d}'.format(plotname, iobs, fid)
    col = config[plotname]['col']
    if col == "corrected":
        col = "CORRECTED_DATA"
    elif col == "data":
        col = "DATA"
    if corr_label:
        corr_label = "_Corr_" + corr_label
    else:
        corr_label = ""

    recipe.add("cab/shadems", step, {
        "ms": msname,
        "field": str(fid),
        "corr": opts['corr'],
        "xaxis": opts['xaxis'],
        "yaxis": opts['yaxis'],
        "col": col,
        "png": '{0:s}-{1:s}-{2:s}-{3:s}-{4:s}-{5:s}.png'.format(prefix, label, field, plotname, ftype, corr_label),
    },
        input=pipeline.input,
        output=output_dir,
        label="{0:s}:: Plotting corrected ".format(step, plotname))


def ragavi_vis(pipeline, recipe, config, plotname, msname, field, iobs, label, prefix, opts, ftype, fid, output_dir, corr_label=None):
    step = 'plot-{0:s}-{1:d}-{2:d}'.format(plotname, iobs, fid)
    col = config[plotname]['col']
    if col == "corrected":
        col = "CORRECTED_DATA"
    elif col == "data":
        col = "DATA"
    if corr_label:
        corr_label = "_Corr_" + corr_label
    else:
        opts['corr'] = "0:"
        corr_label = ""
    recipe.add("cab/ragavi_vis", step, {
        "ms": msname,
        "xaxis": opts['xaxis'],
        "yaxis": opts['yaxis'],
        "canvas-height": opts['canvas-height'],
        "canvas-width": opts['canvas-width'],
        "corr": opts["corr"],
        "mem-limit" : opts['mem-limit'],
        "num-cores" : opts['num-cores'],
        # "cbin": int(config[plotname]['avgchan']),
        # "colour-axis": opts.get("colour-axis", None),
        "data-column": col,
        "field": str(fid),
        "htmlname": "{0:s}-{1:s}-{2:s}-{3:s}-{4:s}-{5:s}".format(prefix, label, field, plotname, ftype, corr_label),
        "iter-axis": sdm(opts.get('iter-axis', None)),
        # "tbin": float(config[plotname]['avgtime']),

    },
        input=pipeline.input,
        output=output_dir,
        label="{0:s}:: Plotting corrected {1:s}".format(step, plotname))


def worker(pipeline, recipe, config):

    def specific_fields(sec):
        if config[sec]['fields'] in ["", [""], None, "null"]:
            return False
        else:
            return config[sec]['fields']

    uvrange = config['uvrange']
    fields = config['fields']
    plotter = config["standard_plotter"]
    if pipeline.virtconcat:
        msnames = [pipeline.vmsname]
        prefixes = [pipeline.prefix]
        nobs = 1
    else:
        msnames = pipeline.msnames
        prefixes = pipeline.prefixes
        nobs = pipeline.nobs

    subdir = config['dirname']
    output_dir = os.path.join(pipeline.diagnostic_plots, subdir) if subdir else pipeline.diagnostic_plots

    for iobs in range(nobs):
        msname = msnames[iobs] if not config['label_in'] else '{0:s}_{1:s}.ms'.format(
            msnames[iobs][:-3], config['label_in'])
        prefix = prefixes[iobs]
        label = config['label_cal']

        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.obsinfo, msname[:-3])

        corr = config['correlation']
        if corr == 'auto':
            with open(msinfo, 'r') as stdr:
                corrs = yaml.load(stdr)['CORR']['CORR_TYPE']
            corrs = ','.join(corrs)
            corr = corrs

        # new-school plots
        if config['shadems']['enable']:
            plot_args = []
            # get field names
            for field_type in fields:
                field_names = getattr(pipeline, field_type)[iobs]
                args = OrderedDict(
                            # shadems uses its own "{}" codes in output name, so put it together like this
                            png="{}-{}-{}-{}".format(prefix, label, field_type,
                                "{field}{_Spw}{_Scan}{_Ant}-{label}{_alphalabel}{_colorlabel}{_suffix}.png"),
                            title="'{ms} "+field_type + "{_field}{_Spw}{_Scan}{_Ant}{_title}{_Alphatitle}{_Colortitle}'",
                            col=config['shadems']['default_column'],
                            corr=corr.replace(' ',''),
                            field=",".join(field_names))

                for iplot, plotspec in enumerate(config['shadems']['plots_by_field']):
                    if plotspec:
                        plotspec = plotspec.split()
                        for arg, value in args.items():
                            arg = "--" + arg
                            if arg not in plotspec:
                                plotspec += [arg, value]
                        plotspec.append("--iter-field")
                        plot_args.append(" ".join(plotspec))

            args = OrderedDict(
                # shadems uses its own "{}" codes in output name, so put it together like this
                png="{}-{}-{}".format(prefix, label,
                                      "{field}{_Spw}{_Scan}{_Ant}-{label}{_alphalabel}{_colorlabel}{_suffix}.png"),
                title="'{ms} {_field}{_Spw}{_Scan}{_Ant}{_title}{_Alphatitle}{_Colortitle}'",
                col=config['shadems']['default_column'],
                corr=corr.replace(' ', ''))
            for iplot, plotspec in enumerate(config['shadems']['plots_by_corr']):
                if plotspec:
                    plotspec = plotspec.split()
                    for arg, value in args.items():
                        arg = "--" + arg
                        if arg not in plotspec:
                            plotspec += [arg, value]
                    plotspec.append("--iter-corr")
                    plot_args.append(" ".join(plotspec))


            if plot_args:
                step = 'plot-shadems-ms{2:d}'.format(iplot, field_type, iobs)
                recipe.add("cab/shadems_direct", step,
                           dict(ms=msname, args=plot_args,
                                ignore_errors=config["shadems"]["ignore_errors"]),
                           input=pipeline.input, output=output_dir,
                           label="{0:s}:: Plotting".format(step))
            else:
                log.warning("The shadems section is enabled, but doesn't specify any plot_by_field or plot_by_corr")

        # old-school plots

        # define plot attributes
        diagnostic_plots = {}
        diagnostic_plots["real_imag"] = dict(
            plotms={"xaxis": "imag", "yaxis": "real",
                    "colouraxis": "baseline", "iteraxis": "corr"},
            shadems={"xaxis": "real", "yaxis": "imag"},
            ragavi_vis={"xaxis": "real", "yaxis": "imaginary",
                        "iter-axis": "scan", "canvas-width": 300,
                        "canvas-height": 300})

        diagnostic_plots["amp_phase"] = dict(
            plotms={"xaxis": "amp", "yaxis": "phase",
                    "colouraxis": "baseline", "iteraxis": "corr"},
            shadems={"xaxis": "amp", "yaxis": "phase"},
            ragavi_vis={"xaxis": "phase", "yaxis": "amplitude",
                        "iter-axis": "corr", "canvas-width": 1080,
                        "canvas-height": 720})

        diagnostic_plots["amp_ant"] = dict(
            plotms={"xaxis": "antenna", "yaxis": "amp",
                    "colouraxis": "baseline", "iteraxis": "corr"},
            shadems={"xaxis": "ANTENNA1", "yaxis": "amp"},
            ragavi_vis=None)

        diagnostic_plots["amp_uvwave"] = dict(
            plotms={"xaxis": "uvwave", "yaxis": "amp",
                    "colouraxis": "baseline", "iteraxis": "corr"},
            shadems={"xaxis": "UV", "yaxis": "amp"},
            ragavi_vis={"xaxis": "uvwave", "yaxis": "amplitude",
                        "iter-axis": "scan", "canvas-width": 300,
                        "canvas-height": 300})

        diagnostic_plots["phase_uvwave"] = dict(
            plotms={"xaxis": "uvwave", "yaxis": "phase",
                    "colouraxis": "baseline", "iteraxis": "corr"},
            shadems={"xaxis": "UV", "yaxis": "phase"},
            ragavi_vis={"xaxis": "uvwave", "yaxis": "phase",
                        "iter-axis": "scan", "canvas-width": 300,
                        "canvas-height": 300})

        diagnostic_plots["amp_scan"] = dict(
            plotms={"xaxis": "scan", "yaxis": "amp"},
            shadems={"xaxis": "SCAN_NUMBER", "yaxis": "amp"},
            ragavi_vis={"xaxis": "scan", "yaxis": "amplitude",
                        "iter-axis": None,
                        "canvas-width": 1080, "canvas-height": 720})

        diagnostic_plots["amp_chan"] = dict(
            plotms={"xaxis": "chan", "yaxis": "amp"},
            shadems={"xaxis": "CHAN", "yaxis": "amp"},
            ragavi_vis={"xaxis": "channel", "yaxis": "amplitude",
                        "iter-axis": "scan", "canvas-width": 300,
                        "canvas-height": 300})

        diagnostic_plots["phase_chan"] = dict(
            plotms={"xaxis": "chan", "yaxis": "phase"},
            shadems={"xaxis": "CHAN", "yaxis": "phase"},
            ragavi_vis={"xaxis": "channel", "yaxis": "phase",
                        "iter-axis": "scan", "canvas-width": 300,
                        "canvas-height": 300})

        if plotter.lower() != "none":
            for plotname in diagnostic_plots:
                if not pipeline.enable_task(config, plotname):
                    continue
                opts = diagnostic_plots[plotname][plotter]
                if opts is None:
                    log.warn("The plotter '{0:s}' cannot make the plot '{1:s}'".format(
                        plotter, plotname))
                    continue
                elif plotter == "ragavi_vis":
                        opts["num-cores"] = config["num_cores"]
                        opts["mem-limit"] = config["mem_limit"]

                if plotter == "shadems":
                    # change the labels to indices
                    with open(msinfo, 'r') as stdr:
                        corrs = yaml.load(stdr)['CORR']['CORR_TYPE']

                    corr = corr.replace(" ", "").split(",")
                    for it, co in enumerate(corr):
                        if co in corrs:
                            corr[it] = str(corrs.index(co))
                    corr = ",".join(corr)
                    # for each corr
                    for co in corr.split(","):
                        opts["corr"] = co
                        for fields_ in specific_fields(plotname) or fields:
                            for field in getattr(pipeline, fields_)[iobs]:
                                fid = utils.get_field_id(msinfo, field)[0]
                                globals()[plotter](pipeline, recipe, config,
                                                   plotname, msname, field,
                                                   iobs, label, prefix, opts,
                                                   ftype=fields_, fid=fid, output_dir=output_dir,
                                                   corr_label=corrs[int(co)])

                elif plotter == "ragavi_vis" and not opts["iter-axis"] == "corr":
                    # change the labels to indices
                    with open(msinfo, 'r') as stdr:
                        corrs = yaml.load(stdr)['CORR']['CORR_TYPE']

                    corr = corr.replace(" ", "").split(",")
                    for it, co in enumerate(corr):
                        if co in corrs:
                            corr[it] = str(corrs.index(co))
                    corr = ",".join(corr)

                    # for each corr
                    for co in corr.split(","):
                        opts["corr"] = co
                        for fields_ in specific_fields(plotname) or fields:
                            for field in getattr(pipeline, fields_)[iobs]:
                                fid = utils.get_field_id(msinfo, field)[0]
                                globals()[plotter](pipeline, recipe, config,
                                                   plotname, msname, field,
                                                   iobs, label, prefix, opts,
                                                   ftype=fields_, fid=fid, output_dir=output_dir,
                                                   corr_label=corrs[int(co)])
                else:
                    opts["corr"] = corr
                    for fields_ in specific_fields(plotname) or fields:
                        for field in getattr(pipeline, fields_)[iobs]:
                            fid = utils.get_field_id(msinfo, field)[0]
                            globals()[plotter](pipeline, recipe, config,
                                               plotname, msname, field, iobs, label,
                                               prefix, opts, ftype=fields_,
                                               fid=fid, output_dir=output_dir)

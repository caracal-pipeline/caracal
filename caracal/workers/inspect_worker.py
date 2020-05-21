# -*- coding: future_fstrings -*-
import os
import sys
from collections import OrderedDict
import itertools
import yaml
from stimela.dismissable import dismissable as sdm
from caracal import log
import caracal.dispatch_crew.utils as utils
import numpy as np
import json

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

    uvrange = config['uvrange']
    plotter = config['standard_plotter']

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

        prefix = prefixes[iobs]
        fields = config['field'].split(',')

        if 'calibrators' in fields:
            fields = ['fcal','bpcal','gcal']

        for fd in fields:
            if fd not in ['target','fcal','bpcal','gcal']:
                raise ValueError("Eligible values for 'field': 'target', 'calibrators', 'fcal', 'bpcal' or 'gcal'. "\
                                 "User selected: {}".format(fields))

        '''GET LIST OF INPUT MS'''
        mslist = []
        msn = pipeline.msnames[iobs][:-3]
        label = config['label_plot']
        label_in = config['label_in']

        if not label_in:
            mslist.append(pipeline.msnames[iobs])
        elif config['field'] == 'target':
            for target in pipeline.target[iobs]:
                field = utils.filter_name(target)
                mslist.append('{0:s}-{1:s}_{2:s}.ms'.format(msn, field, label_in))
        else:
            mslist.append('{0:s}_{1:s}.ms'.format(msn, label_in))

        for m in mslist:
            if not os.path.exists(os.path.join(pipeline.msdir, m)):
                raise IOError(
                    "MS file {0:s} does not exist. Please check that is where it should be.".format(m))

        for msname in mslist:

            msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.obsinfo, msname[:-3])

            corr = config['correlation']
            with open(msinfo, 'r') as stdr:
                ms_corrs = yaml.load(stdr)['CORR']['CORR_TYPE']

            if corr == 'auto' or corr == 'all':
                corr = ','.join(ms_corrs)
            elif corr == 'diag' or corr == 'parallel':
                corr = ','.join([c for c in ms_corrs if len(c) == 2 and c[0] == c[1]])
            if not corr:
                log.warning(f"No correlations found to plot for {msname}")
                continue

            # new-school plots
            if config['shadems']['enable']:
                plot_args = []
                # get field names
                for field_type in fields:

                    if (label_in != '') and (config['field'] == 'target'):
                        with open(msinfo, 'r') as stdr:
                            field_names = yaml.load(stdr,Loader=yaml.FullLoader)['FIELD']['NAME']
                    else:
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
                        corr = corr.replace(" ", "").split(",")
                        for it, co in enumerate(corr):
                            if co in ms_corrs:
                                corr[it] = str(ms_corrs.index(co))
                        corr = ",".join(corr)
                        # for each corr
                        for co in corr.split(","):
                            opts["corr"] = co
                            for fields_ in fields:
                                for field in getattr(pipeline, fields_)[iobs]:
                                    if (label_in != '') and (config['field'] == 'target'):
                                       fid = 0
                                    else:
                                        fid = utils.get_field_id(msinfo, field)[0]

                                    globals()[plotter](pipeline, recipe, config,
                                                       plotname, msname, field,
                                                       iobs, label, prefix, opts,
                                                       ftype=fields_, fid=fid, output_dir=output_dir,
                                                       corr_label=ms_corrs[int(co)])

                    elif plotter == "ragavi_vis" and not opts["iter-axis"] == "corr":
                        # change the labels to indices
                        corr = corr.replace(" ", "").split(",")
                        for it, co in enumerate(corr):
                            if co in ms_corrs:
                                corr[it] = str(ms_corrs.index(co))
                        corr = ",".join(corr)

                        # for each corr
                        for co in corr.split(","):
                            opts["corr"] = co
                            for fields_ in fields:
                                for field in getattr(pipeline, fields_)[iobs]:
                                    if (label_in != '') and (config['field'] == 'target'):
                                        fid = 0
                                    else:
                                        fid = utils.get_field_id(msinfo, field)[0]

                                    globals()[plotter](pipeline, recipe, config,
                                                       plotname, msname, field,
                                                       iobs, label, prefix, opts,
                                                       ftype=fields_, fid=fid, output_dir=output_dir,
                                                       corr_label=ms_corrs[int(co)])
                    else:
                        opts["corr"] = corr
                        for fields_ in fields:
                            for field in getattr(pipeline, fields_)[iobs]:
                                if (label_in != '') and (config['field'] == 'target'):
                                    fid = 0
                                else:
                                    fid = utils.get_field_id(msinfo, field)[0]

                                globals()[plotter](pipeline, recipe, config,
                                                   plotname, msname, field, iobs, label,
                                                   prefix, opts, ftype=fields_,
                                                   fid=fid, output_dir=output_dir)

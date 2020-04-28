# -*- coding: future_fstrings -*-
import os
import sys
import yaml
from stimela.dismissable import dismissable as sdm
from caracal import log
import caracal.dispatch_crew.utils as utils

NAME = 'Inspect data'

# E.g. to split out continuum/<dir> from output/continuum/dir


def get_dir_path(string, pipeline):
    return string.split(pipeline.output)[1][1:]


def plotms(pipeline, recipe, config, plotname, msname, field, iobs, label, prefix, opts, ftype, fid):
    step = 'plot-{0:s}-{1:d}-{2:d}'.format(plotname, iobs, fid)
    colouraxis = opts.get("colouraxis", None)
    recipe.add("cab/casa_plotms", step, {
        "vis": msname,
        "field": field,
        "correlation": opts['corr'],
        "timerange": '',
        "antenna": '',
        "xaxis": opts['xaxis'],
        "xdatacolumn": config[plotname].get('column'),
        "yaxis": opts['yaxis'],
        "ydatacolumn": config[plotname].get('column'),
        "avgtime": config[plotname].get('avgtime'),
        "avgchannel": config[plotname].get('avgchannel'),
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
        output=os.path.join(pipeline.diagnostic_plots, "crosscal"),
        label="{0:s}:: Plotting corrected {1:s}".format(step, plotname))


def shadems(pipeline, recipe, config, plotname, msname, field, iobs, label, prefix, opts, ftype, fid, corr_label=None):
    step = 'plot-{0:s}-{1:d}-{2:d}'.format(plotname, iobs, fid)
    column = config[plotname]['column']
    if column == "corrected":
        column = "CORRECTED_DATA"
    elif column == "data":
        column = "DATA"
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
        "col": column,
        "png": '{0:s}-{1:s}-{2:s}-{3:s}-{4:s}-{5:s}.png'.format(prefix, label, field, plotname, ftype, corr_label),
    },
        input=pipeline.input,
        output=os.path.join(pipeline.diagnostic_plots, "crosscal"),
        label="{0:s}:: Plotting corrected ".format(step, plotname))


def ragavi_vis(pipeline, recipe, config, plotname, msname, field, iobs, label, prefix, opts, ftype, fid, corr_label=None):
    step = 'plot-{0:s}-{1:d}-{2:d}'.format(plotname, iobs, fid)
    column = config[plotname]['column']
    if column == "corrected":
        column = "CORRECTED_DATA"
    elif column == "data":
        column = "DATA"
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
        # "cbin": int(config[plotname].get('avgchannel', None)),
        # "colour-axis": opts.get("colour-axis", None),
        "data-column": column,
        "field": str(fid),
        "htmlname": "{0:s}-{1:s}-{2:s}-{3:s}-{4:s}-{5:s}".format(prefix, label, field, plotname, ftype, corr_label),
        "iter-axis": sdm(opts.get('iter-axis', None)),
        # "tbin": float(config[plotname].get('avgtime', None)),

    },
        input=pipeline.input,
        output=os.path.join(pipeline.diagnostic_plots, "crosscal"),
        label="{0:s}:: Plotting corrected {1:s}".format(step, plotname))


def worker(pipeline, recipe, config):

    def isempty(sec):
        if config[sec].get('fields') in ["", [""], None, "null"]:
            return False
        else:
            return config[sec].get('fields')

    uvrange = config.get('uvrange')
    fields = config.get('fields')
    plotter = config["plotter"]
    if pipeline.virtconcat:
        msnames = [pipeline.vmsname]
        prefixes = [pipeline.prefix]
        nobs = 1
    else:
        msnames = pipeline.msnames
        prefixes = pipeline.prefixes
        nobs = pipeline.nobs

    for i in range(nobs):
        msname = msnames[i] if not config['label_in'] else '{0:s}_{1:s}.ms'.format(
            msnames[i][:-3], config['label_in'])
        prefix = prefixes[i]
        label = config.get('label_out')

        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(
            pipeline.obsinfo, msname[:-3])

        corr = config.get('correlation')
        if corr == 'auto':
            with open(msinfo, 'r') as stdr:
                corrs = yaml.load(stdr)['CORR']['CORR_TYPE']
            corrs = ','.join(corrs)
            corr = corrs

        # define plot attributes
        diagnostic_plots = {}
        diagnostic_plots["real_imag"] = dict(
            plotms={"xaxis": "imag", "yaxis": "real",
                    "colouraxis": "baseline", "iteraxis": "corr"},
            shadems={"xaxis": "r", "yaxis": "i"},
            ragavi_vis={"xaxis": "real", "yaxis": "imaginary",
                        "iter-axis": "scan", "canvas-width": 300,
                        "canvas-height": 300})
        diagnostic_plots["amp_phase"] = dict(
            plotms={"xaxis": "amp", "yaxis": "phase",
                    "colouraxis": "baseline", "iteraxis": "corr"},
            shadems={"xaxis": "a", "yaxis": "p"},
            ragavi_vis={"xaxis": "phase", "yaxis": "amplitude",
                        "iter-axis": "corr", "canvas-width": 1080,
                        "canvas-height": 720})
        diagnostic_plots["amp_ant"] = dict(
            plotms={"xaxis": "antenna", "yaxis": "amp",
                    "colouraxis": "baseline", "iteraxis": "corr"},
            shadems=None,
            ragavi_vis=None)
        diagnostic_plots["amp_uvwave"] = dict(
            plotms={"xaxis": "uvwave", "yaxis": "amp",
                    "colouraxis": "baseline", "iteraxis": "corr"},
            shadems={"xaxis": "uv", "yaxis": "a"},
            ragavi_vis={"xaxis": "uvwave", "yaxis": "amplitude",
                        "iter-axis": "scan", "canvas-width": 300,
                        "canvas-height": 300})
        diagnostic_plots["phase_uvwave"] = dict(
            plotms={"xaxis": "uvwave", "yaxis": "phase",
                    "colouraxis": "baseline", "iteraxis": "corr"},
            shadems={"xaxis": "uv", "yaxis": "p"},
            ragavi_vis={"xaxis": "uvwave", "yaxis": "phase",
                        "iter-axis": "corr", "canvas-width": 1080,
                        "canvas-height": 720})
        diagnostic_plots["amp_scan"] = dict(
            plotms={"xaxis": "scan", "yaxis": "amp"},
            shadems=None,
            ragavi_vis={"xaxis": "scan", "yaxis": "amplitude",
                        "iter-axis": None,
                        "canvas-width": 1080, "canvas-height": 720})
        diagnostic_plots["amp_chan"] = dict(
            plotms={"xaxis": "chan", "yaxis": "amp"},
            shadems={"xaxis": "c", "yaxis": "a"},
            ragavi_vis={"xaxis": "channel", "yaxis": "amplitude",
                        "iter-axis": None,
                        "canvas-width": 1080, "canvas-height": 720})
        diagnostic_plots["phase_chan"] = dict(
            plotms={"xaxis": "chan", "yaxis": "phase"},
            shadems={"xaxis": "c", "yaxis": "p"},
            ragavi_vis={"xaxis": "channel", "yaxis": "phase",
                        "iter-axis": "scan", "canvas-width": 300,
                        "canvas-height": 300})

        for plotname in diagnostic_plots:
            if not pipeline.enable_task(config, plotname):
                continue
            opts = diagnostic_plots[plotname][plotter]
            if opts is None:
                log.warn("The plotter '{0:s}' cannot make the plot '{1:s}'".format(
                    plotter, plotname))
                continue

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
                    opts["corr"] = int(co)
                    for fields_ in isempty(plotname) or fields:
                        for field in getattr(pipeline, fields_)[i]:
                            fid = utils.get_field_id(msinfo, field)[0]
                            globals()[plotter](pipeline, recipe, config,
                                               plotname, msname, field,
                                               i, label, prefix, opts,
                                               ftype=fields_, fid=fid,
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
                    for fields_ in isempty(plotname) or fields:
                        for field in getattr(pipeline, fields_)[i]:
                            fid = utils.get_field_id(msinfo, field)[0]
                            globals()[plotter](pipeline, recipe, config,
                                               plotname, msname, field,
                                               i, label, prefix, opts,
                                               ftype=fields_, fid=fid,
                                               corr_label=corrs[int(co)])
            else:
                opts["corr"] = corr
                for fields_ in isempty(plotname) or fields:
                    for field in getattr(pipeline, fields_)[i]:
                        fid = utils.get_field_id(msinfo, field)[0]
                        globals()[plotter](pipeline, recipe, config,
                                           plotname, msname, field, i, label,
                                           prefix, opts, ftype=fields_,
                                           fid=fid)

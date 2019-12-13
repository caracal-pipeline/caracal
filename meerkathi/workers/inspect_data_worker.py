import os
import sys
import yaml
from stimela.dismissable import dismissable as sdm
from meerkathi import log
import meerkathi.dispatch_crew.utils as utils

NAME = 'Inspect data'

# E.g. to split out continuum/<dir> from output/continuum/dir

def get_dir_path(string, pipeline): 
    return string.split(pipeline.output)[1][1:]

def plotms(pipeline, recipe, config, plotname, msname, field, iobs, label, prefix, opts, ftype, fid):
    step = 'plot_{0:s}_{1:d}_{2:d}'.format(plotname, iobs, fid)
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
                "plotfile": '{0:s}_{1:s}_{2:s}_{3:s}_{4:s}.png'.format(prefix, label, field, plotname, ftype),
                "expformat": 'png',
                "exprange": 'all',
                "overwrite": True,
                "showgui": False,
                "uvrange": config["uvrange"],
        }, 
        input=pipeline.input,
        output=os.path.join(pipeline.diagnostic_plots, "crosscal") ,
        label="{0:s}:: Plotting corrected {1:s}".format(step, plotname))


def shadems(pipeline, recipe, config, plotname, msname, field, iobs, label, prefix, opts, ftype, fid):
    step = 'plot_{0:s}_{1:d}_{2:d}'.format(plotname, iobs, fid)
    column = config[plotname]['column']
    if column == "corrected":
        column = "CORRECTED_DATA"
    elif column == "data":
        column = "DATA"

    recipe.add("cab/shadems", step, {
                "ms": msname,
                "field": field,
#                "corr": opts['corr'],
                "xaxis": opts['xaxis'],
                "yaxis": opts['yaxis'],
                "col": column,
                "png": '{0:s}_{1:s}_{2:s}_{3:s}_{4:s}.png'.format(prefix, label, field, plotname, ftype),
        }, 
        input=pipeline.input,
        output=os.path.join(pipeline.diagnostic_plots, "crosscal"),
        label="{0:s}:: Plotting corrected ".format(step, plotname))


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
        msname = msnames[i]
        prefix = prefixes[i]
        label = config.get('label')

        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, msname[:-3])

        corr = config.get('correlation')
        if corr == 'auto':
            with open(msinfo, 'r') as stdr:
                corrs = yaml.load(stdr)['CORR']['CORR_TYPE']
            corrs = ','.join(corrs)
            corr = corrs

        # define plot attributes
        diagnostic_plots = {}
        diagnostic_plots["real_imag"] = dict(plotms={"xaxis": "imag", "yaxis" : "real", 
                    "colouraxis" : "baseline", "iteraxis": "corr"}, 
                    shadems={"xaxis": "r", "yaxis": "i"})
        diagnostic_plots["amp_phase"] = dict(plotms={"xaxis": "amp", "yaxis" : "phase",
                    "colouraxis": "baseline", "iteraxis": "corr"},
                    shadems={"xaxis": "a", "yaxis": "p"})
        diagnostic_plots["amp_ant"] = dict(plotms={"xaxis": "antenna", "yaxis" :"amp",
                    "colouraxis": "baseline", "iteraxis" : "corr"}, shadems=None)
        diagnostic_plots["amp_uvwave"] = dict(plotms={"xaxis": "uvwave", "yaxis": "amp",
                    "colouraxis" : "baseline", "iteraxis" : "corr"}, shadems={"xaxis": "uv", "yaxis": "a"})
        diagnostic_plots["phase_uvwave"] = dict(plotms={"xaxis" : "uvwave", "yaxis": "phase", 
                    "colouraxis" : "baseline", "iteraxis" : "corr"}, shadems={"xaxis": "uv", "yaxis": "p"})
        diagnostic_plots["amp_scan"] = dict(plotms={"xaxis": "scan", "yaxis": "amp"}, shadems=None)
        diagnostic_plots["amp_chan"] = dict(plotms={"xaxis": "chan", "yaxis": "amp"}, shadems={"xaxis": "c", "yaxis" :"a"})
        diagnostic_plots["phase_chan"] = dict(plotms={"xaxis": "chan", "yaxis": "phase"}, shadems={"xaxis": "c", "yaxis" :"p"})

        for plotname in diagnostic_plots:
            if not pipeline.enable_task(config, plotname):
                continue
            opts = diagnostic_plots[plotname][plotter]
            if opts is None:
                log.warn("The plotter '{0:s}' cannot make the plot '{1:s}'".format(plotter, plotname)) 
                continue
            opts["corr"] = corr
            for fields_ in isempty(plotname) or fields:
                for field in getattr(pipeline, fields_)[i]:
                    fid = utils.get_field_id(msinfo, field)[0]
                    globals()[plotter](pipeline, recipe, config, 
                        plotname, msname, field, i, label, prefix, opts, ftype=fields_, fid=fid)

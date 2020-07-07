# -*- coding: future_fstrings -*-
import stimela
import pickle
import sys
import os
import caracal.dispatch_crew.utils as utils
import caracal
import yaml
import stimela.dismissable as sdm
from caracal.workers.utils import manage_flagsets as manflags
from caracal.workers.utils import manage_fields as manfields
from caracal.workers.utils import manage_caltabs as manGtabs
import copy
import re
import json
import glob

import shutil
import numpy
from casacore.tables import table as tb

NAME = "Polarization calibration"
LABEL = 'polcal'


def get_dir_path(string, pipeline):
    return string.split(pipeline.output)[1][1:]


def exists(outdir, path):
    _path = os.path.join(outdir, path)
    return os.path.exists(_path)


def scan_length(msinfo, field):
    idx = utils.get_field_id(msinfo, field)[0]
    return float(utils.field_observation_length(msinfo, field)) / len(msinfo['SCAN'][str(idx)])


def ben_cal(msname, msinfo, recipe, config, pipeline, i, prefix, polarized_calibrators):
    gaintables = [prefix + '.Gpol1', prefix + '.Kcrs', prefix + '.Xref', prefix + 'Xf',
                  prefix + '.Dref', prefix + '.Df']
    ref = pipeline.refant[i] or '0'
    field = ",".join(getattr(pipeline, config["pol_calib"])[i])
    leak_field = ",".join(getattr(pipeline, config["leakage_calib"])[i])
    avgstring = ',' + config["avg_bw"]
    scandur = scan_length(msinfo, field)
    if config["make_diagnostic_plots"]:
        ant = config['refant_for_plots']
        plotdir = os.path.join(pipeline.diagnostic_plots, "polcal")
        plotname = "%s_%s" % (prefix, config['pol_calib'])
        if not os.path.exists(plotdir):
            os.mkdir(plotdir)

    docal = config['reuse_existing_tables']
    for cal in gaintables:
        if not os.path.exists(os.path.join(pipeline.caltables, cal)):
            docal = False

    if not docal:
        recipe.add("cab/casa_setjy",
                   "set_model_%d" % 0,
                   {
                       "msname": msname,
                       "usescratch": True,
                       "field": field,
                       "standard": polarized_calibrators[field]["standard"],
                       "fluxdensity": polarized_calibrators[field]["fluxdensity"],
                       "spix": polarized_calibrators[field]["spix"],
                       "reffreq": polarized_calibrators[field]["reffreq"],
                       "polindex": polarized_calibrators[field]["polindex"],
                       "polangle": polarized_calibrators[field]["polangle"],
                   },
                   input=pipeline.input, output=pipeline.output,
                   label="set_model_%d" % 0)
        gaintables = []

        # Phaseup diagonal of crosshand cal if available
        recipe.add("cab/casa_gaincal",
                   "crosshand_phaseup",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Gpol1:output',
                       "field": field,
                       "refant": ref,
                       "solint": scandur,
                       "combine": "",
                       "parang": True,
                       "gaintype": "G",
                       "calmode": "p",
                       "spw": '',
                       "gaintable": ["%s:output" % ct for ct in gaintables],
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="crosshand_phaseup")
        gaintables += [prefix + '.Gpol1']

        recipe.add("cab/casa_gaincal",
                   "crosshand_delay",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Kcrs:output',
                       "field": field,
                       "refant": ref,
                       "solint": scandur,
                       "combine": "",
                       "parang": True,
                       "gaintype": "KCROSS",
                       "spw": '',
                       "gaintable": ["%s:output" % ct for ct in gaintables],
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="crosshand_delay")
        gaintables += [prefix + '.Kcrs']

        if config["make_diagnostic_plots"]:
            recipe.add("cab/casa_plotms",
                       "crosshand_delay_plot",
                       {
                           "vis": prefix + '.Kcrs:msfile',
                           "xaxis": "time",
                           "yaxis": "delay",
                           "field": '',
                           "plotfile": plotname + ".Kcrs.png",
                           "overwrite": True,
                       },
                       input=pipeline.input, output=plotdir, msdir=pipeline.caltables,
                       label="crosshand_delay_plot")

        # Solve for the absolute angle (phase) between the feeds
        # (P Jones auto enabled)
        # of the form [e^{2pi.i.b} 0 0 1]

        # remove the DC of the frequency solutions before
        # possibly joining scans to solve for per-frequency solutions
        # a strongly polarized source is needed with known properties
        # to limit the amount of PA coverage needed
        recipe.add("cab/casa_polcal",
                   "crosshand_phase_ref",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Xref:output',
                       "field": field,
                       "solint": scandur,
                       "combine": "",
                       "poltype": "Xf",
                       "refant": ref,
                       "gaintable": ["%s:output" % ct for ct in gaintables],
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="crosshand_phase_ref")
        gaintables += [prefix + '.Xref']

        if config["make_diagnostic_plots"]:
            recipe.add("cab/casa_plotms",
                       "crosshand_phase_ref_plot",
                       {
                           "vis": prefix + '.Xref:msfile',
                           "xaxis": "time",
                           "yaxis": "phase",
                           "overwrite": True,
                           "field": '',
                           "plotfile": plotname + ".Xref.png",
                       },
                       input=pipeline.input, output=plotdir, msdir=pipeline.caltables,
                       label="crosshand_phase_ref_plot")

        recipe.add("cab/casa_polcal",
                   "crosshand_phase_freq",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Xf:output',
                       "field": field,
                       "solint": 'inf' + avgstring,  # solint to obtain SNR on solutions
                       "combine": "scan",
                       "poltype": "Xf",
                       "refant": ref,
                       "gaintable": ["%s:output" % ct for ct in gaintables],
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="crosshand_phase_freq")
        gaintables += [prefix + '.Xf']

        if config["make_diagnostic_plots"]:
            recipe.add("cab/casa_plotms",
                       "crosshand_phase_freq_plot",
                       {
                           "vis": prefix + '.Xf:msfile',
                           "xaxis": "freq",
                           "yaxis": "phase",
                           "field": '',
                           "plotfile": plotname + ".Xf.png",
                           "overwrite": True,
                       },
                       input=pipeline.input, output=plotdir, msdir=pipeline.caltables,
                       label="crosshand_phase_freq_plot")

        # Solve for leakages (off-diagonal terms) using the unpolarized source
        # - first remove the DC of the frequency response and combine scans
        # if necessary to achieve desired SNR

        recipe.add("cab/casa_polcal",
                   "leakage_ref",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Dref:output',
                       "field": leak_field,
                       "solint": scandur,
                       "combine": "",
                       "poltype": "D",
                       "refant": ref,
                       "spw": '',
                       "gaintable": ["%s:output" % ct for ct in gaintables],
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="leakage_ref")
        gaintables += [prefix + '.Dref']

        if config["make_diagnostic_plots"]:
            recipe.add("cab/casa_plotms",
                       "leakage_ref_plot",
                       {
                           "vis": prefix + '.Dref:msfile',
                           "xaxis": "time",
                           "yaxis": "amp",
                           "field": '',
                           "antenna": ant,
                           "plotfile": plotname + ".Dref.png",
                           "overwrite": True
                       },
                       input=pipeline.input, output=plotdir, msdir=pipeline.caltables,
                       label="leakage_ref_plot")

        recipe.add("cab/casa_polcal",
                   "leakage_freq",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Df:output',
                       "spw": '',
                       "field": leak_field,
                       "solint": 'inf' + avgstring,  # ensure SNR criterion is met
                       "combine": "scan",
                       "poltype": "Df",
                       "refant": ref,
                       "gaintable": ["%s:output" % ct for ct in gaintables],

                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="leakage_freq")
        gaintables += [prefix + '.Df']

        if config["make_diagnostic_plots"]:
            recipe.add("cab/casa_plotms",
                       "leakage_freq_plot",
                       {
                           "vis": prefix + '.Df:msfile',
                           "xaxis": "freq",
                           "yaxis": "amp",
                           "field": '',
                           "antenna": ant,
                           "plotfile": plotname + ".Dfreq.png",
                           "overwrite": True
                       },
                       input=pipeline.input, output=plotdir, msdir=pipeline.caltables,
                       label="leakage_freq_plot")

        applycal_recipes = []
        calmodes = []
        interps = ['linear', 'nearest', 'nearest', 'nearest', 'nearest', 'nearest']
        fields = ['', '', '', '', '', '']
        calwt = [True, False, False, False, False, False]
        for ix, gt in enumerate(gaintables):
            applycal_recipes.append(dict(zip(
                ['caltable', 'fldmap', 'interp', 'calwt'], [gt, fields[ix], interps[ix], calwt[ix]])))
            if '.G1' in gt:
                calmodes.append('xcal_gain')
            elif '.KX' in gt:
                calmodes.append('cross_phase')
            elif '.Xref' in gt:
                calmodes.append('phase ref')
            elif '.Xf' in gt:
                calmodes.append('phase')
            elif '.Dref' in gt:
                calmodes.append('leakage_ref')
            elif '.Df' in gt:
                calmodes.append('leakage_freq')
        callib_dir = "{}/callibs".format(pipeline.caltables)
        if not os.path.exists(callib_dir):
            os.mkdir(callib_dir)

        callib_dict = dict(zip(calmodes, applycal_recipes))

        with open(os.path.join(callib_dir, f'callib_{prefix}.json'), 'w') as json_file:
            json.dump(callib_dict, json_file)

    else:
        caracal.log.info("Reusing existing tables as requested")


def floi_calib(msname, msinfo, recipe, config, pipeline, i, prefix):
    gaintables = [prefix + '.Gxyamp', prefix + '.Gpol2', prefix + '.Kcrs', prefix + '.Xfparang',
                  prefix + '.Df0gen']
    ref = pipeline.refant[i] or '0'
    field = ",".join(getattr(pipeline, config["pol_calib"])[i])
    avgstring = ',' + config["avg_bw"]
    scandur = scan_length(msinfo, field)
    if config["make_diagnostic_plots"]:
        ant = config['refant_for_plots']
        plotdir = os.path.join(pipeline.diagnostic_plots, "polcal")
        plotname = "%s_%s" % (prefix, config['pol_calib'])
        if not os.path.exists(plotdir):
            os.mkdir(plotdir)

    docal = config['reuse_existing_tables']
    for cal in gaintables:
        if not os.path.exists(os.path.join(pipeline.caltables, cal)):
            docal = False

    if not docal:
        # G1
        recipe.add("cab/casa_gaincal",
                   "first gaincal",
                   {
                       "vis": msname,
                       "field": field,
                       "caltable": prefix + '.Gpol1:output',
                       "smodel": ['1', '0', '0', '0'],
                       "refantmode": 'strict',
                       "refant": ref,
                       "gaintype": 'G',
                       "calmode": 'ap',
                       "parang": False,
                       "solint": 'int',
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="Gain xcal 1")

        if config["make_diagnostic_plots"]:
            recipe.add("cab/casa_plotms",
                       "plot_firstGpol",
                       {
                           "vis": prefix + '.Gpol1:msfile',
                           "field": '',
                           "xaxis": 'scan',
                           "yaxis": 'amp',
                           "correlation": '/',
                           "coloraxis": 'antenna1',
                           "plotfile": plotname + '.Gpol1.png',
                           "overwrite": True,
                       },
                       input=pipeline.input, output=plotdir, msdir=pipeline.caltables,
                       label="plot Gain xcal 1")
            recipe.add("cab/casa_plotms",
                       "plot_beforeKcrs",
                       {
                           "vis": msname,
                           "field": field,
                           "xaxis": 'freq',
                           "yaxis": 'phase',
                           "ydatacolumn": 'corrected',
                           "avgtime": '1e3',
                           "spw": '',
                           "antenna": ant,
                           "correlation": 'XY,YX',
                           "coloraxis": 'corr',
                           "iteraxis": 'baseline',
                           "overwrite": True,
                           "plotrange": [0, 0, -180, 180],
                           "plotfile": plotname + '.beforeKcross.png',
                       },
                       input=pipeline.input, output=plotdir, msdir=pipeline.msdir,
                       label="plot before Kcrs")

        shutil.rmtree(os.path.join(pipeline.caltables, prefix + '.Gpol1a'), ignore_errors=True)
        # QU
        recipe.add("cab/casa_polfromgain",
                   "QU from gain",
                   {
                       "vis": msname,
                       "tablein": prefix + '.Gpol1:output',
                       "caltable": prefix + '.Gpol1a:output',
                       "save_result": prefix + '_S1_from_QUfit:output',
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="QU from gain")
        recipe.run()
        recipe.jobs = []

        # We search for the scan where the polarization signal is minimum in XX and YY
        # (i.e., maximum in XY and YX):
        with tb(os.path.join(pipeline.caltables, prefix + '.Gpol1')) as t:
            scans = t.getcol('SCAN_NUMBER')
            gains = numpy.squeeze(t.getcol('CPARAM'))
            t.close()
        scanlist = numpy.array(list(set(scans)))
        ratios = numpy.zeros(len(scanlist))

        for si, s in enumerate(scanlist):
            filt = scans == s
            ratio = numpy.sqrt(
                numpy.average(numpy.power(numpy.abs(gains[filt, 0]) / numpy.abs(gains[filt, 1]) - 1.0, 2.)))
            ratios[si] = ratio

        bestscidx = numpy.argmin(ratios)
        bestscan = scanlist[bestscidx]
        caracal.log.info('Scan with highest expected X-Y signal: ' + str(bestscan))

        recipe.run()
        recipe.jobs = []

        # Kcross
        recipe.add("cab/casa_gaincal",
                   "Kcross delay",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Kcrs:output',
                       "selectdata": True,
                       "field": field,
                       "scan": str(bestscan),
                       "gaintype": 'KCROSS',
                       "solint": 'inf' + avgstring,
                       "refantmode": 'strict',
                       "refant": ref,
                       "smodel": ['1', '0', '1', '0'],
                       "gaintable": [prefix + '.Gpol1:output'],
                       "interp": ['linear'],
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="Kcross delay")

        recipe.run()
        recipe.jobs = []

        if config["make_diagnostic_plots"]:
            recipe.add("cab/casa_applycal",
                       "apply 1",
                       {
                           "vis": msname,
                           "field": field,
                           "calwt": True,
                           "gaintable": [prefix + '.Gpol1:output', prefix + '.Kcrs:output'],
                           "interp": ['linear', 'nearest'],
                           "parang": False,
                       },
                       input=pipeline.input, output=pipeline.caltables,
                       label="Apply Gpol1 Kcrs")

            recipe.add("cab/casa_plotms",
                       "plot_afterKcrs",
                       {
                           "vis": msname,
                           "field": field,
                           "xaxis": 'freq',
                           "yaxis": 'phase',
                           "ydatacolumn": 'corrected',
                           "avgtime": '1e3',
                           "spw": '',
                           "antenna": ant,
                           "correlation": 'XY,YX',
                           "coloraxis": 'corr',
                           "iteraxis": 'baseline',
                           "overwrite": True,
                           "plotrange": [0, 0, -180, 180],
                           "plotfile": plotname + '.afterKcross.png',
                       },
                       input=pipeline.input, output=plotdir,
                       label="plot after Kcrs")

            # Cross-hand phase: PHASE vs ch and determine channel averaged pol
            recipe.add("cab/casa_plotms",
                       "plot_beforeX",
                       {
                           "vis": msname,
                           "field": field,
                           "xdatacolumn": 'corrected',
                           "ydatacolumn": 'corrected',
                           "xaxis": 'real',
                           "yaxis": 'imag',
                           "avgtime": '1e3',
                           "correlation": 'XY,YX',
                           "spw": '',
                           "coloraxis": 'corr',
                           "avgchannel": '10',
                           "avgbaseline": True,
                           "clearplots": True,
                           "plotfile": plotname + '.imag_vs_real_beforeXfparang.png',
                           "overwrite": True,
                           "plotrange": [-0.06, 0.06, -0.06, 0.06],
                       },
                       input=pipeline.input, output=plotdir,
                       label="plot before Xf")

        if os.path.isfile(pipeline.output + '/caltables/' + prefix + '_S1_from_QUfit'):
            with open(pipeline.output + '/caltables/' + prefix + '_S1_from_QUfit', 'rb') as stdr:
                S1 = pickle.load(stdr, encoding='latin1')

            S1 = S1[field]['SpwAve']
            caracal.log.info("First [I,Q,U,V] fitted model (with I=1 and Q, U fractional): %s" % S1)
        else:
            raise RuntimeError("Cannot find S1")  # prefix+'S1_from_QUfit:output'

        # QU abs delay
        recipe.add("cab/casa_polcal",
                   "Abs phase and QU fit",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Xfparang:output',
                       "field": field,
                       "spw": '',
                       "poltype": 'Xfparang+QU',
                       "solint": 'inf' + avgstring,
                       "combine": 'scan,obs',
                       "preavg": scandur,
                       "smodel": S1,
                       "gaintable": [prefix + '.Gpol1:output', prefix + '.Kcrs:output'],
                       "interp": ['linear', 'nearest'],
                       "save_result": prefix + '_S2_from_polcal:output',
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="Abs phase and QU fit")

        recipe.run()
        recipe.jobs = []

        if config["make_diagnostic_plots"]:
            recipe.add("cab/casa_plotms",
                       "plot_Xf",
                       {
                           "vis": prefix + '.Xfparang:msfile',
                           "field": '',
                           "xaxis": 'freq',
                           "yaxis": 'phase',
                           "antenna": '0',
                           "coloraxis": 'corr',
                           "gridrows": 2,
                           "rowindex": 0,
                           "clearplots": True,
                           "plotfile": plotname + '.Xfparang.png',
                           "overwrite": True,
                       },
                       input=pipeline.input, output=plotdir, msdir=pipeline.caltables,
                       label="plot Xf")

            recipe.add("cab/casa_applycal",
                       "apply 2",
                       {
                           "vis": msname,
                           "field": field,
                           "calwt": True,
                           "gaintable": [prefix + '.Gpol1:output', prefix + '.Kcrs:output',
                                         prefix + '.Xfparang:output'],
                           "interp": ['linear', 'nearest', 'nearest'],
                           "parang": False,
                       },
                       input=pipeline.input, output=pipeline.caltables, msdir=pipeline.msdir,
                       label="Apply Gpol1 Kcrs Xf")

            recipe.add("cab/casa_plotms",
                       "plot_afterX",
                       {
                           "vis": msname,
                           "field": field,
                           "xdatacolumn": 'corrected',
                           "ydatacolumn": 'corrected',
                           "xaxis": 'real',
                           "yaxis": 'imag',
                           "avgtime": '1e3',
                           "correlation": 'XY,YX',
                           "spw": '',
                           "coloraxis": 'corr',
                           "avgchannel": '10',
                           "avgbaseline": True,
                           "clearplots": True,
                           "overwrite": True,
                           "plotfile": plotname + '.imag_vs_real_afterXfparang.png',
                           "plotrange": [-0.06, 0.06, -0.06, 0.06],
                       },
                       input=pipeline.input, output=plotdir,
                       label="plot after Xf")

        if os.path.isfile(pipeline.output + '/caltables/' + prefix + '_S2_from_polcal'):
            with open(pipeline.output + '/caltables/' + prefix + '_S2_from_polcal', 'rb') as stdr:
                S2 = pickle.load(stdr, encoding='latin1')
            S2 = S2[field]['SpwAve'].tolist()
            caracal.log.info("Second [I,Q,U,V] fitted model (with I=1 and Q, U fractional): %s" % S2)
        else:
            raise RuntimeError("Cannot find " + pipeline.output + "/caltables/" + prefix + "_S2_from_polcal")

        recipe.add("cab/casa_gaincal",
                   "second gaincal",
                   {
                       "vis": msname,
                       "field": field,
                       "caltable": prefix + '.Gpol2:output',
                       "smodel": S2,
                       "refantmode": 'strict',
                       "refant": ref,
                       "gaintype": 'G',
                       "calmode": 'ap',
                       "parang": True,
                       "solint": 'int',
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="Gain polcal 2")

        if config["make_diagnostic_plots"]:
            recipe.add("cab/casa_plotms",
                       "plot_Gpol2",
                       {
                           "vis": prefix + '.Gpol2:msfile',
                           "field": '',
                           "xaxis": 'scan',
                           "yaxis": 'amp',
                           "correlation": '/',
                           "coloraxis": 'antenna1',
                           "plotfile": plotname + '.Gpol2.png',
                           "overwrite": True,
                       },
                       input=pipeline.input, output=plotdir, msdir=pipeline.caltables,
                       label="plot Gain xcal 2")

        # LEAKAGE
        recipe.add("cab/casa_polcal",
                   "leakage terms",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Df0gen:output',
                       "field": field,
                       "spw": '',
                       "solint": 'inf' + avgstring,
                       "combine": 'obs,scan',
                       "preavg": scandur,
                       "poltype": 'Dflls',
                       "refant": '',  # solve absolute D-term
                       "smodel": S2,
                       "gaintable": [prefix + '.Gpol2:output', prefix + '.Kcrs:output',
                                     prefix + '.Xfparang:output'],
                       "gainfield": ['', '', ''],
                       "interp": ['linear', 'nearest', 'nearest'],
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="Leakage terms")

        # solve for global normalized gain amp (to get X/Y ratios) on pol calibrator (TO APPLY ON TARGET)
        # amp-only and normalized, so only X/Y amp ratios matter
        recipe.add("cab/casa_gaincal",
                   "normalize gain ampl for target",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Gxyamp:output',
                       "field": field,
                       "solint": 'inf',
                       "combine": 'scan,obs',
                       "refant": ref,
                       "refantmode": 'strict',
                       "gaintype": 'G',
                       "smodel": S2,
                       "calmode": 'a',
                       "gaintable": [prefix + '.Kcrs:output', prefix + '.Xfparang:output',
                                     prefix + '.Df0gen:output'],
                       "gainfield": ['', '', ''],
                       "interp": ['nearest', 'nearest', 'nearest'],
                       "solnorm": True,
                       "parang": True,
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="Target norm")

        recipe.run()
        recipe.jobs = []

        applycal_recipes = []
        calmodes = []
        gaintables = [prefix + '.Gpol2', prefix + '.Kcrs', prefix + '.Xfparang', prefix + '.Df0gen']
        interps = ['linear', 'nearest', 'nearest', 'nearest']
        fields = ['', '', '', '']
        calwt = [True, False, False, False]

        for ix, gt in enumerate(gaintables):
            applycal_recipes.append(dict(zip(
                ['caltable', 'fldmap', 'interp'], [gt, fields[ix], interps[ix], calwt[ix]])))
            if '.Gpol2' in gt:
                calmodes.append('xcal_gain')
            elif '.Kcrs' in gt:
                calmodes.append('cross_phase')
            elif '.Xfparang' in gt:
                calmodes.append('pol_angle')
            elif '.Df0gen' in gt:
                calmodes.append('abs_leakage')
        callib_dir = "{}/callibs".format(pipeline.caltables)
        if not os.path.exists(callib_dir):
            os.mkdir(callib_dir)

        callib_dict = dict(zip(calmodes, applycal_recipes))

        with open(os.path.join(callib_dir, f'callib_{prefix}_xcal.json'), 'w') as json_file:
            json.dump(callib_dict, json_file)

        applycal_recipes = []
        calmodes = []
        gaintables = [prefix + '.Gxyamp', prefix + '.Kcrs', prefix + '.Xfparang', prefix + '.Df0gen']
        interps = ['linear', 'nearest', 'nearest', 'nearest']
        fields = ['', '', '', '']
        calwt = [True, False, False, False]

        for ix, gt in enumerate(gaintables):
            applycal_recipes.append(dict(zip(
                ['caltable', 'fldmap', 'interp', 'calwt'], [gt, fields[ix], interps[ix], calwt[ix]])))
            if '.Gpol2' in gt:
                calmodes.append('xcal_gain')
            elif '.Kcrs' in gt:
                calmodes.append('cross_phase')
            elif '.Xfparang' in gt:
                calmodes.append('pol_angle')
            elif '.Df0gen' in gt:
                calmodes.append('abs_leakage')
        callib_dir = "{}/callibs".format(pipeline.caltables)
        if not os.path.exists(callib_dir):
            os.mkdir(callib_dir)

        callib_dict = dict(zip(calmodes, applycal_recipes))

        with open(os.path.join(callib_dir, f'callib_{prefix}.json'), 'w') as json_file:
            json.dump(callib_dict, json_file)

    else:
        caracal.log.info("Reusing existing tables as requested")


def applycal(msname, recipe, config, pipeline, i, prefix, field):
    gaintables = [prefix + '.Gxyamp', prefix + '.Gpol2', prefix + '.Kcrs', prefix + '.Xfparang',
                  prefix + '.Df0gen']
    for cal in gaintables:
        if not os.path.exists(os.path.join(pipeline.caltables, cal)):
            raise RuntimeError("Cannot find caltables!")

    f = ",".join(getattr(pipeline, field)[i])
    pcal = ",".join(getattr(pipeline, config["pol_calib"])[i])
    if f == pcal:
        recipe.add("cab/casa_applycal", "apply_caltables", {
            "vis": msname,
            "field": f,
            "gaintable": [prefix + '.Gpol2:output', prefix + '.Kcrs:output', prefix + '.Xfparang:output',
                          prefix + '.Df0gen:output'],
            "interp": ['linear', 'nearest', 'nearest', 'nearest'],
            "calwt": [True, False, False, False],
            "gainfield": ['', '', '', ''],
            "parang": True,
        },
                   input=pipeline.input, output=pipeline.caltables,
                   label="Apply caltables")

    else:
        recipe.add("cab/casa_applycal", "apply_caltables", {
            "vis": msname,
            "field": f,
            "gaintable": [prefix + '.Gxyamp:output', prefix + '.Kcrs:output', prefix + '.Xfparang:output',
                          prefix + '.Df0gen:output'],
            "interp": ['linear', 'nearest', 'nearest', 'nearest'],
            "calwt": [True, False, False, False],
            "gainfield": ['', '', '', ''],
            "parang": True,
        },
                   input=pipeline.input, output=pipeline.caltables,
                   label="Apply caltables")


# def worker
def worker(pipeline, recipe, config):
    wname = pipeline.CURRENT_WORKER
    label = config["label_cal"]
    label_in = config["label_in"]

    # define pol and unpol calibrators
    polarized_calibrators = {"3C138": {"standard": "manual",
                                       "fluxdensity": [8.4012],
                                       "spix": [-0.54890527955337987, -0.069418066176041668,
                                                -0.0018858519926001926],
                                       "reffreq": "1.45GHz",
                                       "polindex": [0.075],
                                       "polangle": [-0.19199]},
                             "3C286": {"standard": "manual",
                                       "fluxdensity": [14.918703],
                                       "spix": [-0.50593909976893958, -0.070580431627712076,
                                                0.0067337240268301466],
                                       "reffreq": "1.45GHz",
                                       "polindex": [0.095],
                                       "polangle": [0.575959]},
                             }
    polarized_calibrators["J1331+3030"] = polarized_calibrators["3C286"]
    polarized_calibrators["J0521+1638"] = polarized_calibrators["3C138"]
    unpolarized_calibrators = ["PKS1934-63", "J1939-6342", "J1938-6341", "PKS 1934-638", "PKS 1934-63", "PKS1934-638"]

    # loop over all MSs for this label
    for i, msbase in enumerate(pipeline.msbasenames):
        inmsname = pipeline.form_msname(msbase, label_in)
        msinfo = pipeline.get_msinfo(inmsname)
        prefix_msbase = f"{pipeline.prefix_msbases[i]}-{label}"

        # Check if feeds are linear
        if set(list(msinfo['CORR']['CORR_TYPE'])) & {'XX', 'XY', 'YX', 'YY'} == 0:
            raise RuntimeError(
                "Cannot calibrate polarization! Allowed strategies are for linear feed data but correlation is: " + str(
                    [
                        'XX', 'XY', 'YX', 'YY']))

        # prepare data (APPLY KGB AND SPLIT a NEW MSDIR)
        if config['crosscal_callib'] != '' and config['crosscal_callib'][-5:] == '.json':
            msname = 'crosscalib_' + inmsname
            if os.path.exists(os.path.join(pipeline.msdir, msname)) or os.path.exists(
                    os.path.join(pipeline.msdir, msname) + ".flagversions"):
                shutil.rmtree(os.path.join(pipeline.msdir, msname), ignore_errors=True)
                shutil.rmtree(os.path.join(pipeline.msdir, msname) + '.flagversions', ignore_errors=True)

            calprefix = config['crosscal_callib'][:-5]
            callib_path = 'caltables/callibs/{}'.format(config['crosscal_callib'])

            if not os.path.exists(os.path.join(pipeline.output, callib_path)):
                raise RuntimeError("Cannot find cross_cal callib, check crosscal_callib parameter in config file !")

            # write calibration library txt file from json file to applycal
            caltablelist, gainfieldlist, interplist = [], [], []

            callib = 'caltables/callibs/{}.txt'.format(calprefix)

            with open(os.path.join(pipeline.output, callib_path)) as f:
                callib_dict = json.load(f)

            for applyme in callib_dict:
                caltablelist.append(callib_dict[applyme]['caltable'])
                gainfieldlist.append(callib_dict[applyme]['fldmap'])
                interplist.append(callib_dict[applyme]['interp'])

            with open(os.path.join(pipeline.output, callib), 'w') as stdw:
                for j in range(len(caltablelist)):
                    stdw.write('caltable="{0:s}/{1:s}/{2:s}"'.format(
                        stimela.recipe.CONT_IO["output"], 'caltables', caltablelist[j]))
                    stdw.write(' calwt=False')
                    stdw.write(' tinterp=\'' + str(interplist[j]) + '\'')
                    stdw.write(' finterp=\'linear\'')
                    stdw.write(' fldmap=\'' + str(gainfieldlist[j]) + '\'')
                    stdw.write(' spwmap=0\n')

            recipe.add("cab/casa_mstransform",
                       "apply 0",
                       {
                           "vis": inmsname,
                           "outputvis": msname,
                           "keepflags": True,
                           "docallib": True,
                           "callib": callib + ':output',
                           "field": "",
                           "uvrange": config['uvrange'],
                       },
                       input=pipeline.input, output=pipeline.output,
                       label="Apply crosscal")
            recipe.run()
            recipe.jobs = []

        else:
            msname = inmsname

        # Set -90 deg receptor angle rotation [if we are using MeerKAT data]
        if float(config['feed_angle_rotation']) != '':
            with tb("%s::FEED" % os.path.join(pipeline.msdir, msname), readonly=False) as t:
                ang = t.getcol("RECEPTOR_ANGLE")
                ang[:, 0] = numpy.deg2rad(float(config['feed_angle_rotation']))
                ang[:, 1] = numpy.deg2rad(float(config['feed_angle_rotation']))
                t.putcol("RECEPTOR_ANGLE", ang)
                caracal.log.info('RECEPTOR_ANGLE has been rotated by %s degrees' % config['feed_angle_rotation'])

        pol_calib = ",".join(getattr(pipeline, config["pol_calib"])[i])
        leakage_calib = ",".join(getattr(pipeline, config["leakage_calib"])[i])

        # choose the strategy according to config parameters
        if leakage_calib in set(unpolarized_calibrators):
            if pol_calib in set(polarized_calibrators):
                caracal.log.info(
                    "You decided to calibrate the polarized angle with a polarized calibrator assuming a model for the calibrator and the leakage with an unpolarized calibrator.")
                ben_cal(msname, msinfo, recipe, config, pipeline, i, prefix_msbase, polarized_calibrators)
            else:
                raise RuntimeError("Unknown pol_calib!"
                                   "Currently only these are known on caracal:\
                                   " + str(polarized_calibrators.keys()) + ". \
                                   You can use one of these source to calibrate polarization \
                                   or if none of them is available you can calibrate both leakage (leakage_calib) and polarization (pol_calib) \
                                   with a source observed at several parallactic angles")
        elif leakage_calib == pol_calib:
            caracal.log.info(
                "You decided to calibrate the polarized angle and leakage with a polarized calibrator without assuming a model for the calibrator.")
            idx = utils.get_field_id(msinfo, leakage_calib)[0]
            if len(msinfo['SCAN'][str(idx)]) >= 3:
                floi_calib(msname, msinfo, recipe, config, pipeline, i,
                           prefix_msbase)  # it would be useful to check at the beginning of the task whether the parallactic angle is well covered
                #       if pol_calib in set(polarized_calibrators):
                #       compare_with_model()
                if config['apply_pcal']:
                    for field in config["applyto"]:
                        applycal(msname, recipe, config, pipeline, i, prefix_msbase, field)
                else:
                    if msname == 'crosscalib_' + inmsname:
                        shutil.rmtree(os.path.join(pipeline.msdir, msname), ignore_errors=True)
                        shutil.rmtree(os.path.join(pipeline.msdir, msname) + '.flagversions', ignore_errors=True)
            else:
                raise RuntimeError(
                    "Cannot calibrate polarization! Insufficient number of scans for the leakage/pol calibrators.")
        else:
            raise RuntimeError("Cannot calibrate polarization! Allowed strategies are: \
                               1. Calibrate leakage with a unpolarized source (i.e. " + str(unpolarized_calibrators) + ") \
                               and polarized angle with a know polarized source (i.e. " + str(polarized_calibrators.keys()) + ") \
                               2. Calibrate both leakage and polarized angle with a (known or unknown) polarized source observed at different parallactic angles.")

        recipe.run()
        recipe.jobs = []

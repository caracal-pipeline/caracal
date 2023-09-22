# -*- coding: future_fstrings -*-
from collections import OrderedDict
import pickle
import sys
import os
import caracal.dispatch_crew.utils as utils
import caracal
import yaml
import stimela.dismissable as sdm
from caracal.workers.utils import manage_flagsets as manflags
from caracal.workers.utils import manage_antennas as manants
from caracal.workers.utils import callibs
import copy
import re
import json
import glob
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


def xcal_model_fcal_leak(msname, msinfo, prefix_msbase, recipe, config, pipeline, i, prefix, ref, polarized_calibrators, caltablelist,
                         gainfieldlist, interplist, calwtlist, applylist, leak_caltablelist,
                         leak_gainfieldlist, leak_interplist, leak_calwtlist, leak_applylist):
    field = ",".join(getattr(pipeline, config["pol_calib"])[i])
    leak_field = ",".join(getattr(pipeline, config["leakage_calib"])[i])

    freqsel = config.get("freqsel")
    gain_solint = config.get("gain_solint")
    time_solint = config.get("time_solint")

    gaintables = [prefix + '.Gpol1', prefix + '.Kcrs', prefix + '.Xf', prefix + '.Df']
    interps = ['linear', 'nearest', 'nearest', 'nearest']
    fields = [field, '', '', '']
    calwts = [True, False, False, False]
    applyfields = [field, '', '', '']
    gfields = [field, field, field, leak_field]
    terms = ['G', 'KCROSS', 'Xf', 'Df']

    if freqsel != '':
        gaintables = [prefix + '.Gpol1', prefix + '.Kcrs', prefix + '.Xref', prefix + '.Xf', prefix + '.Dref',
                      prefix + '.Df']
        interps = ['linear', 'nearest', 'nearest', 'nearest', 'nearest', 'nearest']
        fields = [field, '', '', '', '', '']
        calwts = [True, False, False, False, False, False]
        applyfields = [field, '', '', '', '', '']
        gfields = [field, field, field, field, leak_field, leak_field]
        terms = ['G', 'KCROSS', 'Xref', 'Xf', 'Dref', 'Df']

    docal = config['reuse_existing_tables']
    if docal:
        for cal in gaintables:
            if not os.path.exists(os.path.join(pipeline.caltables, cal)):
                caracal.log.info("No polcal table found in %s" % str(os.path.join(pipeline.caltables, cal)))
                docal = False

    if not docal:
        if pipeline.enable_task(config, 'set_model_leakage'):
            if config['set_model_leakage']['no_verify']:
                opts = {
                    "vis": msname,
                    "field": leak_field,
                    "scalebychan": True,
                    "usescratch": True,
                }
            else:
                modelsky = utils.find_in_native_calibrators(msinfo, leak_field, mode='sky')
                modelpoint = utils.find_in_native_calibrators(msinfo, leak_field, mode='mod')
                standard = utils.find_in_casa_calibrators(msinfo, leak_field)
                if config['set_model_leakage']['meerkat_skymodel'] and modelsky:
                    # use local sky model of calibrator field if exists
                    opts = {
                        "skymodel": modelsky,
                        "msname": msname,
                        "field-id": utils.get_field_id(msinfo, leak_field)[0],
                        "threads": config["set_model_leakage"]['threads'],
                        "mode": "simulate",
                        "tile-size": config["set_model_leakage"]["tile_size"],
                        "column": "MODEL_DATA",
                    }
                elif modelpoint:  # spectral model if specified in our standard
                    opts = {
                        "vis": msname,
                        "field": leak_field,
                        "standard": "manual",
                        "fluxdensity": modelpoint['I'],
                        "reffreq": '{0:f}GHz'.format(modelpoint['ref'] / 1e9),
                        "spix": [modelpoint[a] for a in 'abcd'],
                        "scalebychan": True,
                        "usescratch": True,
                    }
                elif standard:  # NRAO model otherwise
                    opts = {
                        "vis": msname,
                        "field": leak_field,
                        "standard": standard,
                        "usescratch": True,
                        "scalebychan": True,
                    }
                else:
                    raise RuntimeError('The flux calibrator field "{}" could not be '
                                       'found in our database or in the CASA NRAO database'.format(leak_field))
            step = 'set_model_cal-{0:d}'.format(i)
            cabtouse = 'cab/casa_setjy'
            recipe.add(cabtouse if "skymodel" not in opts else 'cab/simulator', step,
                       opts,
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Set jansky ms={1:s}'.format(step, msname))

        recipe.add("cab/casa_setjy", "set_model_%d" % 0,
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
                       "rotmeas": polarized_calibrators[field]["rotmeas"],
                   },
                   input=pipeline.input, output=pipeline.output,
                   label="set_model_%d" % 0)

        gain_opts = {
            "vis": msname,
            "caltable": prefix + '.Gpol1:output',
            "field": field,
            "uvrange": config["uvrange"],
            "refant": ref,
            "solint": gain_solint,
            "combine": "",
            "parang": True,
            "gaintype": "G",
            "calmode": "p",
            "spw": '',
        }
        if caltablelist:
            gain_opts.update({
                "gaintable": ["%s:output" % ct for ct in caltablelist],
                "gainfield": gainfieldlist,
                "interp": interplist,
            })

        # Phaseup diagonal of crosshand cal if available
        recipe.add("cab/casa_gaincal", "gain_xcal",
                   gain_opts,
                   input=pipeline.input, output=pipeline.caltables,
                   label="gain_xcal")

        tmp_gtab = caltablelist + [prefix + '.Gpol1']
        tmp_field = gainfieldlist + ['']
        tmp_interp = interplist + ['linear']
        recipe.add("cab/casa_gaincal", "crosshand_delay",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Kcrs:output',
                       "field": field,
                       "uvrange": config["uvrange"],
                       "refant": ref,
                       "solint": time_solint,
                       "combine": "",
                       "parang": True,
                       "gaintype": "KCROSS",
                       "spw": '',
                       "gaintable": ["%s:output" % ct for ct in tmp_gtab],
                       "gainfield": tmp_field,
                       "interp": tmp_interp,
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="crosshand_delay")

        # Solve for the absolute angle (phase) between the feeds
        # Solve first in a subband free of RFIs and then over the whole bw
        tmp_gtab = caltablelist + [prefix + '.Gpol1', prefix + '.Kcrs']
        tmp_field = gainfieldlist + ['', '']
        tmp_interp = interplist + ['linear', 'nearest']
        if freqsel != '':
            recipe.add("cab/casa_polcal", "crosshand_phase_ref",
                       {
                           "vis": msname,
                           "caltable": prefix + '.Xref:output',
                           "field": field,
                           "uvrange": config["uvrange"],
                           "solint": time_solint,
                           "combine": "",
                           "poltype": "Xf",
                           "refant": ref,
                           "spw": freqsel,
                           "gaintable": ["%s:output" % ct for ct in tmp_gtab],
                           "gainfield": tmp_field,
                           "interp": tmp_interp,
                       },
                       input=pipeline.input, output=pipeline.caltables,
                       label="crosshand_phase_ref")
            tmp_gtab = caltablelist + [prefix + '.Gpol1', prefix + '.Kcrs', prefix + '.Xref']
            tmp_field = gainfieldlist + ['', '', '']
            tmp_interp = interplist + ['linear', 'nearest', 'nearest']
        recipe.add("cab/casa_polcal", "crosshand_phase_freq",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Xf:output',
                       "field": field,
                       "uvrange": config["uvrange"],
                       "solint": time_solint,
                       "combine": "scan",
                       "poltype": "Xf",
                       "refant": ref,
                       "gaintable": ["%s:output" % ct for ct in tmp_gtab],
                       "gainfield": tmp_field,
                       "interp": tmp_interp,
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="crosshand_phase_freq")

        # Smooth the solution
        recipe.add("cab/casa_flagdata", "flag_phase",
                   {
                       "vis": prefix + '.Xf:msfile',
                       "mode": 'tfcrop',
                       "ntime": '60s',
                       "combinescans": True,
                       "datacolumn": 'CPARAM',
                       "usewindowstats": "both",
                       "flagbackup": False,
                   },
                   input=pipeline.input, output=pipeline.caltables, msdir=pipeline.caltables,
                   label="flag_phase_freq")

        # Solve for leakages (off-diagonal terms) using the unpolarized source
        # - first remove the DC of the frequency response and combine scans
        # if necessary to achieve desired SNR
        tmp_gtab = leak_caltablelist + [prefix + '.Kcrs', prefix + '.Xf']
        tmp_field = leak_gainfieldlist + ['', '']
        tmp_interp = leak_interplist + ['nearest', 'nearest']
        if freqsel != '':
            tmp_gtab = leak_caltablelist + [prefix + '.Kcrs', prefix + '.Xref', prefix + '.Xf']
            tmp_field = leak_gainfieldlist + ['', '', '']
            tmp_interp = leak_interplist + ['nearest', 'nearest', 'nearest']
            recipe.add("cab/casa_polcal", "leakage_ref",
                       {
                           "vis": msname,
                           "caltable": prefix + '.Dref:output',
                           "field": leak_field,
                           "uvrange": config["uvrange"],
                           "solint": time_solint,
                           "combine": "",
                           "poltype": "D",
                           "refant": ref,
                           "spw": freqsel,
                           "gaintable": ["%s:output" % ct for ct in tmp_gtab],
                           "gainfield": tmp_field,
                           "interp": tmp_interp,
                       },
                       input=pipeline.input, output=pipeline.caltables,
                       label="leakage_ref")
            tmp_gtab = leak_caltablelist + [prefix + '.Kcrs', prefix + '.Xref', prefix + '.Xf', prefix + '.Dref']
            tmp_field = leak_gainfieldlist + ['', '', '', '']
            tmp_interp = leak_interplist + ['nearest', 'nearest', 'nearest', 'nearest']
        recipe.add("cab/casa_polcal", "leakage_freq",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Df:output',
                       "field": leak_field,
                       "uvrange": config["uvrange"],
                       "solint": time_solint,
                       "combine": "scan",
                       "poltype": "Df",
                       "refant": ref,
                       "gaintable": ["%s:output" % ct for ct in tmp_gtab],
                       "gainfield": tmp_field,
                       "interp": tmp_interp,
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="leakage_freq")

        if config['plotgains']:
            plotdir = os.path.join(pipeline.diagnostic_plots, "polcal")
            if not os.path.exists(plotdir):
                os.mkdir(plotdir)
            plotgains(recipe, pipeline, plotdir, leak_field, prefix + '.Df', i, 'Df')
            recipe.run()
            recipe.jobs = []
            if os.path.exists(os.path.join(plotdir, prefix + '.Df.html')):
                os.rename(os.path.join(plotdir, prefix + '.Df.html'),
                          os.path.join(plotdir, prefix + '.Df_before_flag.html'))
            if os.path.exists(os.path.join(plotdir, prefix + '.Df.png')):
                os.rename(os.path.join(plotdir, prefix + '.Df.png'),
                          os.path.join(plotdir, prefix + '.Df_before_flag.png'))

        # Clip solutions
        recipe.add("cab/casa_flagdata", "flag_leakage",
                   {
                       "vis": prefix + '.Df:msfile',
                       "mode": 'clip',
                       "clipminmax": [-0.6, 0.6],
                       "datacolumn": 'CPARAM',
                       "flagbackup": False,
                   },
                   input=pipeline.input, output=pipeline.caltables, msdir=pipeline.caltables,
                   label="flag_leakage")

        recipe.run()
        recipe.jobs = []
    else:
        caracal.log.info("Reusing existing tables as requested")

    applycal_recipes = callibs.new_callib()
    for _gt, _fldmap, _interp, _calwt, _field in zip(gaintables, fields, interps, calwts, applyfields):
        callibs.add_callib_recipe(applycal_recipes, _gt, _interp, _fldmap, calwt=_calwt, field=_field)
    pipeline.save_callib(applycal_recipes, prefix)

    if config['plotgains']:
        plotdir = os.path.join(pipeline.diagnostic_plots, "polcal")
        if not os.path.exists(plotdir):
            os.mkdir(plotdir)
        for ix, gt in enumerate(gfields):
            plotgains(recipe, pipeline, plotdir, gfields[ix], gaintables[ix], i, terms[ix])

    if config['apply_pcal']:
        for ff in config["applyto"]:
            fld = ",".join(getattr(pipeline, ff)[i])
            _, (caltablelist, gainfieldlist, interplist, calwtlist, applylist) = \
                callibs.resolve_calibration_library(pipeline, prefix_msbase,
                                                    config['otfcal']['callib'],
                                                    config['otfcal']['label_cal'], [fld],
                                                    default_interpolation_types=config['otfcal']['interpolation'])

            _, (pcaltablelist, pgainfieldlist, pinterplist, pcalwtlist, papplylist) = \
                callibs.resolve_calibration_library(pipeline, prefix_msbase,
                                                    '',
                                                    config['label_cal'], [fld])
            pcal = caltablelist + pcaltablelist
            pgain = gainfieldlist + pgainfieldlist
            pinter = interplist + pinterplist
            pcalwt = calwtlist + pcalwtlist
            recipe.add("cab/casa_applycal", "apply_caltables_" + str(ff),
                       {
                           "vis": msname,
                           "field": fld,
                           "calwt": pcalwt,
                           "gaintable": ["%s:output" % ct for ct in pcal],
                           "gainfield": pgain,
                           "interp": pinter,
                           "parang": True,
            },
                input=pipeline.input, output=pipeline.caltables,
                label="Apply_caltables_" + str(ff))


def xcal_model_xcal_leak(msname, msinfo, prefix_msbase, recipe, config, pipeline, i, prefix, ref, polarized_calibrators, caltablelist,
                         gainfieldlist, interplist, calwtlist, applylist):
    field = ",".join(getattr(pipeline, config["pol_calib"])[i])
    scandur = scan_length(msinfo, field)

    freqsel = config.get("freqsel")
    gain_solint = config.get("gain_solint")
    time_solint = config.get("time_solint")

    gaintables = [prefix + '.Gpol1', prefix + '.Kcrs', prefix + '.Xf', prefix + '.Df0gen']
    interps = ['linear', 'nearest', 'nearest', 'nearest']
    fields = [field, '', '', '']
    calwts = [True, False, False, False]
    applyfields = [field, '', '', '']
    gfields = [field, field, field, field]
    terms = ['G', 'KCROSS', 'Xf', 'Df0gen']

    if freqsel != '':
        gaintables = [prefix + '.Gpol1', prefix + '.Kcrs', prefix + '.Xref', prefix + '.Xf', prefix + '.Df0gen']
        interps = ['linear', 'nearest', 'nearest', 'nearest', 'nearest']
        fields = [field, '', '', '', '']
        calwts = [True, False, False, False, False]
        applyfields = [field, '', '', '', '']
        gfields = [field, field, field, field, field]
        terms = ['G', 'KCROSS', 'Xref', 'Xf', 'Df0gen']

    docal = config['reuse_existing_tables']
    if docal:
        for cal in gaintables:
            if not os.path.exists(os.path.join(pipeline.caltables, cal)):
                caracal.log.info("No polcal table found in %s" % str(os.path.join(pipeline.caltables, cal)))
                docal = False

    if not docal:
        msdict = pipeline.get_msinfo(msname)
        chfr = msdict['SPW']['CHAN_FREQ']
        firstchanfreq = [ss[0] for ss in chfr]
        lastchanfreq = [ss[-1] for ss in chfr]
        meanchanfreq = (firstchanfreq[0] + lastchanfreq[0]) / 2.0 / 1.e9
        normfreq = (meanchanfreq / float(polarized_calibrators[field]["reffreq"][:-3]))
        spix = (polarized_calibrators[field]["spix"])
        index = 0
        polindex = (polarized_calibrators[field]["polindex"][0])
        polangle = (polarized_calibrators[field]["polangle"][0])
        for n in range(0, len(spix)):
            index += numpy.sum(spix[n] * pow(numpy.log(normfreq), n))
        c = numpy.sqrt(pow(numpy.tan(2 * polangle), 2) + 1)
        istokes = (polarized_calibrators[field]["fluxdensity"][0]) * pow(normfreq, index)
        qstokes = polindex * istokes / c
        ustokes = polindex * istokes * numpy.tan(2 * polangle) / c
        vstokes = 0
        S = [istokes, qstokes, ustokes, vstokes]

        recipe.add("cab/casa_setjy", "set_model_%d" % 0,
                   {
                       "msname": msname,
                       "usescratch": True,
                       "field": field,
                       "standard": polarized_calibrators[field]["standard"],
                       "fluxdensity": polarized_calibrators[field]["fluxdensity"],
                       "spix": spix,
                       "reffreq": polarized_calibrators[field]["reffreq"],
                       "polindex": polindex,
                       "polangle": polangle,
                   },
                   input=pipeline.input, output=pipeline.output,
                   label="set_model_%d" % 0)

        gain_opts = {
            "vis": msname,
            "caltable": prefix + '.Gpol1:output',
            "field": field,
            "uvrange": config["uvrange"],
            "refant": ref,
            "solint": gain_solint,
            "combine": "",
            "parang": True,
            "gaintype": "G",
            "calmode": "ap",
            "spw": '',
        }
        if caltablelist:
            gain_opts.update({
                "gaintable": ["%s:output" % ct for ct in caltablelist],
                "gainfield": gainfieldlist,
                "interp": interplist,
            })

        recipe.add("cab/casa_gaincal", "gain_xcal",
                   gain_opts,
                   input=pipeline.input, output=pipeline.caltables,
                   label="gain_xcal")

        tmp_gtab = caltablelist + [prefix + '.Gpol1']
        tmp_field = gainfieldlist + ['']
        tmp_interp = interplist + ['linear']
        recipe.add("cab/casa_gaincal", "crosshand_delay",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Kcrs:output',
                       "field": field,
                       "uvrange": config["uvrange"],
                       "refant": ref,
                       "solint": time_solint,
                       "combine": "",
                       "parang": True,
                       "gaintype": "KCROSS",
                       "spw": '',
                       "gaintable": ["%s:output" % ct for ct in tmp_gtab],
                       "gainfield": tmp_field,
                       "interp": tmp_interp,
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="crosshand_delay")

        tmp_gtab = caltablelist + [prefix + '.Gpol1', prefix + '.Kcrs']
        tmp_field = gainfieldlist + ['', '']
        tmp_interp = interplist + ['linear', 'nearest']

        if freqsel != '':
            recipe.add("cab/casa_polcal", "crosshand_phase_ref",
                       {
                           "vis": msname,
                           "caltable": prefix + '.Xref:output',
                           "field": field,
                           "uvrange": config["uvrange"],
                           "solint": time_solint,
                           "combine": "",
                           "poltype": "Xf",
                           "refant": ref,
                           "spw": freqsel,
                           "gaintable": ["%s:output" % ct for ct in tmp_gtab],
                           "gainfield": tmp_field,
                           "interp": tmp_interp,
                       },
                       input=pipeline.input, output=pipeline.caltables,
                       label="crosshand_phase_ref")

            tmp_gtab = caltablelist + [prefix + '.Gpol1', prefix + '.Kcrs', prefix + '.Xref']
            tmp_field = gainfieldlist + ['', '', '']
            tmp_interp = interplist + ['linear', 'nearest', 'nearest']

        recipe.add("cab/casa_polcal", "crosshand_phase_freq",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Xf:output',
                       "field": field,
                       "uvrange": config["uvrange"],
                       "solint": time_solint,
                       "combine": "scan",
                       "poltype": "Xf",
                       "refant": ref,
                       "gaintable": ["%s:output" % ct for ct in tmp_gtab],
                       "gainfield": tmp_field,
                       "interp": tmp_interp,
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="crosshand_phase_freq")

        recipe.add("cab/casa_flagdata", "flag_phase",
                   {
                       "vis": prefix + '.Xf:msfile',
                       "mode": 'tfcrop',
                       "ntime": '60s',
                       "combinescans": True,
                       "datacolumn": 'CPARAM',
                       "usewindowstats": "both",
                       "flagbackup": False,
                   },
                   input=pipeline.input, output=pipeline.caltables, msdir=pipeline.caltables,
                   label="flag_phase_freq")

        tmp_gtab = caltablelist + [prefix + '.Gpol1', prefix + '.Kcrs', prefix + '.Xf']
        tmp_field = gainfieldlist + ['', '', '']
        tmp_interp = interplist + ['linear', 'nearest', 'nearest']
        if freqsel != '':
            tmp_gtab = caltablelist + [prefix + '.Gpol1', prefix + '.Kcrs', prefix + '.Xref', prefix + '.Xf']
            tmp_field = gainfieldlist + ['', '', '', '']
            tmp_interp = interplist + ['linear', 'nearest', 'nearest', 'nearest']

        recipe.add("cab/casa_polcal", "leakage",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Df0gen:output',
                       "field": field,
                       "uvrange": config["uvrange"],
                       "solint": time_solint,
                       "spw": '',
                       "combine": 'obs,scan',
                       "preavg": scandur,
                       "poltype": 'Dflls',
                       "refant": '',
                       "smodel": S,
                       "gaintable": ["%s:output" % ct for ct in tmp_gtab],
                       "gainfield": tmp_field,
                       "interp": tmp_interp,
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="leakage")

        if config['plotgains']:
            plotdir = os.path.join(pipeline.diagnostic_plots, "polcal")
            if not os.path.exists(plotdir):
                os.mkdir(plotdir)
            plotgains(recipe, pipeline, plotdir, field, prefix + '.Df0gen', i, 'Df0gen')
            recipe.run()
            recipe.jobs = []
            if os.path.exists(os.path.join(plotdir, prefix + '.Df0gen.html')):
                os.rename(os.path.join(plotdir, prefix + '.Df0gen.html'),
                          os.path.join(plotdir, prefix + '.Df0gen_before_flag.html'))
            if os.path.exists(os.path.join(plotdir, prefix + '.Df0gen.png')):
                os.rename(os.path.join(plotdir, prefix + '.Df0gen.png'),
                          os.path.join(plotdir, prefix + '.Df0gen_before_flag.png'))

        # Clip solutions
        recipe.add("cab/casa_flagdata", "flag_leakage",
                   {
                       "vis": prefix + '.Df0gen:msfile',
                       "mode": 'clip',
                       "clipminmax": [-0.9, 0.9],
                       "datacolumn": 'CPARAM',
                       "flagbackup": False,
                   },
                   input=pipeline.input, output=pipeline.caltables, msdir=pipeline.caltables,
                   label="flag_leakage")
    else:
        caracal.log.info("Reusing existing tables as requested")

    applycal_recipes = callibs.new_callib()
    for _gt, _fldmap, _interp, _calwt, _field in zip(gaintables, fields, interps, calwts, applyfields):
        callibs.add_callib_recipe(applycal_recipes, _gt, _interp, _fldmap, calwt=_calwt, field=_field)
    pipeline.save_callib(applycal_recipes, prefix)

    if config['plotgains']:
        plotdir = os.path.join(pipeline.diagnostic_plots, "polcal")
        if not os.path.exists(plotdir):
            os.mkdir(plotdir)
        for ix, gt in enumerate(gfields):
            plotgains(recipe, pipeline, plotdir, gfields[ix], gaintables[ix], i, terms[ix])

    if config['apply_pcal']:
        for ff in config["applyto"]:
            fld = ",".join(getattr(pipeline, ff)[i])
            _, (caltablelist, gainfieldlist, interplist, calwtlist, applylist) = \
                callibs.resolve_calibration_library(pipeline, prefix_msbase,
                                                    config['otfcal']['callib'],
                                                    config['otfcal']['label_cal'], [fld],
                                                    default_interpolation_types=config['otfcal']['interpolation'])
            _, (pcaltablelist, pgainfieldlist, pinterplist, pcalwtlist, papplylist) = \
                callibs.resolve_calibration_library(pipeline, prefix_msbase,
                                                    '',
                                                    config['label_cal'], [fld])
            pcal = caltablelist + pcaltablelist
            pgain = gainfieldlist + pgainfieldlist
            pinter = interplist + pinterplist
            pcalwt = calwtlist + pcalwtlist
            recipe.add("cab/casa_applycal", "apply_caltables_" + str(ff),
                       {
                           "vis": msname,
                           "field": fld,
                           "calwt": pcalwt,
                           "gaintable": ["%s:output" % ct for ct in pcal],
                           "gainfield": pgain,
                           "interp": pinter,
                           "parang": True,
            },
                input=pipeline.input, output=pipeline.caltables,
                label="Apply_caltables_" + str(ff))


def xcal_from_pa_xcal_leak(msname, msinfo, prefix_msbase, recipe, config, pipeline, i, prefix, ref, caltablelist, gainfieldlist, interplist,
                           calwtlist, applylist):
    field = ",".join(getattr(pipeline, config["pol_calib"])[i])
    scandur = scan_length(msinfo, field)

    gain_solint = config.get("gain_solint")
    time_solint = config.get("time_solint")

    gaintables = [prefix + '.Gpol2', prefix + '.Gxyamp', prefix + '.Kcrs', prefix + '.Xfparang', prefix + '.Df0gen']
    interps = ['linear', 'linear', 'nearest', 'nearest', 'nearest']
    fields = ['', '', '', '', '']
    calwts = [True, True, False, False, False]
    applyfields = [field, ",".join(set(pipeline.fcal[i] + pipeline.bpcal[i] + pipeline.gcal[i] + pipeline.target[i])),
                   '', '', '']
    gfields = [field, field, field, field, field]
    terms = ['G', 'G', 'KCROSS', 'Xf', 'Df0gen']

    docal = config['reuse_existing_tables']
    if docal:
        for cal in gaintables:
            if not os.path.exists(os.path.join(pipeline.caltables, cal)):
                caracal.log.info("No polcal table found in %s" % str(os.path.join(pipeline.caltables, cal)))
                docal = False

    if not docal:
        gain_opts = {
            "vis": msname,
            "caltable": prefix + '.Gpol1:output',
            "field": field,
            "uvrange": config["uvrange"],
            "refant": ref,
            "solint": gain_solint,
            "combine": "",
            "parang": False,
            "gaintype": 'G',
            "calmode": 'ap',
            "spw": '',
            "refantmode": 'strict',
            "smodel": ['1', '0', '0', '0'],
        }
        if caltablelist:
            gain_opts.update({
                "gaintable": ["%s:output" % ct for ct in caltablelist],
                "gainfield": gainfieldlist,
                "interp": interplist,
            })

        recipe.add("cab/casa_gaincal", "gain_xcal_1",
                   gain_opts,
                   input=pipeline.input, output=pipeline.caltables,
                   label="gain_xcal_1")

        shutil.rmtree(os.path.join(pipeline.caltables, prefix + '.Gpol1a'), ignore_errors=True)

        # Extrapolate QU by fitting the gain at different PAs, save results in prefix + '_S1_from_QUfit:output'
        recipe.add("cab/casa_polfromgain",
                   "QU_from_gain",
                   {
                       "vis": msname,
                       "tablein": prefix + '.Gpol1:output',
                       "caltable": prefix + '.Gpol1a:output',
                       "save_result": prefix + '_S1_from_QUfit:output',
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="QU_from_gain")
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
        tmp_gtab = caltablelist + [prefix + '.Gpol1']
        tmp_field = gainfieldlist + ['']
        tmp_interp = interplist + ['linear']
        recipe.add("cab/casa_gaincal", "crosshand_delay",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Kcrs:output',
                       "field": field,
                       "uvrange": config["uvrange"],
                       "refant": ref,
                       "refantmode": 'strict',
                       "solint": time_solint,
                       "scan": str(bestscan),
                       "gaintype": 'KCROSS',
                       "smodel": ['1', '0', '1', '0'],
                       "selectdata": True,
                       "spw": '',
                       "gaintable": ["%s:output" % ct for ct in tmp_gtab],
                       "gainfield": tmp_field,
                       "interp": tmp_interp,
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="crosshand_delay")

        recipe.run()
        recipe.jobs = []

        # Read the smodel=[1,Q,U,0] of xcal from prefix + '_S1_from_QUfit'
        if os.path.isfile(pipeline.output + '/caltables/' + prefix + '_S1_from_QUfit'):
            with open(pipeline.output + '/caltables/' + prefix + '_S1_from_QUfit', 'rb') as stdr:
                S1 = pickle.load(stdr, encoding='latin1')

            S1 = S1[field]['SpwAve']
            caracal.log.info("First [I,Q,U,V] fitted model (with I=1 and Q, U fractional): %s" % S1)
        else:
            raise RuntimeError("Cannot find S1")

        # Calibrate the abs phase and a better smodel for xcal, saved in prefix + '_S2_from_polcal'
        tmp_gtab = caltablelist + [prefix + '.Gpol1', prefix + '.Kcrs']
        tmp_field = gainfieldlist + ['', '']
        tmp_interp = interplist + ['linear', 'nearest']
        recipe.add("cab/casa_polcal", "crosshand_phase_QU_fit",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Xfparang:output',
                       "field": field,
                       "uvrange": config["uvrange"],
                       "spw": '',
                       "poltype": 'Xfparang+QU',
                       "solint": time_solint,
                       "combine": 'scan,obs',
                       "preavg": scandur,
                       "smodel": S1,
                       "save_result": prefix + '_S2_from_polcal:output',
                       "gaintable": ["%s:output" % ct for ct in tmp_gtab],
                       "gainfield": tmp_field,
                       "interp": tmp_interp,
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="crosshand_phase_QU_fit")

        # Smooth the solutions
        recipe.add("cab/casa_flagdata", "flag_phase",
                   {
                       "vis": prefix + '.Xfparang:msfile',
                       "mode": 'tfcrop',
                       "ntime": '60s',
                       "combinescans": True,
                       "datacolumn": 'CPARAM',
                       "usewindowstats": "both",
                       "flagbackup": False,
                   },
                   input=pipeline.input, output=pipeline.caltables, msdir=pipeline.caltables,
                   label="flag_phase_freq")

        recipe.run()
        recipe.jobs = []

        # Read the new xcal smodel
        if os.path.isfile(pipeline.output + '/caltables/' + prefix + '_S2_from_polcal'):
            with open(pipeline.output + '/caltables/' + prefix + '_S2_from_polcal', 'rb') as stdr:
                S2 = pickle.load(stdr, encoding='latin1')
            S2 = S2[field]['SpwAve'].tolist()
            caracal.log.info("Second [I,Q,U,V] fitted model (with I=1 and Q, U fractional): %s" % S2)
        else:
            raise RuntimeError("Cannot find " + pipeline.output + "/caltables/" + prefix + "_S2_from_polcal")

        # Re-calibrate the gain amp and phase of xcal assuming the last smodel
        gain2_opts = {
            "vis": msname,
            "caltable": prefix + '.Gpol2:output',
            "field": field,
            "uvrange": config["uvrange"],
            "refant": ref,
            "solint": gain_solint,
            "combine": "",
            "parang": True,
            "gaintype": 'G',
            "calmode": 'ap',
            "spw": '',
            "refantmode": 'strict',
            "smodel": S2,
        }
        if caltablelist:
            gain2_opts.update({
                "gaintable": ["%s:output" % ct for ct in caltablelist],
                "gainfield": gainfieldlist,
                "interp": interplist,
            })
        recipe.add("cab/casa_gaincal", "gain_xcal_2",
                   gain2_opts,
                   input=pipeline.input, output=pipeline.caltables,
                   label="gain_xcal_2")

        # LEAKAGE
        tmp_gtab = caltablelist + [prefix + '.Gpol2', prefix + '.Kcrs', prefix + '.Xfparang']
        tmp_field = gainfieldlist + ['', '', '']
        tmp_interp = interplist + ['linear', 'nearest', 'nearest']
        recipe.add("cab/casa_polcal", "leakage",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Df0gen:output',
                       "field": field,
                       "uvrange": config["uvrange"],
                       "solint": time_solint,
                       "spw": '',
                       "combine": 'obs,scan',
                       "preavg": scandur,
                       "poltype": 'Dflls',
                       "refant": '',
                       "smodel": S2,
                       "gaintable": ["%s:output" % ct for ct in tmp_gtab],
                       "gainfield": tmp_field,
                       "interp": tmp_interp,
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="leakage")

        if config['plotgains']:
            plotdir = os.path.join(pipeline.diagnostic_plots, "polcal")
            if not os.path.exists(plotdir):
                os.mkdir(plotdir)
            plotgains(recipe, pipeline, plotdir, field, prefix + '.Df0gen', i, 'Df0gen')
            recipe.run()
            recipe.jobs = []
            if os.path.exists(os.path.join(plotdir, prefix + '.Df0gen.html')):
                os.rename(os.path.join(plotdir, prefix + '.Df0gen.html'),
                          os.path.join(plotdir, prefix + '.Df0gen_before_flag.html'))
            if os.path.exists(os.path.join(plotdir, prefix + '.Df0gen.png')):
                os.rename(os.path.join(plotdir, prefix + '.Df0gen.png'),
                          os.path.join(plotdir, prefix + '.Df0gen_before_flag.png'))

        # Clip solutions
        recipe.add("cab/casa_flagdata", "flag_leakage",
                   {
                       "vis": prefix + '.Df0gen:msfile',
                       "mode": 'clip',
                       "clipminmax": [-0.9, 0.9],
                       "datacolumn": 'CPARAM',
                       "flagbackup": False,
                   },
                   input=pipeline.input, output=pipeline.caltables, msdir=pipeline.caltables,
                   label="flag_leakage")

        # solve for global normalized gain amp (to get X/Y ratios) on xcal (TO APPLY ON TARGET)
        # amp-only and normalized, so only X/Y amp ratios matter
        tmp_gtab = caltablelist + [prefix + '.Kcrs', prefix + '.Xfparang', prefix + '.Df0gen']
        tmp_field = gainfieldlist + ['', '', '']
        tmp_interp = interplist + ['nearest', 'nearest', 'nearest']
        recipe.add("cab/casa_gaincal", "norm_gain_for_target",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Gxyamp:output',
                       "field": field,
                       "uvrange": config["uvrange"],
                       "refant": ref,
                       "refantmode": 'strict',
                       "solint": 'inf',
                       "combine": 'scan,obs',
                       "gaintype": 'G',
                       "smodel": S2,
                       "calmode": 'a',
                       "gaintable": ["%s:output" % ct for ct in tmp_gtab],
                       "gainfield": tmp_field,
                       "interp": tmp_interp,
                       "solnorm": True,
                       "parang": True,
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="norm_gain_for_target")
        recipe.run()
        recipe.jobs = []

    else:
        caracal.log.info("Reusing existing tables as requested")

    applycal_recipes = callibs.new_callib()
    for _gt, _fldmap, _interp, _calwt, _field in zip(gaintables, fields, interps, calwts, applyfields):
        callibs.add_callib_recipe(applycal_recipes, _gt, _interp, _fldmap, calwt=_calwt, field=_field)
    pipeline.save_callib(applycal_recipes, prefix)

    if config['plotgains']:
        plotdir = os.path.join(pipeline.diagnostic_plots, "polcal")
        if not os.path.exists(plotdir):
            os.mkdir(plotdir)
        for ix, gt in enumerate(gfields):
            plotgains(recipe, pipeline, plotdir, gfields[ix], gaintables[ix], i, terms[ix])

    if config['apply_pcal']:
        for ff in config["applyto"]:
            fld = ",".join(getattr(pipeline, ff)[i])
            _, (caltablelist, gainfieldlist, interplist, calwtlist, applylist) = \
                callibs.resolve_calibration_library(pipeline, prefix_msbase,
                                                    config['otfcal']['callib'],
                                                    config['otfcal']['label_cal'], [fld],
                                                    default_interpolation_types=config['otfcal']['interpolation'])
            _, (pcaltablelist, pgainfieldlist, pinterplist, pcalwtlist, papplylist) = \
                callibs.resolve_calibration_library(pipeline, prefix_msbase,
                                                    '',
                                                    config['label_cal'], [fld])
            pcal = caltablelist + pcaltablelist
            pgain = gainfieldlist + pgainfieldlist
            pinter = interplist + pinterplist
            pcalwt = calwtlist + pcalwtlist
            recipe.add("cab/casa_applycal", "apply_caltables_" + str(ff),
                       {
                           "vis": msname,
                           "field": fld,
                           "calwt": pcalwt,
                           "gaintable": ["%s:output" % ct for ct in pcal],
                           "gainfield": pgain,
                           "interp": pinter,
                           "parang": True,
            },
                input=pipeline.input, output=pipeline.caltables,
                label="Apply_caltables_" + str(ff))


def calib_only_leakage(msname, msinfo, prefix_msbase, recipe, config, pipeline, i,
                       prefix, ref, leak_caltablelist, leak_gainfieldlist, leak_interplist, leak_calwtlist, leak_applylist):
    leak_field = ",".join(getattr(pipeline, config["leakage_calib"])[i])

    time_solint = config.get("time_solint")

    gaintables = [prefix + '.Df']
    interps = ['nearest']
    fields = ['']
    calwts = [False]
    applyfields = ['']
    gfields = [leak_field]
    terms = ['Df']

    docal = config['reuse_existing_tables']
    if docal:
        for cal in gaintables:
            if not os.path.exists(os.path.join(pipeline.caltables, cal)):
                caracal.log.info("No polcal table found in %s" % str(os.path.join(pipeline.caltables, cal)))
                docal = False

    if not docal:
        if pipeline.enable_task(config, 'set_model_leakage'):
            if config['set_model_leakage']['no_verify']:
                opts = {
                    "vis": msname,
                    "field": leak_field,
                    "scalebychan": True,
                    "usescratch": True,
                }
            else:
                modelsky = utils.find_in_native_calibrators(msinfo, leak_field, mode='sky')
                modelpoint = utils.find_in_native_calibrators(msinfo, leak_field, mode='mod')
                standard = utils.find_in_casa_calibrators(msinfo, leak_field)
                if config['set_model_leakage']['meerkat_skymodel'] and modelsky:
                    # use local sky model of calibrator field if exists
                    opts = {
                        "skymodel": modelsky,
                        "msname": msname,
                        "field-id": utils.get_field_id(msinfo, leak_field)[0],
                        "threads": config["set_model_leakage"]['threads'],
                        "mode": "simulate",
                        "tile-size": config["set_model_leakage"]["tile_size"],
                        "column": "MODEL_DATA",
                    }
                elif modelpoint:  # spectral model if specified in our standard
                    opts = {
                        "vis": msname,
                        "field": leak_field,
                        "standard": "manual",
                        "fluxdensity": modelpoint['I'],
                        "reffreq": '{0:f}GHz'.format(modelpoint['ref'] / 1e9),
                        "spix": [modelpoint[a] for a in 'abcd'],
                        "scalebychan": True,
                        "usescratch": True,
                    }
                elif standard:  # NRAO model otherwise
                    opts = {
                        "vis": msname,
                        "field": leak_field,
                        "standard": standard,
                        "usescratch": True,
                        "scalebychan": True,
                    }
                else:
                    raise RuntimeError('The flux calibrator field "{}" could not be '
                                       'found in our database or in the CASA NRAO database'.format(leak_field))
            step = 'set_model_cal-{0:d}'.format(i)
            cabtouse = 'cab/casa_setjy'
            recipe.add(cabtouse if "skymodel" not in opts else 'cab/simulator', step,
                       opts,
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Set jansky ms={1:s}'.format(step, msname))

        recipe.add("cab/casa_polcal", "leakage_freq",
                   {
                       "vis": msname,
                       "caltable": prefix + '.Df:output',
                       "field": leak_field,
                       "uvrange": config["uvrange"],
                       "solint": time_solint,
                       "combine": "scan",
                       "poltype": "Df",
                       "refant": ref,
                       "gaintable": ["%s:output" % ct for ct in leak_caltablelist],
                       "gainfield": leak_gainfieldlist,
                       "interp": leak_interplist,
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="leakage_freq")

        if config['plotgains']:
            plotdir = os.path.join(pipeline.diagnostic_plots, "polcal")
            if not os.path.exists(plotdir):
                os.mkdir(plotdir)
            plotgains(recipe, pipeline, plotdir, leak_field, prefix + '.Df', i, 'Df')
            recipe.run()
            recipe.jobs = []
            if os.path.exists(os.path.join(plotdir, prefix + '.Df.html')):
                os.rename(os.path.join(plotdir, prefix + '.Df.html'),
                          os.path.join(plotdir, prefix + '.Df_before_flag.html'))
            if os.path.exists(os.path.join(plotdir, prefix + '.Df.png')):
                os.rename(os.path.join(plotdir, prefix + '.Df.png'),
                          os.path.join(plotdir, prefix + '.Df_before_flag.png'))

        # Clip solutions
        recipe.add("cab/casa_flagdata", "flag_leakage",
                   {
                       "vis": prefix + '.Df:msfile',
                       "mode": 'clip',
                       "clipminmax": [-0.6, 0.6],
                       "datacolumn": 'CPARAM',
                       "flagbackup": False,
                   },
                   input=pipeline.input, output=pipeline.caltables, msdir=pipeline.caltables,
                   label="flag_leakage")

        recipe.run()
        recipe.jobs = []
    else:
        caracal.log.info("Reusing existing tables as requested")

    applycal_recipes = callibs.new_callib()
    for _gt, _fldmap, _interp, _calwt, _field in zip(gaintables, fields, interps, calwts, applyfields):
        callibs.add_callib_recipe(applycal_recipes, _gt, _interp, _fldmap, calwt=_calwt, field=_field)
    pipeline.save_callib(applycal_recipes, prefix)

    if config['plotgains']:
        plotdir = os.path.join(pipeline.diagnostic_plots, "polcal")
        if not os.path.exists(plotdir):
            os.mkdir(plotdir)
        for ix, gt in enumerate(gfields):
            plotgains(recipe, pipeline, plotdir, gfields[ix], gaintables[ix], i, terms[ix])

    if config['apply_pcal']:
        for ff in config["applyto"]:
            fld = ",".join(getattr(pipeline, ff)[i])
            _, (caltablelist, gainfieldlist, interplist, calwtlist, applylist) = \
                callibs.resolve_calibration_library(pipeline, prefix_msbase,
                                                    config['otfcal']['callib'],
                                                    config['otfcal']['label_cal'], [fld])
            _, (pcaltablelist, pgainfieldlist, pinterplist, pcalwtlist, papplylist) = \
                callibs.resolve_calibration_library(pipeline, prefix_msbase,
                                                    '',
                                                    config['label_cal'], [fld])
            pcal = caltablelist + pcaltablelist
            pgain = gainfieldlist + pgainfieldlist
            pinter = interplist + pinterplist
            pcalwt = calwtlist + pcalwtlist
            recipe.add("cab/casa_applycal", "apply_caltables_" + str(ff),
                       {
                           "vis": msname,
                           "field": fld,
                           "calwt": pcalwt,
                           "gaintable": ["%s:output" % ct for ct in pcal],
                           "gainfield": pgain,
                           "interp": pinter,
                           "parang": True,
            },
                input=pipeline.input, output=pipeline.caltables,
                label="Apply_caltables_" + str(ff))


def plotgains(recipe, pipeline, plotdir, field_id, gtab, i, term):
    step = "plotgains-%s-%d-%s" % (term, i, gtab)
    opts = {
        "table": gtab + ":msfile",
        "corr": '',
        "htmlname": gtab,
        "field": field_id,
    }
    if term in ['Xf', 'Df0gen', 'Dffls']:
        opts.update({
            "xaxis": "channel",
        })
    elif term == 'Dref':
        opts.update({
            "xaxis": "antenna1",
        })
    recipe.add('cab/ragavi', step, opts,
               input=pipeline.input, msdir=pipeline.caltables, output=plotdir,
               label='{0:s}:: Plot gaincal phase'.format(step))


def worker(pipeline, recipe, config):
    wname = pipeline.CURRENT_WORKER
    flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
    flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
    label = config["label_cal"]
    label_in = config["label_in"]

    # define pol and unpol calibrators, P&B2017 + updated pol properties from NRAO web site (https://science.nrao.edu/facilities/vla/docs/manuals/obsguide/modes/pol, Table 7.2.7)
    polarized_calibrators = {"3C138": {"standard": "manual",
                                       "fluxdensity": [8.33843],
                                       "spix": [-0.4981, -0.1552, -0.0102, 0.0223],
                                       "reffreq": "1.47GHz",
                                       "polindex": [0.078],
                                       "polangle": [-0.16755],
                                       "rotmeas": 0.0},
                             "3C286": {"standard": "manual",
                                       "fluxdensity": [14.7172],
                                       "spix": [-0.4507, -0.1798, 0.0357],
                                       "reffreq": "1.47GHz",
                                       "polindex": [0.098],
                                       "polangle": [0.575959],
                                       "rotmeas": 0.0},
                             "J1130-1449": {"standard": "manual",
                                            "fluxdensity": [4.940],
                                            "spix": 0,
                                            "reffreq": "1.35GHz",
                                            "polindex": [0.03],
                                            "polangle": [-0.202893],
                                            "rotmeas": 33},
                             }
    polarized_calibrators["J1331+3030"] = polarized_calibrators["3C286"]
    polarized_calibrators["J0521+1638"] = polarized_calibrators["3C138"]
    unpolarized_calibrators = ["PKS1934-63", "J1939-6342", "J1938-6341", "PKS 1934-638", "PKS 1934-63", "PKS1934-638",
                               "PKS0408-65", "J0408-6545", "J0408-6544", "PKS 0408-65", "0407-658", "0408-658", "PKS 0408-658", "0408-65"]

    # loop over all MSs for this label
    for i, (msbase, prefix_msbase) in enumerate(zip(pipeline.msbasenames, pipeline.prefix_msbases)):
        msname = pipeline.form_msname(msbase, label_in)
        msinfo = pipeline.get_msinfo(msname)
        prefix = f"{pipeline.prefix_msbases[i]}-{label}"

        fields = []
        if pipeline.refant[i] in ['auto']:
            refant = manants.get_refant(pipeline, recipe,
                                        prefix, msname, fields,
                                        pipeline.minbase[i],
                                        pipeline.maxdist[i], i)
            if refant:
                caracal.log.info(f"Auto selected ref antenna(s): refant")
            else:
                caracal.log.error("Cannot auto-select ref antenna(s). Set it manually.")

        else:
            refant = pipeline.refant[i]

        # Check if feeds are linear
        if set(list(msinfo['CORR']['CORR_TYPE'])) & {'XX', 'XY', 'YX', 'YY'} == 0:
            raise RuntimeError(
                "Cannot calibrate polarization! Allowed strategies are for linear feed data but correlation is: " + str(
                    [
                        'XX', 'XY', 'YX', 'YY']))

        if config["pol_calib"] != 'none':
            pol_calib = ",".join(getattr(pipeline, config["pol_calib"])[i])
            if pol_calib == 'J1130-1449':
                caracal.log.info("CARACal knows only bandwidth averaged properties of J1130-1449 based on https://archive-gw-1.kat.ac.za/public/meerkat/MeerKAT-L-band-Polarimetric-Calibration.pdf")
        else:
            pol_calib = 'none'
        leakage_calib = ",".join(getattr(pipeline, config["leakage_calib"])[i])

        # check if cross_callib needs to be applied
        if config['otfcal']:
            if pol_calib != 'none':
                _, (caltablelist, gainfieldlist, interplist, calwtlist, applylist) = \
                    callibs.resolve_calibration_library(pipeline, prefix_msbase,
                                                        config['otfcal']['callib'],
                                                        config['otfcal']['label_cal'], [pol_calib],
                                                        default_interpolation_types=config['otfcal']['interpolation'])
            _, (leak_caltablelist, leak_gainfieldlist, leak_interplist, leak_calwtlist, leak_applylist) = \
                callibs.resolve_calibration_library(pipeline, prefix_msbase,
                                                    config['otfcal']['callib'],
                                                    config['otfcal']['label_cal'], [leakage_calib],
                                                    default_interpolation_types=config['otfcal']['interpolation'])
        else:
            _, (caltablelist, gainfieldlist, interplist, calwtlist, applylist) = \
                None, ([],) * 5
            _, (leak_caltablelist, leak_gainfieldlist, leak_interplist, leak_calwtlist, leak_applylist) = \
                None, ([],) * 5

        # Set -90 deg receptor angle rotation [if we are using MeerKAT data]
        if float(config['feed_angle_rotation']) != '':
            with tb("%s::FEED" % os.path.join(pipeline.msdir, msname), readonly=False) as t:
                ang = t.getcol("RECEPTOR_ANGLE")
                ang[:, 0] = numpy.deg2rad(float(config['feed_angle_rotation']))
                ang[:, 1] = numpy.deg2rad(float(config['feed_angle_rotation']))
                t.putcol("RECEPTOR_ANGLE", ang)
                caracal.log.info('RECEPTOR_ANGLE has been rotated by %s degrees' % config['feed_angle_rotation'])

        # save flags before and after
        if {"xcal", "gcal", "fcal", "target"}.intersection(config["applyto"]):
            # Write/rewind flag versions
            available_flagversions = manflags.get_flags(pipeline, msname)
            if config['rewind_flags']['enable']:
                if config['rewind_flags']['mode'] == 'reset_worker':
                    version = flags_before_worker
                    stop_if_missing = False
                elif config['rewind_flags']['mode'] == 'rewind_to_version':
                    version = config['rewind_flags']['version']
                    if version == 'auto':
                        version = flags_before_worker
                    stop_if_missing = True
                if version in available_flagversions:
                    if flags_before_worker in available_flagversions and available_flagversions.index(
                        flags_before_worker) < available_flagversions.index(version) and not config[
                            'overwrite_flagvers']:
                        manflags.conflict('rewind_too_little', pipeline, wname, msname, config, flags_before_worker,
                                          flags_after_worker)
                    substep = 'version-{0:s}-ms{1:d}'.format(version, i)
                    manflags.restore_cflags(pipeline, recipe, version, msname, cab_name=substep)
                    if version != available_flagversions[-1]:
                        substep = 'delete-flag_versions-after-{0:s}-ms{1:d}'.format(version, i)
                        manflags.delete_cflags(pipeline, recipe,
                                               available_flagversions[available_flagversions.index(version) + 1],
                                               msname, cab_name=substep)
                    if version != flags_before_worker:
                        substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                        manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                            msname, cab_name=substep, overwrite=config['overwrite_flagvers'])
                elif stop_if_missing:
                    manflags.conflict('rewind_to_non_existing', pipeline, wname, msname, config, flags_before_worker,
                                      flags_after_worker)
                else:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                        msname, cab_name=substep, overwrite=config['overwrite_flagvers'])
            else:
                if flags_before_worker in available_flagversions and not config['overwrite_flagvers']:
                    manflags.conflict('would_overwrite_bw', pipeline, wname, msname, config, flags_before_worker,
                                      flags_after_worker)
                else:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                        msname, cab_name=substep, overwrite=config['overwrite_flagvers'])

        # preliminary flags
        if config['extendflags'] and pol_calib != 'none':
            recipe.add("cab/casa_flagdata",
                       "extend_flags_polcal",
                       {
                           "vis": msname,
                           "mode": 'extend',
                           "field": pol_calib,
                           "ntime": '60s',
                           "combinescans": True,
                           "growtime": 80.0,
                           "growfreq": 80.0,
                           "growaround": True,
                           "flagnearfreq": True,
                           "flagneartime": True,
                           "flagbackup": False,
                       },
                       input=pipeline.input, output=pipeline.output,
                       label="extend_flags_polcal")
            if pol_calib != leakage_calib:
                recipe.add("cab/casa_flagdata",
                           "extend_flags_polcal",
                           {
                               "vis": msname,
                               "mode": 'extend',
                               "field": leakage_calib,
                               "ntime": '60s',
                               "combinescans": True,
                               "growtime": 80.0,
                               "growfreq": 80.0,
                               "growaround": True,
                               "flagnearfreq": True,
                               "flagneartime": True,
                               "flagbackup": False,
                           },
                           input=pipeline.input, output=pipeline.output,
                           label="extend_flags_polcal")

        # choose the strategy according to config parameters
        if leakage_calib in unpolarized_calibrators:
            if pol_calib in polarized_calibrators:
                caracal.log.info(
                    "You decided to calibrate the polarized angle with a polarized calibrator assuming a model for the calibrator and the leakage with an unpolarized calibrator.")
                xcal_model_fcal_leak(msname, msinfo, prefix_msbase, recipe, config, pipeline, i, prefix, refant, polarized_calibrators,
                                     caltablelist, gainfieldlist, interplist, calwtlist, applylist,
                                     leak_caltablelist, leak_gainfieldlist, leak_interplist, leak_calwtlist, leak_applylist)
            elif pol_calib == 'none':
                caracal.log.info(
                    "You decided to calibrate only the leakage with an unpolarized calibrator. This is experimental.")
                calib_only_leakage(msname, msinfo, prefix_msbase, recipe, config, pipeline, i,
                                   prefix, refant, leak_caltablelist, leak_gainfieldlist, leak_interplist, leak_calwtlist, leak_applylist)
            else:
                raise RuntimeError(f"Unable to determine pol_calib={config['pol_calib']}. Is your obsconf section configured properly?"
                                   f"""Your setting of pol_calib={config['pol_calib']} selects {pol_calib}.
                                    Supported calibrators are {', '.join(polarized_calibrators.keys())}.
                                    Alternatively, you can calibrate both leakage and polarization using a (known or unknown) polarized source
                    observed at several parallactic angles. Configure this source as obsconf:xcal, and leakage_calib=pol_calib=xcal.""")
        elif leakage_calib == pol_calib:
            caracal.log.info(
                "You decided to calibrate the polarized angle and leakage with a polarized calibrator.")
            idx = utils.get_field_id(msinfo, leakage_calib)[0]
            if config['set_model_pol']:
                caracal.log.info("Using a known model for the polarized calibrator.")
                xcal_model_xcal_leak(msname, msinfo, prefix_msbase, recipe, config, pipeline, i,
                                     prefix, refant, polarized_calibrators, caltablelist, gainfieldlist, interplist,
                                     calwtlist, applylist)
            else:
                if len(msinfo['SCAN'][str(idx)]) >= 3:
                    caracal.log.info("The model for the polarized calibrator will be derived from data.")
                    xcal_from_pa_xcal_leak(msname, msinfo, prefix_msbase, recipe, config, pipeline, i,
                                           prefix, refant, caltablelist, gainfieldlist, interplist, calwtlist, applylist)
                else:
                    raise RuntimeError(
                        "Cannot calibrate polarization! Insufficient number of scans for the pol calibrator.")
        else:
            raise RuntimeError(f"""Unable to determine a polarization calibration strategy. Supported strategies are:
                    1. Calibrate leakage using an unpolarized source ({', '.join(unpolarized_calibrators)}), and
                       polarization angle using a known polarized source ({', '.join(polarized_calibrators.keys())}).
                       This is usually achieved by setting leakage_cal=bpcal, pol_cal=xcal.
                    2. Calibrate both leakage and polarized angle with a (known or unknown) polarized source observed at
                       different parallactic angles. This is usually achieved by setting leakage_cal=xcal, pol_cal=xcal.
                       If the polarized source is unknown at least three scans are required.""")

        if pipeline.enable_task(config, 'summary') and pol_calib != 'none':
            step = 'summary-{0:s}-{1:d}'.format(label, i)
            recipe.add('cab/casa_flagdata', step,
                       {
                           "vis": msname,
                           "mode": 'summary',
                           "field": pol_calib,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))

        recipe.run()
        recipe.jobs = []

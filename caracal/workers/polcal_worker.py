# -*- coding: future_fstrings -*-
# import stimela
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


def ben_cal(msname, msinfo, recipe, config, pipeline, i, prefix):
    caltable = "%s_%s_%s" % (prefix, config['pol_calib'], 'ben_cal')
    ref = pipeline.refant[i] or '0'
    print("TBD")


def floi_calib(msname, msinfo, recipe, config, pipeline, i, prefix):
    caltable = "%s_%s_%s" % (prefix, config['pol_calib'], 'floi_cal')
    field = ",".join(getattr(pipeline, config["pol_calib"])[i])
    ref = pipeline.refant[i] or '0'
    avgstring = ',' + config["avg_bw"]
    scandur = scan_length(msinfo, field)
    caracal.log.info("What am I doing?")

    if not config['reuse_existing_tables']:
        # G1
        recipe.add("cab/casa_gaincal",
                   "first gaincal",
                   {
                       "vis": msname,
                       "field": field,
                       "caltable": caltable + '.Gpol1:output',
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

        shutil.rmtree(os.path.join(pipeline.caltables,caltable + '.Gpol1a'), ignore_errors=True)
        # QU
        recipe.add("cab/casa_polfromgain",
                   "QU from gain",
                   {
                       "vis": msname,
                       "tablein": caltable + '.Gpol1:output',
                       "caltable": caltable + '.Gpol1a:output',
                       "save_result": caltable + '_S1_from_QUfit:output',
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="QU from gain")
        recipe.run()
        recipe.jobs = []
        ##################################################
        # We search for the scan where the polarization signal is minimum in XX and YY
        # (i.e., maximum in XY and YX):
        # tb.open(caltable + '.Gpol1:output')
        # scans = tb.getcol('SCAN_NUMBER')
        # gains = numpy.squeeze(tb.getcol('CPARAM'))
        # tb.close()
        # scanlist = numpy.array(list(set(scans)))
        # ratios = numpy.zeros(len(scanlist))
        # for si, s in enumerate(scanlist):
        #     filt = scans == s
        #     ratio = numpy.sqrt(numpy.average(numpy.power(numpy.abs(gains[0, filt]) / numpy.abs(gains[1, filt]) - 1.0, 2.)))
        #     ratios[si] = ratio
        #
        # bestscidx = numpy.argmin(ratios)
        # bestscan = scanlist[bestscidx]
        # print('Scan with highest expected X-Y signal: ' + str(bestscan))
        # #####################################################
        # recipe.run()
        # recipe.jobs = []
        bestscan = '29'

        # Kcross
        recipe.add("cab/casa_gaincal",
                   "Kcross delay",
                   {
                       "vis": msname,
                       "caltable": caltable + '.Kcrs:output',
                       "selectdata": True,
                       "field": field,
                       "scan": str(bestscan),
                       "gaintype": 'KCROSS',
                       "solint": 'inf' + avgstring,
                       "refantmode": 'strict',
                       "refant": ref,
                       "smodel": ['1', '0', '1', '0'],
                       "gaintable": [caltable + '.Gpol1:output'],
                       "interp": ['linear'],
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="Kcross delay")

        recipe.run()
        recipe.jobs = []

        if os.path.isfile(pipeline.output + '/caltables/' + caltable + '_S1_from_QUfit'):
            with open(pipeline.output + '/caltables/' + caltable + '_S1_from_QUfit', 'rb') as stdr:
                S1 = pickle.load(stdr, encoding='latin1')

            S1 = S1[field]['SpwAve']
            caracal.log.info("First [I,Q,U,V] fitted model (with I=1 and Q, U fractional): %s" % S1)
        else:
            raise RuntimeError("Cannot find S1")  # caltable+'S1_from_QUfit:output'

        # QU abs delay
        recipe.add("cab/casa_polcal",
                   "Abs phase and QU fit",
                   {
                       "vis": msname,
                       "caltable": caltable + '.Xfparang:output',
                       "field": field,
                       "spw": '',
                       "poltype": 'Xfparang+QU',
                       "solint": 'inf' + avgstring,
                       "combine": 'scan,obs',
                       "preavg": scandur,
                       "smodel": S1,
                       "gaintable": [caltable + '.Gpol1:output', caltable + '.Kcrs:output'],
                       "interp": ['linear', 'nearest'],
                       "save_result": caltable + '_S2_from_polcal:output',
                   },
                   input=pipeline.input, output=pipeline.caltables,
                   label="Abs phase and QU fit")

        recipe.run()
        recipe.jobs = []

        if os.path.isfile(pipeline.output + '/caltables/' + caltable + '_S2_from_polcal'):
            with open(pipeline.output + '/caltables/' + caltable + '_S2_from_polcal', 'rb') as stdr:
                S2 = pickle.load(stdr, encoding='latin1')
            S2 = S2[field]['SpwAve'].tolist()
            caracal.log.info("Second [I,Q,U,V] fitted model (with I=1 and Q, U fractional): %s" % S2)
        else:
            raise RuntimeError("Cannot find " + pipeline.output + "/caltables/" + caltable + "_S2_from_polcal")

        recipe.add("cab/casa_gaincal",
                   "second gaincal",
                   {
                       "vis": msname,
                       "field": field,
                       "caltable": caltable + '.Gpol2:output',
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

        # LEAKAGE
        recipe.add("cab/casa_polcal",
                   "leakage terms",
                   {
                       "vis": msname,
                       "caltable": caltable + '.Df0gen:output',
                       "field": field,
                       "spw": '',
                       "solint": 'inf' + avgstring,
                       "combine": 'obs,scan',
                       "preavg": scandur,
                       "poltype": 'Dflls',
                       "refant": '',  # solve absolute D-term
                       "smodel": S2,
                       "gaintable": [caltable + '.Gpol2:output', caltable + '.Kcrs:output',
                                     caltable + '.Xfparang:output'],
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
                       "caltable": caltable + '.Gxyamp:output',
                       "field": field,
                       "solint": 'inf',
                       "combine": 'scan,obs',
                       "refant": ref,
                       "refantmode": 'strict',
                       "gaintype": 'G',
                       "smodel": S2,
                       "calmode": 'a',
                       "gaintable": [caltable + '.Kcrs:output', caltable + '.Xfparang:output',
                                     caltable + '.Df0gen:output'],
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
        gaintables = [caltable + '.Gpol2', caltable + '.Kcrs', caltable + '.Xfparang', caltable + '.Df0gen']
        interps = ['linear', 'nearest', 'nearest', 'nearest']
        fields = ['', '', '', '']

        for ix,gt in enumerate(gaintables):
            applycal_recipes.append(dict(zip(
                ['caltable', 'fldmap', 'interp'], [gt, fields[ix], interps[ix]])))
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
        gaintables = [caltable + '.Gxyamp', caltable + '.Kcrs', caltable + '.Xfparang', caltable + '.Df0gen']
        interps = ['linear', 'nearest', 'nearest', 'nearest']
        fields = ['', '', '', '']

        for ix,gt in enumerate(gaintables):
            applycal_recipes.append(dict(zip(
                ['caltable', 'fldmap', 'interp'], [gt, fields[ix], interps[ix]])))
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

        with open(os.path.join(callib_dir, f'callib_{prefix}_target.json'), 'w') as json_file:
            json.dump(callib_dict, json_file)
            
    else:
        caracal.log.info("Reusing existing tables as requested")
        

def make_plots(msname, recipe, config, pipeline, i, prefix):
    caltable = "%s_%s_%s" % (prefix, config['pol_calib'], 'floi_cal')
    if not os.path.exists(os.path.join(pipeline.diagnostic_plots, "polcal")):
        os.mkdir(os.path.join(pipeline.diagnostic_plots, "polcal"))
    plotdir = os.path.join(pipeline.diagnostic_plots, "polcal")
    plotname = "%s_%s_%s" % (prefix, config['pol_calib'], 'floi_cal')
    field = ",".join(getattr(pipeline, config["pol_calib"])[i])
    ant = config['refant_for_plots']
    recipe.add("cab/casa_plotms",
               "plot_firstGpol",
               {
                   "vis": caltable + '.Gpol1:msfile',
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
               input=pipeline.input, output=plotdir,
               label="plot before Kcrs")

    recipe.add("cab/casa_applycal",
               "apply 1",
               {
                   "vis": msname,
                   "field": field,
                   "calwt": True,
                   "gaintable": [caltable + '.Gpol1:output', caltable + '.Kcrs:output'],
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

    recipe.add("cab/casa_plotms",
               "plot_Xf",
               {
                   "vis": caltable + '.Xfparang:msfile',
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
                   "gaintable": [caltable + '.Gpol1:output', caltable + '.Kcrs:output', caltable + '.Xfparang:output'],
                   "interp": ['linear', 'nearest', 'nearest'],
                   "parang": False,
               },
               input=pipeline.input, output=pipeline.caltables,
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

    recipe.add("cab/casa_plotms",
               "plot_Gpol2",
               {
                   "vis": caltable + '.Gpol2:msfile',
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


def applycal(msname, recipe, config, pipeline, i, prefix, field):
    caltable = "%s_%s_%s" % (prefix, config['pol_calib'], 'floi_cal')
    gaintables = [caltable + '.Gxyamp', caltable + '.Gpol2', caltable + '.Kcrs', caltable + '.Xfparang', caltable + '.Df0gen']
    for cal in gaintables:
        if not os.path.exists(os.path.join(pipeline.caltables,cal)):
            raise RuntimeError("Reuse_existing_tables is set to %s but %s doesn't exist! Re-run with reuse_existing_tables: False"%(config["reuse_existing_tables"],os.path.join(pipeline.caltables,cal)))

    if field == ",".join(getattr(pipeline, config["pol_calib"])[i]):
        recipe.add("cab/casa_applycal", "apply_caltables", {
            "vis": msname,
            "field": ",".join(getattr(pipeline, field)[i]),
            "gaintable": [caltable + '.Gpol2:output', caltable + '.Kcrs:output', caltable + '.Xfparang:output',
                          caltable + '.Df0gen:output'],
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
            "field": ",".join(getattr(pipeline, field)[i]),
            "gaintable": [caltable + '.Gxyamp:output', caltable + '.Kcrs:output', caltable + '.Xfparang:output',
                          caltable + '.Df0gen:output'],
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
    flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
    flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
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
        #prefix_cc = f"{pipeline.prefix_msbases[i]}-{config['label_crosscal_in']}"

        # Check if feeds are linear
        if set(list(msinfo['CORR']['CORR_TYPE'])) & {'XX', 'XY', 'YX', 'YY'} == 0:
            raise RuntimeError(
                "Cannot calibrate polarization! Allowed strategies are for linear feed data but correlation is: " + str(
                    [
                        'XX', 'XY', 'YX', 'YY']))

        # Set -90 deg receptor angle rotation [if we are using MeerKAT data]
        

        # prepare data (APPLY KGB AND SPLIT a NEW MSDIR)
        if config['label_crosscal_in'] != '':
            callib_path = os.path.join("{}/callibs".format(pipeline.caltables), config['label_crosscal_in'])
            callib = 'caltables/callibs/{}'.format(config['label_crosscal_in'])
            msname = 'cross_cal_' + inmsname

            if not os.path.exists(callib_path):
                raise RuntimeError("Cannot find cross_cal callib, check label_crosscal_in parameter in config file !")

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

        pol_calib = ",".join(getattr(pipeline, config["pol_calib"])[i])
        leakage_calib = ",".join(getattr(pipeline, config["leakage_calib"])[i])

        # choose the strategy according to config parameters
        if leakage_calib in set(unpolarized_calibrators):
            if pol_calib in set(polarized_calibrators):
                caracal.log.info("You decided to calibrate the polarized angle with a polarized calibrator assuming a model for the calibrator \
                      and the leakage with an unpolarized calibrator.")
                ben_cal(msname, msinfo, recipe, config, pipeline, i, prefix_msbase)
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
                if config['make_checking_plots']:
                    make_plots(msname, recipe, config, pipeline, i, prefix_msbase)
                #       if pol_calib in set(polarized_calibrators):
                #       compare_with_model()
                if config['apply_pcal']:
                    for field in config['apply_pcal']['applyto']:
                        applycal(msname, recipe, config, pipeline, i, prefix_msbase, field)
            else:
                raise RuntimeError(
                    "Cannot calibrate polarization! Insufficient number of scans for the leakage/pol calibrators.")
        else:
            raise RuntimeError("Cannot calibrate polarization! Allowed strategies are: \
                               1. Calibrate leakage with a unpolarized source (i.e. " + str(unpolarized_calibrators) + ") \
                               and polarized angle with a know polarized source (i.e. " + str(
                polarized_calibrators.keys()) + ") \
                               2. Calibrate both leakage and polarized angle with a (known or unknown) polarized source observed at different parallactic angles.")

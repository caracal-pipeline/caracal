# -*- coding: future_fstrings -*-
from collections import OrderedDict
import sys
import os
import caracal.dispatch_crew.utils as utils
import caracal
import stimela.dismissable as sdm
from caracal.workers.utils import manage_flagsets as manflags
from caracal.workers.utils import manage_caltabs as manGtabs
from caracal.workers.utils import manage_antennas as manants
from caracal.workers.utils import callibs
import copy
import re
import json
import glob
import shutil
import numpy as np
from casacore.tables import table
from caracal.utils.requires import extras

NAME = "Cross-calibration"
LABEL = 'crosscal'


def check_config(config, name):
    for primsec in "primary", "secondary":
        order = config[primsec]["order"]
        # check that all order steps are legal
        invalid = [x for x in order if x not in RULES]
        if invalid:
            raise caracal.ConfigurationError(f"{name}: {primsec}: order: invalid steps {','.join(invalid)}")
        # check that numbers match
        for other in "calmode", "solint", "combine":
            if len(config[primsec][other]) != len(order):
                raise caracal.ConfigurationError(
                    f"{name}: {primsec}: {other}: expected {len(order)} elements, found {len(config[primsec][other])}")


# E.g. to split out continuum/<dir> from output/continuum/dir
def get_dir_path(string, pipeline):
    return string.split(pipeline.output)[1][1:]


FLAG_NAMES = [""]


def exists(outdir, path):
    _path = os.path.join(outdir, path)
    return os.path.exists(_path)


# Rules for interpolation mode to use when applying calibration solutions
RULES = {
    "K": {
        "name": "delay_cal",
        "interp": "linear",
        "cab": "cab/casa_gaincal",
        "gaintype": "K",
        "field": "bpcal",
    },
    "G": {
        "name": "gain_cal",
        "interp": "linear",
        "cab": "cab/casa_gaincal",
        "gaintype": "G",
        "mode": "ap",
        "field": "gcal",
    },
    "F": {
        "name": "gaincal_for_Ftable",
        "interp": "linear",
        "cab": "cab/casa_gaincal",
        "gaintype": "F",
        "mode": "ap",
        "field": "gcal",
    },
    "B": {
        "name": "bp_cal",
        "interp": "linear",
        "cab": "cab/casa_bandpass",
        "field": "bpcal",
    },
    "A": {
        "name": "auto_flagging",
        "cab": "cab/casa_flagdata",
        "mode": "tfcrop",
    },
    "I": {
        "name": "image",
        "cab": "cab/wsclean",
    },
    "S": {
        "name": "slope_freq_delay",
        "cab": "cab/casa_fringefit",
    },
}

CALS = {
    "primary": "fcal",
    "secondary": "gcal",
    "bandpass_cal": "bpcal",
}


def first_if_single(items, i):
    try:
        return items[i]
    except IndexError:
        return items[0]


def get_last_gain(gaintables, my_term="dummy"):
    if isinstance(my_term, str):
        my_term = [my_term]
    if gaintables:
        gtype = [tab[-2] for tab in gaintables]
        gtype.reverse()
        last_indices = []
        N = len(gtype)
        for term in set(gtype):
            idx = N - 1 - gtype.index(term)
            if gtype[gtype.index(term)] not in my_term:
                last_indices.append(idx)
        return last_indices
    else:
        return []


def solve(msname, msinfo, recipe, config, pipeline, iobs, prefix, label, ftype,
          append_last_secondary=None, prev=None, prev_name=None, smodel=False):
    """
    """
    gaintables = []
    interps = []
    fields = []
    iters = {}

    if prev and prev_name:
        for item in config[ftype]['apply']:
            gaintables.append("%s_%s.%s%d" % (prefix, prev_name, item, prev["iters"][item]))
            ft = RULES[item]["field"]
            fields.append(",".join(getattr(pipeline, ft)[iobs]))
            interps.append(RULES[item]["interp"])

    field = getattr(pipeline, CALS[ftype])[iobs]
    order = config[ftype]["order"]
    field_id = utils.get_field_id(msinfo, field)

    def do_KGBF(i):
        gtable_ = None
        ftable_ = None
        interp = RULES[term]["interp"]
        if pipeline.refant[iobs] in ['auto']:
            params["refant"] = manants.get_refant(pipeline, recipe,
                                                  prefix, msname, fields,
                                                  pipeline.minbase[iobs],
                                                  pipeline.maxdist[iobs], i)
            if params["refant"]:
                caracal.log.info(f"Auto selected ref antenna(s): {params['refant']}")
            else:
                caracal.log.error("Cannot auto-select ref antenna(s). Set it manually.")

        else:
            params["refant"] = pipeline.refant[iobs]
        params["solint"] = first_if_single(config[ftype]["solint"], i)
        params["combine"] = first_if_single(config[ftype]["combine"], i).strip("'")
        params["field"] = ",".join(field)
        caltable = "%s_%s.%s%d" % (prefix, ftype, term, itern)
        params["caltable"] = caltable + ":output"
        my_term = term
        did_I = 'I' in order[:i + 1]
        if not did_I and smodel and term in "KGF":
            params["smodel"] = ["1", "0", "0", "0"]
        # allow selection of band subset(s) for gaincal see #1204 on github issue tracker
        if term in "GF":
            params["spw"] = config[ftype]["spw_g"]
            params["scan"] = config[ftype]["scanselection"]
        elif term == "K":
            params["spw"] = config[ftype]["spw_k"]
            params["scan"] = config[ftype]["scanselection"]
        if term == "B":
            params["bandtype"] = term
            params["solnorm"] = config[ftype]["b_solnorm"]
            params["fillgaps"] = config[ftype]["b_fillgaps"]
            params["uvrange"] = config["uvrange"]
            params["scan"] = config[ftype]["scanselection"]
        elif term == "K":
            params["gaintype"] = term
            params["scan"] = config[ftype]["scanselection"]
        elif term in "FG":
            my_term = ["F", "G"]
            if term == "F":
                # Never append to the original. Make a copy for each F that is needed
                caltable_original = "%s_%s.G%d" % (prefix, prev_name, prev["iters"]["G"])
                primary_G = "%s_%s_append-%d.G%d" % (prefix, prev_name, itern, prev["iters"]["G"])
                caltable_path_original = os.path.join(pipeline.caltables, caltable_original)
                caltable_path = os.path.join(pipeline.caltables, primary_G)
                params["append"] = True
                caltable = "%s_%s.F%d" % (prefix, ftype, itern)
                params["caltable"] = primary_G + ":output"
            else:
                params["scan"] = config[ftype]["scanselection"]
            params["gaintype"] = "G"
            params["uvrange"] = config["uvrange"]
            params["calmode"] = first_if_single(config[ftype]["calmode"], i).strip("'")

        otf_apply = get_last_gain(gaintables, my_term=my_term)
        if otf_apply:
            params["gaintable"] = [gaintables[count] + ":output" for count in otf_apply]
            params["interp"] = [interps[count] for count in otf_apply]
            params["gainfield"] = [fields[count] for count in otf_apply]

        can_reuse = False
        if config[ftype]["reuse_existing_gains"] and exists(pipeline.caltables, caltable):
            # check if field is in gain table
            fields_in_tab = set(table(os.path.join(pipeline.caltables, caltable), ack=False).getcol("FIELD_ID"))
            if fields_in_tab.issubset(field_id):
                can_reuse = True

        if can_reuse:
            caracal.log.info("Reusing existing gain table '%s' as requested" % caltable)
        else:
            if term == "F":
                if os.path.exists(caltable_path):
                    shutil.rmtree(caltable_path)
                cpstep = "copy_primary_gains_%s-%s-%d-%d-%s" % (name, label, itern, iobs, ftype)
                recipe.add(shutil.copytree, cpstep, {
                    "src": caltable_path_original,
                    "dst": caltable_path,
                }, label="{0}:: Copy parimary gains".format(step))
            recipe.add(RULES[term]["cab"], step,
                       copy.deepcopy(params),
                       input=pipeline.input, output=pipeline.caltables,
                       label="%s:: %s calibration" % (step, term))
            if term == "F":
                transfer_fluxscale(msname, recipe, primary_G + ":output", caltable + ":output", pipeline,
                                   iobs, reference=pipeline.fluxscale_reference, label=label)
            elif term == "B" and config[ftype]["b_smoothwindow"] > 1:
                recipe.add(smooth_bandpass, 'smooth_bandpass', {
                    "bptable": '{0:s}/{1:s}'.format(pipeline.caltables, caltable),
                    "window": config[ftype]["b_smoothwindow"],
                },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='smooth bandpass')

        # Assume gains were plotted when they were created
        if config[ftype]["plotgains"] and not can_reuse:
            plotgains(recipe, pipeline, field_id if term != "F" else None, caltable, iobs, term=term)

        fields.append(",".join(field))
        interps.append(interp)
        gaintables.append(caltable)

    def do_IA(i):
        if i == 0:
            raise RuntimeError("Have encountered an imaging/flagging request before any gains have been computed."
                               "an I only makes sense after a G or K (usually both)."
                               "Please review your 'order' option in the self_cal:secondary section")

        if not applied:
            applycal(latest_KGBF_group, msname, recipe, gaintables,
                     interps, fields, CALS[ftype], pipeline, iobs,
                     calmode="calflag")
        else:
            caracal.log.info(
                "Gains have already been applied using this exact set of gain tables and fields. Skipping unnecessary applycal step")

        if term == "A":
            if not set("KGBF").intersection(order[:i]):
                raise RuntimeError(
                    "Have encountered a request to flag the secondary calibrator without any gain, bandpass or delay tables to apply first.")
            step = "%s-%s-%d-%d-%s" % (name, label, itern, iobs, ftype)
            params["mode"] = RULES[term]["mode"]
            params["field"] = ",".join(field)
            params["datacolumn"] = config[ftype]["flag"]["col"]
            params["usewindowstats"] = config[ftype]["flag"]["usewindowstats"]
            params["combinescans"] = config[ftype]["flag"]["combinescans"]
            params["flagdimension"] = config[ftype]["flag"]["flagdimension"]
            params["flagbackup"] = False
            params["timecutoff"] = config[ftype]["flag"]["timecutoff"]
            params["freqcutoff"] = config[ftype]["flag"]["freqcutoff"]
            params["correlation"] = config[ftype]["flag"]["correlation"]
            recipe.add(RULES[term]["cab"], step,
                       copy.deepcopy(params),
                       input=pipeline.input, output=pipeline.output,
                       label="%s::" % step)

        else:
            for fid in field_id:
                step = "%s-%s-%d-%d-%s-field%d" % (name, label, itern, iobs, ftype, fid)
                calimage = "%s-%s-I%d-%d-field%d:output" % (prefix, ftype, itern, iobs, fid)
                cab_params = {
                    "msname": msname,
                    "name": calimage,
                    "size": config[ftype]["image"]['npix'],
                    "scale": config[ftype]["image"]['cell'],
                    "join-channels": False if config[ftype]["image"]["nchans"] == 1 else True,
                    "fit-spectral-pol": config[ftype]["image"]["fit_spectral_pol"],
                    "channels-out": config[ftype]["image"]['nchans'],
                    "auto-threshold": config[ftype]["image"]['auto_threshold'],
                    "local-rms-window": config[ftype]["image"]['rms_window'],
                    "local-rms": config[ftype]["image"]['local_rms'],
                    "padding": config[ftype]["image"]['padding'],
                    "niter": config[ftype]["image"]['niter'],
                    "weight": config[ftype]["image"]["weight"],
                    "mgain": config[ftype]["image"]['mgain'],
                    "field": fid}
                if config[ftype]["image"]['external_fits_masks']:
                    mask_file = ''
                    for mask in config[ftype]["image"]['external_fits_masks']:
                        if str(fid) in [mask.split('-')[-1]]:
                            mask_file = f"{mask}.fits"
                    if mask_file:
                        cab_params.update({"fits-mask": mask_file})
                    else:
                        cab_params.update({"auto-mask": config[ftype]["image"]['auto_mask']})
                else:
                    cab_params.update({"auto-mask": config[ftype]["image"]['auto_mask']})
                recipe.add(RULES[term]["cab"], step,
                           cab_params,
                           input=pipeline.input, output=pipeline.crosscal_continuum,
                           label="%s:: Image %s field" % (step, ftype))

    nterms = len(order)

    # terms that need an apply
    groups_apply = list(filter(lambda g: g, re.findall("([AI]+)?", order)))
    # terms that need a solve
    groups_solve = list(filter(lambda g: g, re.findall("([KGBF]+)?", order)))
    # Order has to start with solve group.
    # TODO(sphe) in the philosophy of giving user enough roap to hang themselves
    # Release II will allow both starting with I/A in case
    # someone wants to apply primary gains to the secondary
    n_apply = len(groups_apply)
    n_solve = len(groups_solve)
    groups = [None] * (n_apply + n_solve)
    groups[::2] = groups_solve  # even indices
    groups[1::2] = groups_apply  # odd indices

    # no need to apply gains multiple when encountering consecutive terms that need to apply
    applied = False
    i = -1  #
    for jj, group in enumerate(groups):
        for g, term in enumerate(group):
            i += 1
            # if this is not the case, then something has gone horribly wrong
            assert term == order[i]
            if (jj % 2) == 0:  # even counter is solve group
                even = True
                latest_KGBF_group = group
            else:
                latest_IA_group = group
                even = False
                if g == 0:
                    applied = False
                else:
                    applied = True

            name = RULES[term]["name"]
            if term in iters:
                iters[term] += 1
            else:
                iters[term] = 0

            itern = iters[term]
            params = {}
            params["vis"] = msname

            step = "%s-%s-%d-%d-%s" % (name, label, itern, iobs, ftype)

            if even:
                do_KGBF(i)
            else:
                do_IA(i)

    return {
        "gaintables": gaintables,
        "interps": interps,
        "iters": iters,
        "gainfield": fields,
    }


def plotgains(recipe, pipeline, field_id, gtab, i, term):
    step = "plotgains-%s-%d-%s" % (term, i, "".join(map(str, field_id or [])))
    params = {
        "table": f"{gtab}:msfile",
        "corr": '',
        "htmlname": gtab,
        "plotname": "{}.png".format(gtab)
    }
    if field_id is not None:
        params['field'] = ",".join(map(str, field_id))
    recipe.add('cab/ragavi', step, params,
               input=pipeline.input,
               msdir=pipeline.caltables,
               output=os.path.join(pipeline.diagnostic_plots, "crosscal"),
               label='{0:s}:: Plot gaincal phase'.format(step))


def transfer_fluxscale(msname, recipe, gaintable, fluxtable, pipeline, i, reference, label=""):
    """
    Transfer fluxscale
    """
    step = "transfer_fluxscale-%s-%d" % (label, i)
    recipe.add("cab/casa_fluxscale", step, {
        "vis": msname,
        "caltable": gaintable,
        "fluxtable": fluxtable,
        "reference": reference,
        "transfer": "",
    },
        input=pipeline.input, output=pipeline.caltables,
        label="Transfer fluxscale")


def get_caltab_final(order, gaintable, interp, gainfield, field):
    rorder = list(reversed(order))
    if "G" in order:
        gi = rorder.index("G")
    else:
        gi = np.inf

    if "F" in order:
        fi = rorder.index("F")
    else:
        fi = np.inf

    # if both are not there (or = inf), then it does not matter
    if fi == gi:  # ooh, very naughty
        lidx = get_last_gain(gaintable)
    elif gi < fi:
        lidx = get_last_gain(gaintable, my_term="F")
    else:
        lidx = get_last_gain(gaintable, my_term="G")

    gaintables = []
    interps = []
    fields = []
    for idx in lidx:
        gaintables.append(gaintable[idx])
        interps.append(interp[idx])
        if isinstance(gainfield, str):
            fields.append(gainfield)
        else:
            fields.append(gainfield[idx])

    return gaintables, interps, fields


def applycal(order, msname, recipe, gaintable, interp, gainfield, field, pipeline, i,
             calmode="calflag", label=""):
    """
    Apply gains
    -----------------

    Parameters:
      order: order in which to apply gains
    """

    gaintables, interps, fields = get_caltab_final(order, gaintable, interp,
                                                   gainfield, field)

    step = "apply_gains-%s-%s-%d" % (field, label, i)
    recipe.add("cab/casa_applycal", step, {
        "vis": msname,
        "field": ",".join(getattr(pipeline, field)[i]),
        "applymode": calmode,
        "gaintable": [tab + ":output" for tab in gaintables],
        "interp": interps,
        "calwt": [False],
        "gainfield": fields,
        "parang": False,
        "flagbackup": False,
    },
        input=pipeline.input, output=pipeline.caltables,
        label="%s::Apply gain tables" % step)


@extras("scipy")
def smooth_bandpass(bptable, window, filter_type='mean'):
    from scipy import ndimage

    caracal.log.info('Smoothing {0:s} with {2:s} window of width {1:d} channels'.format(bptable, window, filter_type))
    bp = table(bptable, ack=False).getcol('CPARAM')
    bp = [np.real(bp), np.imag(bp)]
    if filter_type == 'median':
        bp = [ndimage.median_filter(bb, size=(1, window, 1)) for bb in bp]
    elif filter_type == 'mean':
        bp = [ndimage.uniform_filter(bb, size=(1, window, 1)) for bb in bp]
    table(bptable, ack=False, readonly=False).putcol('CPARAM', bp[0] + 1j * bp[1])


def worker(pipeline, recipe, config):
    wname = pipeline.CURRENT_WORKER
    flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
    flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
    label = config["label_cal"]
    label_in = config["label_in"]

    # loop over all MSs for this label
    for i, msbase in enumerate(pipeline.msbasenames):
        msname = pipeline.form_msname(msbase, label_in)
        msinfo = pipeline.get_msinfo(msname)
        prefix_msbase = f"{pipeline.prefix_msbases[i]}-{label}"

        if {"gcal", "fcal", "target"}.intersection(config["apply_cal"]["applyto"]):
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

        if len(pipeline.fcal[i]) > 1:
            fluxscale_field = utils.observed_longest(msinfo, pipeline.fcal[i])
            fluxscale_field_id = utils.get_field_id(msinfo, fluxscale_field)[0]
            caracal.log.info("Found more than one flux calibrator."
                             f"Will use the one observed the longest {fluxscale_field}.")
        else:
            fluxscale_field = pipeline.fcal[i][0]
            fluxscale_field_id = utils.get_field_id(msinfo, fluxscale_field)[0]

        pipeline.fluxscale_reference = fluxscale_field

        if pipeline.enable_task(config, 'set_model'):
            if config['set_model']['no_verify']:
                opts = {
                    "vis": msname,
                    "field": fluxscale_field,
                    "scalebychan": True,
                    "usescratch": True,
                }
            else:
                modelsky = utils.find_in_native_calibrators(msinfo, fluxscale_field, mode='sky')
                modelcrystal = utils.find_in_native_calibrators(msinfo, fluxscale_field, mode='crystal')
                modelpoint = utils.find_in_native_calibrators(msinfo, fluxscale_field, mode='mod')
                standard = utils.find_in_casa_calibrators(msinfo, fluxscale_field)
                if config['set_model']['meerkat_skymodel'] and modelsky:
                    # use local sky model of calibrator field if exists
                    opts = {
                        "skymodel": modelsky,
                        "msname": msname,
                        "field-id": utils.get_field_id(msinfo, fluxscale_field)[0],
                        "threads": config["set_model"]['threads'],
                        "mode": "simulate",
                        "tile-size": config["set_model"]["tile_size"],
                        "column": "MODEL_DATA",
                    }
                elif config['set_model']['meerkat_crystalball_skymodel'] and modelcrystal:  # Use Ben's crystalball models
                    opts = {
                        "ms": msname,
                        "sky-model": modelcrystal,
                        "field": fluxscale_field,
                        "memory-fraction": sdm.dismissable(config['set_model']["meerkat_crystalball_memory_fraction"]),
                        "num-workers": sdm.dismissable(config['set_model']['meerkat_crystalball_ncpu']),
                        "row-chunks": sdm.dismissable(config['set_model']["meerkat_crystalball_row_chunks"]),
                        "model-chunks": sdm.dismissable(config['set_model']["meerkat_crystalball_model_chunks"]),
                        "num-sources": sdm.dismissable(config['set_model']['meerkat_crystalball_num_sources']),
                    }
                elif modelpoint:  # spectral model if specified in our standard
                    opts = {
                        "vis": msname,
                        "field": fluxscale_field,
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
                        "field": fluxscale_field,
                        "standard": standard,
                        "usescratch": True,
                        "scalebychan": True,
                    }
                else:

                    raise RuntimeError('The flux calibrator field "{}" could not be '
                                       'found in our database or in the CASA NRAO database'.format(fluxscale_field))
            step = 'set_model_cal-{0:d}'.format(i)
            if "skymodel" in opts:
                cabtouse = 'cab/simulator'
            elif "sky-model" in opts:
                cabtouse = 'cab/crystalball'
            else:
                cabtouse = 'cab/casa_setjy'
            recipe.add(cabtouse, step,
                       opts,
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Set jansky ms={1:s}'.format(step, msname))

        gcal_set = set(pipeline.gcal[i])
        fcal_set = set(pipeline.fcal[i])
        calmode = config["apply_cal"]["calmode"]
        primary_order = config["primary"]["order"]
        secondary_order = config["secondary"]["order"]
        no_secondary = gcal_set == set() or len(gcal_set - fcal_set) == 0
        if no_secondary:
            primary_order = config["primary"]["order"]
            primary = solve(msname, msinfo, recipe, config, pipeline, i,
                            prefix_msbase, label=label, ftype="primary")
            caracal.log.info("Secondary calibrator is the same as the primary. Skipping fluxscale")
            interps = primary["interps"]
            gainfields = primary["gainfield"]
            gaintables = primary["gaintables"]

            if "bpcal" in config["apply_cal"]["applyto"] or "gcal" in config["apply_cal"]["applyto"]:
                applycal(primary_order, msname, recipe, copy.deepcopy(gaintables), copy.deepcopy(interps),
                         "nearest", "bpcal", pipeline, i, calmode=calmode, label=label)
            if "xcal" in config["apply_cal"]["applyto"]:
                applycal(primary_order, msname, recipe, copy.deepcopy(gaintables), copy.deepcopy(interps),
                         "nearest", "xcal", pipeline, i, calmode=calmode, label=label)
            if "target" in config["apply_cal"]["applyto"]:
                applycal(primary_order, msname, recipe, copy.deepcopy(gaintables), copy.deepcopy(interps),
                         "nearest", "target", pipeline, i, calmode=calmode, label=label)
        else:
            primary = solve(msname, msinfo, recipe, config, pipeline, i,
                            prefix_msbase, label=label, ftype="primary")

            secondary = solve(msname, msinfo, recipe, config, pipeline, i,
                              prefix_msbase, label=label, ftype="secondary",
                              prev=primary, prev_name="primary", smodel=True)

            interps = primary["interps"]
            gaintables = primary["gaintables"]

            if "bpcal" in config["apply_cal"]["applyto"]:
                applycal(primary_order, msname, recipe, copy.deepcopy(gaintables), copy.deepcopy(interps),
                         "nearest", "bpcal", pipeline, i, calmode=calmode, label=label)

            interps = secondary["interps"]
            gainfields = secondary["gainfield"]
            gaintables = secondary["gaintables"]

            if "gcal" in config["apply_cal"]["applyto"]:
                applycal(secondary_order, msname, recipe, copy.deepcopy(gaintables), interps,
                         gainfields, "gcal", pipeline, i, calmode=calmode, label=label)
            if "xcal" in config["apply_cal"]["applyto"]:
                applycal(secondary_order, msname, recipe, copy.deepcopy(gaintables), interps,
                         "nearest", "xcal", pipeline, i, calmode=calmode, label=label)
            if "target" in config["apply_cal"]["applyto"]:
                applycal(secondary_order, msname, recipe, copy.deepcopy(gaintables), interps,
                         "nearest", "target", pipeline, i, calmode=calmode, label=label)

        if {"gcal", "fcal", "target"}.intersection(config["apply_cal"]["applyto"]):
            substep = 'save-{0:s}-ms{1:d}'.format(flags_after_worker, i)
            manflags.add_cflags(pipeline, recipe, flags_after_worker, msname, cab_name=substep,
                                overwrite=config['overwrite_flagvers'])

        applycal_recipes = callibs.new_callib()
        # the fluxscale_field has already been chosen, so using "nearest" here does not make sense to FROM(Sphe)
        # see issue #1474 
        primary_tables = get_caltab_final(primary_order, primary["gaintables"], primary["interps"], fluxscale_field, "target")
        if no_secondary:
            for gt, itp, fd in zip(*primary_tables):
                callibs.add_callib_recipe(applycal_recipes, gt, itp, fd)
        else:
            # default recipes from secondary
            for gt, itp, fd in zip(*get_caltab_final(secondary_order, secondary["gaintables"], secondary["interps"],
                                                     "nearest", "target")):
                # if the table is already applied with the primary in it, re-add it with an "all" (empty) field
                # add_callib_recipe(applycal_recipes, gt, itp, fd, '' if gt in applycal_recipes else targets)
                callibs.add_callib_recipe(applycal_recipes, gt, itp, fd)
            # make list of primary recipes that apply specifically to primary
            for gt, itp, fd in zip(*primary_tables):
                callibs.add_callib_recipe(applycal_recipes, gt, itp, fd, field=fluxscale_field)

        pipeline.save_callib(applycal_recipes, prefix_msbase)

        if pipeline.enable_task(config, 'summary'):
            step = 'summary-{0:s}-{1:d}'.format(label, i)
            recipe.add('cab/flagstats', step,
                       {
                           "msname": msname,
                           "plot": True,
                           "outfile": ('{0:s}-{1:s}-'
                                       'crosscal-summary-{2:d}.json').format(
                               prefix_msbase, wname, i),
                           "htmlfile": ('{0:s}-{1:s}-'
                                        'crosscal-summary-plots-{2:d}.html').format(
                               prefix_msbase, wname, i)
                       },
                       input=pipeline.input,
                       output=pipeline.diagnostic_plots,
                       label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))
            recipe.run()
            # Empty job que after execution
            recipe.jobs = []

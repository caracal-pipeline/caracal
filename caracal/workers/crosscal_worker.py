# -*- coding: future_fstrings -*-
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


NAME = "Cross-calibration"
LABEL = 'crosscal'

# E.g. to split out continuum/<dir> from output/continuum/dir
def get_dir_path(string, pipeline):
    return string.split(pipeline.output)[1][1:]

FLAG_NAMES = [""]

def exists(outdir, path):
    _path = os.path.join(outdir, path)
    return os.path.exists(_path)

# Rules for interpolation mode to use when applying calibration solutions
RULES = {
        "K" : {
            "name" : "delay_cal",
            "interp" : "nearest",
            "cab" : "cab/casa_gaincal",
            "gaintype" : "K",
            "field" : "bpcal",
            },
        "G" : {
            "name" : "gain_cal",
            "interp" : "nearest",
            "cab" : "cab/casa_gaincal",
            "gaintype" : "G",
            "mode" : "ap",
            "field" : "gcal",
            },
        "F" : {
            "name" : "gaincal_for_Ftable",
            "interp" : "nearest",
            "cab" : "cab/casa_gaincal",
            "gaintype" : "F",
            "mode" : "ap",
            "field" : "gcal",
            },
        "B" : {
            "name" : "bp_cal",
            "interp" : "linear",
            "cab" : "cab/casa_bandpass",
            "field" : "bpcal",
            },
        "A" : {
            "name" : "auto_flagging",
            "cab" : "cab/casa_flagdata",
            "mode" : "tfcrop",
            },
        "I" : {
            "name" : "image",
            "cab" : "cab/wsclean",
            },
        "S": {
            "name" : "slope_freq_delay",
            "cab" : "cab/casa_fringefit",
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
        gtype = [tab[-2]  for tab in gaintables]
        gtype.reverse()
        last_indices = []
        N = len(gtype)
        for term in set(gtype):
            idx = N-1-gtype.index(term)
            if gtype[gtype.index(term)] not in my_term:
                last_indices.append(idx)
        return last_indices
    else:
        return []

def solve(msname, msinfo,  recipe, config, pipeline, iobs, prefix, label, ftype,
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
        params["refant"] = pipeline.refant[iobs]
        params["solint"] = first_if_single(config[ftype]["solint"], i)
        params["combine"] = first_if_single(config[ftype]["combine"], i).strip("'")
        params["field"] = ",".join(field)
        caltable = "%s_%s.%s%d" % (prefix, ftype, term, itern)
        params["caltable"] = caltable + ":output"
        my_term = term
        if "I" not in order and smodel and term in "KGF":
            params["smodel"] = ["1", "0", "0", "0"]

        if term == "B":
            params["bandtype"] = term
            params["solnorm"] = config[ftype]["b_solnorm"]
            params["fillgaps"] = config[ftype]["b_fillgaps"]
            params["uvrange"] = config["uvrange"]
        elif term == "K":
            params["gaintype"] = term
        elif term in "FG":
            my_term = ["F", "G"]
            if term == "F":
                # Never append to the original. Make a copy for each F that is needed
                caltable_original = "%s_%s.G%d" % (prefix, prev_name, prev["iters"]["G"])
                primary_G = "%s_%s_append-%d.G%d" % (prefix, prev_name, itern,prev["iters"]["G"])
                caltable_path_original = os.path.join(pipeline.caltables, caltable_original)
                caltable_path = os.path.join(pipeline.caltables, primary_G)
                params["append"] = True
                caltable = "%s_%s.F%d" % (prefix, ftype, itern)
                params["caltable"] = primary_G + ":output"

            params["gaintype"] = "G"
            params["uvrange"] = config["uvrange"]
            params["calmode"] = first_if_single(config[ftype]["calmode"], i).strip("'")

        otf_apply = get_last_gain(gaintables, my_term=my_term)
        if otf_apply:
            params["gaintable"] = [gaintables[count]+":output" for count in otf_apply]
            params["interp"] = [interps[count] for count in otf_apply]
            params["gainfield"] = [fields[count] for count in otf_apply]

        can_reuse = False
        if config[ftype]["reuse_existing_gains"] and exists(pipeline.caltables, caltable):
            # check if field is in gain table
            substep = "check_fields-%s-%s-%d-%d-%s" % (name, label, itern, iobs, ftype)
            fields_in_tab = manGtabs.get_fields(pipeline, recipe, pipeline.caltables, caltable, substep)
            if set(fields_in_tab["field_id"]).issubset(field_id):
                can_reuse = True

        if can_reuse:
            caracal.log.info("Reusing existing gain table '%s' as requested" % caltable)
        else:
            if term == "F":
                if os.path.exists(caltable_path):
                    shutil.rmtree(caltable_path)
                cpstep = "copy_primary_gains_%s-%s-%d-%d-%s" % (name, label, itern, iobs, ftype)
                recipe.add(shutil.copytree, cpstep, {
                        "src" : caltable_path_original,
                        "dst": caltable_path,
                        }, label="{0}:: Copy parimary gains".format(step))
            recipe.add(RULES[term]["cab"], step, 
                    copy.deepcopy(params),
                    input=pipeline.input, output=pipeline.caltables,
                    label="%s:: %s calibration" % (step, term))
            if term == "F":
                transfer_fluxscale(msname, recipe, primary_G+":output", caltable+":output", pipeline, 
                iobs, reference=pipeline.fluxscale_reference, label=label)

        # Assume gains were plotted when they were created
        if config[ftype]["plotgains"] and not can_reuse:
            plotgains(recipe, pipeline, field_id, caltable, iobs, term=term)

        fields.append(",".join(field))
        interps.append(interp)
        gaintables.append(caltable)


    def do_IA(i):
        if i==0:
            raise RuntimeError("Have encountred an imaging/flagging request before any gains have been computed."\
                    "an I only makes sense after a G or K (usually both)."
                    "Please review your 'order' option in the self_cal:secondary section")

        if not applied:
            applycal(latest_KGBF_group, msname, recipe, gaintables,
                    interps, fields, CALS[ftype], pipeline, iobs,
                    calmode="calflag")
        else:
            caracal.log.info("Gains have already been applied using this exact set of gain tables and fields. Skipping unnecessary applycal step")

        if term == "A":
            if not set("KGBF").intersection(order[:i]):
                raise RuntimeError("Have encountered a request to flag the secondary calibrator without any gain, bandpass or delay tables to apply first.")
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
                calimage = "%s-%s-I%d-%d-field%d:output" %(prefix, ftype, itern, iobs, fid)
                recipe.add(RULES[term]["cab"], step, {
                        "msname" : msname,
                        "name" : calimage,
                        "size" : config[ftype]["image"]['npix'],
                        "scale" : config[ftype]["image"]['cell'],
                        "join-channels" : False if config[ftype]["image"]["nchans"]==1 else True,
                        "fit-spectral-pol" : config[ftype]["image"]["fit_spectral_pol"],
                        "channels-out" : config[ftype]["image"]['nchans'],
                        "auto-mask" : config[ftype]["image"]['auto_mask'],
                        "auto-threshold" : config[ftype]["image"]['auto_threshold'],
                        "local-rms-window" : config[ftype]["image"]['rms_window'],
                        "local-rms" : config[ftype]["image"]['local_rms'],
                        "padding" : config[ftype]["image"]['padding'],
                        "niter" : config[ftype]["image"]['niter'],
                        "weight" : config[ftype]["image"]["weight"],
                        "mgain" : config[ftype]["image"]['mgain'],
                        "field" : fid,
                    },
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
    groups[::2] = groups_solve # even indices
    groups[1::2] = groups_apply # odd indices

    # no need to apply gains multiple when encountering consecutive terms that need to apply
    applied = False
    i = -1 #
    for jj, group in enumerate(groups):
        for g, term in enumerate(group):
            i += 1
            # if this is not the case, then something has gone horribly wrong
            assert term == order[i]
            if (jj % 2) == 0: # even counter is solve group
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

    return  {
                "gaintables" : gaintables,
                "interps" : interps,
                "iters" : iters,
                "gainfield" : fields,
            }


def plotgains(recipe, pipeline, field_id, gtab, i, term):
    step = "plotgains-%s-%d-%s" % (term, i, "".join(map(str,field_id)))
    recipe.add('cab/ragavi', step,
        {
        "table"         : f"{gtab}:msfile",
        "gaintype"     : term,
        "field"        : ",".join(map(str,field_id)),
        "corr"         : '',
        "htmlname"     : gtab,
        },
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
        "vis" : msname,
        "caltable" : gaintable,
        "fluxtable" : fluxtable,
        "reference" : reference,
        "transfer" : "",
        },
        input=pipeline.input, output=pipeline.caltables,
        label="Transfer fluxscale")

def get_caltab_final(order, gaintable, interp, gainfield, field):

    rorder = list(reversed(order))
    if "G" in order:
        gi = rorder.index("G")
    else:
        gi = numpy.inf

    if "F" in order:
        fi = rorder.index("F")
    else: 
        fi = numpy.inf

    # if both are not there (or = inf), then it does not matter
    if fi == gi: # ooh, very naughty
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
        "vis" : msname,
        "field" : ",".join(getattr(pipeline, field)[i]),
        "applymode" : calmode,
        "gaintable" : [tab+":output" for tab in gaintables],
        "interp" : interps,
        "calwt" : [False],
        "gainfield" : fields,
        "parang" : False,
        "flagbackup" : False,
        },
            input=pipeline.input, output=pipeline.caltables,
            label="%s::Apply gain tables" % step)

def worker(pipeline, recipe, config):
    wname = pipeline.CURRENT_WORKER
    flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
    flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
    label = config["label_cal"]

    if pipeline.virtconcat:
        msnames = [pipeline.vmsname]
        nobs = 1
        prefixes = [pipeline.prefix]
    else:
        msnames = pipeline.msnames
        prefixes = pipeline.prefixes
        nobs = pipeline.nobs

    for i in range(nobs):

        if config["label_in"]:
            msname = '{0:s}_{1:s}.ms'.format(msnames[i][:-3],config["label_in"])
        else: msname = msnames[i]

        refant = pipeline.refant[i] or '0'
        prefix = prefixes[i]
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.obsinfo, msname[:-3])
        prefix = '{0:s}-{1:s}'.format(prefix, label)

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
                    if flags_before_worker in available_flagversions and available_flagversions.index(flags_before_worker) < available_flagversions.index(version) and not config['overwrite_flagvers']:
                        manflags.conflict('rewind_too_little', pipeline, wname, msname, config, flags_before_worker, flags_after_worker)
                    substep = 'version-{0:s}-ms{1:d}'.format(version, i)
                    manflags.restore_cflags(pipeline, recipe, version, msname, cab_name=substep)
                    if version != available_flagversions[-1]:
                        substep = 'delete-flag_versions-after-{0:s}-ms{1:d}'.format(version, i)
                        manflags.delete_cflags(pipeline, recipe,
                            available_flagversions[available_flagversions.index(version)+1],
                            msname, cab_name=substep)
                    if version != flags_before_worker:
                        substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                        manflags.add_cflags(pipeline, recipe, flags_before_worker,
                            msname, cab_name=substep, overwrite=config['overwrite_flagvers'])
                elif stop_if_missing:
                    manflags.conflict('rewind_to_non_existing', pipeline, wname, msname, config, flags_before_worker, flags_after_worker)
                else:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                        msname, cab_name=substep, overwrite=config['overwrite_flagvers'])
            else:
                if flags_before_worker in available_flagversions and not config['overwrite_flagvers']:
                    manflags.conflict('would_overwrite_bw', pipeline, wname, msname, config, flags_before_worker, flags_after_worker)
                else:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, i)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                        msname, cab_name=substep, overwrite=config['overwrite_flagvers'])

        if len(pipeline.fcal[i]) > 1:
            fluxscale_field = utils.observed_longest(msinfo, pipeline.fcal[i])
            fluxscale_field_id = utils.get_field_id(msinfo, fluxscale_field)[0]
            caracal.log.info("Found more than one flux calibrator."\
                               "Will use the one observed the logest (%s)." % fluxscale_field)
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
                        "tile-size": 128,
                        "column": "MODEL_DATA",
                    }
                elif modelpoint:  # spectral model if specified in our standard
                    opts = {
                        "vis": msname,
                        "field": fluxscale_field,
                        "standard": "manual",
                        "fluxdensity": modelpoint['I'],
                        "reffreq": '{0:f}GHz'.format(modelpoint['ref']/1e9),
                        "spix": [modelpoint[a] for a in 'abcd'],
                        "scalebychan": True,
                        "usescratch": True,
                    }
                elif standard:  # NRAO model otherwise
                    opts = {
                        "vis": msname,
                        "field": fluxscale_field,
                        "standard": standard,
                        "usescratch": False,
                        "scalebychan": True,
                    }
                else:

                    raise RuntimeError('The flux calibrator field "{}" could not be '
                                       'found in our database or in the CASA NRAO database'.format(fluxscale_field))
            step = 'set_model_cal-{0:d}'.format(i)
            cabtouse = 'cab/casa_setjy'
            recipe.add(cabtouse if "skymodel" not in opts else 'cab/simulator', step,
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
                    prefix, label=label, ftype="primary")
            caracal.log.info("Secondary calibrator is the same as the primary. Skipping fluxscale")
            interps = primary["interps"]
            gainfields = primary["gainfield"]
            gaintables = primary["gaintables"]

            if "bpcal" in config["apply_cal"]["applyto"] or "gcal" in config["apply_cal"]["applyto"]:
                applycal(primary_order, msname, recipe, copy.deepcopy(gaintables), copy.deepcopy(interps),
                        "nearest", "bpcal", pipeline, i, calmode=calmode, label=label)
            if "target" in config["apply_cal"]["applyto"]:
                applycal(primary_order, msname, recipe, copy.deepcopy(gaintables), copy.deepcopy(interps),
                        "nearest", "target", pipeline, i, calmode=calmode, label=label)
        else:
            primary = solve(msname, msinfo, recipe, config, pipeline, i,
                    prefix, label=label, ftype="primary")

            secondary = solve(msname, msinfo, recipe, config, pipeline, i,
                    prefix, label=label, ftype="secondary", 
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
            if "target" in config["apply_cal"]["applyto"]:
                applycal(secondary_order, msname, recipe, copy.deepcopy(gaintables), interps,
                        "nearest", "target", pipeline, i, calmode=calmode, label=label)

        if {"gcal", "fcal", "target"}.intersection(config["apply_cal"]["applyto"]):
            substep = 'save-{0:s}-ms{1:d}'.format(flags_after_worker, i)
            manflags.add_cflags(pipeline, recipe, flags_after_worker, msname, cab_name=substep, overwrite=config['overwrite_flagvers'])
        
        gt_final, itp_final, fd_final = get_caltab_final(primary_order if no_secondary else secondary_order,
                       copy.deepcopy(gaintables), interps, "nearest", "target")

        applycal_recipes = []
        calmodes = []
        for ix,gt in enumerate(gt_final):
            applycal_recipes.append(dict(zip(
                ['caltable', 'fldmap', 'interp'], [gt, fd_final[ix], itp_final[ix]])))
            if '.K' in gt:
                calmodes.append('delay_cal')
            elif '.B' in gt:
                calmodes.append('bp_cal')
            elif '.F' in gt:
                calmodes.append('transfer_fluxscale')
            elif '.G' in gt:
                calmodes.append('gain_cal')

        callib_dir = "{}/callibs".format(
            pipeline.caltables)
        if not os.path.exists(callib_dir):
            os.mkdir(callib_dir)

        callib_dict = dict(zip(calmodes, applycal_recipes))

        with open(os.path.join(callib_dir, 'callib_{}.json'.format(prefix)), 'w') as json_file:
            json.dump(callib_dict, json_file)

        if pipeline.enable_task(config, 'summary'):
            step = 'summary-{0:s}-{1:d}'.format(label, i)
            recipe.add('cab/casa_flagdata', step,
                       {
                           "vis" : msname,
                           "mode" : 'summary',
                           "field" : ",".join(set(pipeline.bpcal[i]+pipeline.fcal[i]+pipeline.gcal[i]))
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))
            recipe.run()
            # Empty job que after execution
            recipe.jobs = []

            summary_log = glob.glob("{0:s}/log-{1:s}-{2:s}-*"
                                    ".txt".format(pipeline.logs, wname, step))[0]
            json_summary = manflags.get_json_flag_summary(pipeline, summary_log, prefix, wname )
            manflags.flag_summary_plots(pipeline, json_summary, prefix, wname, i)

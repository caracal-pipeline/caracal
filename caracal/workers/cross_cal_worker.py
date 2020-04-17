import sys
import os
import caracal.dispatch_crew.utils as utils
import caracal
import yaml
import stimela.dismissable as sdm
from caracal.workers.utils import manage_flagsets as manflags
from caracal.workers.utils import manage_fields as manfields
import copy
import re
import json

NAME = "Cross calibration"
LABEL = 'cross_cal'

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
        "primary_cal": "fcal",
        "secondary_cal": "gcal",
        "bandpass_cal": "bpcal",
        }

def first_if_single(items, i):
    try:
        return items[i]
    except IndexError:
        return items[0]

def get_last_gain(gaintables, my_term="dummy"):
    if gaintables:
        gtype = [tab[-2]  for tab in gaintables]
        gtype.reverse()
        last_indices = []
        N = len(gtype)
        for term in set(gtype):
            idx = N-1-gtype.index(term)
            if gtype[gtype.index(term)] != my_term:
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
        for item in config[ftype].get("apply", ""):
            gaintables.append("%s_%s.%s%d" % (prefix, prev_name, item, prev["iters"][item]))
            ft = RULES[item]["field"]
            fields.append(",".join(getattr(pipeline, ft)[iobs]))
            interps.append(RULES[item]["interp"])

    field = getattr(pipeline, CALS[ftype])[iobs]
    order = config[ftype]["order"]
#    field_id = getattr(pipeline, CALS[ftype]+"_id")[iobs]
    field_id = utils.get_field_id(msinfo, field)

    for i,term in enumerate(order):
        name = RULES[term]["name"]
        if term in iters:
            iters[term] += 1
        else:
            iters[term] = 0

        itern = iters[term]
        step = "%s_%s_%d_%d_%s" % (name, label, itern, iobs, ftype)
        params = {}
        params["vis"] = msname
        if term == "A":
            params["mode"] = RULES[term]["mode"]
            params["field"] = ",".join(field)
            params["datacolumn"] = config[ftype]["flag"]["column"]
            params["usewindowstats"] = config[ftype]["flag"]["usewindowstats"]
            params["combinescans"] = config[ftype]["flag"]["combinescans"]
            params["flagdimension"] = config[ftype]["flag"]["flagdimension"]
            params["flagbackup"] = False
            params["timecutoff"] = config[ftype]["flag"]["timecutoff"]
            params["freqcutoff"] = config[ftype]["flag"]["freqcutoff"]
            params["correlation"] = config[ftype]["flag"]["correlation"]
            # apply existing gaintables before flagging
            if gaintables:
                applycal(msname, recipe, gaintables,
                    interps, fields, CALS[ftype], pipeline, iobs, calmode="calflag")
            recipe.add(RULES[term]["cab"], step, 
                    copy.deepcopy(params),
                    input=pipeline.input, output=pipeline.output,
                    label="%s::" % step)
        elif term == "I":
            step = "%s_%s_%d_%d_%s" % (name, label, itern, iobs, ftype)
            applycal(msname, recipe, gaintables_gcal, 
                interps_gcal, fields_gcal, CALS[ftype], pipeline, iobs, calmode="calflag")
            mask_prefix = "mask_%s_%s" %(prefix, ftype)
            maskim = "mask_%s_%s-image.fits:output" %(prefix, ftype)
            mask = "mask_%s_%s-mask.fits:output" %(prefix, ftype)
            recipe.add(RULES[term]["cab"], step, {
                    "msname" : msname,
                    "name" : mask_prefix,
                    "size" : 2048,
                    "scale" : "1.5asec",
                    "channels-out" : 1,
                    "auto-mask" : 6,
                    "auto-threshold" : 3,
                    "local-rms-window" : 50,
                    "local-rms" : True,
                    "padding" : 1.4,
                    "niter" : 1000000000,
                    "weight" : "briggs 0.0",
                    "mgain" : 1.0,
                    "field" : field_id,
                },
                    input=pipeline.input, output=pipeline.output,
                    label="%s:: Image %s field" % (step, ftype))

            step = "make_mask_%s_%d__%d_%s_2" % (label, itern, obs, ftype)
            recipe.add("cab/cleanmask", step, {
                "image" : maskim,
                "output" : maskim,
                "boxes" : 13,
                "sigma" : 10,
                "no-negative" : True,
            },
                input=pipeline.input,
                output=pipeline.output,
                label="make mask")

            step = "%s_%s_%d_%d_%s_2" % (name, label, itern, obs, ftype)
            recipe.add(RULES[term]["cab"], step, {
                    "msname" : msname,
                    "name" : "%s_%s" % (prefix, ftype),
                    "size" : 2048,
                    "scale" : "1.5asec",
                    "column" : "CORRECTED_DATA",
                    "auto-mask" : 4,
                    "auto-threshold" : 3,
                    "local-rms-window" : 50,
                    "local-rms" : True,
                    "padding" : 1.4,
                    "fits-mask" : mask,
                    "niter" : 1000000000,
                    "weight" : "briggs 0.0",
                    "mgain" : 0.8,
                    "field" : field_id,
                },
                    input=pipeline.input, output=pipeline.output,
                    label="%s:: Image %s field" % (step, ftype))
        else:
            interp = RULES[term]["interp"]
            caltable = "%s_%s.%s%d" % (prefix, ftype, term, itern)
            params["refant"] = pipeline.reference_antenna[iobs]
            params["solint"] = first_if_single(config[ftype]["solint"], i)
            params["combine"] = first_if_single(config[ftype]["combine"], i).strip("'")
            params["solnorm"] = config[ftype]["solnorm"]
            params["field"] = ",".join(field)
            if term == "B":
                params["bandtype"] = term
            else:
                params["gaintype"] = term
            otf_apply = get_last_gain(gaintables, my_term=term)
            if otf_apply:
                params["gaintable"] = [gaintables[count]+":output" for count in otf_apply]
                params["interp"] = [interps[count] for count in otf_apply]
                params["gainfield"] = [fields[count] for count in otf_apply]

            if term != "K":
                params["uvrange"] = config["uvrange"]

            if append_last_secondary and term == "G" and order.count("G") == itern+1:
                params["caltable"] = append_last_secondary + ":output"
                params["append"] = True
                caltable = append_last_secondary
            else:
                params["caltable"] = caltable

            if "I" not in order and smodel:
                params["smodel"] = ["1", "0", "0", "0"]

            if config[ftype]["reuse_existing_gains"] and exists(pipeline.caltables, 
                    caltable):
                caracal.log.info("Reusing existing gain table '%s' as requested" % caltable)
            else:
                recipe.add(RULES[term]["cab"], step, 
                        copy.deepcopy(params),
                        input=pipeline.input, output=pipeline.caltables,
                        label="%s:: %s calibration" % (step, term))

            if config[ftype]["plotgains"]:
                plotgains(recipe, pipeline, field_id, caltable+":output", iobs, term=term)

            fields.append(",".join(field))
            interps.append(interp)
            gaintables.append(caltable)

    result = {
                "gaintables" : gaintables,
                "interps" : interps,
                "iters" : iters,
                "gainfield" : fields,
                }
    return result


def plotgains(recipe, pipeline, field_id, gtab, i, term):
    step = "plotgains_%s_%d_%s" % (term, i, "".join(map(str,field_id)))
    recipe.add('cab/ragavi', step,
        {
         "table"        : '{0:s}/{1:s}'.format(get_dir_path(pipeline.caltables, pipeline), gtab),
         "gaintype"     : term,
         "field"        : ",".join(map(str,field_id)),
         "corr"         : '',
         "htmlname"     : '{0:s}/{1:s}'.format(get_dir_path(pipeline.reports, pipeline), gtab),
        },
        input=pipeline.input,
        output=pipeline.output,
        label='{0:s}:: Plot gaincal phase'.format(step))

def transfer_fluxscale(msname, recipe, gaintable, fluxtable, pipeline, i, reference, label=""):
    """
    Transfer fluxscale
    """
    step = "transfer_fluxscale_%s_%d" % (label, i)
    recipe.add("cab/casa_fluxscale", step, {
        "vis" : msname,
        "caltable" : gaintable,
        "fluxtable" : fluxtable,
        "reference" : reference,
        "transfer" : "",
        },
        input=pipeline.input, output=pipeline.caltables,
        label="Transfer fluxscale")

def get_caltab_final(gaintable, interp, gainfield, field, ftable=None):
    lidx = get_last_gain(gaintable)
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

    if ftable:
        replaced = False
        for gtab in gaintables:
            gtab_re = re.search(r"(^\S+)(.G\d)", gtab)
            if gtab_re:
                idx = gaintables.index(gtab)
                gaintables[idx] = ftable
                replaced = True
        if replaced is False:
            raise RuntimeError("There is no gaintable to replace with the fluxtable")

    return gaintables, interps, fields

def applycal(msname, recipe, gaintable, interp, gainfield, field, pipeline, i, 
        calmode="calflag", label="", fluxtable=None):
    """
    Apply gains
    -----------------

    Parameters:
      order: order in which to apply gains
    """ 

    gaintables, interps, fields = get_caltab_final(gaintable, interp, gainfield, field, ftable=fluxtable)

    step = "apply_gains_%s_%s_%d" % (field, label, i)
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

        refant = pipeline.reference_antenna[i] or '0'
        prefix = prefixes[i]
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, msname[:-3])
        prefix = '{0:s}-{1:s}'.format(prefix, label)

        if {"gcal", "fcal", "target"}.intersection(config["apply_cal"]["applyto"]):
            # Proceed only if there are no conflicting flag versions or if conflicts are being dealt with
            flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
            flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
            available_flagversions = manflags.get_flags(pipeline,msname)
            if flags_before_worker in available_flagversions and not config['ignore_flag_versions']:
                if not config['rewind_flags']["enable"]:
                    caracal.log.error('A worker named "{0:s}" was already run on the MS file "{1:s}" with pipeline prefix "{2:s}".'.format(wname,msname,pipeline.prefix))
                    ask_what_to_do = True
                else:
                    if available_flagversions.index(config['rewind_flags']["version"]) > available_flagversions.index(flags_before_worker) and not config['ignore_flag_versions']:
                        caracal.log.error('A worker named "{0:s}" was already run on the MS file "{1:s}" with pipeline prefix "{2:s}"'.format(wname,msname,pipeline.prefix))
                        caracal.log.error('and you are rewinding to a later flag version: {0:s} .'.format(config['rewind_flags']["version"]))
                        ask_what_to_do = True
                    else: ask_what_to_do = False
            else: ask_what_to_do  = False
            if ask_what_to_do:
                caracal.log.error('Running "{0:s}" again will create an inconcistency between flag versions, it will get messy.'.format(wname))
                caracal.log.error('Caracal will not overwrite the "{0:s}" flag versions unless you explicitely request that.'.format(wname))
                caracal.log.error('The current flag versions of this MS are (from the oldest to the most recent):')
                for vv in  available_flagversions:
                    if vv == flags_before_worker:
                        caracal.log.error('       {0:s}        <-- (this worker)'.format(vv))
                    elif vv == flags_after_worker:
                        caracal.log.error('       {0:s}         <-- (this worker)'.format(vv))
                    elif config['rewind_flags']["enable"] and vv == config['rewind_flags']["version"]:
                        caracal.log.error('       {0:s}        <-- (rewinding to this version)'.format(vv))
                    else:
                        caracal.log.error('       {0:s}'.format(vv))
                caracal.log.error('You have the following options:')
                caracal.log.error('    1) If you are happy with the flags currently stored in the FLAG column of this MS and')
                caracal.log.error('       want to append new flags to them, change the name of this worker in the configuration')
                caracal.log.error('       file by appending "__n" to it (where n is an integer not already taken in the list')
                caracal.log.error('       above). The new flags will be appended to the FLAG column, and new flag versions will')
                caracal.log.error('       be added to the list above.')
                caracal.log.error('    2) If you want to discard the flags obtained during the previous run of "{0:s}" (and,'.format(wname))
                caracal.log.error('       necessarily, all flags obtained thereafter; see list above) rewind the flag versions')
                caracal.log.error('       by setting in the configuration file:')
                caracal.log.error('           {0:s}: rewind_flags: enable: true'.format(wname))
                caracal.log.error('           {0:s}: rewind_flags: version: {1:s}'.format(wname, flags_before_worker))
                caracal.log.error('       You could rewind to an even earlier flag version if necessary. You will lose all flags')
                caracal.log.error('       appended to the FLAG column after that version, and take it from there.')
                caracal.log.error('    3) If you really know what you are doing ignore the flag versions by setting:')
                caracal.log.error('           {0:s}: ignore_flag_versions: true'.format(wname))
                caracal.log.error('       The worker "{0:s}" will be run again; the new flags will be appended to the current'.format(wname))
                caracal.log.error('       FLAG column (or to whatever flag version you are rewinding to); the flag versions from')
                caracal.log.error('       the previous run of "{0:s}" will be overwritten and appended to the list above (or'.format(wname))
                caracal.log.error('       to that list truncated to the version you are rewinding to); AND all hell will break loose.')
                caracal.log.error('Your choice will be applied to all MS files being processed together in this run of Caracal.')
                raise RuntimeError()

            if config['rewind_flags']["enable"]:
                version = config['rewind_flags']["version"]
                substep = 'rewind_to_{0:s}_ms{1:d}'.format(version, i)
                manflags.restore_cflags(pipeline, recipe, version, msname, cab_name=substep)
                substep = 'delete_flag_versions_after_{0:s}_ms{1:d}'.format(version, i)
                manflags.delete_cflags(pipeline, recipe,
                    available_flagversions[available_flagversions.index(version)+1],
                    msname, cab_name=substep)
                if  version != flags_before_worker:
                    substep = 'save_{0:s}_ms{1:d}'.format(flags_before_worker, i)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker, msname, cab_name=substep, overwrite=config['ignore_flag_versions'])
            else:
                substep = 'save_{0:s}_ms{1:d}'.format(flags_before_worker, i)
                manflags.add_cflags(pipeline, recipe, flags_before_worker, msname, cab_name=substep, overwrite=config['ignore_flag_versions'])

        def flag_gains(cal, opts, datacolumn="CPARAM"):
            opts = dict(opts)
            if 'enable' in opts:
                del(opts['enable'])
            step = 'flag_{0:s}_{1:s}_{2:d}'.format(cal, worker_label, i)
            opts["vis"] = '{0:s}/{1:s}.{2:s}'.format(get_dir_path(
                pipeline.caltables, pipeline), prefix, table_suffix[cal]+':output')
            opts["datacolumn"] = datacolumn
            recipe.add('cab/casa_flagdata', step, opts,
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Flagging gains'.format(step))

        if len(pipeline.fcal[i]) > 1:
            fluxscale_field = utils.observed_longest(msinfo, pipeline.fcal[i])
            fluxscale_field_id = utils.get_field_id(msinfo, fluxscale_field)[0]
            caracal.log.info("Found more than one flux calibrator."\
                               "Will use the one observed the logest (%s)." % fluxscale_field)
        else:
            fluxscale_field = pipeline.fcal[i][0]
            fluxscale_field_id = utils.get_field_id(msinfo, fluxscale_field)[0]

        if pipeline.enable_task(config, 'set_model'):
            if config['set_model'].get('no_verify'):
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
                if config['set_model'].get('caracal_model') and modelsky:

                    # use local sky model of calibrator field if exists
                    opts = {
                        "skymodel": modelsky,
                        "msname": msname,
                        "field-id": utils.get_field_id(msinfo, fluxscale_field)[0],
                        "threads": config["set_model"].get('threads'),
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
                        "standard": config['set_model'].get('standard', standard),
                        "usescratch": False,
                        "scalebychan": True,
                    }
                else:

                    raise RuntimeError('The flux calibrator field "{}" could not be '
                                       'found in our database or in the CASA NRAO database'.format(fluxscale_field))
            step = 'set_model_cal_{0:d}'.format(i)
            cabtouse = 'cab/casa_setjy'
            recipe.add(cabtouse if "skymodel" not in opts else 'cab/simulator', step,
               opts,
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Set jansky ms={1:s}'.format(step, msname))


        gcal_set = set(pipeline.gcal[i])
        fcal_set = set(pipeline.fcal[i])
        calmode = config["apply_cal"]["calmode"]
        if gcal_set == set() or len(gcal_set - fcal_set) == 0:
            primary = solve(msname, msinfo, recipe, config, pipeline, i, 
                    prefix, label=label, ftype="primary_cal")
            caracal.log.info("Secondary calibrator is the same as the primary. Skipping fluxscale")
            interps = primary["interps"]
            gainfields = primary["gainfield"]
            gaintables = primary["gaintables"]
            # apply to calibration fields
            if len(pipeline.bpcal[i]) > 1:
                ftable = "%s_primary_cal.F%d" % (prefix, primary["iters"]["G"])
                if config["primary_cal"]["reuse_existing_gains"] and exists(pipeline.caltables, 
                        ftable):
                    caracal.log.info("Reusing existing gain table '%s' as requested" % ftable)
                else:
                    transfer_fluxscale(msname, recipe, gtable+":output", ftable, 
                            pipeline, i, reference=fluxscale_field, label=label)
                    fstrings = map(str, pipeline.bpcal_id[i])
                    fstrings = ",".join(fstrings)
                    plotgains(recipe, pipeline, fstrings, ftable+":output", i, term='F')
            else:
                ftable = None

            if "bpcal" in config["apply_cal"]["applyto"] or "gcal" in config["apply_cal"]["applyto"]:
                applycal(msname, recipe, copy.deepcopy(gaintables), copy.deepcopy(interps),
                        "nearest", "bpcal", pipeline, i, calmode=calmode, label=label, fluxtable=ftable)
            if "target" in config["apply_cal"]["applyto"]:
                applycal(msname, recipe, copy.deepcopy(gaintables), copy.deepcopy(interps),
                        "nearest", "target", pipeline, i, calmode=calmode, label=label, fluxtable=ftable)
        else:
            primary = solve(msname, msinfo, recipe, config, pipeline, i, 
                    prefix, label=label, ftype="primary_cal")

            gtable = "%s_primary_cal.G%d" % (prefix, primary["iters"]["G"])
            secondary = solve(msname, msinfo, recipe, config, pipeline, i,
                    prefix, label=label, ftype="secondary_cal", append_last_secondary=gtable, 
                    prev=primary, prev_name="primary_cal", smodel=True)

            interps = primary["interps"]
            gainfields = primary["gainfield"]
            gaintables = primary["gaintables"]
            # Transfer fluxscale
            if len(pipeline.bpcal[i]) > 1:
                ftable = "%s_primary_cal.F%d" % (prefix, primary["iters"]["G"])
                if config["primary_cal"]["reuse_existing_gains"] and exists(pipeline.caltables, 
                        ftable):
                    caracal.log.info("Reusing existing gain table '%s' as requested" % ftable)
                else:
                    transfer_fluxscale(msname, recipe, gtable+":output", ftable, 
                            pipeline, i, reference=fluxscale_field, label=label)
                    plotgains(recipe, pipeline, pipeline.bpcal_id[i], 
                            ftable+":output", i, term='F')
            else:
                ftable = None

            if "bpcal" in config["apply_cal"]["applyto"] or "gcal" in config["apply_cal"]["applyto"]:
                applycal(msname, recipe, copy.deepcopy(gaintables), copy.deepcopy(interps),
                        "nearest", "bpcal", pipeline, i, calmode=calmode, label=label, fluxtable=ftable)

            # Transfer fluxscale
            ftable = "%s_secondary_cal.F%d" % (prefix, primary["iters"]["G"])
            if config["secondary_cal"]["reuse_existing_gains"] and exists(pipeline.caltables, 
                    ftable):
                caracal.log.info("Reusing existing gain table '%s' as requested" % ftable)
            else:
                transfer_fluxscale(msname, recipe, gtable+":output", ftable, 
                        pipeline, i, reference=fluxscale_field, label=label)
                plotgains(recipe, pipeline, pipeline.gcal_id[i] + [fluxscale_field_id], 
                    ftable+":output", i, term='F')

            interps = secondary["interps"]
            gainfields = secondary["gainfield"]
            gaintables = secondary["gaintables"]

            if "gcal" in config["apply_cal"]["applyto"]:
                applycal(msname, recipe, copy.deepcopy(gaintables), interps, 
                        gainfields, "gcal", pipeline, i, calmode=calmode, label=label, fluxtable=ftable)
            if "target" in config["apply_cal"]["applyto"]:
                applycal(msname, recipe, copy.deepcopy(gaintables), interps,
                        "nearest", "target", pipeline, i, calmode=calmode, label=label, fluxtable=ftable)

        if {"gcal", "fcal", "target"}.intersection(config["apply_cal"]["applyto"]):
            substep = 'save_{0:s}_ms{1:d}'.format(flags_after_worker, i)
            manflags.add_cflags(pipeline, recipe, flags_after_worker, msname, cab_name=substep, overwrite=config['ignore_flag_versions'])

        gt_final, itp_final, fd_final = get_caltab_final(
                       copy.deepcopy(gaintables), interps, "nearest", "target", ftable=ftable)

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

        if pipeline.enable_task(config, 'flagging_summary'):
            step = 'flagging_summary_crosscal_{0:s}_{1:d}'.format(label, i)
            recipe.add('cab/casa_flagdata', step,
                       {
                           "vis" : msname,
                           "mode" : 'summary',
                           "field" : ",".join(set(pipeline.bpcal[i]+pipeline.fcal[i]+pipeline.gcal[i]))
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))

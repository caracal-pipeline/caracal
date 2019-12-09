import sys
import os
import meerkathi.dispatch_crew.utils as utils
import meerkathi
import yaml
import stimela.dismissable as sdm
from meerkathi.workers.utils import manage_flagsets as manflags
from meerkathi.workers.utils import manage_fields as manfields
import copy

NAME = "Cross calibration"
# E.g. to split out continuum/<dir> from output/continuum/dir


def get_dir_path(string, pipeline): 
    return string.split(pipeline.output)[1][1:]

FLAG_NAMES = [""]

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


def solve(recipe, config, pipeline, iobs, prefix, label):
    """
    Solve for K,G,B
    -----------------

    """
    gaintables = []
    interps = []
    fields = []
    iters = {}
    fluxscale = False
    order = config["bpcal"]["order"]
    field = getattr(pipeline, "bpcal")[iobs]
    field_id = getattr(pipeline, "bpcal_id")[iobs]
    ms = pipeline.msnames[iobs]
    for i,term in enumerate(order):
        name = RULES[term]["name"]
        if term in iters:
            iters[term] += 1
        else:
            iters[term] = 0

        itern = iters[term]
        step = "%s_%s_%d_%d_bpcal" % (name, label, itern, iobs)
        params = {}
        params["vis"] = ms
        if term == "A":
            params["mode"] = RULES[term]["mode"]
            params["field"] = field
            params["datacolumn"] = config["bpcal"]["flag"]["column"]
            params["usewindowstats"] = config["bpcal"]["flag"]["usewindowstats"]
            params["combinescans"] = config["bpcal"]["flag"]["combinescans"]
            params["flagdimension"] = config["bpcal"]["flag"]["flagdimension"]
            params["flagbackup"] = False
            params["timecutoff"] = config["bpcal"]["flag"]["timecutoff"]
            params["freqcutoff"] = config["bpcal"]["flag"]["freqcutoff"]
            params["correlation"] = config["bpcal"]["flag"]["correlation"]

            # apply existing gaintables before flagging
            if gaintables:
                applycal(recipe, [gtab+":output" for gtab in gaintables],
                    interps, fields, "bpcal", pipeline, iobs, calmode="calonly")
            recipe.add(RULES[term]["cab"], step, 
                    copy.deepcopy(params),
                    input=pipeline.input, output=pipeline.caltables,
                    label="%s::" % step)
        else:
            interp = RULES[term]["interp"]
            caltable = "%s_bpcal.%s%d" % (prefix, term, itern)
            if term == "B":
                params["bandtype"] = term
            else:
                params["gaintype"] = term
            params["solint"] = first_if_single(config["bpcal"]["solint"], i)
            params["combine"] = first_if_single(config["bpcal"]["combine"], i)
            otf_apply = get_last_gain(gaintables, my_term=term)
            if otf_apply:
                params["gaintable"] = [gaintables[count]+":output" for count in otf_apply]
                params["interp"] = [interps[count] for count in otf_apply]
                params["gainfield"] = [fields[count] for count in otf_apply]

            if term != 'K':
                params["uvrange"] = config["uvrange"]
                
#            if term == "G":
#                params["smodel"] = ["1","0","0","0"]
                
            params["refant"] = pipeline.reference_antenna[iobs]
            params["caltable"] = caltable
            params["field"] = field

            recipe.add(RULES[term]["cab"], step, 
                    copy.deepcopy(params),
                    input=pipeline.input, output=pipeline.caltables,
                    label="%s::" % step)
            if config["bpcal"]["plotgains"]:
                plotgains(recipe, pipeline, [field_id], caltable, iobs)
            fields.append(field)
            interps.append(interp)
            gaintables.append(caltable)
            
            meerkathi.log.error("params are {}".format(params))

    if pipeline.gcal[iobs] != pipeline.fcal[iobs]:
        fluxscale = True
        order = config["gcal"]["order"]
        gaintables_gcal = []
        fields_gcal = []
        interps_gcal = []
        for item in config["gcal"]["apply"]:
            gaintables_gcal.append('%s_bpcal.%s%d' %(prefix, item, iters[item]))
            fields_gcal.append(field)
            interps_gcal.append(RULES[item]["interp"])

        iters_gcal = {}
        gtable_final = '%s_bpcal.G%d' %(prefix,iters["G"])
        gtable_final_counter = order.count("G")
        field = getattr(pipeline, "gcal")[iobs]
        field_id = getattr(pipeline, "gcal_id")[iobs]
        for i,term in enumerate(order):
            name = RULES[term]["name"]
            if term in iters_gcal:
                iters_gcal[term] += 1
            else:
                iters_gcal[term] = 0

            step = "%s_%s_%d_%d_gcal" % (name, label, itern, iobs)
            if term == "I":
                step = "%s_%s_%d_%d" % (name, label, itern, obs)
                applycal(recipe, [gtab+":output" for gtab in gaintables_gcal], 
                    interps_gcal, fields_gcal, "gcal", pipeline, iobs, calmode="calonly")
                recipe.add(RULES[term]["cab"], step, {
                        "msname" : ms,
                        "name" : "%s_gcal" % (prefix),
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
                        label="%s:: Image gcal field" % step)

                step = "make_mask_%s_%d__%d_2" % (label, itern, obs)
                recipe.add("cab/cleanmask", step, {
                    "image" : "%s_gcal-image.fits:output" % (prefix),
                    "output" : "mask_%d.fits" % iobs,
                    "boxes" : 13,
                    "sigma" : 10,
                    "no-negative" : True,
                },
                    input=pipeline.input,
                    output=pipeline.output,
                    label="make mask")
                
                step = "%s_%s_%d_%d_2" % (name, label, itern, obs)
                recipe.add(RULES[term]["cab"], step, {
                        "msname" : ms,
                        "name" : "%s_gcal" % (prefix),
                        "size" : 2048,
                        "scale" : "1.5asec",
                        "column" : "CORRECTED_DATA",
                        "auto-mask" : 4,
                        "auto-threshold" : 3,
                        "local-rms-window" : 50,
                        "local-rms" : True,
                        "padding" : 1.4,
                        "fits-mask" : "mask_%d.fits:output" % iobs,
                        "niter" : 1000000000,
                        "weight" : "briggs 0.0",
                        "mgain" : 0.8,
                        "field" : field_id,
                    },
                        input=pipeline.input, output=pipeline.output,
                        label="%s:: Image gcal field" % step)
            else:
                interp = RULES[term]["interp"]
                params = {}
                params["vis"] = ms
                params["gaintype"] = term
                plot_field = [field_id]
                N_from_bpcal = len(config["gcal"]["apply"])
                otf_apply = get_last_gain(gaintables_gcal[N_from_bpcal:], my_term=term)
                if otf_apply:
                    tables = gaintables_gcal[:N_from_bpcal] + [gaintables_gcal[count+N_from_bpcal] for count in otf_apply]
                    params["gaintable"] = [tab+":output" for tab in tables]
                    params["interp"] = interps_gcal[:N_from_bpcal] + [interps_gcal[count+N_from_bpcal] for count in otf_apply]
                    params["gainfield"] = fields_gcal[:N_from_bpcal] + [fields_gcal[count+N_from_bpcal] for count in otf_apply]
                if term == "G" and iters_gcal[term]+1 == gtable_final_counter:
                    caltable = gtable_final
                    params["append"] = True
                    plot_field.append(getattr(pipeline, "bpcal_id")[iobs])
                else:
                    params["append"] = False
                    caltable = "%s_gcal.%s%d" % (prefix, term, iters_gcal[term])
                params["solint"] = first_if_single(config["gcal"]["solint"], i)
                params["combine"] = first_if_single(config["gcal"]["combine"], i)
                
                if term != "K":
                    params["uvrange"] = config["uvrange"]
                
                params["refant"] = pipeline.reference_antenna[iobs]
                params["caltable"] = caltable
                params["field"] = field

                itern = iters[term]
                recipe.add(RULES[term]["cab"], step, 
                        copy.deepcopy(params),
                        input=pipeline.input, output=pipeline.caltables,
                        label="%s::" % step)
                if config["gcal"]["plotgains"] and params["append"] == False:
                    plotgains(recipe, pipeline, plot_field, caltable, iobs)

                fields_gcal.append(field)
                interps_gcal.append(interp)
                gaintables_gcal.append(caltable)

    result = {
            "bpcal" : {
                "gaintables" : gaintables,
                "interps" : interps,
                "iters" : iters,
                "gainfield" : fields,
                },
            "fluxscale" : fluxscale,
            }
    if fluxscale:
        result.update({
            "gcal" : {
                "gaintables" : gaintables_gcal,
                "interps" : interps_gcal,
                "iters" : iters_gcal,
                "gainfield" : fields_gcal,
                },
            })

    return result

def plotgains(recipe, pipeline, field_id, gtab, i):
    step = "plotgains_%s_%d_%s" % (gtab[-2:], i, "".join(map(str,field_id)))
    recipe.add('cab/ragavi', step,
        {
         "table"        : '{0:s}/{1:s}:{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), gtab, 'output'),
    
         "gaintype"     : gtab[-2],
         "field"        : " ".join(map(str,field_id)),
         "corr"         : 0,
         "htmlname"     : '{0:s}/{1:s}'.format(get_dir_path(pipeline.reports, pipeline), gtab),
        },
        input=pipeline.input,
        output=pipeline.output,
        label='{0:s}:: Plot gaincal phase'.format(step))

def transfer_fluxscale(recipe, gaintable, fluxtable, pipeline, i, label=""):
    """
    Transfer fluxscale
    """
    step = "transfer_fluxscale_%s_%d" % (label, i)
    recipe.add("cab/casa_fluxscale", step, {
        "vis" : pipeline.msnames[i],
        "caltable" : gaintable,
        "fluxtable" : fluxtable,
        "reference" : getattr(pipeline, "fcal")[i],
        "transfer" : getattr(pipeline, "gcal")[i],
        },
        input=pipeline.input, output=pipeline.caltables,
        label="Transfer fluxscale")

def applycal(recipe, gaintable, interp, gainfield, field, pipeline, i, calmode="calflag", label=""):
    """
    Apply gains
    -----------------

    Parameters:
      order: order in which to apply gains
    """ 
    step = "apply_gains_%s_%s_%d" % (field, label, i)
    recipe.add("cab/casa_applycal", step, {
        "vis" : pipeline.msnames[i],
        "field" : getattr(pipeline, field)[i],
        "applymode" : calmode,
        "gaintable" : gaintable,
        "interp" : interp,
        "calwt" : [False],
        "gainfield" : gainfield,
        "parang" : False,
        },
            input=pipeline.input, output=pipeline.caltables,
            label="%s::Apply gain tables" % step)

def worker(pipeline, recipe, config):
    wname = pipeline.CURRENT_WORKER
    label = config["label"]
    if pipeline.virtconcat:
        msnames = [pipeline.vmsname]
        nobs = 1
        prefixes = [pipeline.prefix]
    else:
        msnames = pipeline.msnames
        prefixes = pipeline.prefixes
        nobs = pipeline.nobs

    for i in range(nobs):
        msname = msnames[i]
        refant = pipeline.reference_antenna[i] or '0'
        prefix = prefixes[i]
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, msname[:-3])
        prefix = '{0:s}-{1:s}'.format(prefix, label)

        def get_gain_field(applyme, applyto=None):
            if applyme == 'delay_cal':
                return manfields.get_field(pipeline, i, config['delay_cal'].get('field'))
            if applyme == 'bp_cal':
                return manfields.get_field(pipeline, i, config['bp_cal'].get('field'))
            if applyme == 'gain_cal_flux':
                return manfields.get_field(pipeline, i, 'fcal')
            if applyme == 'gain_cal_gain':
                return manfields.get_field(pipeline, i, 'gcal')
            if applyme == 'transfer_fluxscale':
                if applyto in ['gcal', 'target']:
                    return manfields.get_field(pipeline, i, 'gcal')
                elif applyto == 'bpcal':
                    return manfields.get_field(pipeline, i, 'fcal')

        def flag_gains(cal, opts, datacolumn="CPARAM"):
            opts = dict(opts)
            if 'enable' in opts:
                del(opts['enable'])
            step = 'flag_{0:s}_{1:d}'.format(cal, i)
            opts["vis"] = '{0:s}/{1:s}.{2:s}'.format(get_dir_path(
                pipeline.caltables, pipeline), prefix, table_suffix[cal]+':output')
            opts["datacolumn"] = datacolumn
            recipe.add('cab/casa_flagdata', step, opts,
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Flagging gains'.format(step))

        # Clear flags from this worker if they already exist
        substep = 'flagset_clear_{0:s}_{1:d}'.format(wname, i)
        manflags.delete_flagset(pipeline, recipe, wname,
                               msname, cab_name=substep)

        if pipeline.enable_task(config, 'set_model'):
            # Set model
            field = manfields.get_field(
                pipeline, i, config['set_model'].get('field'))
            assert len(utils.get_field_id(msinfo, field)
                       ) == 1, "Only one fcal should be set"

            if config['set_model'].get('no_verify'):
                opts = {
                    "vis": msname,
                    "field": field,
                    "scalebychan": True,
                    "usescratch": True,
                }
            else:
                model = utils.find_in_native_calibrators(msinfo, field)
                standard = utils.find_in_casa_calibrators(msinfo, field)
                # Prefer our standard over the NRAO standard
                meerkathi_model = isinstance(model, str)
                if config['set_model'].get('meerkathi_model') and meerkathi_model:
                    # use local sky model of calibrator field if exists
                    opts = {
                        "skymodel": model,
                        "msname": msname,
                        "field-id": utils.get_field_id(msinfo, field)[0],
                        "threads": config["set_model"].get('threads'),
                        "mode": "simulate",
                        "tile-size": 128,
                        "column": "MODEL_DATA",
                    }
                elif isinstance(model, dict):  # spectral model if specified in our standard
                    opts = {
                        "vis": msname,
                        "field": field,
                        "standard": "manual",
                        "fluxdensity": model['I'],
                        "reffreq": '{0:f}GHz'.format(model['ref']/1e9),
                        "spix": [model[a] for a in 'abcd'],
                        "scalebychan": True,
                        "usescratch": True,
                    }
                elif standard:  # NRAO model otherwise
                    opts = {
                        "vis": msname,
                        "field": field,
                        "standard": config['set_model'].get('standard', standard),
                        "usescratch": False,
                        "scalebychan": True,
                    }
                else:
                    raise RuntimeError('The flux calibrator field "{}" could not be \
found in our database or in the CASA NRAO database'.format(field))
            step = 'set_model_cal_{0:d}'.format(i)
#            cabtouse = 'cab/casa47_setjy' if config['casa_version']=='47' else 'cab/casa_setjy'
            cabtouse = 'cab/casa_setjy'
            recipe.add(cabtouse if "skymodel" not in opts else 'cab/simulator', step,
               opts,
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Set jansky ms={1:s}'.format(step, msname))

        solve_outs = solve(recipe, config, pipeline, i, prefix, label=label)
        gaintables_bpcal = solve_outs["bpcal"]["gaintables"]
        apply_bpcal = get_last_gain(gaintables_bpcal)
        # only apply best gain from each gain term
        gaintables_bpcal = [solve_outs["bpcal"]["gaintables"][count] for count in apply_bpcal]
        interps_bpcal = [solve_outs["bpcal"]["interps"][count] for count in apply_bpcal]
        gainfield_bpcal = [solve_outs["bpcal"]["gainfield"][count] for count in apply_bpcal]
        iters_bpcal = solve_outs["bpcal"]["iters"]
        calmode = config["apply_cal"]["calmode"]
        gcal_field = getattr(pipeline, "gcal")[i]
        bpcal_field = getattr(pipeline, "bpcal")[i]
        if solve_outs["fluxscale"]:
            gaintables_gcal = solve_outs["gcal"]["gaintables"]
            apply_gcal = get_last_gain(gaintables_gcal)
            # only apply best gain from each gain term
            gaintables_gcal = [solve_outs["gcal"]["gaintables"][count] for count in apply_gcal]
            interps_gcal = [solve_outs["gcal"]["interps"][count] for count in apply_gcal]
            gainfield_gcal = [solve_outs["gcal"]["gainfield"][count] for count in apply_gcal]
            iters_gcal = solve_outs["gcal"]["iters"]
            # apply to bpcal
            _gaintables = [gtab+":output" for gtab in gaintables_bpcal]
            if "bpcal" in config["apply_cal"]["applyto"]:
                applycal(recipe, copy.deepcopy(_gaintables), interps_bpcal, 
                        gainfield_bpcal, "bpcal", pipeline, i, calmode=calmode, label=label)
            # prepare to apply to gcal 
            gtable = '%s_bpcal.G%d' %(prefix, iters_bpcal["G"])
            ftable = '%s.F%d' %(prefix, iters_bpcal["G"])
            fluxtable = transfer_fluxscale(recipe, gtable+":output", ftable, 
                    pipeline, i, label=label)
            # plot fluxtable
            if config["gcal"]["plotgains"]:
                plotgains(recipe, pipeline, [getattr(pipeline, "gcal_id")[i], 
                    getattr(pipeline, "fcal_id")[i]], ftable, i)
            replace_index = gaintables_gcal.index(gtable)
            gaintables_gcal[replace_index] = ftable
            _gaintables = [gtab+":output" for gtab in gaintables_gcal]
            # apply to gcal
            if "gcal" in config["apply_cal"]["applyto"]:
                applycal(recipe, copy.deepcopy(_gaintables), interps_gcal, 
                        gainfield_gcal, "gcal", pipeline, i, calmode=calmode, label=label)
            if "target" in config["apply_cal"]["applyto"]:
                applycal(recipe, copy.deepcopy(_gaintables), interps_gcal,
                        gainfield_gcal, "target", pipeline, i, calmode=calmode, label=label)
        else:
            same_gcal_bpcal = pipeline.gcal == pipeline.bpcal
            _gaintables = [gtab+":output" for gtab in gaintables_bpcal]
            # apply to bpcal
            if "bpcal" in config["apply_cal"]["applyto"] or same_gcal_bpcal:
                applycal(recipe, copy.deepcopy(_gaintables), interps_bpcal,
                        gainfield_bpcal, "bpcal", pipeline, i, calmode=calmode, label=label)
            if "target" in config["apply_cal"]["applyto"]:
                applycal(recipe, copy.deepcopy(_gaintables), interps_bpcal,
                        gainfield_bpcal, "target", pipeline, i, calmode=calmode, label=label)
        substep = 'flagset_update_{0:s}_{1:d}'.format(wname, i)
        manflags.update_flagset(pipeline, recipe, wname, msname, cab_name=substep)
        

        if pipeline.enable_task(config, 'flagging_summary'):
            step = 'flagging_summary_crosscal_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata', step,
                       {
                           "vis": msname,
                           "mode": 'summary',
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))

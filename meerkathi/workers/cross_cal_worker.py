import sys
import os
import meerkathi.dispatch_crew.utils as utils
import meerkathi
import yaml
import stimela.dismissable as sdm
from meerkathi.workers.utils import manage_flagsets as manflags
from meerkathi.workers.utils import manage_fields as manfields


NAME = "Cross calibration"
# E.g. to split out continuum/<dir> from output/continuum/dir


def get_dir_path(string, pipeline): return string.split(pipeline.output)[1][1:]


# Rules for interpolation mode to use when applying calibration solutions
applycal_interp_rules = {
    'bpcal':  {
        'delay_cal': 'nearest',
        'bp_cal': 'nearest',
                  'transfer_fluxscale': 'linear',
                  'gain_cal_gain': 'linear',
    },
    'gcal':  {
        'delay_cal': 'linear',
        'bp_cal': 'linear',
                  'transfer_fluxscale': 'nearest',
                  'gain_cal_gain': 'linear',
    },
    'target':  {
        'delay_cal': 'linear',
        'bp_cal': 'linear',
                  'transfer_fluxscale': 'linear',
                  'gain_cal_gain': 'linear',
    },
}

table_suffix = {
    "delay_cal": 'K0',
    "bp_cal": 'B0',
    "gain_cal_gain": 'G0',
    "gain_cal_flux": 'G0',
    "transfer_fluxscale": 'F0',
}

corr_indexes = {'H': 0,
                'X': 0,
                'V': 1,
                'Y': 1,
                }

FLAGSETS_SUFFIX = [""]


def worker(pipeline, recipe, config):
    wname = pipeline.CURRENT_WORKER
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
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, prefix)
        prefix = '{0:s}-{1:s}'.format(prefix, config.get('label'))

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
        manflags.clear_flagset(pipeline, recipe, wname,
                               msname, cab_name=substep)

        if pipeline.enable_task(config, 'clear_cal'):
            # Initialize dataset for calibration
            field = manfields.get_field(
                pipeline, i, config['clear_cal'].get('field'))
            addmodel = config['clear_cal'].get('addmodel')
            step = 'clear_cal_{0:d}'.format(i)
            recipe.add('cab/casa_clearcal', step,
                       {
                           "vis": msname,
                           "field": field,
                           "addmodel": addmodel,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Clear calibration ms={1:s}'.format(step, msname))

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
            cabtouse = 'cab/casa47_setjy' if config['casa_version']=='47' else 'cab/casa_setjy'
            recipe.add(cabtouse if "skymodel" not in opts else 'cab/simulator', step,
               opts,
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Set jansky ms={1:s}'.format(step, msname))


        # Delay calibration
        if pipeline.enable_task(config, 'delay_cal'):
            step = 'delay_cal_{0:d}'.format(i)
            field = get_field(config['delay_cal'].get('field'))
            cabtouse = 'cab/casa47_gaincal' if config['casa_version']=='47' else 'cab/casa_gaincal'
            recipe.add(cabtouse, step,
               {
                 "vis"          : msname,
                 "caltable"     : '{0:s}/{1:s}.{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), prefix, 'K0'),
                 "field"        : field,
                 "refant"       : refant, #this reference must be used throughout in the way casa solves the RIME to avoid creating an ambituity in the crosshand phase
                 "solint"       : config['delay_cal'].get('solint'),
                 "combine"      : config['delay_cal'].get('combine'),
                 "minsnr"       : config['delay_cal'].get('minsnr'),
                 "gaintype"     : "K",
                 "uvrange"      : config.get('uvrange'),
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Delay calibration ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config['delay_cal'],'flag'):
                flag_gains('delay_cal', config['delay_cal']['flag'], datacolumn="FPARAM")

            if pipeline.enable_task(config['delay_cal'],'plot'):
                print "fieldtoplot", utils.get_field_id(msinfo, field)[0]
                step = 'plot_delay_cal_{0:d}'.format(i)
                table = prefix+".K0"
                fieldtoplot = []
                fieldtoplot.append(utils.get_field_id(msinfo, field)[0])
                recipe.add('cab/ragavi', step,
                    {
                     "table"        : '{0:s}/{1:s}:{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), table, 'output'),

                     "gaintype"     : "K",
                     #"field"        : utils.get_field_id(msinfo, field)[0],
                     "field"        : fieldtoplot,
                     "corr"         : corr_indexes[config['delay_cal']['plot'].get('corr')],
                     "htmlname"     : '{0:s}/'.format(get_dir_path(pipeline.reports, pipeline)) + '{0:s}-K0'.format(prefix),
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Plot gaincal phase ms={1:s}'.format(step, msname))

        # Bandpass calibration
        if pipeline.enable_task(config, 'bp_cal'):

            # Optionally remove large temporal phase variations from the bandpass calibrator before solving for the final bandpass.
            # This is done by solving for:
            #   1) per-scan normalised bandpass;
            #   2) per-scan flux calibration on the bandpass calibrator.
            # The phase term of the per-scan flux calibration removes large temporal phase variations from the bandpass calibrator.
            # It is applied on the fly to the bandpass calibrator when solving for the final (possibly time-independent) bandpass.
            if config['bp_cal'].get('remove_ph_time_var'):

                # Initial bandpass calibration (will NOT combine scans even if requested for final bandpass)
                if config.get('otfdelay'):
                    gaintables, interpolations = ['{0:s}/{1:s}.{2:s}'.format(get_dir_path(
                        pipeline.caltables, pipeline), prefix, 'K0:output')], ['nearest']
                else:
                    gaintables, interpolations = None, ''
                field = manfields.get_field(
                    pipeline, i, config['bp_cal'].get('field'))
                step = 'pre_bp_cal_{0:d}'.format(i)
                cabtouse = 'cab/casa47_bandpass' if config['casa_version']=='47' else 'cab/casa_bandpass'
               # if config['casa_version']=='47':
               #    cabtouse = 'cab/casa47_bandpass'
               # else:
               #    cabtouse = 'cab/casa_bandpass'
                meerkathi.info('cabtouse=', cabtouse)
                print 'cabtouse=', cabtouse
                recipe.add(cabtouse, step,
                   {
                     "vis"          : msname,
                     "caltable"     : '{0:s}/{1:s}.{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), prefix, 'PREB0'),
                     "field"        : field,
                     "refant"       : refant, #must be enabled to avoid creating an ambiguity in crosshand phase if config['bp_cal'].get('set_refant', True) else '',
                     "solint"       : config['bp_cal'].get('solint'),
                     "combine"      : '',
                     "bandtype"     : "B",
                     "gaintable"    : sdm.dismissable(gaintables),
                     "interp"       : interpolations,
                     "fillgaps"     : 70,
                     "uvrange"      : config['uvrange'],
                     "minsnr"       : config['bp_cal'].get('minsnr'),
                     "minblperant"  : config['bp_cal'].get('minnrbl'),
                     "solnorm"      : config['bp_cal'].get('solnorm'),
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Pre bandpass calibration ms={1:s}'.format(step, msname))

                if pipeline.enable_task(config['bp_cal'],'plot'):

                    step = 'plot_pre_bandpass_{0:d}'.format(i)
                    table = prefix+".PREB0"
                    fieldtoplot = []
                    fieldtoplot.append(utils.get_field_id(msinfo, field)[0])
                    recipe.add('cab/ragavi', step,
                        {
                         "table"        : '{0:s}/{1:s}:{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), table, 'output'),
                         "gaintype"     : "B",
                         "field"        : fieldtoplot,
                         "corr"         : corr_indexes[config['bp_cal']['plot'].get('corr')],
                         "htmlname"     : '{0:s}/'.format(get_dir_path(pipeline.reports, pipeline)) + '{0:s}-PREB0'.format(prefix),
                        },
                        input=pipeline.input,
                        output=pipeline.output,
                        label='{0:s}:: Plot pre bandpass calibration gain caltable={1:s}'.format(step, prefix+".PREB0"))

                # Initial flux calibration ***on BPCAL field*** (will NOT combine scans even if requested for final flux calibration)
                if config.get('otfdelay'):
                    gaintables, interpolations = ['{0:s}/{1:s}.{2:s}'.format(get_dir_path(
                        pipeline.caltables, pipeline), prefix, 'K0:output')], ['nearest']
                else:
                    gaintables, interpolations = [], []
                gaintables += ['{0:s}/{1:s}.{2:s}'.format(get_dir_path(
                    pipeline.caltables, pipeline), prefix, 'PREB0:output')]
                interpolations += ['nearest']
                step = 'pre_gain_cal_flux_{0:d}'.format(i)
                cabtouse = 'cab/casa47_gaincal' if config['casa_version']=='47' else 'cab/casa_gaincal'
                field = manfields.get_field(
                    pipeline, i, config['bp_cal'].get('field'))
                recipe.add('cab/casa_gaincal', step,
                           {
                               "vis": msname,
                               "caltable": '{0:s}/{1:s}.{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), prefix, 'PREG0:output'),
                               "field": field,
                               # must be enabled to avoid creating an ambiguity in crosshand phase if config['gain_cal_flux'].get('set_refant', False) else '',
                               "refant": refant,
                               "solint": config['gain_cal_flux'].get('solint'),
                               "combine": '',
                               "gaintype": "G",
                               "calmode": 'ap',
                               "gaintable": gaintables,
                               "interp": interpolations,
                               "uvrange": config['uvrange'],
                               "minsnr": config['gain_cal_flux'].get('minsnr'),
                               "minblperant": config['gain_cal_flux'].get('minnrbl'),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Pre gain calibration for bandpass ms={1:s}'.format(step, msname))

                if pipeline.enable_task(config['gain_cal_flux'], 'plot'):
                    step = 'plot_pre_gain_cal_flux_{0:d}'.format(i)
                    table = prefix+".PREG0"
                    fieldtoplot = []
                    fieldtoplot.append(utils.get_field_id(msinfo, field)[0])
                    recipe.add('cab/ragavi', step,
                        {
                         "table"        : '{0:s}/{1:s}:{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), table, 'output'),
                         "gaintype"     : "G",
                         "field"        : fieldtoplot,
                         "corr"         : corr_indexes[config['bp_cal']['plot'].get('corr')],
                         "htmlname"     : '{0:s}/'.format(get_dir_path(pipeline.reports, pipeline)) + '{0:s}-PREG0-fcal'.format(prefix),

                        },
                        input=pipeline.input,
                        output=pipeline.output,
                        label='{0:s}:: Plot pre gaincal phase ms={1:s}'.format(step, msname))

            # Final bandpass calibration
            if config.get('otfdelay'):
                gaintables, interpolations = ['{0:s}/{1:s}.{2:s}'.format(get_dir_path(
                    pipeline.caltables, pipeline), prefix, 'K0:output')], ['nearest']

                if config['bp_cal'].get('remove_ph_time_var'):
                    gaintables += ['{0:s}/{1:s}.{2:s}'.format(get_dir_path(
                        pipeline.caltables, pipeline), prefix, 'PREG0:output')]
                    interpolations += ['nearest']
            elif config['bp_cal'].get('remove_ph_time_var'):
                gaintables, interpolations = ['{0:s}/{1:s}.{2:s}'.format(get_dir_path(
                    pipeline.caltables, pipeline), prefix, 'PREG0:output')], ['nearest']
            else:
                gaintables, interpolations = None, ''
            field = manfields.get_field(
                pipeline, i, config['bp_cal'].get('field'))
            step = 'bp_cal_{0:d}'.format(i)
            cabtouse = 'cab/casa47_bandpass' if config['casa_version']=='47' else 'cab/casa_bandpass'
            recipe.add(cabtouse, step,
               {
                 "vis"          : msname,
                 "caltable"     : '{0:s}/{1:s}.{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), prefix, 'B0'),
                 "field"        : field,
                 "refant"       : refant, #must use the reference to avoid creating an ambiguity in crosshand phase if config['bp_cal'].get('set_refant', True) else '',
                 "solint"       : config['bp_cal'].get('solint'),
                 "combine"      : config['bp_cal'].get('combine'),
                 "bandtype"     : "B",
                 "gaintable"    : sdm.dismissable(gaintables),
                 "interp"       : interpolations,
                 "fillgaps"     : 70,
                 "uvrange"      : config['uvrange'],
                 "minsnr"       : config['bp_cal'].get('minsnr'),
                 "minblperant"  : config['bp_cal'].get('minnrbl'),
                 "solnorm"      : config['bp_cal'].get('solnorm'),
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Bandpass calibration ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config['bp_cal'],'flag'):
                flag_gains('bp_cal', config['bp_cal']['flag'])

            if pipeline.enable_task(config['bp_cal'], 'plot'):
                step = 'plot_bandpass_{0:d}'.format(i)
                fieldtoplot = []
                fieldtoplot.append(utils.get_field_id(msinfo, field)[0])
                table = config['bp_cal']['plot'].get('table_name', prefix+".B0")
                recipe.add('cab/ragavi', step,
                    {
                     "table"        : '{0:s}/{1:s}:{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), table, 'output'),
                     "gaintype"     : config['bp_cal']['plot'].get('gaintype'),
                     "field"        : fieldtoplot,
                     "corr"         : corr_indexes[config['bp_cal']['plot'].get('corr')],
                     "htmlname"     : '{0:s}/'.format(get_dir_path(pipeline.reports, pipeline)) + '{0:s}-B0'.format(prefix),

                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Plot bandpass calibration gain caltable={1:s}'.format(step, prefix+".B0"))
        # Final flux calibration
        if pipeline.enable_task(config, 'gain_cal_flux'):
            if config.get('otfdelay'):
                gaintables, interpolations = ['{0:s}/{1:s}.{2:s}'.format(get_dir_path(
                    pipeline.caltables, pipeline), prefix, 'K0:output')], ['nearest']
            else:
                gaintables, interpolations = [], []
            gaintables += ['{0:s}/{1:s}.{2:s}'.format(get_dir_path(
                pipeline.caltables, pipeline), prefix, 'B0:output')]
            interpolations += ['nearest']
            step = 'gain_cal_flux_{0:d}'.format(i)
            field = manfields.get_field(
                pipeline, i, config['gain_cal_flux'].get('field'))
            cabtouse = 'cab/casa47_gaincal' if config['casa_version']=='47' else 'cab/casa_gaincal' 
            recipe.add(cabtouse, step,
               {
                 "vis"          : msname,
                 "caltable"     : '{0:s}/{1:s}.{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), prefix, 'G0:output'),
                 "field"        : field,
                 "refant"       : refant, #must use the reference to avoid creating an ambiguity in the crosshand phase if config['gain_cal_flux'].get('set_refant', True) else '',
                 "solint"       : config['gain_cal_flux'].get('solint'),
                 "combine"      : config['gain_cal_flux'].get('combine'),
                 "gaintype"     : "G",
                 "calmode"      : 'ap',
                 "gaintable"    : gaintables,
                 "interp"       : interpolations,
                 "uvrange"      : config['uvrange'],
                 "minsnr"       : config['gain_cal_flux'].get('minsnr'),
                 "minblperant"  : config['gain_cal_flux'].get('minnrbl'),
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Gain calibration fer bandpass ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config['gain_cal_flux'],'plot'):
                step = 'plot_gain_cal_flux_{0:d}'.format(i)
                table = prefix+".G0"
                fieldtoplot = []
                fieldtoplot.append(utils.get_field_id(msinfo, field)[0])
                recipe.add('cab/ragavi', step,
                    {
                     "table"        : '{0:s}/{1:s}:{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), table, 'output'),
                     "gaintype"     : "G",
                     "field"        : fieldtoplot,
                     "corr"         : corr_indexes[config['bp_cal']['plot'].get('corr')],
                     "htmlname"     : '{0:s}/'.format(get_dir_path(pipeline.reports, pipeline)) + '{0:s}-G0-fcal'.format(prefix)
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Plot gaincal phase ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config['gain_cal_flux'],'flag'):
                flag_gains('gain_cal_flux', config['gain_cal_flux']['flag'])

        # Gain calibration for Gaincal field
        if pipeline.enable_task(config, 'gain_cal_gain'):
            if config.get('otfdelay'):
                gaintables, interpolations = ['{0:s}/{1:s}.{2:s}'.format(get_dir_path(
                    pipeline.caltables, pipeline), prefix, 'K0:output')], ['linear']
            else:
                gaintables, interpolations = [], []
            gaintables += ['{0:s}/{1:s}.{2:s}'.format(get_dir_path(
                pipeline.caltables, pipeline), prefix, 'B0:output')]
            interpolations += ['linear']
            step = 'gain_cal_gain_{0:d}'.format(i)
            field = manfields.get_field(pipeline, i, config['gain_cal_gain'].get('field'))
            cabtouse = 'cab/casa47_gaincal' if config['casa_version']=='47' else 'cab/casa_gaincal'
            recipe.add(cabtouse, step,
               {
                 "vis"          : msname,
                 "caltable"     : '{0:s}/{1:s}.{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), prefix, 'G0:output'),
                 "field"        : field,
                 "refant"       : refant, # must use reference to avoid creating an ambiguity in crosshand phase if config['gain_cal_gain'].get('set_refant', True) else '',
                 "solint"       : config['gain_cal_gain'].get('solint'),
                 "combine"      : config['gain_cal_gain'].get('combine'),
                 "gaintype"     : "G",
                 "calmode"      : 'ap',
                 "gaintable"    : gaintables,
                 "interp"       : interpolations,
                 "uvrange"      : config['uvrange'],
                 "minsnr"       : config['gain_cal_gain'].get('minsnr'),
                 "minblperant"  : config['gain_cal_gain'].get('minnrbl'),
                 "append"       : True,
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Gain calibration ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config['gain_cal_gain'],'flag'):
                flag_gains('gain_cal_gain', config['gain_cal_gain']['flag'])

            if pipeline.enable_task(config['gain_cal_gain'], 'plot'):
                step = 'plot_gain_cal_{0:d}'.format(i)
                table = prefix+".G0"
                fieldtoplot = []
                fieldtoplot.append(utils.get_field_id(msinfo, field)[0])
                recipe.add('cab/ragavi', step,
                    {
                     "table"        : '{0:s}/{1:s}:{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), table, 'output'),
                     "gaintype"     : "G",
                     "field"        : fieldtoplot,
                     "corr"         : corr_indexes[config['bp_cal']['plot'].get('corr')],
                     "htmlname"     : '{0:s}/'.format(get_dir_path(pipeline.reports, pipeline)) +  '{0:s}-G0'.format(prefix)
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Plot gaincal phase ms={1:s}'.format(step, msname))

        # Flux scale transfer
        if pipeline.enable_task(config, 'transfer_fluxscale'):
            ref = manfields.get_field(
                pipeline, i, config['transfer_fluxscale'].get('reference'))
            trans = manfields.get_field(
                pipeline, i, config['transfer_fluxscale'].get('transfer'))
            step = 'transfer_fluxscale_{0:d}'.format(i)
            recipe.add('cab/casa_fluxscale', step,
                       {
                           "vis": msname,
                           "caltable": '{0:s}/{1:s}.{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), prefix, 'G0:output'),
                           "fluxtable": '{0:s}/{1:s}.{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), prefix, 'F0:output'),
                           "reference": ref,
                           "transfer": trans,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Flux scale transfer ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config['transfer_fluxscale'], 'plot'):
                step = 'plot_fluxscale_{0:d}'.format(i)
                table = prefix+".F0"
                fieldtoplot = []
                fieldtoplot.append(utils.get_field_id(msinfo, ref)[0])
                recipe.add('cab/ragavi', step,
                    {
                     "table"        : '{0:s}/{1:s}:{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), table, 'output'),
                     "gaintype"     : "G",
                     "field"        : fieldtoplot,
                     "corr"         : corr_indexes[config['bp_cal']['plot'].get('corr')],
                     "htmlname"     : '{0:s}/'.format(get_dir_path(pipeline.reports, pipeline)) + '{0:s}-F0'.format(prefix)
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Plot gaincal phase ms={1:s}'.format(step, msname))

        applied = []
        for ft in ['bpcal', 'gcal', 'target']:
            gaintablelist, gainfieldlist, interplist = [], [], []
            no_table_to_apply = True
            field = getattr(pipeline, ft)[i]
            for applyme in 'delay_cal bp_cal gain_cal_flux gain_cal_gain transfer_fluxscale'.split():
                if not pipeline.enable_task(config, 'apply_'+applyme):
                    continue
                if ft not in config['apply_'+applyme].get('applyto'):
                    continue
                suffix = table_suffix[applyme]
                interp = applycal_interp_rules[ft][applyme]
                gainfield = get_gain_field(applyme, ft)
                gaintablelist.append('{0:s}/{1:s}.{2:s}:output'.format(
                    get_dir_path(pipeline.caltables, pipeline), prefix, suffix))
                gainfieldlist.append(gainfield)
                interplist.append(interp)
                no_table_to_apply = False

            if no_table_to_apply or field in applied:
                continue

            applied.append(field)
            step = 'apply_{0:s}_{1:d}'.format(ft, i)
            cabtouse = 'cab/casa47_applycal' if config['casa_version']=='47' else 'cab/casa_applycal'
            recipe.add(cabtouse, step,
               {
                "vis"       : msname,
                "field"     : field,
                "gaintable" : gaintablelist,
                "gainfield" : gainfieldlist,
                "interp"    : interplist,
                "calwt"     : [False],
                "parang"    : False,
                "applymode" : config['apply_'+applyme]['applymode'],
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Apply calibration to field={1:s}, ms={2:s}'.format(step, field, msname))

        # auto flag closure errors and systematic issues based on calibrated calibrator phase (chi-squared thresholds)
        # Physical assertion: Expect calibrator phase == 0 (calibrator at phase centre). Corrected phases should be at phase centre
        # Any deviation from this expected value per baseline means that there are baseline-based gains (closure errors)
        # that couldn't be corrected solely antenna-based gains. These baselines must be flagged out of the calibrator and the
        # target fields between calibrator scans which were marked bad.
        # Compare in-between scans per baseline per field per channel
        # Also compare in-between baselines per scan per field per channel
        if pipeline.enable_task(config, 'autoflag_closure_error'):
            step = 'autoflag_closure_error_{0:d}'.format(i)
            def_fields = ','.join(
                [pipeline.bpcal_id[i], pipeline.gcal_id[i], pipeline.target_id[i]])
            def_calfields = ','.join(
                [pipeline.bpcal_id[i], pipeline.gcal_id[i]])
            if config['autoflag_closure_error'].get('fields') != 'auto' and \
               not set(config['autoflag_closure_error'].get('fields').split(',')) <= set(['gcal', 'bpcal', 'target']):
                raise KeyError(
                    "autoflag on phase fields can only be 'auto' or be a combination of 'gcal', 'bpcal' or 'target'")
            if config['autoflag_closure_error'].get('calibrator_fields') != 'auto' and \
               not set(config['autoflag_closure_error'].get('calibrator_fields')) <= set(['gcal', 'bpcal']):
                raise KeyError(
                    "autoflag on phase calibrator fields can only be 'auto' or be a combination of 'gcal', 'bpcal'")

            fields = def_fields if config['autoflag_closure_error'].get('fields') == 'auto' else \
                ",".join([getattr(pipeline, key + "_id")[i]
                          for key in config['autoflag_closure_error'].get('fields').split(',')])
            calfields = def_calfields if config['autoflag_closure_error'].get('calibrator_fields') == 'auto' else \
                ",".join([getattr(pipeline, key + "_id")[i]
                          for key in config['autoflag_closure_error'].get('calibrator_fields').split(',')])

            recipe.add("cab/politsiyakat_cal_phase", step,
                       {
                           "msname": msname,
                           "field": fields,
                           "cal_field": calfields,
                           "scan_to_scan_threshold": config["autoflag_closure_error"]["scan_to_scan_threshold"],
                           "baseline_to_group_threshold": config["autoflag_closure_error"]["baseline_to_group_threshold"],

                           "dpi": config['autoflag_closure_error'].get('dpi'),
                           "plot_size": config['autoflag_closure_error'].get('plot_size'),
                           "nproc_threads": config['autoflag_closure_error'].get('threads'),
                           "data_column": config['autoflag_closure_error'].get('column')
                       },
                       input=pipeline.input, output=pipeline.output,
                       label="{0:s}: Flag out baselines with closure errors")

        if applied or pipeline.enable_task(config, 'autoflag_closure_error'):
            substep = 'flagset_update_{0:s}_{1:d}'.format(wname, i)
            manflags.update_flagset(
                pipeline, recipe, wname, msname, cab_name=substep)

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

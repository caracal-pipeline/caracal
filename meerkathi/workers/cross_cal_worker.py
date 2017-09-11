import sys
import os
import meerkathi.dispatch_crew.utils as utils
import yaml

NAME = "Cross calibration"

# Rules for interpolation mode to use when applying calibration solutions
applycal_interp_rules = {
   'bpcal'    :  {
                  'delay_cal'      : 'nearest', 
                  'bp_cal'         : 'nearest', 
                  'transfer_fluxscale': 'linear',
                  'gain_cal_gain'  : 'linear',
                 },
   'gcal'     :  {
                  'delay_cal'      : 'linear', 
                  'bp_cal'         : 'linear', 
                  'transfer_fluxscale': 'nearest',
                  'gain_cal_gain'  : 'linear',
                 },
   'target'   :  {
                  'delay_cal'      : 'linear', 
                  'bp_cal'         : 'linear', 
                  'transfer_fluxscale': 'linear',
                  'gain_cal_gain'  : 'linear',
                 },
}

table_suffix = {
    "delay_cal"             : 'K0',
    "bp_cal"                : 'B0', 
    "gain_cal_gain"         : 'G0', 
    "gain_cal_flux"         : 'G0', 
    "transfer_fluxscale"    : 'F0', 
}


def worker(pipeline, recipe, config):

    for i in range(pipeline.nobs):

        msname = pipeline.msnames[i]
        refant = pipeline.reference_antenna[i] or '0'
        prefix = pipeline.prefixes[i]
        dataid = pipeline.dataid[i]
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, prefix)
        prefix = '{0:s}-{1:s}'.format(prefix, config.get('label', ''))

        # Check if field was specified as known key, else return the 
        # same value. 
        def get_field(field):
            if field in ['bpcal', 'gcal', 'target', 'fcal']:
                name = getattr(pipeline, field)[i]
            else:
                name = field
            return str(name)

        def get_gain_field(applyme, applyto=None):
            if applyme == 'delay_cal':
                return get_field('bpcal')
            if applyme == 'bp_cal':
                return get_field('bpcal')
            if applyme == 'gain_cal_flux':
                return get_field('fcal')
            if applyme == 'gain_cal_gain':
                return get_field('gcal')
            if applyme == 'transfer_fluxscale':
                if applyto in ['gcal', 'target']:
                    return get_field('gcal')
                elif applyto == 'bpcal':
                    return get_field('bpcal')
            
        def flag_gains(cal, opts):
            opts = dict(opts)
            step = 'plot_{0:s}_{1:d}'.format(cal, i)
            opts["vis"] = '{0:s}.{1:s}:output'.format(prefix, table_suffix[cal])
            opts["datacolumn"] = 'CPARAM'
            recipe.add('cab/casa_flagdata', step, opts,
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Flagging gains'.format(step))

        if pipeline.enable_task(config, 'set_model'):
            # Set model
            field = get_field(config['set_model'].get('field', 'fcal'))
            if config['set_model'].get('no_verify', False):
                opts = {
                    "vis"        : msname,
                    "field"      : field,
                    "scalebychan": True,
                    "usescratch" : True,
                }
            else:
                model = utils.find_in_native_calibrators(msinfo, field)
                standard = utils.find_in_casa_calibrators(msinfo, field)
                # Prefer our standard over the NRAO standard
                if model:
                    opts = {
                      "vis"         : msname,
                      "field"       : field,
                      "standard"    : "manual",
                      "fluxdensity" : model['I'],
                      "reffreq"     : '{0:f}GHz'.format(model['ref']/1e9),
                      "spix"        : [model[a] for a in 'abcd'],
                      "scalebychan" : True,
                      "usescratch"  : False,
                    }
                elif standard:
                   opts = {
                      "vis"         : msname,
                      "field"       : field,
                      "standard"    : config['set_model'].get('standard', standard),
                      "usescratch"  : False,
                      "scalebychan" : True,
                    }
                else:
                    raise RuntimeError('The flux calibrator field "{}" could not be \
found in our database or in the CASA NRAO database'.format(field))
                      
            step = 'set_model_cal_{0:d}'.format(i)
            recipe.add('cab/casa_setjy', step,
               opts,
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Set jansky ms={1:s}'.format(step, msname))
           
        # Delay calibration
        if pipeline.enable_task(config, 'delay_cal'):
            step = 'delay_cal_{0:d}'.format(i)

            field = get_field(config['delay_cal'].get('field', 'bpcal'))
            recipe.add('cab/casa_gaincal', step,
               {
                 "vis"          : msname,
                 "caltable"     : prefix+".K0",
                 "field"        : field,
                 "refant"       : refant,
                 "solint"       : config['delay_cal'].get('solint', 'inf'),
                 "gaintype"     : "K",
                 "uvrange"      : config.get('uvrange', ''),
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Delay calibration ms={1:s}'.format(step, msname))

            if  config['delay_cal'].get('plot', True):
                step = 'plot_delay_cal_{0:d}'.format(i)
                recipe.add('cab/casa_plotcal', step,
                   {
                    "caltable"  : prefix+".K0:output",
                    "xaxis"     : 'antenna',
                    "yaxis"     : 'delay',
                    "field"     : field,
                    "iteration" : 'antenna',
                    "subplot"   : 441,
                    "plotsymbol": 'o',
                    "figfile"   : '{0:s}-K0.png'.format(prefix),
                    "showgui"   : False,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Plot gaincal phase ms={1:s}'.format(step, msname))

        # Set "Combine" to 'scan' for getting combining all scans for BP soln.
        if pipeline.enable_task(config, 'bp_cal'):
            field = get_field(config['bp_cal'].get('field', 'bpcal'))
            step = 'bp_cal_{0:d}'.format(i)
            recipe.add('cab/casa_bandpass', step,
               {
                 "vis"          : msname,
                 "caltable"     : prefix+'.B0',
                 "field"        : field,
                 "refant"       : refant,
                 "solint"       : config['bp_cal'].get('solint', 'inf'),
                 "combine"      : config['bp_cal'].get('combine', ''),
                 "bandtype"     : "B",
                 "gaintable"    : [prefix+'.K0:output'],
                 "fillgaps"     : 70,
                 "uvrange"      : config['uvrange'],
                 "minsnr"       : config['bp_cal'].get('minsnr', 5),
                 "minblperant"  : config['bp_cal'].get('minnrbl', 4),
                 "solnorm"      : config['bp_cal'].get('solnorm', False),
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Bandpass calibration ms={1:s}'.format(step, msname))

            if config['bp_cal'].get('flag', False):
                flag_gains('bp_cal', config['bp_cal']['flag'])

            if config['bp_cal'].get('plot', True):
                 for plot in 'amp','phase':
                    step = 'plot_bandpass_{0:s}_{1:d}'.format(plot, i)
                    recipe.add('cab/casa_plotcal', step,
                       {
                        "caltable"  : prefix+".B0:output",
                        "xaxis"     : 'freq',
                        "yaxis"     : plot,
                        "field"     : field,
                        "iteration" : 'antenna',
                        "subplot"   : 441,
                        "plotsymbol": ',',
                        "figfile"   : '{0:s}-B0-{1:s}.png'.format(prefix, plot),
                        "showgui"   : False,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: plot bandpass calibration gain caltable={1:s}'.format(step, prefix+".B0:output"))

        # Gain calibration for Flux calibrator field
        if pipeline.enable_task(config, 'gain_cal_flux'):
            step = 'gain_cal_flux_{0:d}'.format(i)
            field = get_field(config['gain_cal_flux'].get('field', 'fcal'))
            recipe.add('cab/casa_gaincal', step,
               {
                 "vis"          : msname,
                 "caltable"     : prefix+".G0:output",
                 "field"        : field,
                 "refant"       : refant,
                 "solint"       : config['gain_cal_flux'].get('solint', 'inf'),
                 "combine"      : config['gain_cal_flux'].get('combine', ''),
                 "gaintype"     : "G",
                 "calmode"      : 'ap',
                 "gaintable"    : [prefix+".K0:output",prefix+".B0:output"],
                 "interp"       : ['nearest','nearest'],
                 "uvrange"      : config['uvrange'],
                 "minsnr"       : config['gain_cal_flux'].get('minsnr', 5),
                 "minblperant"  : config['gain_cal_flux'].get('minnrbl', 4),
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Gain calibration for bandpass ms={1:s}'.format(step, msname))

            if config['gain_cal_flux'].get('plot', True):
                for plot in 'amp','phase':
                    step = 'plot_gain_cal_flux_{0:s}_{1:d}'.format(plot, i)
                    recipe.add('cab/casa_plotcal', step,
                       {
                        "caltable"  : prefix+".G0:output",
                        "xaxis"     : 'time',
                        "yaxis"     : plot,
                        "field"     : field,
                        "iteration" : 'antenna',
                        "subplot"   : 441,
                        "plotsymbol": 'o',
                        "figfile"   : '{0:s}-G0-fcal-{1:s}.png'.format(prefix, plot),
                        "showgui"   : False,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Plot gaincal phase ms={1:s}'.format(step, msname))

            if config['gain_cal_flux'].get('flag', False):
                flag_gains('gain_cal_flux', config['gain_cal_flux']['flag'])

        # Gain calibration for Gaincal field
        if pipeline.enable_task(config, 'gain_cal_gain'):
            step = 'gain_cal_gain_{0:d}'.format(i)
            field = get_field(config['gain_cal_gain'].get('field', 'gcal'))
            recipe.add('cab/casa_gaincal', step,
               {
                 "vis"          : msname,
                 "caltable"     : prefix+".G0:output",
                 "field"        : field,
                 "refant"       : refant,
                 "solint"       : config['gain_cal_gain'].get('solint', 'inf'),
                 "combine"      : config['gain_cal_gain'].get('combine', ''),
                 "gaintype"     : "G",
                 "calmode"      : 'ap',
                 "gaintable"    : [prefix+".K0:output",prefix+".B0:output"],
                 "interp"       : ['linear','linear'],
                 "uvrange"      : config['uvrange'],
                 "minsnr"       : config['gain_cal_gain'].get('minsnr', 5),
                 "minblperant"  : config['gain_cal_gain'].get('minnrbl', 4),
                 "append"       : True,
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Gain calibration ms={1:s}'.format(step, msname))

            if config['gain_cal_gain'].get('flag', False):
                flag_gains('gain_cal_gain', config['gain_cal_gain']['flag'])

            if config['gain_cal_gain'].get('plot', True):
                for plot in 'amp','phase':
                    step = 'plot_gain_cal_{0:s}_{1:d}'.format(plot, i)
                    recipe.add('cab/casa_plotcal', step,
                       {
                        "caltable"  : prefix+".G0:output",
                        "xaxis"     : 'time',
                        "yaxis"     : plot,
                        "field"     : field,
                        "iteration" : 'antenna',
                        "subplot"   : 441,
                        "plotsymbol": 'o',
                        "figfile"   : '{0:s}-G0-gcal-{1:s}.png'.format(prefix, plot),
                        "showgui"   : False,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Plot gaincal phase ms={1:s}'.format(step, msname))

        # Flux scale transfer
        if pipeline.enable_task(config, 'transfer_fluxscale'):
            ref = get_field(config['transfer_fluxscale'].get('reference', 'fcal'))
            trans = get_field(config['transfer_fluxscale'].get('transfer', 'gcal'))
            step = 'transfer_fluxscale_{0:d}'.format(i)
            recipe.add('cab/casa_fluxscale', step,
               {
                "vis"          : msname,
                "caltable"      : prefix+".G0:output",
                "fluxtable"     : prefix+".F0:output",
                "reference"     : [ref],
                "transfer"      : [trans],
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Flux scale transfer ms={1:s}'.format(step, msname))

            if config['transfer_fluxscale'].get('plot', True):
                for plot in 'amp','phase':
                    step = 'plot_fluxscale_{0:s}_{1:d}'.format(plot, i)
                    recipe.add('cab/casa_plotcal', step,
                       {
                        "caltable"  : prefix+".F0:output",
                        "xaxis"     : 'time',
                        "yaxis"     : plot,
                        "field"     : '',
                        "iteration" : 'antenna',
                        "subplot"   : 441,
                        "plotsymbol": 'o',
                        "figfile"   : '{0:s}-F0-{1:s}.png'.format(prefix, plot),
                        "showgui"   : False,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Plot gaincal phase ms={1:s}'.format(step, msname))

        applied = []
        for ft in ['bpcal','gcal','target']:
            gaintablelist,gainfieldlist,interplist = [],[],[]
            no_table_to_apply = True
            field = getattr(pipeline, ft)[i]
            for applyme in 'delay_cal bp_cal gain_cal_flux gain_cal_gain transfer_fluxscale'.split():
                if not pipeline.enable_task(config, 'apply_'+applyme):
                    continue
                if ft not in config['apply_'+applyme].get('applyto', 'bpcal,gcal,target').split(','):
                   continue
                suffix = table_suffix[applyme]
                interp = applycal_interp_rules[ft][applyme]
                 
                gainfield = get_gain_field(applyme, ft)
                gaintablelist.append(prefix+'.{:s}:output'.format(suffix))
                gainfieldlist.append(gainfield)
                interplist.append(interp)
                no_table_to_apply = False

            if no_table_to_apply or field in applied:
                continue

            applied.append(field)
            step = 'apply_{0:s}_{1:d}'.format(ft, i)
            recipe.add('cab/casa_applycal', step,
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

        if pipeline.enable_task(config, 'flagging_summary'):
            step = 'flagging_summary_crosscal_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata', step,
                {
                  "vis"         : msname,
                  "mode"        : 'summary',
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))

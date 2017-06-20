import sys
import os

NAME = "First generation (cross) calibration"

# Rules for interpolation mode to use when applying calibration solutions
applycal_inpterp_rules = {
   'bpcal'    :  {
                  'delay_cal'    : 'nearest', 
                  'gain_cal_bp'  : 'nearest', 
                  'gain_cal_gain': 'linear',
                  'transfer_fluxscale': 'linear',
                 },
   'gcal'     :  {
                  'delay_cal'    : 'linear', 
                  'gain_cal_bp'  : 'linear', 
                  'transfer_fluxscale': 'nearest',
                 },
   'target'   :  {
                  'delay_cal'    : 'linear', 
                  'gain_cal_bp'  : 'linear', 
                  'gain_cal_gain': 'linear',
                  'transfer_fluxscale': 'linear',
                 },
}

table_suffix = {
    "delay_cal"             : 'K0',
    "bp_cal"                : 'B0', 
    "gain_cal"              : 'G0', 
    "transfer_fluxscale"    : 'F0', 
}

def worker(pipeline, recipe, config):

    for i in range(pipeline.nobs):
        msname = pipeline.msnames[i]
        prefix = pipeline.prefixes[i]
        bpcal = pipeline.bpcal[i]
        gcal = pipeline.gcal[i]
        target = pipeline.target[i]
        refant = pipeline.refants[i]
 
        # Set model
        if config['set_model']['enable']:
            step = 'set_model_cal_{0:d}'.format(i)
            recipe.add('cab/casa_setjy', step,
               {
                  "vis"         : msname,
                  "field"       : bpcal,
                  "standard"    : config['set_model']['standard'],
                  "usescratch"  : False,
                  "scalebychan" : True,
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Set jansky ms={1:s}'.format(step, msname))
            steps.append(step)
           
        # Delay calibration
        if config['delay_cal']['enable']:
            step = 'delay_cal_{0:d}'.format(i)
            recipe.add('cab/casa_gaincal', step,
               {
                 "vis"          : msname,
                 "caltable"     : prefix+".K0",
                 "field"        : bpcal,
                 "refant"       : config['delay_cal']['refant'],
                 "solint"       : "inf",
                 "gaintype"     : "K",
                 "uvrange"      : config['delay_cal']['uvrange'],
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Delay calibration ms={1:s}'.format(step, msname))
            steps.append(step)
 
        # Set "Combine" to 'scan' for getting combining all scans for BP soln.
        if config['bp_cal']['enable']:
            step = 'bp_cal_{0:d}'.format(i)
            recipe.add('cab/casa_bandpass', step,
               {
                 "vis"          : msname,
                 "caltable"     : prefix+'.B0',
                 "field"        : bpcal,
                 "refant"       : refant,
                 "solint"       : "inf",
                 "combine"      : config['bp_cal']['combine'],
                 "bandtype"     : "B",
                 "gaintable"    : [prefix+'.K0:output'],
                 "fillgaps"     : 70,
                 "uvrange"      : config['bp_cal']['uvrange'],
                 "minsnr"       : config['bp_cal']['minsnr'],
                 "minblperant"  : config['bp_cal']['minnrbl'],
                 "solnorm"      : config['bp_cal']['solnorm'],
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Bandpass calibration ms={1:s}'.format(step, msname))
            steps.append(step)

        # Gain calibration for Bandpass field
        if config['gain_cal_bp']['enable']:
            step = 'gain_cal_bp_{0:d}'.format(i)
            recipe.add('cab/casa_gaincal', step,
               {
                 "vis"          : msname,
                 "caltable"     : prefix+".G0:output",
                 "field"        : bpcal,
                 "refant"       : refant,
                 "solint"       : "inf",
                 "combine"      : config['gain_cal_bp']['combine'],
                 "gaintype"     : "G",
                 "calmode"      : 'ap',
                 "gaintable"    : [prefix+".B0:output",prefix+".K0:output"],
                 "interp"       : ['nearest','nearest'],
                 "uvrange"      : config['gain_cal_bp']['uvrange'],
                 "minsnr"       : config['gain_cal_bp']['minsnr'],
                 "minblperant"  : config['gain_cal_bp']['minnrbl'],
                 "append"       : False,
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Gain calibration for bandpass ms={1:s}'.format(step, msname))
            steps.append(step)

        # Gain calibration for Gaincal field
        if config['gain_cal_gain']['enable']:
            step = 'gain_cal_gain_{0:d}'.format(i)
            recipe.add('cab/casa_gaincal', step,
               {
                 "vis"          : msname,
                 "caltable"     : prefix+".G0:output",
                 "field"        : gcal,
                 "refant"       : refant,
                 "solint"       : "inf",
                 "gaintype"     : "G",
                 "calmode"      : 'ap',
                 "minsnr"       : 5,
                 "gaintable"    : [prefix+".B0:output",prefix+".K0:output"],
                 "interp"       : ['linear','linear'],
                 "append"       : True,
                 "uvrange"      : config['gain_cal_gain']'uvrange'],
                 "minsnr"       : config['gain_cal_gain']['minsnr'],
                 "minblperant"  : config['gain_cal_gain']['minnrbl'],
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Gain calibration ms={1:s}'.format(step, msname))
            steps.append(step)

        #Flux scale transfer
        if config['transfer_fluxscale']['enable']:
            step = 'transfer_fluxscale_{0:d}'.format(i)
            recipe.add('cab/casa_fluxscale', step,
               {
                 "vis"          : msname,
                "caltable"      : prefix+".G0:output",
                "fluxtable"     : prefix+".F0:output",
                "reference"     : [bpcal],
                "transfer"      : [gcal],
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Flux scale transfer ms={1:s}'.format(step, msname))
            steps.append(step)

        for field in [getattr(pipeline, f) for f in 'bpcal','gcal','target']
            gaintablelist,gainfieldlist,interplist = [],[],[]
            for applyme in ['delay_cal', 'bp_cal', 'gain_cal_bp', 'transfer_fluxscale']:
               suffix = table_suffix[applyme]
               interp = applycal_interp_rules[field][applyme]
               if config['apply_'+applyme]['enable']:
                   gaintablelist.append(prefix+'.{:s}:output'.format(suffix))
                   gainfieldlist.append(field)
                   interplist.append(interp)

            step = 'apply_{0:s}_{1:d}'.format(applyme, i)
            recipe.add('cab/casa_applycal', step,
               {
                "vis"       : msname,
                "field"     : field,
                "gaintable" : gaintablelist,
                "gainfield" : gainfieldlist,
                "interp"    : interplist,
                "calwt"     : [False],
                "parang"    : False,
                "applymode" : config[applyme]['applymode'],
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Apply calibration to field={1:s}, ms={2:s}'.format(step, field, msname))
            steps.append(step)

        # Make plots
        if config['make_plots']['enable']:
            # Plot bandpass amplitude
            if config['make_plots']['bandpass']:
                suffix = config['bp_cal']['table_suffix']
                for plot in 'amp','phase':
                    step = 'plot_bandpass_{0:s}_{1:d}'.format(plot, i)
                    recipe.add('cab/casa_plotcal', step,
                       {
                        "caltable"  : prefix+".B0:output",
                        "xaxis"     : 'chan',
                        "yaxis"     : plot,
                        "field"     : bpcal,
                        "iteration" : 'antenna',
                        "subplot"   : 441,
                        "plotsymbol": ',',
                        "figfile"   : '{0:s}-B0-{1:s}.png'.format(prefix, plot),
                        "showgui"   : False,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Plot bandpass amplitude ms={1:s}'.format(step, msname))
                    steps.append(step)

            # Plot gain phase vs time
            if config['make_plots']['fluxscale']:
                suffix = config['transfer_fluxscale']['table_suffix']
                field = config['transfer_fluxscale']['field']
                for plot in 'amp','phase':
                    step = 'plot_fluxscale_{0:s}_{1:d}'.format(plot, i)
                    recipe.add('cab/casa_plotcal', step,
                       {
                        "caltable"  : prefix+".F0:output",
                        "xaxis"     : 'time',
                        "yaxis"     : plot,
                        "field"     : field,
                        "iteration" : 'antenna',
                        "subplot"   : 441,
                        "plotsymbol": 'o',
                        "figfile"   : '{0:s}-F0-{1:s}.png'.format(prefix, plot),
                        "showgui"   : False,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Plot gaincal phase ms={1:s}'.format(step, msname))
                    steps.append(step)

          # Plot gain phase vs time
            if config['make_plots']['gain_cal']:
                field = config['gain_cal']['field']
                for plot in 'amp','phase':
                    step = 'plot_gain_cal_{0:s}_{1:d}'.format(plot, i)
                    recipe.add('cab/casa_plotcal', step,
                       {
                        "caltable"  : prefix+"G:output",
                        "xaxis"     : 'time',
                        "yaxis"     : plot,
                        "field"     : field,
                        "iteration" : 'antenna',
                        "subplot"   : 441,
                        "plotsymbol": 'o',
                        "figfile"   : '{0:s}-G0-{1:s}.png'.format(prefix, plot),
                        "showgui"   : False,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Plot gaincal phase ms={1:s}'.format(step, msname))
                    steps.append(step)

            # Plot corrected real vs imag for bandpass field
            if config['make_plots']['bandpass_reim']:
                step = 'plot_bp_reim_{0:d}'.format(i)
                recipe.add('cab/casa_plotms', step,
                   {
                    "vis"           : msname,
                    "field"         : bpcal,
                    "correlation"   : 'XX,YY',
                    "timerange"     : '',
                    "antenna"       : '',
                    "xaxis"         : 'imag',
                    "xdatacolumn"   : 'corrected',
                    "yaxis"         : 'real',
                    "ydatacolumn"   : 'corrected',
                    "coloraxis"     : 'corr',
                    "plotfile"      : prefix+'-bpcal-reim.png',
                    "uvrange"       : config['bp_cal']['uvrange'],
                    "overwrite"     : True,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Plot imag vs real for bandpass calibrator ms={1:s}'.format(step, msname))
                steps.append(step)

    return steps

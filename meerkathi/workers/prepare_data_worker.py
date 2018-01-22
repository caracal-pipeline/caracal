import os
import sys

NAME = "Prepare data for calibration"

def worker(pipeline, recipe, config):

    for i in range(pipeline.nobs):

        msname = pipeline.msnames[i]
        prefix = pipeline.prefixes[i]
        
        if pipeline.enable_task(config, 'fixvis'):
            step = 'fixvis_{:d}'.format(i)
            recipe.add('cab/casa_fixvis', step,
                {
                    "vis"        : msname,
                    "reuse"      : False,
                    "outputvis"  : msname,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Fix UVW coordinates ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'prepms'):
            step = 'prepms_{:d}'.format(i)
            recipe.add('cab/msutils', step,
                {
                  "msname"  : msname,
                  "command" : 'prep' ,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Add BITFLAG column ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'add_spectral_weights'):
            step = 'estimate_weights_{:d}'.format(i)
            recipe.add('cab/msutils', step,
                {
                  "msname"          : msname,
                  "command"         : 'estimate_weights',
                  "stats_data"      : config['add_spectral_weights'].get('stats_data', 'use_package_meerkat_spec'),
                  "weight_columns"  : config['add_spectral_weights'].get('weight_columns', ['WEIGHT', 'WEIGHT_SPECTRUM']),
                  "noise_columns"   : config['add_spectral_weights'].get('noise_columns', ['SIGMA', 'SIGMA_SPECTRUM']),
                  "write_to_ms"     : config['add_spectral_weights'].get('write_to_ms', True),
                  "plot_stats"      : prefix + '-noise_weights.png',
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Adding Spectral weights using MeerKAT noise specs ms={1:s}'.format(step, msname))

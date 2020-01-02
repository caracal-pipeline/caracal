import os
import sys
from meerkathi.workers.utils import manage_flagsets as manflags

NAME = "Prepare data for calibration"


def worker(pipeline, recipe, config):

    wname = pipeline.CURRENT_WORKER
    for i in range(pipeline.nobs):

        msname = pipeline.msnames[i]
        prefix = pipeline.prefixes[i]

        if pipeline.enable_task(config, 'fixvis'):
            step = 'fixvis_{:d}'.format(i)
            recipe.add('cab/casa_fixvis', step,
                       {
                           "vis": msname,
                           "reuse": False,
                           "outputvis": msname,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Fix UVW coordinates ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, "manage_flags"):

            if config["manage_flags"].get("remove_flagsets", False):
                step = "remove_flags_{0:s}_{1:d}".format(wname, i)
                manflags.delete_cflags(pipeline, recipe, "all", msname, cab_name=step)

            step = "init_legacy_flags_{0:s}_{1:d}".format(wname, i)
            manflags.add_cflags(pipeline, recipe, "legacy", msname, cab_name=step)

        if config["clear_cal"]:
            step = 'clear_cal_{:d}'.format(i)
            fields = set(pipeline.fcal[i] + pipeline.bpcal[i])
            recipe.add('cab/casa_clearcal', step,
                       {
                           "vis": msname,
                           "field" : ",".join(fields),
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Reset MODEL_DATA ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'add_spectral_weights'):
            step = 'estimate_weights_{:d}'.format(i)
            recipe.add('cab/msutils', step,
                       {
                           "msname": msname,
                           "command": 'estimate_weights',
                           "stats_data": config['add_spectral_weights'].get('stats_data'),
                           "weight_columns": config['add_spectral_weights'].get('weight_columns'),
                           "noise_columns": config['add_spectral_weights'].get('noise_columns'),
                           "write_to_ms": config['add_spectral_weights'].get('write_to_ms'),
                           "plot_stats": "{0:s}/{1:s}-{2:s}.png".format('diagnostic_plots', prefix, 'noise_weights'),
                           "plot_stats": prefix + '-noise_weights.png',
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Adding Spectral weights using MeerKAT noise specs ms={1:s}'.format(step, msname))

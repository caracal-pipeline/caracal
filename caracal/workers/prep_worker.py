import os
import sys
import caracal
import numpy as np
from caracal.workers.utils import manage_flagsets as manflags
from caracal.dispatch_crew import utils

NAME = "Prepare Data for Processing"
LABEL = 'prep'

def worker(pipeline, recipe, config):
    label = config['label_in']
    wname = pipeline.CURRENT_WORKER

    for i in range(pipeline.nobs):
        mslist = []
        msn = pipeline.msnames[i][:-3]
        prefix = pipeline.prefixes[i]

        if label=='':
            mslist.append(pipeline.msnames[i])

        elif config['field'] == 'target':
           for target in pipeline.target[i]:
                field = utils.filter_name(target)
                mslist.append('{0:s}-{1:s}_{2:s}.ms'.format(msn, field, label))

        elif config['field'] == 'calibrators':
            mslist.append('{0:s}_{1:s}.ms'.format(msn, label))

        for m in mslist:
            if not os.path.exists(os.path.join(pipeline.msdir, m)):
                raise IOError(
                    "MS file {0:s} does not exist. Please check that is where it should be.".format(m))

        for msname in mslist:
            if pipeline.enable_task(config, 'fixuvw'):
                step = 'fixuvw-ms{:d}'.format(i)
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
                mode = config["manage_flags"]["mode"]
                available_flagversions = manflags.get_flags(pipeline,msname)

                if mode == "legacy":
                    version = "caracal_legacy"
                    if version not in available_flagversions:
                        caracal.log.info('The file {0:s} does not yet have a flag version called "caracal_legacy". Saving the current FLAG column to "caracal_legacy".'.format(msname))
                        step = "save-legacy-{0:s}-ms{1:d}".format(wname, i)
                        manflags.add_cflags(pipeline, recipe, version, msname, cab_name=step)
                    else:
                        caracal.log.info('The file {0:s} already has a flag version called "caracal_legacy". Restoring it.'.format(msname))
                        version = "caracal_legacy"
                        step = "restore-flags-{0:s}-ms{1:d}".format(wname, i)
                        manflags.restore_cflags(pipeline, recipe, version,
                                msname, cab_name=step)
                        if available_flagversions[-1] != version:
                            step = 'delete-flag_versions-after-{0:s}-ms{1:d}'.format(version, i)
                            manflags.delete_cflags(pipeline, recipe,
                                available_flagversions[available_flagversions.index(version)+1],
                                msname, cab_name=step)
                elif mode == "restore":
                    version = config["manage_flags"]["version"]
                    if version == 'auto':
                        version = '{0:s}_{1:s}_before'.format(pipeline.prefix,wname)
                    if version in available_flagversions:
                        step = "restore-flags-{0:s}-ms{1:d}".format(wname, i)
                        manflags.restore_cflags(pipeline, recipe, version,
                                msname, cab_name=step)
                        if available_flagversions[-1] != version:
                            step = 'delete-flag_versions-after-{0:s}-ms{1:d}'.format(version, i)
                            manflags.delete_cflags(pipeline, recipe,
                                available_flagversions[available_flagversions.index(version)+1],
                                msname, cab_name=step)
                    else:
                        caracal.log.error('The flag version {0:s} you asked to restore does not exist for {1:s}.'.format(version, msname))
                        if version == "caracal_legacy":
                            caracal.log.error('You may actually want to create that "caracal legacy" flag version with:')
                            caracal.log.error('    prepare_data: manage_flags: mode: save_legacy_flags')
                        raise RuntimeError('Flag version conflicts')

            if config["clearcal"]:
                step = 'clearcal-ms{:d}'.format(i)
                fields = set(pipeline.fcal[i] + pipeline.bpcal[i])
                recipe.add('cab/casa_clearcal', step,
                           {
                               "vis": msname,
                               "field" : ",".join(fields),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Reset MODEL_DATA ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, "specweights"):
                specwts = config['specweights']["mode"]
                if specwts == "uniform":
                    step = 'init_ws-ms{:d}'.format(i)
                    recipe.add('cab/casa_script', step,
                               {
                                   "vis": msname,
                                   "script" : "vis = os.path.join(os.environ['MSDIR'], '{:s}')\n" \
                                               "initweights(vis=vis, wtmode='weight', dowtsp=True)".format(msname),
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: Adding Spectral weights using MeerKAT noise specs ms={1:s}'.format(step, msname))

                elif specwts == "calculate":
                    _config = config["specweights"]
                    step = 'calculate_ws-ms{:d}'.format(i)
                    recipe.add('cab/msutils', step,
                               {
                                   "msname": msname,
                                   "command": 'estimate_weights',
                                   "stats_data": _config['calculate']['statsfile'],
                                   "weight_columns": _config['calculate']['weightcols'],
                                   "noise_columns": _config['calculate']['noisecols'],
                                   "write_to_ms": _config['calculate']['apply'],
                                   "plot_stats": prefix + '-noise_weights.png',
                               },
                               input=pipeline.input,
                               output=pipeline.diagnostic_plots,
                               label='{0:s}:: Adding Spectral weights using MeerKAT noise specs ms={1:s}'.format(step, msname))

                elif specwts == "delete":
                    step = 'delete_ws-ms{:d}'.format(i)
                    recipe.add('cab/casa_script', step,
                               {
                                   "vis": msname,
                                   "script" : "vis = os.path.join(os.environ['MSDIR'], '{msname:s}') \n" \
                                              "colname = '{colname:s}' \n" \
                                              "tb.open(vis, nomodify=False) \n" \
                                              "try: tb.colnames().index(colname) \n" \
                                              "except ValueError: pass \n" \
                                              "finally: tb.close(); quit \n" \
                                              "tb.open(vis, nomodify=False) \n" \
                                              "try: tb.removecols(colname) \n" \
                                              "except RuntimeError: pass \n" \
                                              "finally: tb.close()".format(msname=msname, colname="WEIGHT_SPECTRUM"),
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: deleting WEIGHT_SPECTRUM if it exists ms={1:s}'.format(step, msname))
                else:
                    raise RuntimeError("Specified specweights [{0:s}] mode is unknown".format(specwts))

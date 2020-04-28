import os
import sys
import caracal
import numpy as np
from caracal.workers.utils import manage_flagsets as manflags
from caracal.dispatch_crew import utils

NAME = "Prepare data for calibration"
LABEL = 'prepare_data'

def worker(pipeline, recipe, config):
    label = config['label_in']
    wname = pipeline.CURRENT_WORKER

    for i in range(pipeline.nobs):
        mslist = []
        msn = pipeline.msnames[i][:-3]
        prefix = pipeline.prefixes[i]

        if config['field'] == 'target':
           for target in pipeline.target[i]:
                field = utils.filter_name(target)
                mslist.append(pipeline.msnames[i] if label == \
                   '' else '{0:s}-{1:s}_{2:s}.ms'.format(msn, field, label))

        elif config['field'] == 'calibrators':
            mslist.append(pipeline.msnames[i] if label == \
                  '' else '{0:s}_{1:s}.ms'.format(msn, label))

        else:
            raise ValueError("Eligible values for 'field' if 'label_in' is not empty: 'target' or 'calibrators'. "\
                                 "User selected: '{}'".format(config['field']))

        mslist = np.unique(np.array(mslist)).tolist()

        for m in mslist:
            if not os.path.exists(os.path.join(pipeline.msdir, m)):
                raise IOError(
                    "MS file {0:s} does not exist. Please check that is where it should be.".format(m))

        for msname in mslist:
            if pipeline.enable_task(config, 'fixvis'):
                step = 'fixvis-ms{:d}'.format(i)
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
                version = config["manage_flags"]["version"]
                available_flagversions = manflags.get_flags(pipeline,msname)

                if mode == "save_legacy_flags":
                    if "caracal_legacy" not in available_flagversions:
                        step = "save-legacy-{0:s}-ms{1:d}".format(wname, i)
                        manflags.add_cflags(pipeline, recipe, "caracal_legacy", msname, cab_name=step)
                    elif config['manage_flags']['overwrite_legacy_flags']:
                        step = "save-legacy-{0:s}-ms{1:d}".format(wname, i)
                        manflags.add_cflags(pipeline, recipe, "caracal_legacy", msname,
                            cab_name=step, overwrite=config['manage_flags']['overwrite_legacy_flags'])
                    else:
                        caracal.log.error('You asked to save the current FLAG column to a legacy flag version called "caracal_legacy"')
                        caracal.log.error('but that already exists. Caracal will not overwrite it unless you explicitely request that')
                        caracal.log.error('by setting in the configuration file:')
                        caracal.log.error('    prepare_data: manage_flags: overwrite_legacy_flags: true')
                        caracal.log.error('Think twice whether you really need to do this.')
                        raise RuntimeError('Flag version conflicts')

                elif mode == "restore":
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
                            caracal.log.error('    prepare_data: manage_flags: mode: save_legacy')
                        raise RuntimeError('Flag version conflicts')

                #elif mode == "unflag_and_reset":
                #    step = "reset_flags_{0:s}_{1:d}".format(wname, i)
                #    manflags.delete_cflags(pipeline, recipe, "all", msname, cab_name=step)
                #    # Unflag data
                #    step = "unflag_all_{0:s}_{1:d}".format(wname, i)
                #    recipe.add("cab/casa_flagdata", step,
                #            {
                #                "vis" : msname,
                #                "mode" : "unflag",
                #                "flagbackup" : False,
                #            },
                #            input=pipeline.input,
                #            output=pipeline.output,
                #            label="{0:s}:: Save current flags".format(step))

                #elif mode == "save":
                #    step = "save_flags_{0:s}_{1:d}".format(wname, i)
                #    manflags.add_cflags(pipeline, recipe, version, msname, cab_name=step)

                #elif mode == "list":
                #    step = "list_flags_{0:s}_{1:d}".format(wname, i)
                #    recipe.add("cab/casa_flagmanager", step,
                #            {
                #                "vis" : msname,
                #                "mode" : "list",
                #            },
                #            input=pipeline.input,
                #            output=pipeline.output,
                #            label="{0:s}:: List flag versions".format(step))
                #    caracal.log.warning("manage_flags mode is 'list'. Listing flag versions only!")

            if config["clear_cal"]:
                step = 'clear_cal-ms{:d}'.format(i)
                fields = set(pipeline.fcal[i] + pipeline.bpcal[i])
                recipe.add('cab/casa_clearcal', step,
                           {
                               "vis": msname,
                               "field" : ",".join(fields),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Reset MODEL_DATA ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, "spectral_weights"):
                specwts = config['spectral_weights']["mode"]
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

                elif specwts == "estimate":
                    _config = config["spectral_weights"]
                    step = 'estimate_ws-ms{:d}'.format(i)
                    recipe.add('cab/msutils', step,
                               {
                                   "msname": msname,
                                   "command": 'estimate_weights',
                                   "stats_data": _config['estimate'].get('stats_data'),
                                   "weight_columns": _config['estimate'].get('weight_columns'),
                                   "noise_columns": _config['estimate'].get('noise_columns'),
                                   "write_to_ms": _config['estimate'].get('write_to_ms'),
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
                    raise RuntimeError("Specified spectral_weights [{0:s}] mode is unknown".format(specwts))

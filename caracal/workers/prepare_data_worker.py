import os
import sys
import caracal
from caracal.workers.utils import manage_flagsets as manflags

NAME = "Prepare data for calibration"
LABEL = 'prepare_data'

def worker(pipeline, recipe, config):

    wname = pipeline.CURRENT_WORKER
    for i in range(pipeline.nobs):
        msname = pipeline.msnames[i]
        prefix = pipeline.prefixes[i]

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
            versions = manflags.get_flags(pipeline, msname)
            nolegacy = "caracal_legacy" not in versions

            if mode == "restore":
                if version == "caracal_legacy" and nolegacy:
                    step = "save_legacy-{0:s}-ms{1:d}".format(wname, i)
                    manflags.add_cflags(pipeline, recipe, "caracal_legacy", msname, cab_name=step)
                elif version in versions:
                    step = "restore_flags-{0:s}-ms{1:d}".format(wname, i)
                    manflags.restore_cflags(pipeline, recipe, version, 
                            msname, cab_name=step)
                else:
                    raise RuntimeError("Flag version '{0:s}' not found in" \
                            "flagversions file".format(version))
            elif mode == "initialize":
                step = "save_legacy-{0:s}-ms{1:d}".format(wname, i)
                manflags.add_cflags(pipeline, recipe, "caracal_legacy", msname, cab_name=step)
            elif mode == "reset":
                step = "reset_flags-{0:s}-ms{1:d}".format(wname, i)
                manflags.delete_cflags(pipeline, recipe, "all", msname, cab_name=step)
                # Unflag data
                step = "unflag_all-{0:s}-ms{1:d}".format(wname, i)
                recipe.add("cab/casa_flagdata", step, 
                        {
                            "vis" : msname,
                            "mode" : "unflag",
                            "flagbackup" : False,
                        }, 
                        input=pipeline.input, 
                        output=pipeline.output, 
                        label="{0:s}:: Save current flags".format(step))
            elif mode == "save":
                step = "save_flags-{0:s}-ms{1:d}".format(wname, i)
                manflags.add_cflags(pipeline, recipe, version, msname, cab_name=step)
            elif mode == "list":
                step = "list_flags-{0:s}-ms{1:d}".format(wname, i)
                recipe.add("cab/casa_flagmanager", step, 
                        {
                            "vis" : msname,
                            "mode" : "list",
                        }, 
                        input=pipeline.input, 
                        output=pipeline.output,
                        label="{0:s}:: List flag versions".format(step))
                caracal.log.warning("manage_flags mode is 'list'. Listing flag versions only!")
            else:
                raise ValueError("Mode given for manage_flags worker is invalid. Valid options are reset, restore, save, list")

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

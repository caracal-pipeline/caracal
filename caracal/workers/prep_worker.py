import os

import numpy as np

import caracal
import caracal.dispatch_crew.caltables as mkct
from caracal.dispatch_crew.utils import closeby
from caracal.workers.utils import manage_flagsets as manflags

NAME = "Prepare Data for Processing"
LABEL = "prep"


def getfield_coords(info, field, db, tol=2.9e-3, tol_diff=4.8481e-6):
    """
    Shameless copy of the hetfield function to return field coordinates
    from the database.
    Find match of fields in info
    Parameters:
    info (dict): dictionary of obsinfo as read by yaml
    field (str): field name
    db (dict):   calibrator data base as returned by
                calibrator_database()
    Go through all calibrators in db and return the first that matches
    the coordinates of field in msinfo. Return empty string if not
    found.
    If coordinates difference is larger than tol_diff, return the correct coordinates, else return empty string.
    """

    # Get position of field in msinfo
    ind = info["FIELD"]["NAME"].index(field)
    firade = info["FIELD"]["DELAY_DIR"][ind][0]
    firade[0] = np.mod(firade[0], 2 * np.pi)
    dbcp = db.db
    caracal.log.info("Checking for crossmatch")
    caracal.log.info(f"Database keys: {dbcp.keys()}")
    for key in dbcp.keys():  # noqa: SIM118
        carade = [dbcp[key]["ra"], dbcp[key]["decl"]]
        if closeby(carade, firade, tol=tol):
            if not closeby(carade, firade, tol=tol_diff):
                return key, dbcp[key]["ra"], dbcp[key]["decl"]
            else:
                caracal.log.info("Calibrator coordinates match within the specified tolerance.")
                return None, None, None
    return None, None, None


def worker(pipeline, recipe, config):
    label = config["label_in"]
    wname = pipeline.CURRENT_WORKER
    field_name = config["field"]
    msdir = pipeline.msdir
    for i in range(pipeline.nobs):
        prefix_msbase = pipeline.prefix_msbases[i]
        mslist = pipeline.get_mslist(i, label, target=(field_name == "target"))

        for msname in mslist:
            if not os.path.exists(os.path.join(msdir, msname)):
                caracal.log.error(f"MS file {msdir}/{msname} does not exist. Please check that is where it should be.")
                raise OSError

            # if pipeline.enable_task(config, 'fixcalcoords'):
            tol = config["tol"]
            tol_diff = config["tol_diff"]
            # Convert tolerance from arcseconds to radians:
            tol = tol * np.pi / (180.0 * 3600.0)
            tol_diff = tol_diff * np.pi / (180.0 * 3600.0)
            db = mkct.calibrator_database()
            dbc = mkct.casa_calibrator_database()
            msdict = pipeline.get_msinfo(msname)
            ra_corr = None
            dec_corr = None

            if field_name != "target":
                for f in pipeline.bpcal[i]:
                    fielddb, ra_corr, dec_corr = getfield_coords(msdict, f, db, tol=tol, tol_diff=tol_diff)
                    if fielddb is None:
                        caracal.log.info("Checking the CASA database of calibrators.")
                        fielddb, ra_corr, dec_corr = getfield_coords(msdict, f, dbc, tol=tol, tol_diff=tol_diff)
                    if fielddb is not None:
                        caracal.log.info(f"The coordinates of calibrator {f} in the MS are offset. This is a known problem for some vintage MeerKAT MSs.")

                        if pipeline.enable_task(config, "fixcalcoords"):
                            caracal.log.info("We will now attempt to fix this by rephasing the visibilities using the CASA fixvis task.")
                            ra_corr = float(ra_corr * 180.0 / np.pi)
                            dec_corr = float(dec_corr * 180.0 / np.pi)

                            def needs_astropy():
                                from astropy.coordinates import SkyCoord

                                return SkyCoord(ra_corr, dec_corr, unit="deg")  # noqa: B023

                            c = needs_astropy()
                            coordstring = "J2000 " + c.to_string("hmsdms")
                            step = f"fixuvw-ms{i:d}-{f:s}"
                            recipe.add(
                                "cab/casa_fixvis",
                                step,
                                {
                                    "vis": msname,
                                    "field": f,
                                    "phasecenter": coordstring,
                                    "reuse": False,
                                    "outputvis": msname,
                                },
                                input=pipeline.input,
                                output=pipeline.output,
                                label=f"{step:s}:: Fix bpcal coordinates ms={msname:s}",
                            )
                        else:
                            caracal.log.error("###### WE RECOMMEND SWITCHING ON THE fixcalcoords OPTION #######")

            if pipeline.enable_task(config, "fixuvw"):
                # fielddb, ra_corr, dec_corr = getfield_coords(msdict, f, db)
                step = f"fixuvw-ms{i:d}"
                recipe.add(
                    "cab/casa_fixvis",
                    step,
                    {
                        "vis": msname,
                        "reuse": False,
                        "outputvis": msname,
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label=f"{step:s}:: Fix UVW coordinates ms={msname:s}",
                )

            if pipeline.enable_task(config, "manage_flags"):
                mode = config["manage_flags"]["mode"]
                available_flagversions = manflags.get_flags(pipeline, msname)

                if mode == "legacy":
                    version = "caracal_legacy"
                    if version not in available_flagversions:
                        caracal.log.info(f"The file {msname} does not yet have a flag version called 'caracal_legacy'.Saving the current FLAG column to 'caracal_legacy'.")
                        step = f"save-legacy-{wname:s}-ms{i:d}"
                        manflags.add_cflags(pipeline, recipe, version, msname, cab_name=step)
                    else:
                        caracal.log.info(f"The file {msname:s} already has a flag version called 'caracal_legacy'. Restoring it.")
                        version = "caracal_legacy"
                        step = f"restore-flags-{wname:s}-ms{i:d}"
                        manflags.restore_cflags(pipeline, recipe, version, msname, cab_name=step)
                        if available_flagversions[-1] != version:
                            step = f"delete-flag_versions-after-{version:s}-ms{i:d}"
                            manflags.delete_cflags(
                                pipeline,
                                recipe,
                                available_flagversions[available_flagversions.index(version) + 1],
                                msname,
                                cab_name=step,
                            )
                elif mode == "restore":
                    version = config["manage_flags"]["version"]
                    if version == "auto":
                        version = f"{pipeline.prefix:s}_{wname:s}_before"
                    if version in available_flagversions:
                        step = f"restore-flags-{wname:s}-ms{i:d}"
                        manflags.restore_cflags(pipeline, recipe, version, msname, cab_name=step)
                        if available_flagversions[-1] != version:
                            step = f"delete-flag_versions-after-{version:s}-ms{i:d}"
                            manflags.delete_cflags(
                                pipeline,
                                recipe,
                                available_flagversions[available_flagversions.index(version) + 1],
                                msname,
                                cab_name=step,
                            )
                    else:
                        caracal.log.error(f"The flag version {version:s} you asked to restore does not exist for {msname:s}.")
                        if version == "caracal_legacy":
                            caracal.log.error("You may actually want to create that 'caracal legacy' flag version with:")
                            caracal.log.error("    prepare_data: manage_flags: mode: save_legacy_flags")
                        raise RuntimeError("Flag version conflicts")

            if pipeline.enable_task(config, "clearcal"):
                step = f"clearcal-ms{i:d}"
                fields = set(pipeline.fcal[i] + pipeline.bpcal[i])
                recipe.add(
                    "cab/casa_clearcal",
                    step,
                    {"vis": msname, "field": ",".join(fields), "addmodel": config["clearcal"]["addmodel"]},
                    input=pipeline.input,
                    output=pipeline.output,
                    label=f"{step:s}:: Reset MODEL_DATA ms={msname:s}",
                )

            if pipeline.enable_task(config, "specweights"):
                specwts = config["specweights"]["mode"]
                if specwts == "uniform":
                    step = f"init_ws-ms{i:d}"
                    recipe.add(
                        "cab/casa_script",
                        step,
                        {
                            "vis": msname,
                            "script": f"vis = os.path.join(os.environ['MSDIR'], '{msname:s}')\ninitweights(vis=vis, wtmode='ones', dowtsp=True)",
                        },
                        input=pipeline.input,
                        output=pipeline.output,
                        label=f"{step:s}:: Adding Spectral weights using MeerKAT noise specs ms={msname:s}",
                    )

                elif specwts == "calculate":
                    _config = config["specweights"]
                    step = f"calculate_ws-ms{i:d}"
                    recipe.add(
                        "cab/msutils",
                        step,
                        {
                            "msname": msname,
                            "command": "estimate_weights",
                            "stats_data": _config["calculate"]["statsfile"],
                            "weight_columns": _config["calculate"]["weightcols"],
                            "noise_columns": _config["calculate"]["noisecols"],
                            "write_to_ms": _config["calculate"]["apply"],
                            "plot_stats": prefix_msbase + "-noise_weights.png",
                        },
                        input=pipeline.input,
                        output=pipeline.diagnostic_plots,
                        label=f"{step:s}:: Adding Spectral weights using MeerKAT noise specs ms={msname:s}",
                    )

                elif specwts == "delete":
                    step = f"delete_ws-ms{i:d}"
                    recipe.add(
                        "cab/casa_script",
                        step,
                        {
                            "vis": msname,
                            "script": "vis = os.path.join(os.environ['MSDIR'], '{msname:s}') \n"
                            "colname = '{colname:s}' \n"
                            "tb.open(vis, nomodify=False) \n"
                            "try: tb.colnames().index(colname) \n"
                            "except ValueError: pass \n"
                            "finally: tb.close(); quit \n"
                            "tb.open(vis, nomodify=False) \n"
                            "try: tb.removecols(colname) \n"
                            "except RuntimeError: pass \n"
                            "finally: tb.close()".format(msname=msname, colname="WEIGHT_SPECTRUM"),
                        },
                        input=pipeline.input,
                        output=pipeline.output,
                        label=f"{step:s}:: deleting WEIGHT_SPECTRUM if it exists ms={msname:s}",
                    )
                else:
                    raise RuntimeError(f"Specified specweights [{specwts:s}] mode is unknown")

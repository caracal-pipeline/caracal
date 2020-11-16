# -*- coding: future_fstrings -*-
import os
import sys
import caracal
from caracal.dispatch_crew.utils import closeby
import caracal.dispatch_crew.caltables as mkct
import numpy as np
from caracal.workers.utils import manage_flagsets as manflags
from caracal.dispatch_crew import utils
from astropy.coordinates import SkyCoord

NAME = "Prepare Data for Processing"
LABEL = 'prep'

def getfield_coords(info, field, db, tol=2.9E-3, tol_diff=4.8481E-6):
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
    ind = info['FIELD']['NAME'].index(field)
    firade = info['FIELD']['DELAY_DIR'][ind][0]
    firade[0] = np.mod(firade[0],2*np.pi)
    dbcp = db.db
    print("Checking for crossmatch")
    print("Database keys:", dbcp.keys())
    for key in dbcp.keys():
        carade = [dbcp[key]['ra'],dbcp[key]['decl']]
        if closeby(carade, firade, tol=tol):
            if not closeby(carade, firade, tol=tol_diff):
               return key, dbcp[key]['ra'], dbcp[key]['decl']
            else :
               print("Calibrator coordinates match within the specified tolerance.")
               return None, None, None
        return None, None, None   

def worker(pipeline, recipe, config):
    label = config['label_in']
    wname = pipeline.CURRENT_WORKER
    field_name = config["field"]
    msdir = pipeline.msdir
    for i in range(pipeline.nobs):
        prefix_msbase = pipeline.prefix_msbases[i]
        mslist  = pipeline.get_mslist(i, label, target=(field_name == "target"))


        for msname in mslist:
            if not os.path.exists(os.path.join(msdir, msname)):
                caracal.log.error(f"MS file {msdir}/{msname} does not exist. Please check that is where it should be.")
                raise IOError
            
            #if pipeline.enable_task(config, 'fixcalcoords'):
            tol = config["tol"]
            tol_diff = config["tol_diff"]
            #Convert tolerance from arcseconds to radians:
            tol = tol*np.pi/(180.0*3600.0)
            tol_diff = tol_diff*np.pi/(180.0*3600.0)
            db = mkct.calibrator_database()
            dbc = mkct.casa_calibrator_database()
            msdict = pipeline.get_msinfo(msname)
            ra_corr = None
            dec_corr = None
            if field_name != 'target':
                for f in pipeline.bpcal[i]:
                    print(f, tol, tol_diff)
                    fielddb, ra_corr, dec_corr = getfield_coords(msdict, f, db, tol = tol, tol_diff = tol_diff)
                    print("fielddb", fielddb)
                    if fielddb is None:
                      print("Checking the CASA database of calibrators.")
                      fielddb, ra_corr, dec_corr = getfield_coords(msdict, f, dbc, tol = tol, tol_diff = tol_diff)
                    if fielddb is not None:
                      caracal.log.info("The coordinates of calibrator {0:s} in the MS are offset. This is a known problem for some vintage MeerKAT MSs.".format(f))

                      if pipeline.enable_task(config, 'fixcalcoords'): 

                         caracal.log.info("We will now attempt to fix this by rephasing the visibilities using the CASA fixvis task.")
                         ra_corr = float(ra_corr*180.0/np.pi)
                         dec_corr = float(dec_corr*180.0/np.pi)
                         c = SkyCoord(ra_corr, dec_corr, unit='deg')
                         rahms = c.ra.hms
                         decdms = c.dec.dms
                         coordstring = 'J2000 '+c.to_string('hmsdms')
                         step = 'fixuvw-ms{0:d}-{1:s}'.format(i,f)
                         recipe.add('cab/casa_fixvis', step,
                             {
                                "vis": msname,
                                "field": f,
                                "phasecenter": coordstring,
                                "reuse": False,
                                "outputvis": msname,
                             },
                             input=pipeline.input,
                             output=pipeline.output,
                             label='{0:s}:: Fix bpcal coordinates ms={1:s}'.format(step, msname))
                      else:
                        caracal.log.error("###### WE RECOMMEND SWITCHING ON THE fixcalcoords OPTION #######")
                  
            if pipeline.enable_task(config, 'fixuvw'):
                #fielddb, ra_corr, dec_corr = getfield_coords(msdict, f, db)
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
                                   "plot_stats": prefix_msbase + '-noise_weights.png',
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

import sys
import os
from meerkathi import log
import meerkathi.dispatch_crew.utils as utils
import yaml
from stimela.dismissable import dismissable as sdm
from meerkathi.workers.utils import manage_flagsets as manflields
import stimela

NAME = "Crosshand calibration"


def worker(pipeline, recipe, config):
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
        REFANT = refant = pipeline.reference_antenna[i] or '0'
        prefix = prefixes[i]
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, prefix)
        PREFIX = prefix = '{0:s}-{1:s}'.format(prefix, config.get('label'))
        avgmsname = PREFIX + ".avg.ms"
        INPUT = pipeline.input
        MSDIR = pipeline.msdir
        OUTPUT = pipeline.output
        solve_uvdist = config.get("solve_uvdist")
        pol_mstimeavg = config.get("preaverage_time")
        pol_solchanavg = config.get("preaverage_freq")
        # default to the entire band, but it may be better to use a clean section
        time_solfreqsel = config.get("timesol_solfreqsel")
        time_solint = config.get("timesol_soltime")  # default 1 per scan
        # all time after DC is removed
        freq_solint = config.get("freqsol_soltime")

        # !! IMPORTANT !!
        # 1. We assume a linear feed system
        # 2. We assume that 1GC has already been run prior to this worker
        # such that the diagonals are properly calibrated, save for X-Y phases (X Jones)

        # we'll calibrate the crosshand slope and phase
        # if we have SNR on the crosshands (e.g 3C286)
        # this will calibrate out rotation of U into V
        G1 = PREFIX + ".G1"
        KX = PREFIX + ".KX"
        Xref = PREFIX + ".Xref"
        Xf = PREFIX + ".Xf"

        # we'll calibrate leakages from I -> U +/- V either way
        # for this we need a source devoid of polarized emission
        # e.g. PKS 1934-638 or a very good modelled polarized calibrator
        Dref = PREFIX + ".Dref"
        Df = PREFIX + ".Df"

        polarized_calibrators = {"3C138": {"standard": "manual",
                                           "fluxdensity": [8.4012],
                                           "spix": [-0.54890527955337987, -0.069418066176041668, -0.0018858519926001926],
                                           "reffreq": "1.45GHz",
                                           "polindex": [0.075],
                                           "polangle": [-0.19199]},
                                 "3C286": {"standard": "manual",
                                           "fluxdensity": [14.918703],
                                           "spix": [-0.50593909976893958, -0.070580431627712076, 0.0067337240268301466],
                                           "reffreq": "1.45GHz",
                                           "polindex": [0.095],
                                           "polangle": [0.575959]},
                                 }
        polarized_calibrators["J1331+3030"] = polarized_calibrators["3C286"]
        polarized_calibrators["J0521+1638"] = polarized_calibrators["3C138"]
        # only 1934 is really unpolarized, don't recommend anything else
        # 0407-65 is very partially polarized (< 1%), so it is probably okish as leakage calibrator
        unpolarized_calibrators = ["PKS1934-63", "J1939-6342", "J1938-6341", "PKS 1934-638", "PKS 1934-63", "PKS1934-638",
                                   "PKS0407-65", "0408-65", "J0407-6552"]

        DISABLE_CROSSHAND_PHASE_CAL = False
        if get_field("xcal") == "":
            DISABLE_CROSSHAND_PHASE_CAL = True
            log.warn("SEVERE: No crosshand calibrators set. Cannot calibrate U -> V rotation. You will have an arbitrary "
                     "instrumental polarization angle in your data! DO NOT TAKE ANGLE MEASUREMENTS")

        if not DISABLE_CROSSHAND_PHASE_CAL and get_field("xcal") not in set(polarized_calibrators.keys()):
            raise RuntimeError("Field(s) {0:s} not recognized crosshand calibrators. Unset xcal marked sources to disable X Jones calibration."
                               "Currently the following are recognized "
                               "crosshand phase calibrators: {1:s}".format(get_field("xcal"), ", ".join(list(polarized_calibrators.keys()))))

        if get_field("fcal") not in set(unpolarized_calibrators):
            raise RuntimeError("Field(s) {0:s} not recognized leakage calibrators. Currently the following are recognized "
                               "leakage calibrators: {1:s}".format(get_field("fcal"), ", ".join(unpolarized_calibrators)))

        def __check_linear_feeds(ms):
            from pyrap.tables import table as tbl
            import numpy as np
            with tbl("%s::FEED" % os.path.join(MSDIR, ms), readonly=False, ack=False) as t:
                ptype = t.getcol("POLARIZATION_TYPE")["array"]
                feed_types = set(ptype)
                if set(["X", "Y"]) != feed_types:
                    raise RuntimeError(
                        "Only linear feed systems supported at the moment. Cannot proceed with crosshand calibration.")

        __check_linear_feeds(msname)

        ######################################################################################################################
        #
        # Prepare crosshand data for calibration
        #
        ######################################################################################################################
        global_recipe = recipe
        recipe = stimela.Recipe(
            "Prepare crosshand calibration data", ms_dir=pipeline.msdir)

        # Need to average down the calibrators to get enough SNR on the crosshands
        # for solving
        recipe.add("cab/casa_oldsplit", "split_avg_data", {
            "vis": msname,
            "outputvis": avgmsname,
            "datacolumn": "corrected",
            # KEEP fieldIDs the same for now
            "field": get_field("bpcal,gcal,fcal,xcal") if not DISABLE_CROSSHAND_PHASE_CAL else get_field("bpcal,gcal,fcal"),
            "timebin": pol_mstimeavg,
            "width": pol_solchanavg,
        },
            input=INPUT, output=OUTPUT, label="split_avg_data")

        # First solve for crosshand delays with respect to the refant
        # A stronly polarized source is needed for SNR purposes

        def __correct_feed_convention(ms):
            from pyrap.tables import table as tbl
            import numpy as np
            with tbl("%s::FEED" % os.path.join(MSDIR, ms), readonly=False) as t:
                ang = t.getcol("RECEPTOR_ANGLE")
                ang[:, 0] = np.deg2rad(config.get("feed_angle_rotation"))
                ang[:, 1] = np.deg2rad(config.get("feed_angle_rotation"))
                t.putcol("RECEPTOR_ANGLE", ang)
                log.info("Receptor angle rotated")

        recipe.add(__correct_feed_convention, "correct_feed_convention", {
            "ms": os.path.abspath(os.path.join(pipeline.msdir, msname)),
        },
            input=INPUT, output=OUTPUT, label="correct_feed_convention")

        recipe.add(__correct_feed_convention, "correct_feed_convention_avg", {
            "ms": os.path.abspath(os.path.join(pipeline.msdir, avgmsname)),
        },
            input=INPUT, output=OUTPUT, label="correct_feed_convention_avg")
        step = 'clearcal_{:d}'.format(i)
        recipe.add('cab/casa_clearcal', step,
                   {
                       "vis": avgmsname,
                       "addmodel": True
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label="INIT averaged dataset for cross hand cal")

        step = 'summary_avg_json_{:d}'.format(i)
        recipe.add('cab/msutils', step,
                   {
                       "msname": msname,
                       "command": 'summary',
                       "outfile": avgmsname+'-obsinfo.json',
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Get observation information as a json file ms={1:s}'.format(step, avgmsname))
        msinfo = os.path.abspath(os.path.join(
            pipeline.output, avgmsname+'-obsinfo.json'))

        ######################################################################################################################
        #
        # Crosshand calibration procedure for linear feeds
        #
        ######################################################################################################################
        recipe.run()
        recipe = global_recipe

        # Set model
        # First do all the polarized calibrators
        if config['set_model'].get('enable'):
            cf = get_field('xcal')
            if not DISABLE_CROSSHAND_PHASE_CAL:
                recipe.add("cab/casa_setjy", "set_model_calms_%d" % 0, {
                    "msname": avgmsname,
                    "usescratch": True,
                    "field": cf,
                    "standard": polarized_calibrators[cf]["standard"],
                    "fluxdensity": sdm(polarized_calibrators[cf]["fluxdensity"]),
                    "spix": sdm(polarized_calibrators[cf]["spix"]),
                    "reffreq": sdm(polarized_calibrators[cf]["reffreq"]),
                    "polindex": sdm(polarized_calibrators[cf]["polindex"]),
                    "polangle": sdm(polarized_calibrators[cf]["polangle"]),
                },
                    input=INPUT, output=OUTPUT, label="set_model_calms_%d" % 0)
            # now set the fluxscale reference
            field = get_field('fcal')
            assert len(utils.get_field_id(msinfo, field)
                       ) == 1, "Only one fcal may be set"
            model = utils.find_in_native_calibrators(msinfo, field)
            standard = utils.find_in_casa_calibrators(msinfo, field)
            # Prefer our standard over the NRAO standard
            meerkathi_model = isinstance(model, str)

            if config['set_model'].get('meerkathi_model') and meerkathi_model:
                # use local sky model of calibrator field if exists
                opts = {
                    "skymodel": model,
                    "msname": avgmsname,
                    "field-id": utils.get_field_id(msinfo, field)[0],
                    "threads": config["set_model"].get('threads'),
                    "mode": "simulate",
                    "tile-size": 128,
                    "column": "MODEL_DATA",
                }
            elif isinstance(model, dict):  # spectral model if specified in our standard
                opts = {
                    "vis": avgmsname,
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
                    "vis": avgmsname,
                    "field": field,
                    "standard": config['set_model'].get('standard', standard),
                    "usescratch": False,
                    "scalebychan": True,
                }
            else:
                raise RuntimeError('The flux calibrator field "{}" could not be found in our database or in the '
                                   'CASA NRAO database'.format(field))
            step = 'set_model_cal_{0:d}'.format(i)

            recipe.add('cab/casa_setjy' if "skymodel" not in opts else 'cab/simulator', step,
                       opts,
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Set jansky ms={1:s}'.format(step, msname))

        toapply = []

        # Phaseup diagonal of crosshand cal if available
        if config.get('do_phaseup_crosshand_calibrator') and not DISABLE_CROSSHAND_PHASE_CAL:
            recipe.add("cab/casa_gaincal", "crosshand_phaseup", {
                "vis": avgmsname,
                "caltable": G1,
                "field": get_field("xcal"),
                "refant": REFANT,
                "solint": time_solint,
                "combine": "",
                "parang": True,
                "gaintype": "G",
                "calmode": "p",
                "spw": time_solfreqsel,
                "uvrange": solve_uvdist,  # EXCLUDE RFI INFESTATION!
                "gaintable": ["%s:output" % ct for ct in toapply],
                "parang": True,
            },
                input=INPUT, output=OUTPUT, label="crosshand_phaseup")
            toapply += [G1]

        # Solve for X slope
        # of the form [e^{2pi.i.a\nu} 0 0 1]
        if config.get('do_solve_crosshand_slope') and not DISABLE_CROSSHAND_PHASE_CAL:
            recipe.add("cab/casa_gaincal", "crosshand_delay", {
                "vis": avgmsname,
                "caltable": KX,
                "field": get_field("xcal"),
                "refant": REFANT,
                "solint": time_solint,
                "combine": "",
                "parang": True,
                "gaintype": "KCROSS",
                "spw": time_solfreqsel,
                "uvrange": solve_uvdist,  # EXCLUDE RFI INFESTATION!
                "gaintable": ["%s:output" % ct for ct in toapply],
            },
                input=INPUT, output=OUTPUT, label="crosshand_delay")
            toapply += [KX]
            recipe.add("cab/casa_plotcal", "crosshand_delay_plot", {
                "caltable": "%s:output" % KX,
                "xaxis": "time",
                "yaxis": "delay",
                "field": get_field("xcal"),
                "plotsymbol": "o",
                "figfile": PREFIX+".KX.png",
                "showgui": False
            },
                input=INPUT, output=OUTPUT, label="crosshand_delay_plot")

        # Solve for the absolute angle (phase) between the feeds
        # (P Jones auto enabled)
        # of the form [e^{2pi.i.b} 0 0 1]

        # remove the DC of the frequency solutions before
        # possibly joining scans to solve for per-frequency solutions
        # a strongly polarized source is needed with known properties
        # to limit the amount of PA coverage needed
        if config.get('do_solve_crosshand_phase') and not DISABLE_CROSSHAND_PHASE_CAL:
            recipe.add("cab/casa_polcal", "crosshand_phase_ref", {
                "vis": avgmsname,
                "caltable": Xref,
                "field": get_field("xcal"),
                "solint": time_solint,
                "combine": "",
                "poltype": "Xf",
                "refant": REFANT,
                "uvrange": solve_uvdist,  # EXCLUDE RFI INFESTATION!
                "gaintable": ["%s:output" % ct for ct in toapply],
            },
                input=INPUT, output=OUTPUT, label="crosshand_phase_ref")
            toapply += [Xref]
            recipe.add("cab/casa_plotms", "crosshand_phase_ref_plot", {
                "xaxis": "time",
                "yaxis": "phase",
                "expformat": "png",
                "exprange": "all",
                "overwrite": True,
                "showgui": False,
                "vis": "%s:output" % Xref,
                "field": get_field("xcal"),
                "plotfile": PREFIX+".Xref.png",
                "showgui": False
            },
                input=INPUT, output=OUTPUT, label="crosshand_phase_ref_plot")

            recipe.add("cab/casa_polcal", "crosshand_phase_freq", {
                "vis": avgmsname,
                "caltable": Xf,
                "field": get_field("xcal"),
                "solint": freq_solint,  # solint to obtain SNR on solutions
                "combine": "scan",
                "poltype": "Xf",
                "refant": REFANT,
                "uvrange": solve_uvdist,  # EXCLUDE RFI INFESTATION!
                "gaintable": ["%s:output" % ct for ct in toapply],
            },
                input=INPUT, output=OUTPUT, label="crosshand_phase_freq")
            toapply += [Xf]

            recipe.add("cab/casa_plotcal", "crosshand_phase_freq_plot", {
                "caltable": "%s:output" % Xf,
                "xaxis": "freq",
                "yaxis": "phase",
                "field": get_field("xcal"),
                "subplot": 111,
                "plotsymbol": "o",
                "figfile": PREFIX+".Xf.png",
                "showgui": False
            },
                input=INPUT, output=OUTPUT, label="crosshand_phase_freq_plot")

        # Solve for leakages (off-diagonal terms) using the unpolarized source
        # - first remove the DC of the frequency response and combine scans
        # if necessary to achieve desired SNR
        if config.get('do_solve_leakages'):
            recipe.add("cab/casa_polcal", "leakage_ref", {
                "vis": avgmsname,
                "caltable": Dref,
                "field": field,
                "solint": time_solint,
                "combine": "",
                "poltype": "D",
                "uvrange": solve_uvdist,  # EXCLUDE RFI INFESTATION!
                "refant": REFANT,
                "spw": time_solfreqsel,
                "gaintable": ["%s:output" % ct for ct in toapply],
            },
                input=INPUT, output=OUTPUT, label="leakage_ref")
            toapply += [Dref]
            recipe.add("cab/casa_plotcal", "leakage_ref_plot", {
                "caltable": "%s:output" % Dref,
                "xaxis": "time",
                "yaxis": "amp",
                "field": field,
                "subplot": 441,
                "iteration": "antenna",
                "plotsymbol": "o",
                "figfile": PREFIX+".Dref.png",
                "showgui": False
            },
                input=INPUT, output=OUTPUT, label="leakage_ref_plot")

            recipe.add("cab/casa_polcal", "leakage_freq", {
                "vis": avgmsname,
                "caltable": Df,
                "field": field,
                "solint": freq_solint,  # ensure SNR criterion is met
                "combine": "scan",
                "poltype": "Df",
                "refant": REFANT,
                "uvrange": solve_uvdist,  # EXCLUDE RFI INFESTATION!
                "gaintable": ["%s:output" % ct for ct in toapply],
            },
                input=INPUT, output=OUTPUT, label="leakage_freq")
            toapply += [Df]
            recipe.add("cab/casa_plotcal", "leakage_freq_plot", {
                "caltable": "%s:output" % Df,
                "xaxis": "freq",
                "yaxis": "amp",
                "field": field,
                "subplot": 441,
                "iteration": "antenna",
                "plotsymbol": "o",
                "figfile": PREFIX+".Dfreq.png",
                "showgui": False
            },
                input=INPUT, output=OUTPUT, label="leakage_freq_plot")

        if config.get('do_apply_XD'):
            # Before application lets transfer KGB corrected data to DATA and backup DATA
            recipe.add("cab/msutils", "backup_raw", {
                "command": "copycol",
                "fromcol": "DATA",
                "tocol": "RAWBACKUP",
                "msname": msname,
            }, input=INPUT, output=OUTPUT, label="backup_raw_visibilities")
            recipe.add("cab/msutils", "corr2data", {
                "command": "copycol",
                "fromcol": "CORRECTED_DATA",
                "tocol": "DATA",
                "msname": msname,
            }, input=INPUT, output=OUTPUT, label="corr2data")

            recipe.add("cab/casa_applycal", "apply_polcal_sols_to_avg", {
                "vis": avgmsname,
                "field": "",
                "parang": True,  # P Jones is autoenabled in the polarization calibration, ensure it is enabled now
                "gaintable": ["%s:output" % ct for ct in toapply]
            },
                input=INPUT, output=OUTPUT, label="apply_polcal_solutions_to_avg")

            recipe.add("cab/casa_applycal", "apply_polcal_sols", {
                "vis": msname,
                "field": "",
                "parang": True,  # Keep copy in SKY_FRAME_CORRECTED for imaging
                "gaintable": ["%s:output" % ct for ct in toapply]
            },
                input=INPUT, output=OUTPUT, label="apply_polcal_solutions")

            recipe.add("cab/msutils", "copy_corrected_to_skyframe_corrected", {
                "command": "copycol",
                "tocol": "SKY_FRAME_CORRECTED",
                "fromcol": "CORRECTED_DATA",
                "msname": msname,
            }, input=INPUT, output=OUTPUT, label="copy_corrected_to_skyframe_corrected")

            recipe.add("cab/casa_applycal", "apply_polcal_sols_feed", {
                "vis": msname,
                "field": "",
                "parang": False,  # Keep CORRECTED_DATA in FEED frame for further calibration
                "gaintable": ["%s:output" % ct for ct in toapply]
            },
                input=INPUT, output=OUTPUT, label="apply_polcal_sols_feed")

            # Finally restore raw data
            recipe.add("cab/msutils", "restore_raw", {
                "command": "copycol",
                "tocol": "DATA",
                "fromcol": "RAWBACKUP",
                "msname": msname,
            }, input=INPUT, output=OUTPUT, label="restore_raw_visibilities")

            def __del_col(ms):
                from pyrap.tables import table as tbl
                import numpy as np
                with tbl("%s" % ms, readonly=False) as t:
                    t.removecols(["RAWBACKUP"])

        recipe.add(__del_col, "delete_backup_raw", {
            "ms": os.path.abspath(os.path.join(pipeline.msdir, msname)),
        },
            input=INPUT, output=OUTPUT, label="delete_backup_raw")

        try:
            from meerkathi.scripts import reporter as mrr
            rep = mrr(pipeline)
        except ImportError:
            log.warning(
                "Modules for creating pipeline disgnostic reports are not installed. Please install \"meerkathi[extra_diagnostics]\" if you want these reports")

        if config.get('do_dump_precalibration_leakage_reports'):
            recipe.add(rep.generate_leakage_report, "polarization_leakage_precal", {
                "ms": os.path.abspath(os.path.join(MSDIR, msname)),
                "rep": "precal_polleakage_{0:s}.ipynb.html".format(msname),
                "field": get_field("fcal")
            },
                input=INPUT, output=OUTPUT, label="precal_polleak_rep")

        if config.get('do_dump_postcalibration_leakage_reports'):
            recipe.add(rep.generate_leakage_report, "polarization_leakage_postcal", {
                "ms": os.path.abspath(os.path.join(MSDIR, avgmsname)),
                "rep": "postcal_polleakage_{0:s}.ipynb.html".format(avgmsname),
                "field": get_field("fcal"),
            },
                input=INPUT, output=OUTPUT, label="precal_polleak_rep")

        if pipeline.enable_task(config, 'flagging_summary_crosshand_cal'):
            step = 'flagging_summary_crosshand_cal_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata', step,
                       {
                           "vis": msname,
                           "mode": 'summary',
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))

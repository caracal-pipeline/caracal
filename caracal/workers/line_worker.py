import datetime
import glob
import itertools
import json
import os
import re
import shutil

import numpy as np
import psutil
import stimela.dismissable as sdm
import stimela.recipe
from astropy.io import fits
from casacore.tables import table

import caracal
from caracal import log
from caracal.dispatch_crew import noisy, utils
from caracal.workers.utils import flag_Uzeros, remove_output_products
from caracal.workers.utils import manage_flagsets as manflags

C = 2.99792458e8  # m/s
HI = 1.4204057517667e9  # Hz

NAME = "Process and Image Line Data"
LABEL = "line"


def get_relative_path(path, pipeline):
    """Returns e.g. cubes/<dir> given output/cubes/<dir>"""
    return os.path.relpath(path, pipeline.output)


def add_ms_label(msname, label="mst"):
    """Adds _label to end of MS name, before the extension"""
    msbase, ext = os.path.splitext(msname)
    return f"{msbase}_{label}{ext}"


def freq_to_vel(filename, reverse):
    if not os.path.exists(filename):
        caracal.log.warn(f"Skipping conversion for {filename:s}. File does not exist.")
    else:
        with fits.open(filename, mode="update") as cube:
            headcube = cube[0].header
            if "restfreq" in headcube:
                restfreq = float(headcube["restfreq"])
            else:
                restfreq = HI
                # add rest frequency to FITS header
                headcube["restfreq"] = restfreq

            # convert from frequency to radio velocity
            if (headcube["naxis"] > 2) and ("FREQ" in headcube["ctype3"]) and not reverse:
                headcube["cdelt3"] = -C * float(headcube["cdelt3"]) / restfreq
                headcube["crval3"] = C * (1 - float(headcube["crval3"]) / restfreq)
                # FITS standard for radio velocity as per
                # https://fits.gsfc.nasa.gov/standard40/fits_standard40aa-le.pdf

                headcube["ctype3"] = "VRAD"
                headcube["cunit3"] = "m/s"
                # 3 lines below commented out because of issue 1209
                # if 'cunit3' in headcube:
                #    # delete cunit3 because we adopt the default units = m/s
                #    del headcube['cunit3']

            # convert from radio velocity to frequency
            elif (headcube["naxis"] > 2) and ("VRAD" in headcube["ctype3"]) and (headcube["naxis"] > 2) and reverse:
                headcube["cdelt3"] = -restfreq * float(headcube["cdelt3"]) / C
                headcube["crval3"] = restfreq * (1 - float(headcube["crval3"]) / C)
                headcube["ctype3"] = "FREQ"
                headcube["cunit3"] = "Hz"
                # 3 lines below commented out because of issue 1209
                # if 'cunit3' in headcube:
                #    # delete cunit3 because we adopt the default units = Hz
                #    del headcube['cunit3']
            else:
                if not reverse:
                    caracal.log.warn(f"Skipping conversion for {filename:s}. Input is not a cube or not in frequency.")
                else:
                    caracal.log.warn(f"Skipping conversion for {filename:s}. Input is not a cube or not in velocity.")


def remove_stokes_axis(filename):
    from astropy.io import fits

    if not os.path.exists(filename):
        caracal.log.warn(f"Skipping Stokes axis removal for {filename:s}. File does not exist.")
    else:
        with fits.open(filename, mode="update") as cube:
            headcube = cube[0].header
            if (headcube["naxis"] == 4) and (headcube["ctype4"] == "STOKES"):
                caracal.log.info(f"Working on {filename}")
                cube[0].data = cube[0].data[0]
                del headcube["cdelt4"]
                del headcube["crpix4"]
                del headcube["crval4"]
                del headcube["ctype4"]
                if "cunit4" in headcube:
                    del headcube["cunit4"]
            else:
                caracal.log.warning(f'Skipping Stokes axis removal for {filename}. Input cube has less than 4 axis or the 4th axis type is not "STOKES".')


def fix_specsys_ra(filename, specframe):
    from astropy.io import fits

    # Reference frame codes below from from http://www.eso.org/~jagonzal/telcal/Juan-Ramon/SDMTables.pdf, Sec. 2.50 and
    # FITS header notation from
    # https://fits.gsfc.nasa.gov/standard40/fits_standard40aa-le.pdf
    specsys3 = {0: "LSRD", 1: "LSRK", 2: "GALACTOC", 3: "BARYCENT", 4: "GEOCENTR", 5: "TOPOCENT"}[np.unique(np.array(specframe))[0]]
    if not os.path.exists(filename):
        caracal.log.warn(f"Skipping SPECSYS fix for {filename:s}. File does not exist.")
    else:
        with fits.open(filename, mode="update") as cube:
            headcube = cube[0].header
            if "specsys" in headcube:
                del headcube["specsys"]
            headcube["specsys3"] = specsys3
            if headcube["CRVAL1"] < 0:
                headcube["CRVAL1"] += 360.0


def make_pb_cube(filename, apply_corr, typ, dish_size, cutoff):
    from astropy.io import fits

    # C = 2.99792458e8  # m/s
    # HI = 1.4204057517667e9  # Hz

    if not os.path.exists(filename):
        caracal.log.warn(f"Skipping primary beam cube for {filename:s}. File does not exist.")
    else:
        with fits.open(filename) as cube:
            headcube = cube[0].header
            datacube = np.indices((headcube["naxis2"], headcube["naxis1"]), dtype=np.float32)
            datacube[0] -= headcube["crpix2"] - 1
            datacube[1] -= headcube["crpix1"] - 1
            datacube = np.sqrt((datacube**2).sum(axis=0))
            datacube.resize((1, datacube.shape[0], datacube.shape[1]))

            datacube = np.repeat(datacube, headcube["naxis3"], axis=0) * np.abs(headcube["cdelt1"])

            cdelt3 = float(headcube["cdelt3"])
            crval3 = float(headcube["crval3"])

            # Convert radio velocity to frequency if required
            if "VRAD" in headcube["ctype3"]:
                if "restfreq" in headcube:
                    restfreq = float(headcube["restfreq"])
                else:
                    restfreq = HI
                cdelt3 = -restfreq * cdelt3 / C
                crval3 = restfreq * (1 - crval3 / C)

            freq = crval3 + cdelt3 * (np.arange(headcube["naxis3"], dtype=np.float32) - headcube["crpix3"] + 1)

            if typ == "gauss":
                sigma_pb = 17.52 / (freq / 1e9) / dish_size / 2.355
                sigma_pb.resize((sigma_pb.shape[0], 1, 1))
                datacube = np.exp(-(datacube**2) / 2 / sigma_pb**2)
            elif typ == "mauch":
                FWHM_pb = (57.5 / 60) * (freq / 1.5e9) ** -1
                FWHM_pb.resize((FWHM_pb.shape[0], 1, 1))
                datacube = (np.cos(1.189 * np.pi * (datacube / FWHM_pb)) / (1 - 4 * (1.189 * datacube / FWHM_pb) ** 2)) ** 2

            datacube[datacube < cutoff] = np.nan
            if headcube["naxis"] == 4:
                datacube = np.expand_dims(datacube, 0)
            fits.writeto(filename.replace("image.fits", "pb.fits"), datacube, header=headcube, overwrite=True)
            if apply_corr:
                fits.writeto(
                    filename.replace("image.fits", "pb_corr.fits"),
                    cube[0].data / datacube,
                    header=headcube,
                    overwrite=True,
                )  # Applying the primary beam correction
            caracal.log.info("Created primary beam cube FITS {0:s}".format(filename.replace("image.fits", "pb.fits")))  # noqa: UP030


def calc_rms(filename, linemaskname):
    from astropy.io import fits

    if linemaskname is None:
        if not os.path.exists(filename):
            caracal.log.info(f"Noise not determined in cube for {filename:s}. File does not exist.")
        else:
            with fits.open(filename) as cube:
                datacube = cube[0].data
                y = datacube[~np.isnan(datacube)]
            return np.sqrt(np.sum(y * y, dtype=np.float64) / y.size)
    else:
        with fits.open(filename) as cube:
            if len(cube[0].data.shape) == 4:
                datacube = cube[0].data[0]
            else:
                datacube = cube[0].data
        with fits.open(linemaskname) as mask:
            datamask = mask[0].data
            # select channels
            selchans = datamask.sum(axis=(-2, -1)) > 0  # To get the right channels regardless of naxis.
            newcube = datacube[selchans]
            newmask = datamask[selchans]
            y2 = newcube[newmask == 0]
        return np.sqrt(np.nansum(y2 * y2, dtype=np.float64) / y2.size)


def worker(pipeline, recipe, config):
    import astropy
    from astropy import constants, units
    from astropy.coordinates import EarthLocation, SkyCoord
    from astropy.io import fits

    # Modules useful to calculate common barycentric frequency grid
    from astropy.time import Time

    wname = pipeline.CURRENT_WORKER
    flags_before_worker = f"{pipeline.prefix:s}_{wname:s}_before"
    flags_after_worker = f"{pipeline.prefix:s}_{wname:s}_after"
    flag_main_ms = (pipeline.enable_task(config, "flag_u_zeros") or pipeline.enable_task(config, "sunblocker")) and not config["sunblocker"]["use_mstransform"]
    flag_mst_ms = (
        (pipeline.enable_task(config, "sunblocker") and config["sunblocker"]["use_mstransform"])
        or (pipeline.enable_task(config, "flag_u_zeros") and config["flag_u_zeros"]["use_mstransform"])
        or pipeline.enable_task(config, "flag_mst_errors")
    )
    rewind_main_ms = config["rewind_flags"]["enable"] and (config["rewind_flags"]["mode"] == "reset_worker" or config["rewind_flags"]["version"] != "null")
    rewind_mst_ms = config["rewind_flags"]["enable"] and (config["rewind_flags"]["mode"] == "reset_worker" or config["rewind_flags"]["mstransform_version"] != "null")
    label = config["label_in"]
    line_name = config["line_name"]
    if label != "":
        flabel = label
    else:
        flabel = label
    all_targets, all_msfiles, ms_dict = pipeline.get_target_mss(flabel)
    RA, Dec = [], []
    firstchanfreq_all, chanw_all, lastchanfreq_all = [], [], []
    restfreq = config["restfreq"]

    # distributed deconvolution settings
    ncpu = config["ncpu"]
    if ncpu == 0:
        ncpu = psutil.cpu_count()
    else:
        ncpu = min(ncpu, psutil.cpu_count())
    nrdeconvsubimg = ncpu if config["make_cube"]["wscl_nrdeconvsubimg"] == 0 else config["make_cube"]["wscl_nrdeconvsubimg"]
    if nrdeconvsubimg == 1:
        wscl_parallel_deconv = None
    else:
        wscl_parallel_deconv = int(np.ceil(max(config["make_cube"]["npix"]) / np.sqrt(nrdeconvsubimg)))

    for i, msfile in enumerate(all_msfiles):
        # Update pipeline attributes (useful if, e.g., channel averaging was
        # performed by the split_data worker)
        msinfo = pipeline.get_msinfo(msfile)
        spw = msinfo["SPW"]["NUM_CHAN"]
        caracal.log.info(f"MS #{i:d}: {msfile:s}")
        caracal.log.info("  {0:d} spectral windows, with NCHAN={1:s}".format(len(spw), ",".join(map(str, spw))))  # noqa: UP030

        # Get first chan, last chan, chan width
        chfr = msinfo["SPW"]["CHAN_FREQ"]
        # To be done: add user selected  spw
        firstchanfreq = [ss[0] for ss in chfr]
        lastchanfreq = [ss[-1] for ss in chfr]
        chanwidth = [(ss[-1] - ss[0]) / (len(ss) - 1) for ss in chfr]
        firstchanfreq_all.append(firstchanfreq), chanw_all.append(chanwidth), lastchanfreq_all.append(lastchanfreq)
        caracal.log.info(
            "  CHAN_FREQ from {0:s} Hz to {1:s} Hz with average channel width of {2:s} Hz".format(  # noqa: UP030
                ",".join(map(str, firstchanfreq)), ",".join(map(str, lastchanfreq)), ",".join(map(str, chanwidth))
            )
        )

        tinfo = msinfo["FIELD"]
        targetpos = tinfo["REFERENCE_DIR"]
        while len(targetpos) == 1:
            targetpos = targetpos[0]
        tRA = targetpos[0] / np.pi * 180.0
        tDec = targetpos[1] / np.pi * 180.0
        RA.append(tRA)
        Dec.append(tDec)
        caracal.log.info(f"  Target RA, Dec for Doppler correction: {RA[i]:.3f} deg, {Dec[i]:.3f} deg")

    # Find common barycentric frequency grid for all input .MS, or set it as
    # requested in the config file
    if pipeline.enable_task(config, "mstransform") and pipeline.enable_task(config["mstransform"], "doppler") and config["mstransform"]["doppler"]["changrid"] == "auto":
        firstchanfreq = list(itertools.chain.from_iterable(firstchanfreq_all))
        chanw = list(itertools.chain.from_iterable(chanw_all))
        lastchanfreq = list(itertools.chain.from_iterable(lastchanfreq_all))
        teldict = {
            "meerkat": [21.4430, -30.7130],
            "gmrt": [73.9723, 19.1174],
            "vla": [-107.6183633, 34.0783584],
            "wsrt": [52.908829698, 6.601997592],
            "atca": [-30.307665436, 149.550164466],
            "askap": [116.5333, -16.9833],
        }
        tellocation = teldict[config["mstransform"]["doppler"]["telescope"]]
        telloc = EarthLocation.from_geodetic(tellocation[0], tellocation[1])
        firstchanfreq_dopp, chanw_dopp, lastchanfreq_dopp = firstchanfreq, chanw, lastchanfreq
        corr_order = False
        if len(chanw) > 1 and np.max(chanw) > 0 and np.min(chanw) < 0:
            corr_order = True

        for i, msfile in enumerate(all_msfiles):
            msinfo = f"{pipeline.msdir:s}/{os.path.splitext(msfile)[0]:s}-obsinfo.txt"
            with open(msinfo, "r") as searchfile:
                for longdatexp in searchfile:
                    if "Observed from" in longdatexp:
                        dates = longdatexp
                        matches = re.findall(
                            r"(\d{2}[- ](\d{2}|January|Jan|February|Feb|March|Mar|April|Apr|May|May|June|Jun|July|Jul|"
                            r"August|Aug|September|Sep|October|Oct|November|Nov|December|Dec)[\- ]\d{2,4})",
                            dates,
                        )
                        obsstartdate = str(matches[0][0])
                        obsdate = datetime.datetime.strptime(obsstartdate, "%d-%b-%Y").strftime("%Y-%m-%d")  # noqa: DTZ007
                        targetpos = SkyCoord(RA[i], Dec[i], frame="icrs", unit="deg")
                        v = targetpos.radial_velocity_correction(kind="barycentric", obstime=Time(obsdate), location=telloc).to("km/s")
                        corr = np.sqrt((constants.c - v) / (constants.c + v))
                        if corr_order:
                            if chanw_dopp[0] > 0.0:
                                firstchanfreq_dopp[i], chanw_dopp[i], lastchanfreq_dopp[i] = (
                                    lastchanfreq_dopp[i] * corr,
                                    chanw_dopp[i] * corr,
                                    firstchanfreq_dopp[i] * corr,
                                )
                            else:
                                firstchanfreq_dopp[i], chanw_dopp[i], lastchanfreq_dopp[i] = (
                                    firstchanfreq_dopp[i] * corr,
                                    chanw_dopp[i] * corr,
                                    lastchanfreq_dopp[i] * corr,
                                )
                        else:
                            firstchanfreq_dopp[i], chanw_dopp[i], lastchanfreq_dopp[i] = (
                                firstchanfreq_dopp[i] * corr,
                                chanw_dopp[i] * corr,
                                lastchanfreq_dopp[i] * corr,
                            )  # Hz, Hz, Hz

        # WARNING: the following line assumes a single SPW for the line data
        # being processed by this worker!
        if np.min(chanw_dopp) < 0:
            comfreq0, comfreql, comchanw = (
                np.min(firstchanfreq_dopp),
                np.max(lastchanfreq_dopp),
                -1 * np.max(np.abs(chanw_dopp)),
            )
            # safety measure to avoid wrong Doppler settings due to change of
            # Doppler correction during a day
            comfreq0 += comchanw
            # safety measure to avoid wrong Doppler settings due to change of
            # Doppler correction during a day
            comfreql -= comchanw
        else:
            comfreq0, comfreql, comchanw = np.max(firstchanfreq_dopp), np.min(lastchanfreq_dopp), np.max(chanw_dopp)
            # safety measure to avoid wrong Doppler settings due to change of
            # Doppler correction during a day
            comfreq0 += comchanw
            # safety measure to avoid wrong Doppler settings due to change of
            # Doppler correction during a day
            comfreql -= comchanw
        nchan_dopp = int(np.floor((comfreql - comfreq0) / comchanw)) + 1
        comfreq0 = f"{comfreq0:.3f}Hz"
        comchanw = f"{comchanw:.3f}Hz"
        caracal.log.info(
            f"Calculated common Doppler-corrected channel grid for all input .MS: {nchan_dopp:d} channels starting at {comfreq0:s}  and with channel width {comchanw:s}."
        )
        if pipeline.enable_task(config, "make_cube") and config["make_cube"]["image_with"] == "wsclean" and corr_order:
            caracal.log.error("wsclean requires a consistent ordering of the frequency axis across multiple MSs")
            caracal.log.error("(all increasing or all decreasing). Use casa_image if this is not the case.")
            raise caracal.BadDataError("inconsistent frequency axis ordering across MSs")

    elif pipeline.enable_task(config, "mstransform") and pipeline.enable_task(config["mstransform"], "doppler") and config["mstransform"]["doppler"]["changrid"] != "auto":
        if len(config["mstransform"]["doppler"]["changrid"].split(",")) != 3:
            caracal.log.error("Incorrect format for mstransform:doppler:changrid in the .yml config file.")
            caracal.log.error('Current setting is mstransform:doppler:changrid:"{0:s}"'.format(config["mstransform"]["doppler"]["changrid"]))  # noqa: UP030
            caracal.log.error(
                "Expected 'nchan,chan0,chanw' (note the commas) where nchan is an integer, and chan0 and chanw must include units appropriate for the chosen mstransform:mode"
            )
            raise caracal.ConfigurationError("can't parse mstransform:doppler:changrid setting")
        nchan_dopp, comfreq0, comchanw = config["mstransform"]["doppler"]["changrid"].split(",")
        nchan_dopp = int(nchan_dopp)
        caracal.log.info(f"Set requested Doppler-corrected channel grid for all input .MS: {nchan_dopp:d} channels starting at {comfreq0:s} and with channel width {comchanw:s}.")

    elif pipeline.enable_task(config, "mstransform"):
        nchan_dopp, comfreq0, comchanw = None, None, None

    for i, msname in enumerate(all_msfiles):
        # Write/rewind flag versions only if flagging tasks are being
        # executed on these .MS files, or if the user asks to rewind flags
        if flag_main_ms or rewind_main_ms:
            available_flagversions = manflags.get_flags(pipeline, msname)
            if rewind_main_ms:
                if config["rewind_flags"]["mode"] == "reset_worker":
                    version = flags_before_worker
                    stop_if_missing = False
                elif config["rewind_flags"]["mode"] == "rewind_to_version":
                    version = config["rewind_flags"]["version"]
                    if version == "auto":
                        version = flags_before_worker
                    stop_if_missing = True
                if version in available_flagversions:
                    if (
                        flags_before_worker in available_flagversions
                        and available_flagversions.index(flags_before_worker) < available_flagversions.index(version)
                        and not config["overwrite_flagvers"]
                    ):
                        manflags.conflict(
                            "rewind_too_little",
                            pipeline,
                            wname,
                            msname,
                            config,
                            flags_before_worker,
                            flags_after_worker,
                        )
                    substep = f"version-{version:s}-ms{i:d}"
                    manflags.restore_cflags(pipeline, recipe, version, msname, cab_name=substep)
                    if version != available_flagversions[-1]:
                        substep = f"delete-flag_versions-after-{version:s}-ms{i:d}"
                        manflags.delete_cflags(
                            pipeline,
                            recipe,
                            available_flagversions[available_flagversions.index(version) + 1],
                            msname,
                            cab_name=substep,
                        )
                    if version != flags_before_worker and flag_main_ms:
                        substep = f"save-{flags_before_worker:s}-ms{i:d}"
                        manflags.add_cflags(
                            pipeline,
                            recipe,
                            flags_before_worker,
                            msname,
                            cab_name=substep,
                            overwrite=config["overwrite_flagvers"],
                        )
                elif stop_if_missing:
                    manflags.conflict(
                        "rewind_to_non_existing",
                        pipeline,
                        wname,
                        msname,
                        config,
                        flags_before_worker,
                        flags_after_worker,
                    )
                elif flag_main_ms:
                    substep = f"save-{flags_before_worker:s}-ms{i:d}"
                    manflags.add_cflags(
                        pipeline,
                        recipe,
                        flags_before_worker,
                        msname,
                        cab_name=substep,
                        overwrite=config["overwrite_flagvers"],
                    )
            else:
                if flags_before_worker in available_flagversions and not config["overwrite_flagvers"]:
                    manflags.conflict("would_overwrite_bw", pipeline, wname, msname, config, flags_before_worker, flags_after_worker)
                else:
                    substep = f"save-{flags_before_worker:s}-ms{i:d}"
                    manflags.add_cflags(
                        pipeline,
                        recipe,
                        flags_before_worker,
                        msname,
                        cab_name=substep,
                        overwrite=config["overwrite_flagvers"],
                    )

        if pipeline.enable_task(config, "subtractmodelcol"):
            # Check if a model subtraction has already been done
            t = table(f"{pipeline.msdir:s}/{msname:s}", readonly=False)
            caracal.log.info(f"Check the MS name: {msname}")
            try:
                nModelSub = t.getcolkeyword("CORRECTED_DATA", "modelSub")
                caracal.log.info(f"NmodelSub here = {nModelSub}")
            except RuntimeError:
                nModelSub = 0

            if (nModelSub <= -1) and not config["subtractmodelcol"]["force"]:
                caracal.log.error(f"The model has been subtracted {np.abs(nModelSub)} times.")
                caracal.log.error("Exiting CARACal.")
                raise caracal.PlayingWithFire("I am very confident you shouldn't be doing this. If you know better, use the 'force' option.")

            if (nModelSub <= -1) & (config["subtractmodelcol"]["force"]):
                caracal.log.warn(f"The model has been subtracted {np.abs(nModelSub)} times.")
                caracal.log.warn("You have chosen to force another model subtraction.")
                caracal.log.warn("God speed!")

            step = f"modelsub-ms{i:d}"
            recipe.add(
                "cab/msutils",
                step,
                {
                    "command": "sumcols",
                    "msname": msname,
                    "subtract": True,
                    "col1": "CORRECTED_DATA",
                    "col2": "MODEL_DATA",
                    "column": "CORRECTED_DATA",
                },
                input=pipeline.input,
                output=pipeline.output,
                label=f"{step:s}:: Subtract model column",
            )

            t.putcolkeyword("CORRECTED_DATA", "modelSub", nModelSub - 1)
            t.close()

        if pipeline.enable_task(config, "addmodelcol"):
            # Check if a model addition has already been done
            t = table(f"{pipeline.msdir:s}/{msname:s}", readonly=False)
            caracal.log.info(f"Check the MS name: {msname}")
            try:
                nModelSub = t.getcolkeyword("CORRECTED_DATA", "modelSub")
            except RuntimeError:
                nModelSub = 0

            if (nModelSub >= 0) and not config["addmodelcol"]["force"]:
                caracal.log.error(f"The model has been added {np.abs(nModelSub)} times.")
                caracal.log.error("Exiting CARACal.")
                raise caracal.PlayingWithFire("I am very confident you shouldn't be doing this. If you know better, use the 'force' option.")

            if (nModelSub >= 0) & (config["addmodelcol"]["force"]):
                caracal.log.warn(f"The model has been added  {np.abs(nModelSub)} times.")
                caracal.log.warn("You have chosen to force another model addition.")
                caracal.log.warn("God speed!")

            step = f"modeladd-ms{i:d}"
            recipe.add(
                "cab/msutils",
                step,
                {
                    "command": "sumcols",
                    "msname": msname,
                    "col1": "CORRECTED_DATA",
                    "col2": "MODEL_DATA",
                    "column": "CORRECTED_DATA",
                },
                input=pipeline.input,
                output=pipeline.output,
                label=f"{step:s}:: Add model column",
            )

            t.putcolkeyword("CORRECTED_DATA", "modelSub", nModelSub + 1)
            t.close()

        msname_mst = add_ms_label(msname, "mst")
        msname_mst_base = os.path.splitext(msname_mst)[0]
        flagv = msname_mst + ".flagversions"
        summary_file = f"{msname_mst_base}-summary.json"
        obsinfo_file = f"{msname_mst_base}-obsinfo.txt"

        if pipeline.enable_task(config, "mstransform"):
            # Set UVLIN fit channel range
            if pipeline.enable_task(config["mstransform"], "uvlin") and config["mstransform"]["uvlin"]["exclude_known_sources"]:
                # C = 2.99792458e5  # km/s
                chanfreqs = np.arange(firstchanfreq_all[i][0], lastchanfreq_all[i][0] + chanw_all[i][0], chanw_all[i][0])
                chanids = np.arange(chanfreqs.shape[0])
                linechans = chanids < 0  # Array of False's used to build the fitspw settings
                line_id, line_ra, line_dec, line_vmin, line_vmax, line_flux = np.loadtxt(
                    "{0:s}/{1:s}".format(pipeline.input, config["mstransform"]["uvlin"]["known_sources_cat"]),  # noqa: UP030
                    dtype="str",
                    unpack=True,
                )
                line_ra = astropy.coordinates.Angle(line_ra, unit="hour").degree
                line_dec = astropy.coordinates.Angle(line_dec, unit="degree").degree
                line_flux = line_flux.astype(float)
                dv = config["mstransform"]["uvlin"]["known_sources_dv"]
                line_fmax = (units.Quantity(config["restfreq"]) / ((line_vmin.astype(float) - dv) * 1e3 / C + 1)).to_value(units.hertz)
                line_fmin = (units.Quantity(config["restfreq"]) / ((line_vmax.astype(float) + dv) * 1e3 / C + 1)).to_value(units.hertz)
                distance = (
                    180
                    / np.pi
                    * np.arccos(
                        np.sin(Dec[i] / 180 * np.pi) * np.sin(line_dec / 180 * np.pi)
                        + np.cos(Dec[i] / 180 * np.pi) * np.cos(line_dec / 180 * np.pi) * np.cos((RA[i] - line_ra) / 180 * np.pi)
                    )
                )
                # Select line sources:
                #     within the search radius;
                #     above the line flux threashold (not PB-corrected);
                #     and (at least partly) within the MS frequency range.
                line_selected = (
                    (distance < config["mstransform"]["uvlin"]["known_sources_radius"])
                    * (line_flux >= config["mstransform"]["uvlin"]["known_sources_flux"])
                    * ((line_fmin >= chanfreqs.min()) * (line_fmin <= chanfreqs.max()) + (line_fmax >= chanfreqs.min()) * (line_fmax <= chanfreqs.max()))
                )
                line_id, line_fmin, line_fmax = (
                    line_id[line_selected],
                    line_fmin[line_selected],
                    line_fmax[line_selected],
                )
                line_chanmin, line_chanmax = [], []
                caracal.log.info("Excluding the following line sources and channel intervals from the UVLIN fit:")
                for ll in range(line_id.shape[0]):
                    if line_fmin[ll] < chanfreqs[0]:
                        line_chanmin.append(chanids[0])
                    else:
                        line_chanmin.append(chanids[chanfreqs < line_fmin[ll]].max())
                    if line_fmax[ll] > chanfreqs[-1]:
                        line_chanmax.append(chanids[-1])
                    else:
                        line_chanmax.append(chanids[chanfreqs > line_fmax[ll]].min())
                    caracal.log.info(f"  {line_id[ll]:20s}:  {line_chanmin[ll]:5d} - {line_chanmax[ll]:5d}")
                    linechans += (chanids >= line_chanmin[ll]) * (chanids <= line_chanmax[ll])
                autofitchans = ~linechans
                if config["mstransform"]["uvlin"]["fitspw"]:
                    caracal.log.info("Combining the above channel intervals with the user input {0:s}".format(config["mstransform"]["uvlin"]["fitspw"]))  # noqa: UP030
                    userfitchans = [qq.split(";") for qq in config["mstransform"]["uvlin"]["fitspw"].split(":")[1::2]]
                    while len(userfitchans) > 1:
                        userfitchans[0] = userfitchans[0] + userfitchans[1]
                        del userfitchans[1]
                    userfitchans = [list(map(int, qq.split("~"))) for qq in userfitchans[0]]
                    userfitchans = np.array([(chanids >= qq[0]) * (chanids <= qq[1]) for qq in userfitchans]).sum(axis=0).astype("bool")
                    autofitchans *= userfitchans
                fitspw = "0~" if autofitchans[0] else ""
                for cc in chanids[1:]:
                    if not autofitchans[cc - 1] and autofitchans[cc] and (not fitspw or fitspw[-1] == ";"):
                        fitspw += f"{cc:d}~"
                    elif autofitchans[cc - 1] and not autofitchans[cc]:
                        fitspw += f"{cc - 1:d};"
                if not fitspw:
                    raise caracal.BadDataError("No channels available for UVLIN fit.")
                elif fitspw[-1] == "~":
                    fitspw += f"{chanids[-1]:d}"
                elif fitspw[-1] == ";":
                    fitspw = fitspw[:-1]
                fitspw = f"0:{fitspw:s}"
                caracal.log.info(f"The UVLIN fit will be executed on the channels {fitspw:s}")

            else:
                fitspw = config["mstransform"]["uvlin"]["fitspw"]

            # If the output of this run of mstransform exists, delete it first
            remove_output_products((msname_mst, flagv, summary_file, obsinfo_file), directory=pipeline.msdir, log=log)

            col = config["mstransform"]["col"]
            step = f"mstransform-ms{i:d}"
            recipe.add(
                "cab/casa_mstransform",
                step,
                {
                    "msname": msname,
                    "outputvis": msname_mst,
                    "regridms": pipeline.enable_task(config["mstransform"], "doppler"),
                    "mode": config["mstransform"]["doppler"]["mode"],
                    "nchan": sdm.dismissable(nchan_dopp),
                    "start": sdm.dismissable(comfreq0),
                    "width": sdm.dismissable(comchanw),
                    "interpolation": "nearest",
                    "datacolumn": col,
                    "restfreq": restfreq,
                    "outframe": config["mstransform"]["doppler"]["frame"],
                    "veltype": config["mstransform"]["doppler"]["veltype"],
                    "douvcontsub": pipeline.enable_task(config["mstransform"], "uvlin"),
                    "fitspw": sdm.dismissable(fitspw),
                    "fitorder": config["mstransform"]["uvlin"]["fitorder"],
                },
                input=pipeline.input,
                output=pipeline.output,
                label=f"{step:s}:: Doppler tracking corrections",
            )

            substep = "save-{0:s}-ms{1:d}".format("caracal_legacy", i)  # noqa: UP030
            manflags.add_cflags(pipeline, recipe, "caracal_legacy", msname_mst, cab_name=substep, overwrite=False)

            if config["mstransform"]["obsinfo"]:
                step = f"listobs-ms{i:d}"
                recipe.add(
                    "cab/casa_listobs",
                    step,
                    {
                        "vis": msname_mst,
                        "listfile": f"{msname_mst_base:s}-obsinfo.txt:msfile",
                        "overwrite": True,
                    },
                    input=pipeline.input,
                    output=pipeline.obsinfo,
                    label=f"{step:s}:: Get observation information ms={msname_mst:s}",
                )

                step = f"summary_json-ms{i:d}"
                recipe.add(
                    "cab/msutils",
                    step,
                    {
                        "msname": msname_mst,
                        "command": "summary",
                        "display": False,
                        "outfile": f"{msname_mst_base:s}-summary.json:msfile",
                    },
                    input=pipeline.input,
                    output=pipeline.obsinfo,
                    label=f"{step:s}:: Get observation information as a json file ms={msname_mst:s}",
                )

        recipe.run()
        recipe.jobs = []

        if os.path.exists(f"{pipeline.msdir:s}/{msname_mst:s}"):
            mst_exist = True
        else:
            mst_exist = False

        # Write/rewind flag versions of the mst .MS files only if they have just
        # been created, their FLAG is being changed, or the user asks to rewind flags
        if mst_exist and (pipeline.enable_task(config, "mstransform") or flag_mst_ms or rewind_mst_ms):
            available_flagversions = manflags.get_flags(pipeline, msname_mst)
            if rewind_mst_ms:
                if config["rewind_flags"]["mode"] == "reset_worker":
                    version = flags_before_worker
                    stop_if_missing = False
                elif config["rewind_flags"]["mode"] == "rewind_to_version":
                    version = config["rewind_flags"]["mstransform_version"]
                    if version == "auto":
                        version = flags_before_worker
                    stop_if_missing = True
                if version in available_flagversions:
                    if (
                        flags_before_worker in available_flagversions
                        and available_flagversions.index(flags_before_worker) < available_flagversions.index(version)
                        and not config["overwrite_flagvers"]
                    ):
                        manflags.conflict(
                            "rewind_too_little",
                            pipeline,
                            wname,
                            msname_mst,
                            config,
                            flags_before_worker,
                            flags_after_worker,
                            read_version="mstransform_version",
                        )
                    substep = f"version_{version:s}_ms{i:d}"
                    manflags.restore_cflags(pipeline, recipe, version, msname_mst, cab_name=substep)
                    if version != available_flagversions[-1]:
                        substep = f"delete-flag_versions-after-{version:s}-ms{i:d}"
                        manflags.delete_cflags(
                            pipeline,
                            recipe,
                            available_flagversions[available_flagversions.index(version) + 1],
                            msname_mst,
                            cab_name=substep,
                        )
                    if version != flags_before_worker and flag_mst_ms:
                        substep = f"save-{flags_before_worker:s}-ms{i:d}"
                        manflags.add_cflags(
                            pipeline,
                            recipe,
                            flags_before_worker,
                            msname_mst,
                            cab_name=substep,
                            overwrite=config["overwrite_flagvers"],
                        )
                elif stop_if_missing:
                    manflags.conflict(
                        "rewind_to_non_existing",
                        pipeline,
                        wname,
                        msname_mst,
                        config,
                        flags_before_worker,
                        flags_after_worker,
                        read_version="mstransform_version",
                    )
                elif flag_mst_ms:
                    substep = f"save-{flags_before_worker:s}-ms{i:d}"
                    manflags.add_cflags(
                        pipeline,
                        recipe,
                        flags_before_worker,
                        msname_mst,
                        cab_name=substep,
                        overwrite=config["overwrite_flagvers"],
                    )
            else:
                if flags_before_worker in available_flagversions and not config["overwrite_flagvers"]:
                    manflags.conflict(
                        "would_overwrite_bw",
                        pipeline,
                        wname,
                        msname_mst,
                        config,
                        flags_before_worker,
                        flags_after_worker,
                        read_version="mstransform_version",
                    )
                else:
                    substep = f"save-{flags_before_worker:s}-ms{i:d}"
                    manflags.add_cflags(
                        pipeline,
                        recipe,
                        flags_before_worker,
                        msname_mst,
                        cab_name=substep,
                        overwrite=config["overwrite_flagvers"],
                    )

        if pipeline.enable_task(config, "flag_mst_errors"):
            step = f"flag_mst_errors-ms{i:d}"
            recipe.add(
                "cab/autoflagger",
                step,
                {
                    "msname": msname_mst,
                    "column": "DATA",
                    "strategy": config["flag_mst_errors"]["strategy"],
                    "indirect-read": True if config["flag_mst_errors"]["readmode"] == "indirect" else False,  # noqa: SIM210
                    "memory-read": True if config["flag_mst_errors"]["readmode"] == "memory" else False,  # noqa: SIM210
                    "auto-read-mode": True if config["flag_mst_errors"]["readmode"] == "auto" else False,  # noqa: SIM210
                },
                input=pipeline.input,
                output=pipeline.output,
                label=f"{step:s}:: file ms={msname_mst:s}",
            )

        recipe.run()
        recipe.jobs = []

        if pipeline.enable_task(config, "flag_u_zeros"):
            uZeros = flag_Uzeros.UzeroFlagger(config)

            if config["flag_u_zeros"]["use_mstransform"]:
                msname_Flag = msname_mst
            else:
                msname_Flag = msname

            uZeros.run_flagUzeros(pipeline, all_targets, msname_Flag)

        if pipeline.enable_task(config, "sunblocker"):
            if config["sunblocker"]["use_mstransform"]:
                msnamesb = msname_mst
            else:
                msnamesb = msname
            step = f"sunblocker-ms{i:d}"
            recipe.add(
                "cab/sunblocker",
                step,
                {
                    "command": "phazer",
                    "inset": msnamesb,
                    "outset": msnamesb,
                    "imsize": config["sunblocker"]["imsize"],
                    "cell": config["sunblocker"]["cell"],
                    "pol": "i",
                    "threshmode": "mad",
                    "threshold": config["sunblocker"]["thr"],
                    "mode": "all",
                    "radrange": 0,
                    "angle": 0,
                    "show": pipeline.prefix + ".sunblocker.svg",
                    "verb": True,
                    "dryrun": False,
                    "uvmax": config["sunblocker"]["uvmax"],
                    "uvmin": config["sunblocker"]["uvmin"],
                    "vampirisms": config["sunblocker"]["vampirisms"],
                    "flagonlyday": config["sunblocker"]["flagonlyday"],
                },
                input=pipeline.input,
                output=pipeline.output,
                label=f"{step:s}:: Block out sun",
            )

        if flag_main_ms:
            substep = f"save-{flags_after_worker:s}-ms{i:d}"
            manflags.add_cflags(pipeline, recipe, flags_after_worker, msname, cab_name=substep, overwrite=config["overwrite_flagvers"])

        if mst_exist and flag_mst_ms:
            substep = f"save-{flags_after_worker:s}-mst{i:d}"
            manflags.add_cflags(
                pipeline,
                recipe,
                flags_after_worker,
                msname_mst,
                cab_name=substep,
                overwrite=config["overwrite_flagvers"],
            )

        recipe.run()
        recipe.jobs = []
        # Move the sunblocker plots to the diagnostic_plots
        if pipeline.enable_task(config, "sunblocker"):
            sunblocker_plots = glob.glob(f"{pipeline.output:s}/*_{pipeline.prefix:s}.sunblocker.svg")
            for plot in sunblocker_plots:
                shutil.copyfile(plot, f"{pipeline.diagnostic_plots:s}/{os.path.basename(plot):s}")
                os.remove(plot)

    if pipeline.enable_task(config, "predict_noise"):
        tsyseff = config["predict_noise"]["tsyseff"]
        diam = config["predict_noise"]["diam"]
        kB = 1380.6  # Boltzmann constant (Jy m^2 / K)
        Aant = np.pi * (diam / 2) ** 2  # collecting area of 1 antenna (m^2)
        SEFD = 2 * kB * tsyseff / Aant  # system equivalent flux density (Jy)
        caracal.log.info(f"Predicting natural noise of line cubes (Stokes I, single channel of MS file) for Tsys/eff = {tsyseff} K, diam = {diam} m -> SEFD = {SEFD:.1f} Jy")
        for tt, target in enumerate(all_targets):
            if config["make_cube"]["use_mstransform"]:
                mslist = [add_ms_label(ms, "mst") for ms in ms_dict[target]]
            else:
                mslist = ms_dict[target]
            caracal.log.info(f"  Target #{tt:d}: {target}, files {mslist}")
            noisy.PredictNoise([f"{pipeline.msdir:s}/{mm:s}" for mm in mslist], str(tsyseff), diam, target, verbose=2)

    if pipeline.enable_task(config, "make_cube") and config["make_cube"]["image_with"] == "wsclean":
        nchans_all, specframe_all = [], []
        label = config["label_in"]
        if label != "":
            flabel = label
        else:
            flabel = label

        caracal.log.info("Collecting spectral info on MS files being imaged")
        if config["make_cube"]["use_mstransform"]:
            for i, msfile in enumerate(all_msfiles):
                # Get channelisation of _mst.ms file
                msbase, ext = os.path.splitext(msfile)
                msinfo = pipeline.get_msinfo(f"{msbase}_mst{ext}")
                spw = msinfo["SPW"]["NUM_CHAN"]
                nchans = spw
                nchans_all.append(nchans)
                caracal.log.info("MS #{0:d}: {1:s}".format(i, msfile.replace(".ms", "_mst.ms")))  # noqa: UP030
                caracal.log.info("  {0:d} spectral windows, with NCHAN={1:s}".format(len(spw), ",".join(map(str, spw))))  # noqa: UP030
                # Get first chan, last chan, chan width
                chfr = msinfo["SPW"]["CHAN_FREQ"]
                firstchanfreq = [ss[0] for ss in chfr]
                lastchanfreq = [ss[-1] for ss in chfr]
                chanwidth = [(ss[-1] - ss[0]) / (len(ss) - 1) for ss in chfr]
                caracal.log.info(
                    "  CHAN_FREQ from {0:s} Hz to {1:s} Hz with average channel width of {2:s} Hz".format(  # noqa: UP030
                        ",".join(map(str, firstchanfreq)),
                        ",".join(map(str, lastchanfreq)),
                        ",".join(map(str, chanwidth)),
                    )
                )
                # Get spectral reference frame
                specframe = msinfo["SPW"]["MEAS_FREQ_REF"]
                specframe_all.append(specframe)
                caracal.log.info(f"  The spectral reference frame is {specframe}")

        else:
            for i, msfile in enumerate(all_msfiles):
                msinfo = pipeline.get_msinfo(msfile)
                spw = msinfo["SPW"]["NUM_CHAN"]
                nchans = spw
                nchans_all.append(nchans)
                caracal.log.info(f"MS #{i:d}: {msfile:s}")
                caracal.log.info("  {0:d} spectral windows, with NCHAN={1:s}".format(len(spw), ",".join(map(str, spw))))  # noqa: UP030
                specframe = msinfo["SPW"]["MEAS_FREQ_REF"]
                specframe_all.append(specframe)
                caracal.log.info(f"  The spectral reference frame is {specframe}")

        spwid = config["make_cube"]["spwid"]
        nchans = config["make_cube"]["nchans"]
        if nchans == 0:
            # Assuming user wants same spw for all msfiles and they have same
            # number of channels
            nchans = nchans_all[0][spwid]
        # Assuming user wants same spw for all msfiles and they have same
        # specframe
        specframe_all = [ss[spwid] for ss in specframe_all][0]  # noqa: RUF015
        firstchan = config["make_cube"]["firstchan"]
        binchans = config["make_cube"]["binchans"]
        channelrange = [firstchan, firstchan + nchans * binchans]
        npix = config["make_cube"]["npix"]
        if len(npix) == 1:
            npix = [npix[0], npix[0]]

        # Construct weight specification
        if config["make_cube"]["weight"] == "briggs":
            weight = "briggs {0:.3f}".format(config["make_cube"]["robust"])  # noqa: UP030
        else:
            weight = config["make_cube"]["weight"]
        wscl_niter = max(1, config["make_cube"]["wscl_sofia_niter"])
        wscl_tol = config["make_cube"]["wscl_sofia_converge"]

        line_image_opts = {
            "weight": weight,
            "taper-gaussian": str(config["make_cube"]["taper"]),
            "pol": config["make_cube"]["stokes"],
            "npix": npix,
            "padding": config["make_cube"]["padding"],
            "scale": config["make_cube"]["cell"],
            "channelsout": nchans,
            "channelrange": channelrange,
            "niter": config["make_cube"]["niter"] if not config["make_cube"]["wscl_onlypsf"] else 1,
            "gain": config["make_cube"]["gain"],
            "mgain": config["make_cube"]["wscl_mgain"],
            "auto-threshold": config["make_cube"]["wscl_auto_thr"],
            "multiscale": config["make_cube"]["wscl_multiscale"],
            "multiscale-scale-bias": config["make_cube"]["wscl_multiscale_bias"],
            "parallel-deconvolution": sdm.dismissable(wscl_parallel_deconv),
            "no-update-model-required": config["make_cube"]["wscl_noupdatemod"],
            "make-psf-only": config["make_cube"]["wscl_onlypsf"],
        }
        if config["make_cube"]["wscl_multiscale_scales"]:
            line_image_opts.update({"multiscale-scales": list(map(int, config["make_cube"]["wscl_multiscale_scales"].split(",")))})

        if config["make_cube"]["wscl_beam"] != [0, 0, 0]:
            line_image_opts.update({"beamshape": config["make_cube"]["wscl_beam"]})

        for tt, target in enumerate(all_targets):
            caracal.log.info(f"Starting to make line cube for target {target}")
            if config["make_cube"]["use_mstransform"]:
                mslist = [add_ms_label(ms, "mst") for ms in ms_dict[target]]
            else:
                mslist = ms_dict[target]
            field = utils.filter_name(target)
            line_clean_mask_file = None
            rms_values = []
            line_image_opts.pop("fitsmask", None)
            line_image_opts.pop("auto-mask", None)
            for jj in range(1, wscl_niter + 1):
                cube_path = f"{pipeline.cubes:s}/cube_{jj:d}"
                if not os.path.exists(cube_path):
                    os.mkdir(cube_path)
                cube_dir = f"{get_relative_path(pipeline.cubes, pipeline):s}/cube_{jj:d}"

                line_image_opts.update({
                    "msname": mslist,
                    "prefix": f"{cube_dir:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}",
                })

                if jj == 1:
                    own_line_clean_mask = config["make_cube"]["wscl_user_clean_mask"]
                    if own_line_clean_mask:
                        """
                        MAKE HDR FILE FOR REGRIDDING THE USER SUPPLIED MASK
                        """
                        doProj = False
                        doSpec = False
                        # C = 2.99792458e8  # m/s
                        femit = [r.strip() for r in re.split(r"([-+]?\d+\.\d+)|([-+]?\d+)", restfreq.strip()) if r is not None and r.strip() != ""]
                        femit = (eval(femit[0]) * units.Unit(femit[1])).to(units.Hz).value  # Hz
                        t = mslist[0].replace(".ms", "-summary.json")  # first file given to WSClean as input
                        with open(f"{pipeline.msdir}/{t}") as f:
                            obsDict = json.load(f)
                        raTarget = np.round(obsDict["FIELD"]["REFERENCE_DIR"][0][0][0] / np.pi * 180, 5)
                        decTarget = np.round(obsDict["FIELD"]["REFERENCE_DIR"][0][0][1] / np.pi * 180, 5)
                        cubeHeight = config["make_cube"]["npix"][0]
                        cubeWidth = config["make_cube"]["npix"][1] if len(config["make_cube"]["npix"]) == 2 else cubeHeight

                        preGridMask = own_line_clean_mask

                        caracal.log.info("Checking Mask dimensions")
                        caracal.log.info(f"doProj = {doProj}")
                        caracal.log.info(f"RA = {raTarget}")
                        caracal.log.info(f"Dec = {decTarget}")
                        caracal.log.info(f"CubeHeight (px) = {cubeHeight}")
                        caracal.log.info(f"CubeWidht (px) = {cubeWidth}")

                        postGridMask = preGridMask.replace(".fits", f"_{pipeline.prefix}_{target}_regrid.fits")

                        with fits.open(f"{pipeline.masking}/{preGridMask}") as hdul:
                            if hdul[0].header["CRVAL1"] < 0:
                                hdul[0].header["CRVAL1"] += 360.0
                            caracal.log.info("MaskRA = {}".format(hdul[0].header["CRVAL1"]))
                            caracal.log.info("MaskDec = {}".format(hdul[0].header["CRVAL2"]))

                            caracal.log.info("MaskWidth = {}".format(hdul[0].header["NAXIS1"]))
                            caracal.log.info("MaskHeight = {}".format(hdul[0].header["NAXIS2"]))

                            if hdul[0].header["NAXIS1"] != cubeWidth:
                                caracal.log.info("NAXIS1")
                                doProj = True
                            if hdul[0].header["NAXIS2"] != cubeHeight:
                                caracal.log.info("NAXIS2")
                                doProj = True
                            if np.round(hdul[0].header["CRVAL1"], 5) != np.round(raTarget, 5):
                                caracal.log.info("CRVAL1")
                                doProj = True
                            if np.round(hdul[0].header["CRVAL2"], 5) != np.round(decTarget, 5):
                                doProj = True
                                caracal.log.info("CRVAL2")

                            caracal.log.info("MaskLength = {}".format(hdul[0].header["NAXIS3"]))
                            caracal.log.info(f"#CHans= {nchans}")
                            if int(hdul[0].header["NAXIS3"]) > int(nchans):
                                doSpec = True
                            else:
                                # this should work in both a request for a subset, and if the cube is to be binned.
                                doSpec = None

                            if "FREQ" in hdul[0].header["CTYPE3"]:
                                cdelt = np.round(hdul[0].header["CDELT3"], 5)
                            else:
                                cdelt = np.round(hdul[0].header["CDELT3"] * femit / (-C), 5)

                            caracal.log.info(f"CDELT = {cdelt}")
                            caracal.log.info(f"ChWidth = {chanwidth[0]}")

                            if np.round(cdelt, 5) > np.round(chanwidth[0] * binchans, 5):
                                doSpec = True
                            elif doProj:
                                pass
                            else:
                                # likely will fail/produce incorrect result in the case
                                #  that the ms file and mask were not created with the same original spectral grid.
                                doSpec = None

                            if doProj:
                                ax3param = []
                                for key in ["NAXIS3", "CTYPE3", "CRPIX3", "CRVAL3", "CDELT3"]:
                                    ax3param.append(hdul[0].header[key])

                        caracal.log.info(f"doSpecProj = {doSpec}")
                        caracal.log.info(f"doSpaceProj = {doProj}")

                        if doProj:
                            """
                            MAKE HDR FILE FOR REGRIDDING THE USER SUPPLIED MASK AND REPROJECT
                            """
                            with open(f"{pipeline.masking}/tmp.hdr", "w") as file:
                                file.write("SIMPLE  =   T\n")
                                file.write("BITPIX  =   -64\n")
                                file.write("NAXIS   =   2\n")
                                file.write(f"NAXIS1  =   {cubeWidth}\n")
                                file.write("CTYPE1  =   'RA---SIN'\n")
                                file.write(f"CRVAL1  =   {raTarget}\n")
                                file.write(f"CRPIX1  =   {cubeWidth / 2 + 1}\n")
                                file.write("CDELT1  =   {}\n".format(-1 * config["make_cube"]["cell"] / 3600.0))
                                file.write(f"NAXIS2  =   {cubeHeight}\n")
                                file.write("CTYPE2  =   'DEC--SIN'\n")
                                file.write(f"CRVAL2  =   {decTarget}\n")
                                file.write(f"CRPIX2  =   {cubeHeight / 2 + 1}\n")
                                file.write("CDELT2  =   {}\n".format(config["make_cube"]["cell"] / 3600.0))
                                file.write("EXTEND  =   T\n")
                                file.write("EQUINOX =   2000.0\n")
                                file.write("END\n")

                            if os.path.exists(f"{pipeline.masking}/{postGridMask}"):
                                os.remove(f"{pipeline.masking}/{postGridMask}")

                            with fits.open(f"{pipeline.masking}/{preGridMask}") as hdul:
                                if np.amax(hdul[0].data) > 1:
                                    mask = np.where(hdul[0].data > 0)
                                    hdul[0].data[mask] = 1
                                    preGridMaskNew = preGridMask.replace(".fits", "_01.fits")
                                    hdul.writeto(f"{pipeline.masking}/{preGridMaskNew}", overwrite=True)
                                    preGridMask = preGridMaskNew
                            caracal.log.info(f"Reprojecting mask {preGridMask} to match the grid of the cube.")

                            step = f"reprojectMask-field{tt}"
                            recipe.add(
                                "cab/mProjectCube",
                                step,
                                {
                                    "in.fits": preGridMask,
                                    "out.fits": postGridMask,
                                    "hdr.template": "tmp.hdr",
                                },
                                input=pipeline.masking,
                                output=pipeline.masking,
                                label=f"{step:s}:: Reprojecting user input mask {preGridMask:s} to match the grid of the cube",
                            )

                            # In order to make sure that we actually find
                            # stuff in the images we execute the rec ipe here
                            recipe.run()
                            # Empty job que after execution
                            recipe.jobs = []

                            if not os.path.exists(f"{pipeline.masking}/{postGridMask}"):
                                raise OSError(f"The regridded mask {postGridMask} does not exist. The original mask likely has no overlap with the cube")

                            with fits.open(f"{pipeline.masking}/{postGridMask}", mode="update") as hdul:
                                for i, key in enumerate(["NAXIS3", "CTYPE3", "CRPIX3", "CRVAL3", "CDELT3"]):
                                    hdul[0].header[key] = ax3param[i]
                                axDict = {"1": [2, cubeWidth], "2": [1, cubeHeight]}
                                for i in ["1", "2"]:
                                    cent, _ = hdul[0].header["CRPIX" + i], hdul[0].header["NAXIS" + i]
                                    if cent < axDict[i][1] / 2 + 1:
                                        delt = int(axDict[i][1] / 2 + 1 - cent)
                                        if i == "1":
                                            toAdd = np.zeros([hdul[0].header["NAXIS3"], hdul[0].data.shape[1], delt])
                                        else:
                                            toAdd = np.zeros([hdul[0].header["NAXIS3"], delt, hdul[0].data.shape[2]])
                                        hdul[0].data = np.concatenate([toAdd, hdul[0].data], axis=axDict[i][0])
                                        hdul[0].header["CRPIX" + i] = cent + delt
                                    if hdul[0].data.shape[axDict[i][0]] < axDict[i][1]:
                                        delt = int(axDict[i][1] - hdul[0].data.shape[axDict[i][0]])
                                        if i == "1":
                                            toAdd = np.zeros([hdul[0].header["NAXIS3"], hdul[0].data.shape[1], delt])
                                        else:
                                            toAdd = np.zeros([hdul[0].header["NAXIS3"], delt, hdul[0].data.shape[2]])
                                        hdul[0].data = np.concatenate([hdul[0].data, toAdd], axis=axDict[i][0])
                                    if hdul[0].data.shape[axDict[i][0]] > axDict[i][1]:
                                        delt = int(hdul[0].data.shape[axDict[i][0]] - axDict[i][1])
                                        hdul[0].data = hdul[0].data[:, :, -delt] if i == "1" else hdul[0].data[:, -delt, :]
                                        if cent > axDict[i][1] / 2 + 1:
                                            hdul[0].header["CRPIX" + i] = hdul[0].data.shape[axDict[i][0]] / 2 + 1

                                hdul[0].data = np.around(hdul[0].data.astype(np.float32)).astype(np.int16)
                                try:
                                    del hdul[0].header["EN"]
                                except KeyError:
                                    pass
                                hdul.flush()

                            line_image_opts.update({"fitsmask": "{0:s}/{1:s}:output".format(get_relative_path(pipeline.masking, pipeline), postGridMask.split("/")[-1])})  # noqa: UP030

                        else:
                            if not doSpec:
                                line_image_opts.update({"fitsmask": "{0:s}/{1:s}:output".format(get_relative_path(pipeline.masking, pipeline), preGridMask.split("/")[-1])})  # noqa: UP030
                            else:
                                pass

                        if doSpec:
                            gridMask = postGridMask if doProj else preGridMask
                            caracal.log.info(f"Reprojecting mask {gridMask} to match the spectral axis of the cube.")
                            if doProj:
                                hdul = fits.open(f"{pipeline.masking}/{postGridMask}", mode="update")
                            else:
                                hdul = fits.open(f"{pipeline.masking}/{preGridMask}")

                                if os.path.exists(f"{pipeline.masking}/{postGridMask}"):
                                    os.remove(f"{pipeline.masking}/{postGridMask}")

                            if "FREQ" in hdul[0].header["CTYPE3"]:
                                # all in Hz
                                crval = firstchanfreq[0] + chanwidth[0] * firstchan
                                cdeltm = hdul[0].header["CDELT3"]
                            else:
                                # all in m/s
                                crval = C * (femit - (firstchanfreq[0] + chanwidth[0] * firstchan)) / femit
                                cdeltm = hdul[0].header["CDELT3"] * femit / (-C)
                                crvale = C * (femit - (firstchanfreq[0] + chanwidth[0] * firstchan + nchans * binchans * chanwidth[0])) / femit

                            cdelt = chanwidth[0] * binchans  # in Hz

                            hdr = hdul[0].header
                            ax3 = np.arange(
                                hdr["CRVAL3"] - hdr["CDELT3"] * (hdr["CRPIX3"] - 1),
                                hdr["CRVAL3"] + hdr["CDELT3"] * (hdr["NAXIS3"] - hdr["CRPIX3"] + 1),
                                hdr["CDELT3"],
                            )

                            caracal.log.info(f"CDELT = {cdelt}")
                            caracal.log.info(f"ChWidth = {chanwidth[0]}")

                            caracal.log.info(f"CRVAL = {crval}")
                            caracal.log.info(f"CRVALE = {crvale}")

                            caracal.log.info(f"ax3[0] = {ax3[0]}")
                            caracal.log.info(f"ax3[-1] = {ax3[-1]}")

                            if (np.max([crval, crvale]) <= np.max([ax3[0], ax3[-1]])) & (np.min([crval, crvale]) >= np.min([ax3[0], ax3[-1]])):
                                caracal.log.info(f"Requested channels are contained in mask {gridMask}.")

                                idx = np.argmin(abs(ax3 - crval))
                                ide = np.argmin(abs(ax3 - crvale))

                                if cdelt > cdeltm:
                                    hdul[0].data = hdul[0].data[idx:ide]
                                    hdul[0].header["CRPIX3"] = 1
                                    hdul[0].header["CRVAL3"] = crval
                                    hdul[0].header["NAXIS3"] = nchans
                                    hdul[0].header["CDELT3"] = hdul[0].header["CDELT3"] * binchans
                                    if binchans > 1:
                                        if (nchans % binchans) > 0:
                                            rdata = (hdul[0].data[: -(nchans % binchans)]).reshape((
                                                nchans - 1 * (nchans % binchans),
                                                binchans,
                                                hdul[0].header["NAXIS1"],
                                                hdul[0].header["NAXIS2"],
                                            ))
                                            rdata = np.nansum(rdata, axis=1)
                                            rdata = np.concatenate((rdata, np.nansum(hdul[0].data[-(nchans % binchans) :])), axis=0)
                                        else:
                                            rdata = (hdul[0].data).reshape(nchans, binchans, hdul[0].header["NAXIS1"], hdul[0].header["NAXIS2"])
                                            rdata = np.nansum(rdata, axis=1)
                                        rdata[rdata > 0] = 1
                                        hdul[0].data = rdata
                                    else:
                                        pass

                                else:
                                    rdata = np.zeros((nchans, hdr["NAXIS1"], hdr["NAXIS2"]))
                                    rr = int(cdeltm / cdelt)
                                    for nn in range(nchans):
                                        rdata[nn] = hdul[0].data[idx + nn // rr]

                                    hdul[0].header["NAXIS3"] = nchans
                                    hdul[0].header["CRPIX3"] = 1
                                    hdul[0].header["CRVAL3"] = crval
                                    hdul[0].header["CDELT3"] = hdul[0].header["CDELT3"] / rr
                                    hdul[0].data = rdata

                                hdul[0].data = np.around(hdul[0].data.astype(np.float32)).astype(np.int16)
                                if doProj:
                                    hdul.flush()
                                else:
                                    hdul.writeto(f"{pipeline.masking}/{postGridMask}")

                                line_image_opts.update({"fitsmask": "{0:s}/{1:s}:output".format(get_relative_path(pipeline.masking, pipeline), gridMask.split("/")[-1])})  # noqa: UP030
                            else:
                                raise OSError(f"Requested channels are not contained in mask {gridMask}.")

                        else:
                            if not doProj:
                                line_image_opts.update({"fitsmask": "{0:s}/{1:s}:output".format(get_relative_path(pipeline.masking, pipeline), preGridMask.split("/")[-1])})  # noqa: UP030
                            else:
                                pass

                        step = f"make_cube-{line_name:s}-field{tt:d}-iter{jj:d}-with_user_mask"
                    else:
                        line_image_opts.update({"auto-mask": config["make_cube"]["wscl_auto_mask"]})
                        step = f"make_cube-{line_name:s}-field{tt:d}-iter{jj:d}-with_automasking"
                else:
                    step = f"make_SoFiA-2_mask-field{tt:d}-iter{jj - 1:d}"
                    line_clean_mask = f"{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}.image_clean_mask.fits:output"
                    line_clean_mask_file = f"{cube_path:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}.image_clean_mask.fits"
                    cubename = f"{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj - 1:d}.image.fits:input"
                    cubename_file = f"{cube_path:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj - 1:d}.image.fits"
                    outmask = f"{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}.image_clean"

                    sofia2_opts = {
                        "pipeline.threads": 0,
                        "input.data": cubename,
                        "input.mask": config["sofia2_settings"]["input_mask"],
                        "input.noise": config["sofia2_settings"]["input_noise"],
                        "input.weights": config["sofia2_settings"]["input_weights"],
                        "flag.auto": config["sofia2_settings"]["flag_auto"],
                        "flag.catalog": config["sofia2_settings"]["flag_catalog"],
                        "flag.radius": config["sofia2_settings"]["flag_radius"],
                        "flag.region": config["sofia2_settings"]["flag_region"],
                        "flag.threshold": config["sofia2_settings"]["flag_threshold"],
                        "scaleNoise.enable": config["sofia2_settings"]["scaleNoise"]["enable"],
                        "scaleNoise.fluxRange": config["sofia2_settings"]["scaleNoise"]["fluxRange"],
                        "scaleNoise.interpolate": config["sofia2_settings"]["scaleNoise"]["interpolate"],
                        "scaleNoise.mode": config["sofia2_settings"]["scaleNoise"]["mode"],
                        "scaleNoise.scfind": config["sofia2_settings"]["scaleNoise"]["scfind"],
                        "scaleNoise.statistic": config["sofia2_settings"]["scaleNoise"]["statistic"],
                        "scaleNoise.windowXY": config["sofia2_settings"]["scaleNoise"]["windowXY"],
                        "scaleNoise.windowZ": config["sofia2_settings"]["scaleNoise"]["windowZ"],
                        "scfind.enable": config["sofia2_settings"]["scfind"]["enable"],
                        "scfind.fluxRange": config["sofia2_settings"]["scfind"]["fluxRange"],
                        "scfind.kernelsXY": config["sofia2_settings"]["scfind"]["kernelsXY"],
                        "scfind.kernelsZ": config["sofia2_settings"]["scfind"]["kernelsZ"],
                        "scfind.statistic": config["sofia2_settings"]["scfind"]["statistic"],
                        "scfind.threshold": config["sofia2_settings"]["scfind"]["threshold"],
                        "linker.enable": config["sofia2_settings"]["linker"]["enable"],
                        "linker.maxSizeXY": config["sofia2_settings"]["linker"]["maxSizeXY"],
                        "linker.maxSizeZ": config["sofia2_settings"]["linker"]["maxSizeZ"],
                        "linker.minSizeXY": config["sofia2_settings"]["linker"]["minSizeXY"],
                        "linker.minSizeZ": config["sofia2_settings"]["linker"]["minSizeZ"],
                        "linker.radiusXY": config["sofia2_settings"]["linker"]["radiusXY"],
                        "linker.radiusZ": config["sofia2_settings"]["linker"]["radiusZ"],
                        "reliability.enable": config["sofia2_settings"]["reliability"]["enable"],
                        "reliability.minPixels": config["sofia2_settings"]["reliability"]["minPixels"],
                        "reliability.minSNR": config["sofia2_settings"]["reliability"]["minSNR"],
                        "reliability.plot": config["sofia2_settings"]["reliability"]["plot"],
                        "reliability.scaleKernel": config["sofia2_settings"]["reliability"]["scaleKernel"],
                        "reliability.autoKernel": config["sofia2_settings"]["reliability"]["autoKernel"],
                        "reliability.threshold": config["sofia2_settings"]["reliability"]["threshold"],
                        "dilation.enable": config["sofia2_settings"]["dilation"]["enable"],
                        "dilation.iterationsXY": config["sofia2_settings"]["dilation"]["iterationsXY"],
                        "dilation.iterationsZ": config["sofia2_settings"]["dilation"]["iterationsZ"],
                        "dilation.threshold": config["sofia2_settings"]["dilation"]["threshold"],
                        "parameter.enable": config["sofia2_settings"]["parameter"]["enable"],
                        "parameter.physical": True,
                        "output.writeCubelets": config["sofia2_settings"]["output_writeCubelets"],
                        "output.thresholdMom12": config["sofia2_settings"]["output_thresholdMom12"],
                        "output.writeCatASCII": config["sofia2_settings"]["output_writeCatASCII"],
                        "output.writeCatXML": config["sofia2_settings"]["output_writeCatXML"],
                        "output.writeRawMask": config["sofia2_settings"]["output_writeRawMask"],
                        "output.writeMask": True,
                        "output.writeMask2d": config["sofia2_settings"]["output_writeMask2d"],
                        "output.writeMoments": config["sofia2_settings"]["output_writeMoments"],
                        "output.filename": outmask,
                    }
                    # If not user supplied, remove the following inputs
                    keys_to_test = ["input.mask", "input.noise", "input.weights", "flag.catalog"]
                    for key in keys_to_test:
                        if not sofia2_opts.get(key):
                            sofia2_opts.pop(key)

                    recipe.add(
                        "cab/sofia2",
                        step,
                        sofia2_opts,
                        input=pipeline.cubes + "/cube_" + str(jj - 1),
                        output=pipeline.output + "/" + cube_dir,
                        label=f"{step:s}:: Make SoFiA-2 mask",
                    )

                    recipe.run()
                    recipe.jobs = []

                    if not os.path.exists(line_clean_mask_file):
                        caracal.log.info("SoFiA-2 mask_" + str(jj - 1) + " was not found. Exiting and saving the cube")
                        jj -= 1
                        break

                    step = f"make_cube-{line_name:s}-field{tt:d}-iter{jj:d}-with_SoFiA-2_mask"
                    line_image_opts.update({"fitsmask": f"{cube_dir:s}/{line_clean_mask:s}"})
                    line_image_opts.pop("auto-mask", None)

                recipe.add(
                    "cab/wsclean",
                    step,
                    line_image_opts,
                    input=pipeline.input,
                    output=pipeline.output,
                    label=f"{step:s}:: Image Line",
                )
                recipe.run()
                recipe.jobs = []

                # delete line "MFS" images made by WSclean by averaging all channels
                for mfs in glob.glob(f"{pipeline.output:s}/{cube_dir:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}-MFS*fits"):
                    os.remove(mfs)

                # Stack channels together into cubes and fix spectral frame
                if config["make_cube"]["wscl_make_cube"]:
                    if config["make_cube"]["wscl_onlypsf"]:
                        imagetype = [
                            "psf",
                        ]
                    elif not config["make_cube"]["niter"]:
                        imagetype = ["dirty", "image"]
                    else:
                        imagetype = ["dirty", "image", "psf", "residual", "model"]
                        if config["make_cube"]["wscl_mgain"] < 1.0:
                            imagetype.append("first-residual")
                    for mm in imagetype:
                        step = "{0:s}-cubestack-field{1:d}-iter{2:d}".format(mm.replace("-", "_"), tt, jj)  # noqa: UP030
                        if not os.path.exists(f"{pipeline.output:s}/{cube_dir:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}-0000-{mm:s}.fits"):
                            caracal.log.warn(f"Skipping container {step:s}. Single channels do not exist.")
                        else:
                            stacked_cube = f"{cube_dir:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}.{mm:s}.fits"
                            recipe.add(
                                "cab/fitstool",
                                step,
                                {
                                    "file_pattern": f"{cube_dir:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}-*-{mm:s}.fits:output",
                                    "output": stacked_cube,
                                    "stack": True,
                                    "delete-files": True,
                                    "fits-axis": "FREQ",
                                },
                                input=pipeline.input,
                                output=pipeline.output,
                                label="{0:s}:: Make {1:s} cube from wsclean {1:s} channels".format(step, mm.replace("-", "_")),
                            )

                            recipe.run()
                            recipe.jobs = []

                            # Replace channels that are single-valued (usually zero-ed) in the dirty cube with blanks
                            #   in all cubes assuming that channels run along numpy axis 1 (axis 0 is for Stokes)
                            if not config["make_cube"]["wscl_onlypsf"]:
                                with fits.open(f"{pipeline.output:s}/{stacked_cube:s}") as stck:
                                    cubedata = stck[0].data
                                    cubehead = stck[0].header
                                    if mm == "dirty":
                                        tobeblanked = (
                                            cubedata
                                            == np.nanmean(cubedata, axis=(0, 2, 3)).reshape((
                                                1,
                                                cubedata.shape[1],
                                                1,
                                                1,
                                            ))
                                        ).all(axis=(0, 2, 3))
                                    cubedata[:, tobeblanked] = np.nan
                                    fits.writeto(
                                        f"{pipeline.output:s}/{stacked_cube:s}",
                                        cubedata,
                                        header=cubehead,
                                        overwrite=True,
                                    )

                    caracal.log.info(f"Fixing the spectral system of all cubes for target {tt:d}, iteration {jj:d}")
                    for ss in ["dirty", "psf", "first-residual", "residual", "model", "image"]:
                        cubename = f"{pipeline.output:s}/{cube_dir:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}.{ss:s}.fits"
                        recipe.add(
                            fix_specsys_ra,
                            "fixspecsysra-{0:s}-cube-field{1:d}-iter{2:d}".format(ss.replace("_", "-"), tt, jj),  # noqa: UP030
                            {
                                "filename": cubename,
                                "specframe": specframe_all,
                            },
                            input=pipeline.input,
                            output=pipeline.output,
                            label=f"Fix spectral reference frame for cube {cubename:s}",
                        )

                    recipe.run()
                    recipe.jobs = []

                if not config["make_cube"]["wscl_onlypsf"]:
                    cubename_file = f"{pipeline.cubes:s}/cube_{jj:d}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}.image.fits"
                    rms_values.append(calc_rms(cubename_file, line_clean_mask_file))
                    caracal.log.info(f"RMS = {rms_values[-1]:.3e} Jy/beam for {cubename_file:s}")

                # if the RMS has decreased by a factor < wscl_tol compared to the previous cube then cleaning
                # is no longer improving the cube and we can stop
                if len(rms_values) > 1 and wscl_tol and rms_values[-2] / rms_values[-1] <= wscl_tol:
                    caracal.log.info(f"The cube RMS noise has decreased by a factor <= {wscl_tol:.3f} compared to the previous WSclean iteration. Noise convergence achieved.")
                    break

                # If the RMS has decreased by a factor > wscl_tol compared to the previous cube then cleaning
                # is still improving the cube and it's worth continuing with a new SoFiA-2 + WSclean iteration
                elif len(rms_values) > 1 and wscl_tol and rms_values[-2] / rms_values[-1] > wscl_tol:
                    # rms_old = rms_new
                    caracal.log.info(
                        f"The cube RMS noise has decreased by a factor > {wscl_tol:.3f} compared to the previous WSclean iteration. "
                        "The noise has not converged yet and we should continue iterating SoFiA-2 + WSclean."
                    )
                    if jj == wscl_niter:
                        caracal.log.info("Stopping anyway. Maximum number of SoFiA-2 + WSclean iterations reached.")
                    else:
                        caracal.log.info("Starting a new SoFiA-2 + WSclean iteration.")

            # Out of SoFiA-2 + WSclean loop -- prepare final data products
            for ss in ["dirty", "psf", "first-residual", "residual", "model", "image"]:
                if "dirty" in ss:
                    caracal.log.info("Preparing final cubes.")
                cubename = f"{cube_path:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}.{ss:s}.fits"
                finalcubename = f"{cube_path:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}.{ss:s}.fits"
                line_clean_mask_file = f"{cube_path:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}.image_clean_mask.fits"
                final_line_clean_mask_file = f"{cube_path:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}.image_clean_mask.fits"
                MFScubename = f"{cube_path:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}-MFS-{ss:s}.fits"
                finalMFScubename = f"{cube_path:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}-MFS-{ss:s}.fits"
                if os.path.exists(cubename):
                    os.rename(cubename, finalcubename)
                if os.path.exists(line_clean_mask_file):
                    os.rename(line_clean_mask_file, final_line_clean_mask_file)
                if os.path.exists(MFScubename):
                    os.rename(MFScubename, finalMFScubename)

            for jj in range(1, wscl_niter):
                if config["make_cube"]["wscl_removeintermediate"]:
                    for ss in ["dirty", "psf", "first-residual", "residual", "model", "image"]:
                        cubename = f"{pipeline.cubes:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}.{ss:s}.fits"
                        line_clean_mask_file = f"{pipeline.cubes:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}.image_clean_mask.fits"
                        MFScubename = f"{pipeline.cubes:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}_{jj:d}-MFS-{ss:s}.fits"
                        if os.path.exists(cubename):
                            os.remove(cubename)
                        if os.path.exists(line_clean_mask_file):
                            os.remove(line_clean_mask_file)
                        if os.path.exists(MFScubename):
                            os.remove(MFScubename)

    if pipeline.enable_task(config, "make_cube") and config["make_cube"]["image_with"] == "casa":
        cube_dir = get_relative_path(pipeline.cubes, pipeline)
        nchans_all, specframe_all = [], []
        label = config["label_in"]
        if label != "":
            flabel = "_" + label
        else:
            flabel = label

        caracal.log.info("Collecting spectral info on MS files being imaged")
        if config["make_cube"]["use_mstransform"]:
            for i, msfile in enumerate(all_msfiles):
                if not pipeline.enable_task(config, "mstransform"):
                    msinfo = pipeline.get_msinfo(msfile)
                    spw = msinfo["SPW"]["NUM_CHAN"]
                    nchans = spw
                    nchans_all.append(nchans)
                    caracal.log.info(f"MS #{i:d}: {msfile:s}")
                    caracal.log.info("  {0:d} spectral windows, with NCHAN={1:s}".format(len(spw), ",".join(map(str, spw))))  # noqa: UP030

                    # Get first chan, last chan, chan width
                    chfr = msinfo["SPW"]["CHAN_FREQ"]
                    firstchanfreq = [ss[0] for ss in chfr]
                    lastchanfreq = [ss[-1] for ss in chfr]
                    chanwidth = [(ss[-1] - ss[0]) / (len(ss) - 1) for ss in chfr]
                    caracal.log.info(
                        "  CHAN_FREQ from {0:s} Hz to {1:s} Hz with average channel width of {2:s} Hz".format(  # noqa: UP030
                            ",".join(map(str, firstchanfreq)),
                            ",".join(map(str, lastchanfreq)),
                            ",".join(map(str, chanwidth)),
                        )
                    )

                    specframe = msinfo["SPW"]["MEAS_FREQ_REF"]
                    specframe_all.append(specframe)
                    caracal.log.info(f"  The spectral reference frame is {specframe}")

                elif pipeline.enable_task(config["mstransform"], "doppler"):
                    nchans_all[i] = [nchan_dopp for kk in chanw_all[i]]
                    specframe_all.append([{"lsrd": 0, "lsrk": 1, "galacto": 2, "bary": 3, "geo": 4, "topo": 5}[config["mstransform"]["doppler"]["frame"]] for kk in chanw_all[i]])
        else:
            for i, msfile in enumerate(all_msfiles):
                msinfo = pipeline.get_msinfo(msfile)
                spw = msinfo["SPW"]["NUM_CHAN"]
                nchans = spw
                nchans_all.append(nchans)
                caracal.log.info(f"MS {i:d}: {msfile:s}")
                caracal.log.info("  {0:d} spectral windows, with NCHAN={1:s}".format(len(spw), ",".join(map(str, spw))))  # noqa: UP030
                specframe = msinfo["SPW"]["MEAS_FREQ_REF"]
                specframe_all.append(specframe)
                caracal.log.info(f"  The spectral reference frame is {specframe}")

        spwid = config["make_cube"]["spwid"]
        nchans = config["make_cube"]["nchans"]
        if nchans == 0:
            # Assuming user wants same spw for all msfiles and they have same
            # number of channels
            nchans = nchans_all[0][spwid]
        # Assuming user wants same spw for all msfiles and they have same
        # specframe
        specframe_all = [ss[spwid] for ss in specframe_all][0]  # noqa: RUF015
        firstchan = config["make_cube"]["firstchan"]
        binchans = config["make_cube"]["binchans"]
        channelrange = [firstchan, firstchan + nchans * binchans]
        # Construct weight specification
        if config["make_cube"]["weight"] == "briggs":
            weight = "briggs {0:.3f}".format(config["make_cube"]["robust"])  # noqa: UP030
        else:
            weight = config["make_cube"]["weight"]

        for tt, target in enumerate(all_targets):
            if config["make_cube"]["use_mstransform"]:
                mslist = [add_ms_label(ms, "mst") for ms in ms_dict[target]]
            else:
                mslist = ms_dict[target]
            field = utils.filter_name(target)

            step = f"make_line_cube-field{tt:d}"
            image_opts = {
                "msname": mslist,
                "prefix": f"{cube_dir:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}",
                "mode": "channel",
                "nchan": nchans,
                "start": config["make_cube"]["firstchan"],
                "interpolation": "nearest",
                "niter": config["make_cube"]["niter"],
                "gain": config["make_cube"]["gain"],
                "psfmode": "hogbom",
                "threshold": config["make_cube"]["casa_thr"],
                "npix": config["make_cube"]["npix"],
                "cellsize": config["make_cube"]["cell"],
                "weight": config["make_cube"]["weight"],
                "robust": config["make_cube"]["robust"],
                "stokes": config["make_cube"]["stokes"],
                "port2fits": config["make_cube"]["casa_port2fits"],
                "restfreq": restfreq,
            }
            if config["make_cube"]["taper"] != "":
                image_opts.update({
                    "uvtaper": True,
                    "outertaper": config["make_cube"]["taper"],
                })
            recipe.add(
                "cab/casa_clean",
                step,
                image_opts,
                input=pipeline.input,
                output=pipeline.output,
                label=f"{step:s}:: Image Line",
            )

    recipe.run()
    recipe.jobs = []

    # Once all cubes have been made fix the headers etc.
    # Search cubes and cubes/cubes_*/ for cubes whose header should be fixed
    cube_dir = get_relative_path(pipeline.cubes, pipeline)
    for tt, target in enumerate(all_targets):
        field = utils.filter_name(target)

        casa_cube_list = glob.glob(f"{pipeline.output:s}/{cube_dir:s}/{pipeline.prefix:s}_{field:s}_{line_name:s}*.fits")
        wscl_cube_list = glob.glob(f"{pipeline.output:s}/{cube_dir:s}/cube_*/{pipeline.prefix:s}_{field:s}_{line_name:s}*.fits")
        cube_list = casa_cube_list + wscl_cube_list
        # image_cube_list = [cc for cc in cube_list if "image.fits" in cc]

        final_wscl_cube_list = glob.glob(f"{pipeline.output:s}/{cube_dir:s}/cube_*/{pipeline.prefix:s}_{field:s}_{line_name:s}.*.fits")
        final_cube_list = casa_cube_list + final_wscl_cube_list
        final_image_cube_list = [cc for cc in final_cube_list if "image.fits" in cc]

        if pipeline.enable_task(config, "pb_cube"):
            caracal.log.info(f"Will create primary beam cube for the final image cube of target {tt:d}: {final_image_cube_list}")
            for uu in range(len(final_image_cube_list)):
                recipe.add(
                    make_pb_cube,
                    f"make pb_cube-{uu:d}",
                    {
                        "filename": final_image_cube_list[uu],
                        "apply_corr": config["pb_cube"]["apply_pb"],
                        "typ": config["pb_cube"]["pb_type"],
                        "dish_size": config["pb_cube"]["dish_size"],
                        "cutoff": config["pb_cube"]["cutoff"],
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label=f"Make primary beam cube for {final_image_cube_list[uu]:s}",
                )
                cube_list.append(final_image_cube_list[uu].replace("image.fits", "pb.fits"))
                if config["pb_cube"]["apply_pb"]:
                    cube_list.append(final_image_cube_list[uu].replace("image.fits", "pb_corr.fits"))

        if pipeline.enable_task(config, "remove_stokes_axis"):
            caracal.log.info(f"Will remove Stokes axis of all cubes of target {tt:d}: {cube_list}")
            for uu in range(len(cube_list)):
                recipe.add(
                    remove_stokes_axis,
                    f"remove_cube_stokes_axis-{uu:d}",
                    {
                        "filename": cube_list[uu],
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label=f"Remove Stokes axis for cube {cube_list[uu]:s}",
                )

        if pipeline.enable_task(config, "freq_to_vel"):
            if not config["freq_to_vel"]["reverse"]:
                caracal.log.info(f"Will convert spectral axis of all cubes from frequency to radio velocity for target {tt:d}: {cube_list}")
            else:
                caracal.log.info(f"Will convert spectral axis of all cubes from radio velocity to frequency for target {tt:d}: {cube_list}")
            for uu in range(len(cube_list)):
                recipe.add(
                    freq_to_vel,
                    f"convert-spectral_header-cube{uu:d}",
                    {
                        "filename": cube_list[uu],
                        "reverse": config["freq_to_vel"]["reverse"],
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label=f"Convert spectral axis from frequency to radio velocity for cube {cube_list[uu]:s}",
                )

        recipe.run()
        recipe.jobs = []

        # The following runs on all cubes in final_image_cube_list instead of all cubes in image_cube_list

        if config["do_sourcefinding"]:
            if config["sofia2_settings"]["imcontsub"]:
                simage_cube_list = []
                for uu in range(len(final_image_cube_list)):
                    icsname = final_image_cube_list[uu].replace(".image.fits", ".image-line.fits")
                    if len(glob.glob(icsname)) > 0:
                        simage_cube_list.append(icsname)
                    else:
                        simage_cube_list.append(final_image_cube_list[uu])
            else:
                simage_cube_list = final_image_cube_list

            caracal.log.info(f"Will find sources with SoFiA for the final image cube of target {tt:d}: {final_image_cube_list}")
            for uu in range(len(final_image_cube_list)):
                step = f"sofia2-source_finding-{uu:d}"
                sofia2_opts = {
                    "pipeline.threads": 0,
                    "input.data": simage_cube_list[uu].split("/")[-1] + ":input",
                    "input.mask": config["sofia2_settings"]["input_mask"],
                    "input.noise": config["sofia2_settings"]["input_noise"],
                    "input.weights": config["sofia2_settings"]["input_weights"],
                    "flag.auto": config["sofia2_settings"]["flag_auto"],
                    "flag.catalog": config["sofia2_settings"]["flag_catalog"],
                    "flag.radius": config["sofia2_settings"]["flag_radius"],
                    "flag.region": config["sofia2_settings"]["flag_region"],
                    "flag.threshold": config["sofia2_settings"]["flag_threshold"],
                    "scaleNoise.enable": config["sofia2_settings"]["scaleNoise"]["enable"],
                    "scaleNoise.fluxRange": config["sofia2_settings"]["scaleNoise"]["fluxRange"],
                    "scaleNoise.interpolate": config["sofia2_settings"]["scaleNoise"]["interpolate"],
                    "scaleNoise.mode": config["sofia2_settings"]["scaleNoise"]["mode"],
                    "scaleNoise.scfind": config["sofia2_settings"]["scaleNoise"]["scfind"],
                    "scaleNoise.statistic": config["sofia2_settings"]["scaleNoise"]["statistic"],
                    "scaleNoise.windowXY": config["sofia2_settings"]["scaleNoise"]["windowXY"],
                    "scaleNoise.windowZ": config["sofia2_settings"]["scaleNoise"]["windowZ"],
                    "scfind.enable": config["sofia2_settings"]["scfind"]["enable"],
                    "scfind.fluxRange": config["sofia2_settings"]["scfind"]["fluxRange"],
                    "scfind.kernelsXY": config["sofia2_settings"]["scfind"]["kernelsXY"],
                    "scfind.kernelsZ": config["sofia2_settings"]["scfind"]["kernelsZ"],
                    "scfind.statistic": config["sofia2_settings"]["scfind"]["statistic"],
                    "scfind.threshold": config["sofia2_settings"]["scfind"]["threshold"],
                    "linker.enable": config["sofia2_settings"]["linker"]["enable"],
                    "linker.maxSizeXY": config["sofia2_settings"]["linker"]["maxSizeXY"],
                    "linker.maxSizeZ": config["sofia2_settings"]["linker"]["maxSizeZ"],
                    "linker.minSizeXY": config["sofia2_settings"]["linker"]["minSizeXY"],
                    "linker.minSizeZ": config["sofia2_settings"]["linker"]["minSizeZ"],
                    "linker.radiusXY": config["sofia2_settings"]["linker"]["radiusXY"],
                    "linker.radiusZ": config["sofia2_settings"]["linker"]["radiusZ"],
                    "reliability.enable": config["sofia2_settings"]["reliability"]["enable"],
                    "reliability.minPixels": config["sofia2_settings"]["reliability"]["minPixels"],
                    "reliability.minSNR": config["sofia2_settings"]["reliability"]["minSNR"],
                    "reliability.plot": config["sofia2_settings"]["reliability"]["plot"],
                    "reliability.scaleKernel": config["sofia2_settings"]["reliability"]["scaleKernel"],
                    "reliability.autoKernel": config["sofia2_settings"]["reliability"]["autoKernel"],
                    "reliability.threshold": config["sofia2_settings"]["reliability"]["threshold"],
                    "dilation.enable": config["sofia2_settings"]["dilation"]["enable"],
                    "dilation.iterationsXY": config["sofia2_settings"]["dilation"]["iterationsXY"],
                    "dilation.iterationsZ": config["sofia2_settings"]["dilation"]["iterationsZ"],
                    "dilation.threshold": config["sofia2_settings"]["dilation"]["threshold"],
                    "parameter.enable": config["sofia2_settings"]["parameter"]["enable"],
                    "parameter.physical": True,
                    "output.writeCubelets": config["sofia2_settings"]["output_writeCubelets"],
                    "output.thresholdMom12": config["sofia2_settings"]["output_thresholdMom12"],
                    "output.writeCatASCII": config["sofia2_settings"]["output_writeCatASCII"],
                    "output.writeCatXML": config["sofia2_settings"]["output_writeCatXML"],
                    "output.writeRawMask": config["sofia2_settings"]["output_writeRawMask"],
                    "output.writeMask": True,
                    "output.writeMask2d": config["sofia2_settings"]["output_writeMask2d"],
                    "output.writeMoments": config["sofia2_settings"]["output_writeMoments"],
                }
                # If not user supplied, remove the following inputs
                keys_to_test = ["input.mask", "input.noise", "input.weights", "flag.catalog"]
                for key in keys_to_test:
                    if not sofia2_opts.get(key):
                        sofia2_opts.pop(key)

                recipe.add(
                    "cab/sofia2",
                    step,
                    sofia2_opts,
                    input="/".join(simage_cube_list[uu].split("/")[:-1]),
                    output="/".join(simage_cube_list[uu].split("/")[:-1]) + "/sofia",
                    label=f"{step:s}:: Make SoFiA-2 mask and images for cube {simage_cube_list[uu]:s}",
                )

        if pipeline.enable_task(config, "imcontsub"):
            imcontsub_opts = {
                "output-prefix": config["imcontsub"]["label_out"],
                "order": config["imcontsub"]["order"],
                "sigma-clip": config["imcontsub"]["sigma_clip"],
                "ra-chunks": sdm.dismissable(config["imcontsub"]["ra_chunks"]),
                "nworkers": sdm.dismissable(config["imcontsub"]["ncpus"]),
                "cont-fit-tol": sdm.dismissable(config["imcontsub"]["cont_fit_tol"]),
                "segments": sdm.dismissable(config["imcontsub"]["segments"]),
            }
            # C = 2.99792458e8  # m/s
            # HI = 1.4204057517667e9  # Hz
            caracal.log.info("Image-plane continuum subtraction")
            # find where cubes generally are

            dirlist = glob.glob(f"{pipeline.output:s}/{cube_dir:s}/cube_*")
            maxcube_dir = max([int(gi[-1]) for gi in dirlist])  # noqa: C419

            # Here starts the loop within the logic of the line worker.
            # Imcontsub will use subtract into the datacube in the cubes directory of maximum order.
            # caracal will look for the corresponding mask saved by sofia, if this does not exist
            #  imcontsub will use the automasking method
            if not config["imcontsub"]["input_cube"]:
                imsub_image_cube_list = final_image_cube_list.copy()
                caracal.log.info(f"Continum subtraction in the image-plane for the final cube of target {tt:d}: {imsub_image_cube_list}")
                for uu in range(len(imsub_image_cube_list)):
                    step = f"Image-continuum-subtraction-{uu:d}"
                    input_cube = imsub_image_cube_list[uu].split("/")[-1]

                    if config["imcontsub"]["mask_image"] == "sofia":
                        mask_name = input_cube.split(".image")[0] + ".image_mask.fits"
                        if os.path.exists(f"{pipeline.cubes:s}/cube_{maxcube_dir:d}/{mask_name:s}"):
                            caracal.log.info("Using the mask produced by SoFiA within the CARACal line-worker loop")
                            imcontsub_opts.update({"mask-image": f"{cube_dir:s}/cube_{maxcube_dir:d}/{mask_name:s}" + ":input"})
                        else:
                            caracal.log.info("Mask generated by SoFiA not found, using automasking method. Suggest to activate sofia:enable to generate a mask")
                    else:
                        if len(config["imcontsub"]["mask_image"].strip()) != 0 and os.path.exists("{0:s}/{1:s}".format(pipeline.masking, config["imcontsub"]["mask_image"])):  # noqa: UP030
                            caracal.log.info("Using mask defined by user {0:s}".format(config["imcontsub"]["mask_image"]))  # noqa: UP030
                            print("{0:s}/{1:s}".format(pipeline.masking, config["imcontsub"]["mask_image"]))  # noqa: UP030
                            imcontsub_opts.update({"mask-image": "masking/{0:s}".format(config["imcontsub"]["mask_image"])})  # noqa: UP030

                        else:
                            caracal.log.info("Mask datacube not found in output/masking")
                            caracal.log.info("Will proceed with automasking")

                    ##the segment size is chosen as the datacube velocity range / spline order
                    if all(item == 0.0 for item in config["imcontsub"]["segments"]):
                        hdul_cube = fits.getheader(f"{pipeline.cubes:s}/cube_{maxcube_dir:d}/{input_cube:s}")

                        if "FREQ" in hdul_cube["CTYPE3"]:
                            if "RESTFREQ" in hdul_cube:
                                restfreq_cube = hdul_cube["RESTFREQ"]
                            else:
                                restfreq_cube = config["imcontsub"]["rest_freq"] * 1e6
                            vel_step = -C * float(hdul_cube["cdelt3"]) / restfreq_cube
                            vel_range = abs(vel_step * hdul_cube["naxis3"] / 1e3)
                        else:
                            vel_range = abs(hdul_cube["cdelt3"] * hdul_cube["naxis3"])
                            if "km" in hdul_cube["cdelt3"].lower():
                                # if cube in m/s then convert the velocity_range in km/s
                                vel_range = vel_range / 1e3

                        for item in config["imcontsub"]["segments"]:
                            if vel_range < config["imcontsub"]["segments"]:
                                caracal.log.warning("The width of the spline is larger than the spectral width of the cube, please check your segments keyword")

                        # config["imcontsub"]["segments"] = [round(vel_range, 0) / item for item in config["imcontsub"]["order"]]

                    # imcontsub_opts.update({"segments": config["imcontsub"]["segments"]})

                    if len(config["imcontsub"]["label_out"]) == 0:
                        imcontsub_opts.update({"output-prefix": input_cube.split(".fits")[0]})

                    imcontsub_opts.update({"infits": f"{cube_dir:s}/cube_{maxcube_dir:d}/{input_cube:s}" + ":input"})
                    recipe.add(
                        "cab/imcontsub",
                        step,
                        imcontsub_opts,
                        input=pipeline.output,
                        output=f"{pipeline.output:s}/cubes/cube_{maxcube_dir:d}/",
                        label=f"{step:s}:: Image continuum subtraction for cube ",
                    )
                    recipe.run()
                    recipe.jobs = []

                caracal.log.info(f"Subtracted continuum in the image domain for target {tt:d}")

            # this is the option if the input datacube is provided by the user in the config file
            else:
                cubepaths_to_check = [
                    "{0:s}/{1:s}".format(pipeline.output, config["imcontsub"]["input_cube"]),  # noqa: UP030
                    "{0:s}/cubes/{1:s}".format(pipeline.output, config["imcontsub"]["input_cube"]),  # noqa: UP030
                    "{0:s}/cubes/cube_{1:d}/{2:s}".format(pipeline.output, maxcube_dir, config["imcontsub"]["input_cube"]),  # noqa: UP030
                ]
                cubepath = next((path for path in cubepaths_to_check if os.path.exists(path)), None)
                cubepath = cubepath.split(pipeline.output)[-1]
                cubedirname = os.path.dirname(os.path.abspath(cubepath))

                if cubepath:
                    caracal.log.info("Continum subtraction in the image plage on datacube {0:s} provided by user ".format(config["imcontsub"]["input_cube"]))  # noqa: UP030
                    step = "Image-continuum-subtraction-{0:s}".format(config["imcontsub"]["input_cube"])  # noqa: UP030
                    imcontsub_opts["infits"] = f"{cubepath}:input"

                    ##the segment size is chosen as the datacube velocity range / spline order
                    if all(item == 0.0 for item in config["imcontsub"]["segments"]):
                        hdul_cube = fits.getheader(f"{pipeline.output:s}/{cubepath:s}")
                        if "FREQ" in hdul_cube["CTYPE3"]:
                            if "RESTFREQ" in hdul_cube:
                                restfreq_cube = hdul_cube["RESTFREQ"]
                            else:
                                restfreq_cube = config["imcontsub"]["rest_freq"] * 1e6
                            hdul_cube["cdelt3"] = -C * float(hdul_cube["cdelt3"]) / restfreq_cube
                            vel_range = abs(hdul_cube["cdelt3"] * hdul_cube["naxis3"])
                        else:
                            vel_range = abs(hdul_cube["cdelt3"] * hdul_cube["naxis3"])
                            if "km" in hdul_cube["cdelt3"].lower():
                                # if cube in m/s then convert the velocity_range in km/s
                                vel_range = vel_range / 1e3
                        for item in config["imcontsub"]["segments"]:
                            if vel_range < config["imcontsub"]["segments"]:
                                caracal.log.warning("The width of the spline is larger than the spectral width of the cube, please check your segments keyword")

                else:
                    caracal.log.error(
                        "Input datacube {0:1} not found in output/ or output/cubes or output/cubes/cube_{1:d} please check your configuration file.".format(  # noqa: UP030
                            config["imcontsub"]["input_cube"], maxcube_dir
                        )
                    )
                    raise caracal.ConfigurationError("check imcontsub:input_cube: setting")

                if config["imcontsub"]["mask_image"].split(".fits")[-1] != (None and "sofia"):  # noqa: SIM223
                    caracal.log.info("Mask provided by user, continuum subtraction will exclude the masked pixels from fitting.")
                    path_mask = "{0:s}/{1:s}".format(pipeline.masking, config["imcontsub"]["mask_image"])  # noqa: UP030

                    if os.path.exists(path_mask):
                        imcontsub_opts.update({"mask-image": "masking/{0:s}".format(config["imcontsub"]["mask_image"])})  # noqa: UP030
                    else:
                        caracal.log.info(f"Mask datacube {path_mask:s} not found")
                        caracal.log.info("Will proceed with automasking")

                if len(config["imcontsub"]["label_out"]) == 0:
                    imcontsub_opts.update({"output-prefix": config["imcontsub"]["input_cube"].split(".fits")[0]})

                recipe.add(
                    "cab/imcontsub",
                    step,
                    imcontsub_opts,
                    input=pipeline.output,
                    output=f"{pipeline.output:s}/{cubedirname:s}",
                    label=f"{step:s}:: Single cube continuum subtraction",
                )
                recipe.run()
                recipe.jobs = []

                caracal.log.info("Subtracted continuum in the image domain for datacube {0:s} provided by user ".format(config["imcontsub"]["input_cube"]))  # noqa: UP030

                if len(config["imcontsub"]["input_cube"]) < tt:
                    break

        if pipeline.enable_task(config, "sharpener"):
            caracal.log.info(f"Running Sharpener on the final cube of target {tt:d}: {final_image_cube_list}")
            for uu in range(len(final_image_cube_list)):
                step = f"continuum-spectral_extraction-{uu:d}"

                params = {
                    "enable_spec_ex": True,
                    "enable_source_catalog": True,
                    "enable_abs_plot": True,
                    "enable_source_finder": False,
                    "cubename": final_image_cube_list[uu] + ":output",
                    "channels_per_plot": config["sharpener"]["chans_per_plot"],
                    "workdir": "{0:s}/".format(stimela.recipe.CONT_IO["output"]),  # noqa: UP030
                    "label": config["sharpener"]["label"],
                }

                runsharp = False
                if config["sharpener"]["catalog"] == "PYBDSF":
                    catalogs = []
                    nimages = glob.glob(f"{pipeline.continuum:s}/image_*")

                    for ii in range(len(nimages)):
                        catalog = glob.glob(f"{pipeline.continuum:s}/image_{ii + 1:d}/{pipeline.prefix:s}_{field:s}_*.lsm.html")
                        catalogs.append(catalog)

                    catalogs = sorted(catalogs)
                    catalogs = [cat for catalogs in catalogs for cat in catalogs]
                    # Right now, this is the last catalog made
                    if len(catalogs):
                        catalog_file = catalogs[-1].split("output/")[-1]
                        params["catalog_file"] = f"{catalog_file:s}:output"
                    else:
                        catalog_file = []

                    if len(catalog_file) > 0:
                        runsharp = True
                        params["catalog"] = "PYBDSF"
                        recipe.add(
                            "cab/sharpener",
                            step,
                            params,
                            input="/".join(f"{pipeline.output:s}/{final_image_cube_list[uu]:s}".split("/")[:-1]),
                            output=pipeline.output,
                            label=f"{step:s}:: Continuum Spectral Extraction",
                        )
                    else:
                        caracal.log.warn("No PyBDSM catalogs found. Skipping continuum spectral extraction.")

                elif config["sharpener"]["catalog"] == "NVSS":
                    runsharp = True
                    params["thr"] = config["sharpener"]["thr"]
                    params["width"] = config["sharpener"]["width"]
                    params["catalog"] = "NVSS"
                    recipe.add(
                        "cab/sharpener",
                        step,
                        params,
                        input="/".join(f"{pipeline.output:s}/{final_image_cube_list[uu]:s}".split("/")[:-1]),
                        output=pipeline.output,
                        label=f"{step:s}:: Continuum Spectral Extraction",
                    )

                recipe.run()
                recipe.jobs = []

                # Move the sharpener output to diagnostic_plots
                if runsharp:
                    sharpOut = "{0:s}/{1:s}".format(pipeline.output, "sharpOut")  # noqa: UP030
                    finalsharpOut = "{0:s}/{1:s}_{2:s}_{3:s}".format(pipeline.diagnostic_plots, pipeline.prefix, field, "sharpOut")  # noqa: UP030
                    if os.path.exists(finalsharpOut):
                        shutil.rmtree(finalsharpOut)
                    shutil.move(sharpOut, finalsharpOut)

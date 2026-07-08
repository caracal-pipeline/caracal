import os

from caracal import log


def conflict(conflict_type, pipeline, wname, ms, config, flags_bw, flags_aw, read_version="version"):
    av_flagversions = get_flags(pipeline, ms)
    req_version = config["rewind_flags"][read_version]
    if req_version == "auto":
        req_version = flags_bw
    if conflict_type == "would_overwrite_bw" or conflict_type == "rewind_too_little":
        log.error(f"Flag version conflicts for {ms:s} . If you are running Caracal on multiple targets")
        log.error("and/or .MS files please read the warning at the end of this message.")
        log.error("---------------------------------------------------------------------------------------------------")
        log.error(f'A worker named "{wname:s}" was already run on the .MS file {ms:s} with pipeline prefix "{pipeline.prefix:s}".')
        if conflict_type == "rewind_too_little":
            log.error(f"and you are rewinding to a later flag version: {req_version:s} .")
        log.error(f'Running "{wname:s}" again will attempt to overwrite existing flag versions, it might get messy.')
        log.error(f'Caracal will not overwrite the "{wname:s}" flag versions unless you explicitely request that.')
        log.error("The current flag versions of this MS are (from the oldest to the most recent):")
        for vv in av_flagversions:
            if vv == flags_bw:
                log.error(f"       {vv:s}        <-- (this worker)")
            elif vv == flags_aw:
                log.error(f"       {vv:s}         <-- (this worker)")
            elif config["rewind_flags"]["enable"] and vv == req_version:
                log.error(f"       {vv:s}        <-- (rewinding to this version)")
            else:
                log.error(f"       {vv:s}")
        log.error("You have the following options:")
        log.error("    1) If you are happy with the flags currently stored in the FLAG column of this MS and")
        log.error("       want to append new flags to them, change the name of this worker in the configuration")
        log.error('       file by appending "__n" to it (where n is an integer not already taken in the list')
        log.error("       above). The new flags will be appended to the FLAG column, and new flag versions will")
        log.error("       be added to the list above.")
        log.error(f'    2) If you want to discard the flags obtained during the previous run of "{wname:s}" (and,')
        log.error(f'       necessarily, all flags obtained thereafter; see list above) reset the "{wname:s}" worker')
        log.error("       to its starting flag version by setting in the configuration file:")
        log.error(f"           {wname:s}:")
        log.error("             rewind_flags:")
        log.error("               enable: true")
        log.error("               mode: reset_worker")
        log.error(f"       This will rewind to the flag version {flags_bw:s}. You will loose all flags")
        log.error("       appended to the FLAG column after that version, and take it from there.")
        log.error(f'    3) If you want to discard the flags obtained during the previous run of "{wname:s}" and')
        log.error("       rewind to an even earlier flag version from the list above set:")
        log.error(f"           {wname:s}:")
        log.error("             rewind_flags:")
        log.error("               enable: true")
        log.error("               mode: rewind_to_version")
        log.error(f"               {read_version:s}: <version_name>")
        log.error("       This will rewind to the requested flag version. You will loose all flags appended")
        log.error("       to the FLAG column after that version, and take it from there.")
        log.error("    4) If you really know what you are doing, allow Caracal to overwrite flag versions by setting:")
        log.error(f"           {wname:s}:")
        log.error("             overwrite_flagvers: true")
        log.error(f'       The worker "{wname:s}" will be run again; the new flags will be appended to the current')
        log.error("       FLAG column (or to whatever flag version you are rewinding to); the flag version")
        log.error(f'       "{flags_bw:s}" will be overwritten and appended to the list above (or to')
        log.error("       that list truncated to the flag version you are rewinding to).")
        log.error("---------------------------------------------------------------------------------------------------")
        log.error(f'Warning - Your choice will be applied to all .MS files being processed by the worker "{wname:s}".')
        log.error('If using the rewind_flags mode "rewind_to_version", make sure to rewind to a flag version that')
        log.error('exists for all .MS files. If using the rewind_flags mode "reset_worker" each .MS file is taken')
        log.error("care of automatically and you do not need to worry about it.")

    elif conflict_type == "rewind_to_non_existing":
        log.error(f'You have asked to rewind the flags of {ms:s} to the version "{req_version:s}" but this version')
        log.error("does not exist. The available flag versions for this .MS file are:")
        for vv in av_flagversions:
            log.error(f"       {vv:s}")
        log.error("Note that if you are running Caracal on multiple targets and/or .MS files you should rewind to a flag")
        log.error("version that exists for all of them.")

    raise RuntimeError("Flag version conflicts.")


def get_flags(pipeline, ms):
    flaglist_file = f"{pipeline.msdir:s}/{ms:s}.flagversions/FLAG_VERSION_LIST"
    flaglist = []
    if not os.path.exists(flaglist_file):
        return []
    with open(flaglist_file) as stdr:
        for line in stdr:
            flag = line.split()[0]
            flaglist.append(flag)
    return flaglist


def delete_cflags(pipeline, recipe, flagname, ms, cab_name="rando_cab", label=""):
    flaglist = get_flags(pipeline, ms)
    if flagname == "all":
        remove_us = flaglist
    elif flagname in flaglist:
        index = flaglist.index(flagname)
        remove_us = flaglist[index:]
    else:
        return

    for i, flag in enumerate(remove_us):
        recipe.add(
            "cab/casa_flagmanager",
            f"{cab_name:s}_{i:d}",
            {
                "vis": ms,
                "mode": "delete",
                "versionname": flag,
            },
            input=pipeline.input,
            output=pipeline.output,
            label=f"{label or cab_name:s}:: Delete flags (step {i:d})",
        )


def restore_cflags(pipeline, recipe, flagname, ms, cab_name="rando_cab", label="", merge=False):
    if flagname in get_flags(pipeline, ms):
        recipe.add(
            "cab/casa_flagmanager",
            cab_name,
            {
                "vis": ms,
                "mode": "restore",
                "versionname": flagname,
                "merge": "replace",
            },
            input=pipeline.input,
            output=pipeline.output,
            label=f"{label or cab_name:s}:: Restoring flags to flag version [{flagname:s}]",
        )
    else:
        log.warn(f"Flag version [{flagname:s}] could not be found")


def add_cflags(pipeline, recipe, flagname, ms, cab_name="rando_cab", label="", overwrite=False):
    if flagname in get_flags(pipeline, ms) and overwrite:
        recipe.add(
            "cab/casa_flagmanager",
            cab_name.replace("save", "delete"),
            {
                "vis": ms,
                "mode": "delete",
                "versionname": flagname,
            },
            input=pipeline.input,
            output=pipeline.output,
            label="{0:s}:: Delete flag version".format(label or cab_name.replace("save", "delete")),  # noqa: UP030
        )

    recipe.add(
        "cab/casa_flagmanager",
        cab_name,
        {
            "vis": ms,
            "mode": "save",
            "versionname": flagname,
        },
        input=pipeline.input,
        output=pipeline.output,
        label=f"{label or cab_name:s}:: Save flag version",
    )


def delete_flagset(pipeline, recipe, flagset, ms, clear_existing=True, cab_name="rando_cab", label=""):
    """Add flagset if it does not exist, clear its flags if exists"""

    recipe.add(
        "cab/pycasacore",
        cab_name,
        {
            "msname": ms,
            "script": f"""
import Owlcat.Flagger
import os
import subprocess

Owlcat.Flagger.has_purr = False
MSDIR = os.environ["MSDIR"]
ms = os.path.join(MSDIR, "{ms:s}")

fms = Owlcat.Flagger.Flagger(ms)

fms.add_bitflags()

if hasattr(fms.flagsets, "names"):
    names = fms.flagsets.names()
else:
    names = []

fms.close()
flagset = "{flagset:s}"
if names and flagset in names:
    idx = names.index(flagset)
    remove_us = names[idx:]
    subprocess.check_call(["flag-ms.py", "--remove", ",".join(remove_us), ms])
else:
    print("INFO::: Flagset does not exist. Will exit gracefully (exit status 0).")
""",
        },
        input=pipeline.input,
        output=pipeline.output,
        label=label or cab_name,
    )


def clear_flagset(pipeline, recipe, flagset, ms, clear_existing=True, cab_name="rando_cab", label=""):
    """Add flagset if it does not exist, clear its flags if exists"""

    recipe.add(
        "cab/pycasacore",
        cab_name,
        {
            "msname": ms,
            "script": f"""
import Owlcat.Flagger
import os
import subprocess

Owlcat.Flagger.has_purr = False
MSDIR = os.environ["MSDIR"]
ms = os.path.join(MSDIR, "{ms:s}")

fms = Owlcat.Flagger.Flagger(ms)

fms.add_bitflags()

if hasattr(fms.flagsets, "names"):
    names = fms.flagsets.names()
else:
    names = []
fms.close()
flagset = "{flagset:s}"

if flagset in names:
    subprocess.check_call(["flag-ms.py", "--unflag", flagset, ms])
""",
        },
        input=pipeline.input,
        output=pipeline.output,
        label=label or cab_name,
    )


def update_flagset(pipeline, recipe, flagset, ms, clear_existing=True, cab_name="rando_cab", label=""):
    """Add flagset if it does not exist, clear its flags if exists"""

    recipe.add(
        "cab/pycasacore",
        cab_name,
        {
            "msname": ms,
            "script": f"""
import Owlcat.Flagger
import os
import subprocess

Owlcat.Flagger.has_purr = False
MSDIR = os.environ["MSDIR"]
ms = os.path.join(MSDIR, "{ms:s}")

fms = Owlcat.Flagger.Flagger(ms)
fms.add_bitflags()

if hasattr(fms.flagsets, "names"):
    names = fms.flagsets.names()
else:
    names = []

fms.close()

flagset = "{flagset:s}"

if flagset not in names:
    subprocess.check_call(["flag-ms.py", "--flag", flagset, "--flagged-any", "+L", "--create", ms])
else:
    subprocess.check_call(["flag-ms.py", "--flag", flagset, "--flagged-any", "+L", ms])
""",
        },
        input=pipeline.input,
        output=pipeline.output,
        label=label or cab_name,
    )

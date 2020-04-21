# Manage flagsets
import os
import sys
from caracal import log

def handle_conflicts(pipeline, wname, ms, config, flags_bw, flags_aw, read_version = 'version'):
    av_flagversions = get_flags(pipeline, ms)
    if flags_bw in av_flagversions and not config['overwrite_flag_versions']:
        if not config['rewind_flags']["enable"] or config['rewind_flags'][read_version] == 'null':
            log.error('A worker named "{0:s}" was already run on the MS file "{1:s}" with pipeline prefix "{2:s}".'.format(wname, ms, pipeline.prefix))
            ask_what_to_do = True
        else:
            if av_flagversions.index(config['rewind_flags'][read_version]) > av_flagversions.index(flags_bw) and not config['overwrite_flag_versions']:
                log.error('A worker named "{0:s}" was already run on the MS file "{1:s}" with pipeline prefix "{2:s}"'.format(wname, ms, pipeline.prefix))
                log.error('and you are rewinding to a later flag version: {0:s} .'.format(config['rewind_flags'][read_version]))
                ask_what_to_do = True
            else: ask_what_to_do = False
    else: ask_what_to_do  = False
    if ask_what_to_do:
        log.error('Running "{0:s}" again will attempt to overwrite existing flag versions, it might get messy.'.format(wname))
        log.error('Caracal will not overwrite the "{0:s}" flag versions unless you explicitely request that.'.format(wname))
        log.error('The current flag versions of this MS are (from the oldest to the most recent):')
        for vv in  av_flagversions:
            if vv == flags_bw:
                log.error('       {0:s}        <-- (this worker)'.format(vv))
            elif vv == flags_aw:
                log.error('       {0:s}         <-- (this worker)'.format(vv))
            elif config['rewind_flags']["enable"] and vv == config['rewind_flags'][read_version]:
                log.error('       {0:s}        <-- (rewinding to this version)'.format(vv))
            else:
                log.error('       {0:s}'.format(vv))
        log.error('You have the following options:')
        log.error('    1) If you are happy with the flags currently stored in the FLAG column of this MS and')
        log.error('       want to append new flags to them, change the name of this worker in the configuration')
        log.error('       file by appending "__n" to it (where n is an integer not already taken in the list')
        log.error('       above). The new flags will be appended to the FLAG column, and new flag versions will')
        log.error('       be added to the list above.')
        log.error('    2) If you want to discard the flags obtained during the previous run of "{0:s}" (and,'.format(wname))
        log.error('       necessarily, all flags obtained thereafter; see list above) rewind the flag versions')
        log.error('       by setting in the configuration file:')
        log.error('           {0:s}: rewind_flags: enable: true'.format(wname))
        log.error('           {0:s}: rewind_flags: version: {1:s}'.format(wname, flags_bw))
        log.error('       You could rewind to an even earlier flag version if necessary. You will lose all flags')
        log.error('       appended to the FLAG column after that version, and take it from there.')
        log.error('    3) If you really know what you are doing, allow Caracal to overwrite flag versions by setting:')
        log.error('           {0:s}: overwrite_flag_versions: true'.format(wname))
        log.error('       The worker "{0:s}" will be run again; the new flags will be appended to the current'.format(wname))
        log.error('       FLAG column (or to whatever flag version you are rewinding to); the flag versions from')
        log.error('       the previous run of "{0:s}" will be overwritten and appended to the list above (or'.format(wname))
        log.error('       to that list truncated to the flag version you are rewinding to).')
        log.error('Your choice will be applied to all MS files being processed together in this run of Caracal.')
        raise RuntimeError('Flag version conflicts.')
    else:
        return av_flagversions

def get_flags(pipeline, ms):
    flaglist_file = "{folder:s}/{ms:s}.flagversions/FLAG_VERSION_LIST".format(folder=pipeline.msdir, ms=ms)
    flaglist = []
    if not os.path.exists(flaglist_file):
        return []
    with open(flaglist_file) as stdr:
        for line in stdr.readlines():
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

    for i,flag in enumerate(remove_us):
        recipe.add("cab/casa_flagmanager", '{0:s}_{1:d}'.format(cab_name,i), {
            "vis": ms,
            "mode": "delete",
            "versionname": flag,
            },
            input=pipeline.input,
            output=pipeline.output,
            label="{0:s}:: Delete flags (step {1:d})".format(label or cab_name, i))

def restore_cflags(pipeline, recipe, flagname, ms, cab_name="rando_cab", label="", merge=False):
    if flagname in get_flags(pipeline, ms):
        recipe.add("cab/casa_flagmanager", cab_name, {
                "vis": ms,
                "mode": "restore",
                "versionname": flagname,
                "merge": "replace",
            },
            input=pipeline.input,
            output=pipeline.output,
            label="{0:s}:: Restoring flags to flag version [{1:s}]".format(label or cab_name, flagname))
    else:
        log.warn("Flag version [{0:s}] could not be found".format(flagname))

def add_cflags(pipeline, recipe, flagname, ms, cab_name="rando_cab", label="", overwrite=False):
    if flagname in get_flags(pipeline, ms) and overwrite:
        recipe.add("cab/casa_flagmanager", cab_name.replace('save','delete'), {
            "vis": ms,
            "mode": "delete",
            "versionname": flagname,
            },
            input=pipeline.input,
            output=pipeline.output,
            label="{0:s}:: Delete flag version".format(label or cab_name.replace('save','delete')))

    recipe.add("cab/casa_flagmanager", cab_name, {
        "vis": ms,
        "mode": "save",
        "versionname": flagname,
        },
        input=pipeline.input,
        output=pipeline.output,
        label="{0:s}:: Save flag version".format(label or cab_name))


def delete_flagset(pipeline, recipe, flagset, ms, clear_existing=True, cab_name="rando_cab", label=""):
    """ Add flagset if it does not exist, clear its flags if exists"""

    recipe.add("cab/pycasacore", cab_name, {
        "msname": ms,
        "script": """
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
""".format(ms=ms, flagset=flagset),
    },
        input=pipeline.input,
        output=pipeline.output, label=label or cab_name)



def clear_flagset(pipeline, recipe, flagset, ms, clear_existing=True, cab_name="rando_cab", label=""):
    """ Add flagset if it does not exist, clear its flags if exists"""

    recipe.add("cab/pycasacore", cab_name, {
        "msname": ms,
        "script": """
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
""".format(ms=ms, flagset=flagset),
    },
        input=pipeline.input,
        output=pipeline.output, label=label or cab_name)


def update_flagset(pipeline, recipe, flagset, ms, clear_existing=True, cab_name="rando_cab", label=""):
    """ Add flagset if it does not exist, clear its flags if exists"""

    recipe.add("cab/pycasacore", cab_name, {
        "msname": ms,
        "script": """
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
""".format(ms=ms, flagset=flagset),
    },
        input=pipeline.input,
        output=pipeline.output,
        label=label or cab_name)

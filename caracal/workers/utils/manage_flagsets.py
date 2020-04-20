# Manage flagsets
import os
import sys
from caracal import log

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

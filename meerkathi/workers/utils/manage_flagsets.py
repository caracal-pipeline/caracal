# Manage flagsets
import os
import sys


def delete_cflags(pipeline, recipe, flagname, ms, cab_name="rando_cab", label=""):
    flagversions = "{folder:s}/{ms:s}.flagversions".format(folder=pipeline.msdir, ms=ms)

    index = pipeline.flag_names.index(flagname)
    remove_us = pipeline.flag_names[index:]
    for i,flag in enumerate(remove_us):
        step = "{0:s}_{0:d}".format(cab_name, i)
        recipe.add("cab/casa_flagmanager", step, {
            "vis": ms,
            "mode": "delete",
            "versionname": flag,
            },
            input=pipeline.input,
            output=pipeline.output,
            label="{0:s}:: Delete flags".format(step))
        pipeline.flag_names.remove(flag)

def add_cflags(pipeline, recipe, flagname, ms, cab_name="rando_cab", label=""):
    step = "{0:s}_{0:d}".format(cab_name)
    recipe.add("cab/casa_flagmanager", step, {
        "vis": ms,
        "mode": "replace",
        "versionname": flag,
        },
        input=pipeline.input,
        output=pipeline.output,
        label="{0:s}:: Delete flags".format(label or step))
    pipeline.flag_names.remove(flag)



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

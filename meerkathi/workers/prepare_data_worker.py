import os
import sys

NAME = "Prepare data for calibration"

def worker(pipeline, recipe, config):

    wname = pipeline.CURRENT_WORKER
    for i in range(pipeline.nobs):

        msname = pipeline.msnames[i]
        prefix = pipeline.prefixes[i]
        
        if pipeline.enable_task(config, 'fixvis'):
            step = 'fixvis_{:d}'.format(i)
            recipe.add('cab/casa_fixvis', step,
                {
                    "vis"        : msname,
                    "reuse"      : False,
                    "outputvis"  : msname,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Fix UVW coordinates ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, "manage_flags"):
            if config["manage_flags"].get("add_bitflag_col", True):
                step = "add_bitflag_col_{0:s}_{1:d}".format(wname, i)
                recipe.add("cab/pycasacore", step, {
                    "msname" : msname,
                    "script" : """
import os
import Owlcat.Flagger

Owlcat.Flagger.has_purr = False
ms = os.path.join(os.environ["MSDIR"], "{0:s}")

fms = Owlcat.Flagger.Flagger(ms)
fms.add_bitflags()
fms.close()
""".format(msname),
                },
                    input=pipeline.input,
                    output=pipeline.output,
                    label="{0:s}:: Adding BITFLAG columns. ms={1:s}".format(step, msname))
    
            if config["manage_flags"].get("init_legacy_flagset", True):
                step = "init_legacy_flagset_{0:s}_{1:d}".format(wname, i)
                recipe.add("cab/pycasacore", step, {
                    "msname" : msname,
                    "script" : """
from Owlcat.Flagger import Flagger
import os
import subprocess
ms = os.path.join(os.environ["MSDIR"], "{0:s}")
fms = Flagger(ms)

names = fms.flagsets.names() or []
fms.close()
if "legacy" in names:
    pass
else:
    subprocess.check_output(["flag-ms.py",  "--flag", "legacy", "--flagged-any", "+L", "--create", ms])
""".format(msname),
                },
                    input=pipeline.input,
                    output=pipeline.output,
                    label="{0:s}:: Initialise legacy flags ms={1:s}".format(step, msname))
    
    
            if config["manage_flags"].get("remove_flagsets", False):
                flagsets = []
                for vals in list(pipeline.flagsets.values()):
                    flagsets += vals
                flagsets = set(flagsets)
                flagsets.discard("legacy")
                step = "remove_flags_{0:s}_{1:d}".format(wname, i)
                recipe.add("cab/pycasacore", step, {
                    "msname" : msname,
                    "script" : """
from Owlcat.Flagger import Flagger
import os
import subprocess
flagsets = "{1:s}"
ms = os.path.join(os.environ["MSDIR"], "{0:s}")

fms = Flagger(ms)
names = set(fms.flagsets.names())
fms.close()
names.discard("legacy")

if flagsets == "all":
    if names:
        subprocess.check_call("flag-ms.py --remove {{}}".format(",".join(names)))
else:
    subprocess.check_call(["flag-ms.py", "--remove", flagsets, ms])
""".format(msname, ",".join(flagsets)),
                },
                    input=pipeline.input,
                    output=pipeline.output,
                    label="{0:s}:: Clear bitflags ms={1:s}".format(step, msname))

        if pipeline.enable_task(config, 'prepms'):
            step = 'prepms_{:d}'.format(i)
            recipe.add('cab/msutils', step,
                {
                  "msname"  : msname,
                  "command" : 'prep' ,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: PREP MS ms={1:s}'.format(step, msname))


        if pipeline.enable_task(config, 'add_spectral_weights'):
            step = 'estimate_weights_{:d}'.format(i)
            recipe.add('cab/msutils', step,
                {
                  "msname"          : msname,
                  "command"         : 'estimate_weights',
                  "stats_data"      : config['add_spectral_weights'].get('stats_data'),
                  "weight_columns"  : config['add_spectral_weights'].get('weight_columns'),
                  "noise_columns"   : config['add_spectral_weights'].get('noise_columns'),
                  "write_to_ms"     : config['add_spectral_weights'].get('write_to_ms'),
                  "plot_stats"      : "{0:s}/{1:s}-{2:s}.png".format('diagnostic_plots', prefix, 'noise_weights'),
                  "plot_stats"      : prefix + '-noise_weights.png',
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Adding Spectral weights using MeerKAT noise specs ms={1:s}'.format(step, msname))

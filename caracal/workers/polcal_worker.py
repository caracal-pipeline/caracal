# import stimela
import pickle
import sys
import os
import caracal.dispatch_crew.utils as utils
import caracal
import yaml
import stimela.dismissable as sdm
from caracal.workers.utils import manage_flagsets as manflags
from caracal.workers.utils import manage_fields as manfields
from caracal.workers.utils import manage_caltabs as manGtabs
import copy
import re
import json
import glob

import shutil
import numpy

NAME = "Polarization calibration"
LABEL = 'polcal'

def get_dir_path(string, pipeline):
    return string.split(pipeline.output)[1][1:]

# def worker
def worker(pipeline, recipe, config):
    wname = pipeline.CURRENT_WORKER
    label = config["label_cal"]
    label_in = config["label_in"]

    # loop over all MSs for this label
    for i, msbase in enumerate(pipeline.msbasenames):
        msname = pipeline.form_msname(msbase, label_in)
        msinfo = pipeline.get_msinfo(msname)
        prefix = f"{pipeline.prefix_msbases[i]}-{label}"

        ######## set global param
        refant = pipeline.refant[i] or '0'

        field = config['pol_calib']
        # G1
        step = f'listobs-ms{i}'
        recipe.add('cab/casa_listobs', step,
                   {
                       "vis": msname,
                       "listfile": obsinfo,
                       "overwrite": True,
                   },
                   input=pipeline.input,
                   output=pipeline.msdir,
                   label='{0:s}:: Get observation information ms={1:s}'.format(step, msname))


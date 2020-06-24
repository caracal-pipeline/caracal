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
    flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
    flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
    label = config["label_cal"]

    # loop over all MSs for this label
    for i, msbase in enumerate(pipeline.msbasenames):
        msname = pipeline.form_msname(msbase, label_in)
        msinfo = pipeline.get_msinfo(msname)
        prefix = f"{pipeline.prefix_msbases[i]}-{label}"

        ######## set global param
        refant = pipeline.refant[i] or '0'

        field = config['pol_calib']
        # G1
        recipe.add("cab/casa_gaincal",
                   "pol_gain",
                   {
                       "vis": msname,
                       "field": field,
                       "refant": refant,
                   },
                   input=pipeline.input, output=pipeline.output,
                   label="pol_gain")

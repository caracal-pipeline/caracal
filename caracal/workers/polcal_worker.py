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

    if pipeline.virtconcat:
        msnames = [pipeline.vmsname]
        nobs = 1
        prefixes = [pipeline.prefix]
    else:
        msnames = pipeline.msnames
        prefixes = pipeline.prefixes
        nobs = pipeline.nobs

    for i in range(nobs):

        ######## define msname
        if config["label_in"]:
            msname = '{0:s}_{1:s}.ms'.format(msnames[i][:-3],config["label_in"])
        else: msname = msnames[i]

        recipe.add('cab/casa_listobs', 'listpro',
                   {
                       "vis": msname,
                       "listfile": 'ccc',
                       "overwrite": True,
                   },
                   input=pipeline.input,
                   output=pipeline.msdir,
                   label='prova')


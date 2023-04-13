# -*- coding: future_fstrings -*-
import os
import sys
import subprocess
import itertools
import caracal
import stimela.dismissable as sdm
import warnings

NAME = "Get Data"
LABEL = 'getdata'


def worker(pipeline, recipe, config):
    pipeline.init_names(config["dataid"])
    if pipeline.nobs == 0:
        raise RuntimeError(f'No MS files matching any of {pipeline.dataid} were found at {pipeline.rawdatadir}. '
                           'Please make sure that general: msdir , getdata: dataid, and (optionally) general: '
                           'rawdatadir are set properly.')

    for i, msname in enumerate(pipeline.msnames):
        if pipeline.enable_task(config, 'untar'):
            step = 'untar-{:d}'.format(i)
            tar_options = config['untar']['tar_options']

            # Function to untar Ms from .tar file
            def untar(ms):
                mspath = os.path.abspath(pipeline.rawdatadir)
                subprocess.check_call(['tar', tar_options,
                                       os.path.join(mspath, ms + '.tar'),
                                       '-C', mspath])
            # add function to recipe
            recipe.add(untar, step,
                       {
                           "ms": msname,
                       },
                       label='{0:s}:: Get MS from tarbal ms={1:s}'.format(step, msname),
                       output=pipeline.rawdatadir,
                       input=pipeline.input)

# -*- coding: future_fstrings -*-
import os
import sys
import subprocess
import itertools
import caracal
import caracal.dispatch_crew.meerkat_archive_interface as mai
import stimela.dismissable as sdm
import warnings

NAME = "Get Data"
LABEL = 'getdata'


def worker(pipeline, recipe, config):

    pipeline.init_names(config["dataid"])

    for i in range(pipeline.nobs):
        msname = pipeline.msnames[i]
        h5file = pipeline.h5files[i]
        basename = os.path.splitext(os.path.basename(h5file))[0]
        prefix = pipeline.prefixes[i]
        if isinstance(pipeline.rawdatadir, list):
            rawdatadir = pipeline.rawdatadir[i]
        else:
            rawdatadir = pipeline.rawdatadir

        if pipeline.enable_task(config, 'mvftoms'):
            step = 'mvftoms-{:d}'.format(i)

            if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, msname)):
                os.system('rm -rf {0:s}/{1:s}'.format(pipeline.msdir, msname))

            recipe.add('cab/mvftoms', step,
                       {
                           "mvffiles": [h5file],
                           "output-ms": msname,
                           "no-auto": False,
                           "tar": True,
                           "model-data": True,
                           "verbose": False,
                           "channel-range": sdm.dismissable(config['mvftoms']['chanrange']),
                           "full-pol": config['mvftoms']['fullpol'],
                       },
                       input=rawdatadir,
                       output=pipeline.output,
                       label='{0:s}:: Convert hd5file to MS. ms={1:s}'.format(step, msname))

    for i, msname in enumerate(pipeline.msnames):
        if pipeline.enable_task(config, 'untar'):
            step = 'untar-{:d}'.format(i)
            tar_options = config['untar']['tar_options']

            # Function to untar Ms from .tar file
            def untar(ms):
                mspath = os.path.abspath(pipeline.msdir)
                subprocess.check_call(['tar', tar_options,
                                       os.path.join(mspath, ms+'.tar'),
                                       '-C', mspath])
            # add function to recipe
            recipe.add(untar, step,
                       {
                           "ms": msname,
                       },
                       label='{0:s}:: Get MS from tarbal ms={1:s}'.format(step, msname), 
                       output=pipeline.output,
                       input=pipeline.input)

    if pipeline.enable_task(config, 'combine'):
        pipeline.virtconcat = True
        step = 'combine_data'
        msnames = pipeline.msnames
        if hasattr(pipeline, "metadata"):
            metadata = pipeline.metadata[0]
            pipeline.metada = [metadata]
        pipeline.vmsname = msname = config["combine"]['vmsname']
        pipeline.msnames = [
            "{0:s}/SUBMSS/{1:s}".format(pipeline.vmsname, _m) for _m in msnames]

        if not os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, msname)) or config['combine']['reset']:

            if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, msname)):
                os.system('rm -rf {0:s}/{1:s}'.format(pipeline.msdir, msname))

            recipe.add('cab/casa_virtualconcat', step,
                       {
                           "vis": msnames,
                           "concatvis": msname,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Virtually concatenate datasets'.format(step))

        if config['combine']['tar']['enable']:
            step = 'tar_vc-{:d}'.format(i)
            # Function to untar Ms from .tar file

            def tar(ms):
                mspath = os.path.abspath(pipeline.msdir)
                subprocess.check_call(['tar', config["combine"]['tar']['tar_options'],
                                       os.path.join(mspath, ms+'.tar'),
                                       os.path.join(mspath, ms),
                                       ])
            # add function to recipe
            recipe.add(tar, step,
                       {
                           "ms": msname,
                       },
                       label='{0:s}:: Create tarbal ms={1:s}'.format(step, msname))

        elif config['combine']['untar']['enable']:
            step = 'untar_vc-{:d}'.format(i)
            # Function to untar Ms from .tar file

            def untar(ms):
                mspath = os.path.abspath(pipeline.msdir)
                subprocess.check_call(['tar', config["combine"]['untar']['tar_options'],
                                       os.path.join(mspath, ms+'.tar'),
                                       '-C', mspath])
            # add function to recipe
            recipe.add(untar, step,
                       {
                           "ms": msname,
                       },
                       label='{0:s}:: Get MS from tarbal ms={1:s}'.format(step, msname))

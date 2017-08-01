import os
import sys
import subprocess
import meerkathi

NAME = "Convert data from hdf5 to MS format"

def worker(pipeline, recipe, config):

    for i in range(pipeline.nobs):

        msname = pipeline.msnames[i]
        h5file = pipeline.h5files[i]
        prefix = pipeline.prefix
        if isinstance(pipeline.data_path, list):
            data_path = pipeline.data_path[i]
        else:
            data_path = pipeline.data_path

        if isinstance(pipeline.data_url, list):
            data_url = pipeline.data_url[i]
        else:
            data_url = pipeline.data_url


        if pipeline.enable_task(config, 'download'):
            step = 'download_{:d}'.format(i)
            if os.path.exists('{0:s}/{1:s}'.format(pipeline.data_path, h5file)) \
                and not config['download'].get('reset', False):
                meerkathi.log('File already exists, and reset is not enabled. Will attempt to resume')
                recipe.add('cab/curl', step, {
                    "url"   : data_url,
                    "output": h5file,
                    "continue-at": "-"
                },
                input=pipeline.input,
                output=data_path,
                label='{0:s}:: Downloading data'.format(step))
            else:
                os.system('rm -rf {0:s}/{1:s}'.format(pipeline.data_path, h5file))
                recipe.add('cab/curl', step, {
                    "url"   : data_url,
                    "output": h5file,
                },
                input=pipeline.input,
                output=data_path,
                label='{0:s}:: Downloading data'.format(step))
            
        if pipeline.enable_task(config, 'h5toms'):
            step = 'h5toms_{:d}'.format(i)
            if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, msname)):
                os.system('rm -rf {0:s}/{1:s}'.format(pipeline.msdir, msname))

            recipe.add('cab/h5toms', step,
                {
                    "hdf5files"     : [h5file],
                    "output-ms"     : msname,
                    "no-auto"       : False,
                    "tar"           : True,
                    "model-data"    : True,
                    "channel-range" : config['h5toms']['channel_range'],
                    "full-pol"      : True,
                },
                input=data_path,
                output=pipeline.output,
                label='{0:s}:: Convert hd5file to MS. ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'untar'):
            step = 'untar_{:d}'.format(i)
            # Function to untar Ms from .tar file
            def untar():
                mspath = os.path.abspath(pipeline.msdir)
                subprocess.check_call(['tar', 'xvf', 
                    '{0:s}/{1:s}'.format(mspath, msname+'.tar'),
                    '-C', mspath])
            # add function to recipe
            recipe.add(untar, step, {}, 
                label='{0:s}:: Get Ms from tarbal ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'obsinfo'):
            step = 'listobs_{:d}'.format(i)
            recipe.add('cab/casa_listobs', step,
                {
                  "vis"         : msname,
                  "listfile"    : prefix+'-obsinfo.txt' ,
                  "overwrite"   : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Get observation information ms={1:s}'.format(step, msname))

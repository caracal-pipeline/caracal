import os
import sys

NAME = "Convert data from hdf5 to MS format"

def worker(pipeline, recipe, config):
    steps = []

    for i in range(pipeline.nobs):

        msname = pipeline.msnames[i]
        h5file = pipeline.h5files[i]
        prefix = pipeline.prefixes[i]
        data_path = pipeline.data_path[i]
        if config['h5toms']['enable']:
            step = 'h5toms_{:d}'.format(i)
            if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, msname)):
                os.system('rm -rf {0:s}/{1:s}'.format(pipeline.msdir, msname))

            recipe.add('cab/h5toms', step,
                {
                    "hdf5files"      : [h5file],
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
            steps.append(step)

        if config['obsinfo']['enable']:
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
            steps.append(step)
            
    return steps

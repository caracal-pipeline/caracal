import os
import sys

def get_data(pipeline, config, label):
    steps = []

    for i in range(pipeline.nobs):

        msname = pipeline.msnames[i]
        if config['h5toms']['enable']:
            step = 'h5toms_{0:s}_{1:d}'.format(label, i)
            if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, msname):
                os.system('rm -rf {0:s}/{1:s}'.format(pipeline.msdir, msname))

            pipeline.get_data.add('cab/h5toms', step,
                {
                    "hd5files"   : [pipeline.h5files[i]],
                    "output-ms"  : msname,
                    "no-auto"    : False,
                    "tar"        : True,
                    "model-data" : True,
                    "channel-range" : config['h5toms']['channelrange'],
                    "full-pol"   : True,
                },
                input=INPUT
                output=OUTPUT
                label='{:s}:: Convert hd5file to MS. ms={1:s}'.format(step, msname))
            steps.append(step)

            step = 'fixvis_{0:s}_{1:d}'.format(label, i)
            pipeline.get_data.add('cab/casa_fixvis', step,
                {
                    "vis"        : msname,
                    "reuse"      : False,
                    "outputvis"  : msname,
                },
                input=INPUT
                output=OUTPUT
                label='{:s}:: Fix UVW coordinates ms={1:s}'.format(step, msname))
            steps.append(step)

            step = 'listobs_{0:s}_{1:d}'.format(label, i)
            pipeline.get_data.add('cab/casa_listobs', step,
                {
                  "vis"       :    msname,
                  "listfile"  :   pipeline.prefixes[i]+'-obsinfo.txt' ,
                  "overwrite" :   True,
                },
                input=INPUT,
                output=OUTPUT,
                label='{0:s}:: Get observation information ms={1:s}'.format(step, msname))
            steps.append(step)
            
            if config['prepms']:
                step = 'prepms_{0:s}_{1:d}'.format(label, i)
                pipeline.get_data.add('cab/msutils', step,
                    {
                      "msname"   :    msname,
                      "command"  :    'prep' ,
                    },
                    input=INPUT,
                    output=OUTPUT,
                    label='{0:s}:: Add BITFLAG column ms={1:s}'.format(step, msname))
                steps.append(step)
            
            for column in ['MODEL_DATA', 'CORRECTED_DATA']:
                step = 'add_imaging_{0:s}_{1:d}_{2:s}'.format(label, i, column)
                pipeline.get_data.add('cab/msutils', step,
                    {
                      "msname"   : msname,
                      "command"  : 'copycol', 
                      "tocol"    : column,
                      "fromcol"  : 'DATA',
                    },
                    input=INPUT,
                    output=OUTPUT,
                    label='{0:s}:: Get observation information ms={1:s}'.format(step, msname))
                steps.append(step)

    return steps

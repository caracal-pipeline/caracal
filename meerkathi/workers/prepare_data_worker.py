import os
import sys

NAME = "Prepare data for calibration"

def worker(pipeline, recipe, config):
    steps = []

    for i in range(pipeline.nobs):

        msname = pipeline.msnames[i]
        prefix = pipeline.prefixes[i]
        
        if config['fixvis']['enable']:
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
            steps.append(step)

            
        if config['prepms']['enable']:
            step = 'prepms_{:d}'.format(i)
            recipe.add('cab/msutils', step,
                {
                  "msname"  : msname,
                  "command" : 'prep' ,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Add BITFLAG column ms={1:s}'.format(step, msname))
            steps.append(step)

            if config['prepms']['add_imaging_cols']: 
                for column in ['MODEL_DATA', 'CORRECTED_DATA']:
                    step = 'add_imaging_{0:d}_{1:s}'.format(i, column)
                    recipe.add('cab/msutils', step,
                        {
                          "msname"   : msname,
                          "command"  : 'copycol', 
                          "tocol"    : column,
                          "fromcol"  : 'DATA',
                        },
                        input=pipeline.input,
                        output=pipeline.output,
                        label='{0:s}:: Get observation information ms={1:s}'.format(step, msname))
                    steps.append(step)

    return steps

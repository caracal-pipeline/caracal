import os
import sys

NAME = 'Split and average target data'

def worker(pipeline, recipe, config):
    steps = []

    for i in range(pipeline.nobs):
        msname = pipeline.msnames[i]
        target = pipeline.target[i]
        tms = '{0:s}-{1:s}.ms'.format(msname[:-3], config['split_target']['label']),

        if config['split_target']['enable']:
            step = 'split_target_{:d}'.format(i)
            if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, tms)):
                os.system('rm -rf {0:s}/{1:s}'.format(pipeline.msdir, tms))

            recipe.add('cab/casa_split', step,
                {
                    "vis"           : msname,
                    "outputvis"     : tms,
                    "timebin"       : config['split_target']['time_average'],
                    "width"         : config['split_target']['freq_average'],
                    "datacolumn"    : 'corrected',
                    "field"         : target,
                    "keepflags"     : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Split and average data ms={1:s}'.format(step, msname))
            steps.append(step)
            
    return steps

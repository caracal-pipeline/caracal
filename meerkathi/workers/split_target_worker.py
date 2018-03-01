import os
import sys

NAME = 'Split and average target data'

def worker(pipeline, recipe, config):
    label = config['label']
    pipeline.set_cal_msnames(label)

    for i in range(pipeline.nobs):
        msname = pipeline.msnames[i]
        target = pipeline.target[0]
        prefix = pipeline.prefixes[i]
        tms = pipeline.cal_msnames[i]
        flagv = tms + '.flagversions'
        if pipeline.enable_task(config, 'split_target'):
            step = 'split_target_{:d}'.format(i)
            if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, tms)) or \
                   os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, flagv)):

                os.system('rm -rf {0:s}/{1:s} {0:s}/{2:s}'.format(pipeline.msdir, tms, flagv))

            recipe.add('cab/casa_split', step,
                {
                    "vis"           : msname,
                    "outputvis"     : tms,
                    "timebin"       : config['split_target'].get('time_average', ''),
                    "width"         : config['split_target'].get('freq_average', 1),
                    "datacolumn"    : 'corrected',
                    "correlation"   : config['split_target'].get('correlation', ''),
                    "field"         : str(target),
                    "keepflags"     : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Split and average data ms={1:s}'.format(step, msname))
            
        if pipeline.enable_task(config, 'prepms'):
            step = 'prepms_{:d}'.format(i)
            recipe.add('cab/msutils', step,
                {
                  "msname"  : tms,
                  "command" : 'prep' ,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Add BITFLAG column ms={1:s}'.format(step, tms))

        if pipeline.enable_task(config, 'obsinfo'):
            if config['obsinfo'].get('listobs', True):
                step = 'listobs_{:d}'.format(i)
                recipe.add('cab/casa_listobs', step,
                    {
                      "vis"         : tms,
                      "listfile"    : '{0:s}-{1:s}-obsinfo.txt'.format(prefix, label),
                      "overwrite"   : True,
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Get observation information ms={1:s}'.format(step, tms))
    
            if config['obsinfo'].get('summary_json', True):
                 step = 'summary_json_{:d}'.format(i)
                 recipe.add('cab/msutils', step,
                    {
                      "msname"      : tms,
                      "command"     : 'summary',
                      "outfile"     : '{0:s}-{1:s}-obsinfo.json'.format(prefix, label),
                    },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Get observation information as a json file ms={1:s}'.format(step, tms))

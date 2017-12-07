import os
import sys

NAME = "Conbine datasets"

def worker(pipeline, recipe, config):

    newid = config.get('newid')
    step = 'combine_{:s}'.format(newid)
    msnames = list(pipeline.msnames)
    msmeta = '{0:s}/{1:s}.json'.format(pipeline.data_path, pipeline.dataid[0])
    pipeline.init_names([newid])
    msname = pipeline.msnames[0]
    os.system('rm -fr {0:s}/{1:s}'.format(pipeline.msdir, msname))

    recipe.add('cab/casa_concat', step,
        {   
          "vis"         : msnames,
          "concatvis"   : msname,
        },  
        input=pipeline.input,
        output=pipeline.output,
        label='{0:s}:: Combine datasets ms={1:s}'.format(step, msname))

    os.system('cp {0:s} {1:s}/{2:s}.json'.format(msmeta, pipeline.data_path, pipeline.dataid[0]))

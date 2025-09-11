import os
import subprocess
from caracal.workers import PIPELINE_MIN_REQUIRES
from caracal.utils import ObjDict

NAME = "Get Data"
LABEL = 'getdata'


PIPELINE_REQUIRES = PIPELINE_MIN_REQUIRES
DIRECTORIES = []

pipeline = ObjDict.from_dict({
    "msname" : "Name of input measurement set",
    "obsid": "Observation index",
    "name": "Name of worker"
})


def worker(pipeline, recipe, config):

    if not pipeline.enable_task(config, 'untar'):
        return
    
    step = 'untar-{:d}'.format(pipeline.obsid)
    tar_options = config['untar']['tar_options']
    tar_ext = config['untar']['tar_extension']

    # Function to untar Ms from .tar file
    def untar(ms):
        mspath = os.path.abspath(pipeline.rawdatadir)
        subprocess.check_call(['tar', tar_options,
                               os.path.join(mspath, f"{ms}.{tar_ext}"),
                               '-C', mspath])
    # add function to recipe
    recipe.add(untar, step,
               {
                   "ms": pipeline.msnane,
               },
               label='{0:s}:: Get MS from tarbal ms={1:s}'.format(step, pipeline.msname),
               output=pipeline.rawdatadir,
               input=pipeline.input)
    

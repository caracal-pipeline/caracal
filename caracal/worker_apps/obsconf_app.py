import click
import os
import glob
from scabha.basetypes import File
from scabha.schema_utils import paramfile_loader
from caracal.worker_apps import WorkerSchema


command = "obsconf"

thisdir = os.path.dirname(__file__)
source_files = glob.glob(f"{thisdir}/*.yaml")
sources = [File(item) for item in source_files]
parserfile = File(f"{thisdir}/{command}_schema_scabha.yml")
config = paramfile_loader(parserfile, sources, WorkerSchema)[command]


#@cli.command(command)
#@click.version_option(str(simms.__version__))
#@clickify_parameters(config)




def runit():
    print("I'm obsconf")
    print(config)
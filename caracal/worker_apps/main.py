import os.path
import click
import glob
import importlib.util
import sys


thisdir = os.path.dirname(os.path.abspath(__file__))

@click.group()
def cli():
    pass


def load_app(module_path):
    worker_module = os.path.basename(module_path)
    worker_module = os.path.splitext(module_path)[0]
    spec = importlib.util.spec_from_file_location(worker_module, module_path)
    my_module = importlib.util.module_from_spec(spec)
    
    # Add to sys.modules for proper import behavior
    sys.modules[worker_module] = my_module
    spec.loader.exec_module(my_module)

    return my_module


def add_commands():
    app_files = glob.glob(os.path.join(thisdir,"*_app.py"))
    for app_file in app_files:
        load_app(app_file).runit()

add_commands()

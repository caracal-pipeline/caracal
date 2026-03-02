import os.path
import shutil
import tempfile
from typing import Dict, Union

from ruamel.yaml import YAML

yaml = YAML(typ="rt")

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class InitTest:
    def __init__(self):
        self.test_files = []

    def update_config_dirs(self, caracal_config: Union[Dict, str], outfile: str = None):
        if isinstance(caracal_config, str):
            caracal_config = self.read_yaml(caracal_config)

        caracal_config["general"]["input"] = os.path.join(TESTDIR, "input")
        caracal_config["general"]["msdir"] = os.path.join(TESTDIR, "msdir")
        caracal_config["general"]["output"] = os.path.join(TESTDIR, "output")

        if outfile:
            with open(outfile, "w") as stdw:
                yaml.dump(caracal_config, stdw)

        return caracal_config

    def random_named_file(self, suffix: str = None):
        if not hasattr(self, "test_files"):
            self.test_files = []

        file_obj = tempfile.NamedTemporaryFile(suffix=suffix, dir=TESTDIR, delete=False)
        name = file_obj.name
        file_obj.close()

        self.test_files.append(name)
        return name

    def read_yaml(self, yfile):
        with open(yfile) as stdr:
            ydict = yaml.load(stdr)
        return ydict

    def random_named_directory(self, suffix: str = None):
        if not hasattr(self, "test_files"):
            self.test_files = []

        dir_obj = tempfile.TemporaryDirectory(suffix=suffix, dir=TESTDIR, delete=False)
        name = dir_obj.name

        self.test_files.append(name)
        return name

    def __del__(self):
        for path in getattr(self, "test_files", []):
            if os.path.exists(path):
                if os.path.isfile(path):
                    try:
                        os.remove(path)
                    except OSError as e:
                        print(f"Error deleting file '{path}': {e}")
                elif os.path.isdir(path):
                    try:
                        shutil.rmtree(path)
                    except OSError as e:
                        print(f"Error deleting directory '{path}': {e}")

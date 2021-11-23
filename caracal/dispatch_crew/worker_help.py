import yaml
import sys
from argparse import ArgumentParser


class worker_options(object):
    def __init__(self, name, worker_dict, parser=None):
        """
            Prints out help for a worker
        """
        self.worker = name
        self.desc = worker_dict["desc"]
        self.parser = parser or ArgumentParser(
            "{0:s}: {1:s}".format(self.worker, self.desc))
        self.worker_dict = worker_dict

    def traverse_worker(self, section, lineage=None):
        """
          Recursively add options to worker help
        """

        if section["type"] == "map":
            for name in section["mapping"]:
                segment = section["mapping"][name]
                # Find segment lineage
                if lineage is None:
                    _lineage = name
                else:
                    _lineage = "{0:s}-{1:s}".format(lineage, name.replace("-", "_"))
                # send back if its a mapping
                if segment.get("type", False) == "map":
                    self.traverse_worker(segment, _lineage)
                    continue

                args = {}
                dtype = None
                if "seq" in segment:
                    args["action"] = "append"
                    dtype = segment["seq"][0]["type"]
                    ptype = "list:" + dtype
                elif segment["type"] not in ["bool"]:
                    args["type"] = eval(dtype or segment["type"])
                    if "enum" in segment:
                        args["choices"] = segment["enum"]
                    ptype = segment["type"]
                else:
                    args["action"] = "store_true"
                    ptype = "bool"
                desc = segment.get("desc",
                                   "!!! option %s missing schema description. Please file this bug !!!" % name).replace("%", "%%")
                desc = desc + " [type: %s]" % ptype
                self.parser.add_argument(
                    "--{0:s}".format(_lineage), help=desc, **args)
        else:
            return

    def print_worker(self):
        """
            Print worker options
        """

        self.traverse_worker(section=self.worker_dict, lineage=self.worker)
        self.parser.parse_args(["--help"])

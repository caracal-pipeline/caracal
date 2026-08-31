import os
import subprocess

NAME = "Get Data"
LABEL = "getdata"


def worker(pipeline, recipe, config):
    pipeline.init_names(config["dataid"])
    if pipeline.nobs == 0:
        raise RuntimeError(
            f"No MS files matching any of {pipeline.dataid} were found at {pipeline.rawdatadir}. "
            "Please make sure that general: msdir , getdata: dataid, and (optionally) general: "
            "rawdatadir are set properly."
        )

    for i, msname in enumerate(pipeline.msnames):
        if pipeline.enable_task(config, "untar"):
            step = f"untar-{i:d}"
            tar_options = config["untar"]["tar_options"]

            # Function to untar Ms from .tar file
            def untar(ms):
                mspath = os.path.abspath(pipeline.rawdatadir)
                subprocess.check_call(["tar", tar_options, os.path.join(mspath, ms + ".tar"), "-C", mspath])  # noqa: B023

            # add function to recipe
            recipe.add(
                untar,
                step,
                {
                    "ms": msname,
                },
                label=f"{step:s}:: Get MS from tarbal ms={msname:s}",
                output=pipeline.rawdatadir,
                input=pipeline.input,
            )

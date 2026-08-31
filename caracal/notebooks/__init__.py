import glob
import os.path
import shutil
import time
import traceback

import jinja2
from stimela.utils import StimelaCabRuntimeError, xrun

import caracal
from caracal import log

_j2env = None

SOURCE_NOTEBOOK_DIR = os.path.dirname(__file__)


def setup_default_notebooks(notebooks, output_dir, prefix, config):
    # setup logos
    logodir = os.path.join(output_dir, "reports")
    if not os.path.exists(logodir):
        os.mkdir(logodir)
    for png in glob.glob(os.path.join(SOURCE_NOTEBOOK_DIR, "*.png")):
        shutil.copyfile(png, os.path.join(logodir, os.path.basename(png)))

    for notebook in notebooks:
        nbfile = notebook + ".ipynb"
        nbdest = os.path.join(output_dir, f"{prefix}-{nbfile}" if prefix else nbfile)

        # overwrite destination only if source is newer
        dest_mtime = os.path.getmtime(nbdest) if os.path.exists(nbdest) else 0

        # if source is a template, invoke jinja
        nbsrc = os.path.join(SOURCE_NOTEBOOK_DIR, nbfile + ".j2")
        if os.path.exists(nbsrc):
            if os.path.getmtime(nbsrc) > dest_mtime:
                global _j2env
                if _j2env is None:
                    _j2env = jinja2.Environment(
                        loader=jinja2.PackageLoader("caracal", "notebooks"),
                        autoescape=jinja2.select_autoescape(["html", "xml"]),
                    )

                template = _j2env.get_template(nbfile + ".j2")
                log.info(f"Creating standard notebook {nbdest} from template")

                with open(nbdest, "wt") as file:
                    try:
                        print(template.render(**config), file=file)
                    except jinja2.TemplateError as exc:
                        log.error(f"Error rendering notebook template: {exc}", extra=dict(boldface=True))  # noqa: C408
                        log.info(f"  More information can be found in the logfile at {caracal.CARACAL_LOG:s}")
                        for line in traceback.format_exc().splitlines():
                            log.error(line, extra=dict(traceback_report=True))  # noqa: C408
                        log.info("This is not fatal, continuing")
            else:
                log.info(f"Standard notebook {nbdest} already exists, won't overwrite")
            continue

        # if source exists as is, copy
        nbsrc = os.path.join(SOURCE_NOTEBOOK_DIR, nbfile)
        if os.path.exists(nbsrc):
            if os.path.getmtime(nbsrc) > dest_mtime:
                log.info(f"Creating standard notebook {nbdest}")
                shutil.copyfile(nbsrc, nbdest)
            else:
                log.info(f"Standard notebook {nbdest} already exists, won't overwrite")
            continue

        log.error(f"Standard notebook {nbsrc} does not exist")


_radiopadre_updated = False


def generate_report_notebooks(notebooks, output_dir, prefix, container_tech):
    opts = ["--non-interactive", "--auto-init"]

    if container_tech == "docker":
        opts.append("--docker")
    elif container_tech == "singularity":
        opts.append("--singularity")
    else:
        log.warning("Container technology '{}' not supported by radiopadre, skipping report rendering")
        return

    if caracal.DEBUG:
        opts += ["-v", "2", "--container-debug"]

    # disabling as per https://github.com/caracal-pipeline/caracal/issues/1161
    # # first time run with -u
    # global _radiopadre_updated
    # if not _radiopadre_updated:
    #     opts.append('--update')
    #     _radiopadre_updated = True
    start_time = time.time()

    log.info("Rendering report(s)")
    for notebook in notebooks:
        if prefix:
            notebook = f"{prefix}-{notebook}"
        nbdest = os.path.join(output_dir, notebook + ".ipynb")
        nbhtml = os.path.join(output_dir, notebook + ".html")
        if os.path.exists(nbdest):
            try:
                xrun("run-radiopadre", opts + ["--nbconvert", nbdest], log=log)
            except StimelaCabRuntimeError as exc:
                log.warning(f"Report {nbhtml} failed to render ({exc}). HTML report will not be available.")
            # check that HTML file actually showed up (sometimes the container doesn't report an error)
            if os.path.exists(nbhtml) and os.path.getmtime(nbhtml) >= start_time:
                log.info(f"Rendered report {nbhtml}")
            else:
                log.warning(f"Report {nbhtml} failed to render")
        else:
            log.warning(f"Report notebook {nbdest} not found, skipping report rendering")

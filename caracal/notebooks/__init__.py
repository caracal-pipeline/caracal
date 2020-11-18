import os.path
import glob
import shutil
import jinja2
import traceback
import time

import caracal
from caracal import log

from stimela.utils import xrun, StimelaCabRuntimeError

_j2env = None

SOURCE_NOTEBOOK_DIR = os.path.dirname(__file__)

def setup_default_notebooks(notebooks, output_dir, prefix, config):
    # setup logos
    logodir = os.path.join(output_dir, "reports")
    if not os.path.exists(logodir):
        os.mkdir(logodir)
    for png in glob.glob(os.path.join(SOURCE_NOTEBOOK_DIR, "*.png")):
        shutil.copy2(png, logodir)

    for notebook in notebooks:
        nbfile = notebook + ".ipynb"
        nbdest = os.path.join(output_dir, "{}-{}".format(prefix, nbfile) if prefix else nbfile)

        # overwrite destination only if source is newer
        dest_mtime = os.path.getmtime(nbdest) if os.path.exists(nbdest) else 0

        # if source is a template, invoke jinja
        nbsrc = os.path.join(SOURCE_NOTEBOOK_DIR, nbfile+".j2")
        if os.path.exists(nbsrc):
            if os.path.getmtime(nbsrc) > dest_mtime:
                global _j2env
                if _j2env is None:
                    _j2env = jinja2.Environment(loader=jinja2.PackageLoader('caracal', 'notebooks'),
                                                autoescape=jinja2.select_autoescape(['html', 'xml']))

                template = _j2env.get_template(nbfile+".j2")
                log.info("Creating standard notebook {} from template".format(nbdest))

                with open(nbdest, "wt") as file:
                    try:
                        print(template.render(**config), file=file)
                    except jinja2.TemplateError as exc:
                        log.error("Error rendering notebook template: {}".format(exc), extra=dict(boldface=True))
                        log.info("  More information can be found in the logfile at {0:s}".format(caracal.CARACAL_LOG))
                        for line in traceback.format_exc().splitlines():
                            log.error(line, extra=dict(traceback_report=True))
                        log.info("This is not fatal, continuing")
            else:
                log.info("Standard notebook {} already exists, won't overwrite".format(nbdest))
            continue

        # if source exists as is, copy
        nbsrc = os.path.join(SOURCE_NOTEBOOK_DIR, nbfile)
        if os.path.exists(nbsrc):
            if os.path.getmtime(nbsrc) > dest_mtime:
                log.info("Creating standard notebook {}".format(nbdest))
                shutil.copyfile(nbsrc, nbdest)
            else:
                log.info("Standard notebook {} already exists, won't overwrite".format(nbdest))
            continue


        log.error("Standard notebook {} does not exist".format(nbsrc))

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
        opts += ['-v', '2', '--container-debug']

    ## disabling as per https://github.com/caracal-pipeline/caracal/issues/1161
    # # first time run with -u
    # global _radiopadre_updated
    # if not _radiopadre_updated:
    #     opts.append('--update')
    #     _radiopadre_updated = True
    start_time = time.time()

    log.info("Rendering report(s)")
    for notebook in notebooks:
        if prefix:
            notebook = "{}-{}".format(prefix, notebook)
        nbdest = os.path.join(output_dir, notebook + ".ipynb")
        nbhtml = os.path.join(output_dir, notebook + ".html")
        if os.path.exists(nbdest):
            try:
                xrun("run-radiopadre", opts + ["--nbconvert", nbdest], log=log)
            except StimelaCabRuntimeError as exc:
                log.warning("Report {} failed to render ({}). HTML report will not be available.".format(nbhtml, exc))
            # check that HTML file actually showed up (sometimes the container doesn't report an error)
            if os.path.exists(nbhtml) and os.path.getmtime(nbhtml) >= start_time:
                log.info("Rendered report {}".format(nbhtml))
            else:
                log.warning("Report {} failed to render".format(nbhtml))
        else:
            log.warning("Report notebook {} not found, skipping report rendering".format(nbdest))

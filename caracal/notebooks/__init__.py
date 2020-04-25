import os.path
import glob
import shutil
import jinja2
import traceback

import caracal
from caracal import log

_j2env = None

SOURCE_NOTEBOOK_DIR = os.path.dirname(__file__)


def setup_default_notebooks(notebooks, output_dir, prefix, config):

    # setup logos
    logodir = os.path.join(output_dir, ".logo")
    if not os.path.exists(logodir):
        os.mkdir(logodir)
    for png in glob.glob(os.path.join(SOURCE_NOTEBOOK_DIR, "*.png")):
        shutil.copy2(png, logodir)

    for notebook in notebooks:
        nbfile = notebook + ".ipynb"
        nbdest = os.path.join(output_dir, "{}-{}".format(prefix, nbfile) if prefix else nbfile)

        # overwrite destination only if source is newer
        dest_mtime = os.path.getmtime(nbdest) if os.path.exists(nbdest) else 0

        # if source exists as is, copy
        nbsrc = os.path.join(SOURCE_NOTEBOOK_DIR, nbfile)
        if os.path.exists(nbsrc):
            if os.path.getmtime(nbsrc) > dest_mtime:
                log.info("Creating standard notebook {}".format(nbdest))
                shutil.copyfile(nbsrc, nbdest)
            else:
                log.info("Standard notebook {} already exists, won't overwrite".format(nbdest))
            continue

        # if source is a template, invoke jinja
        nbsrc = nbsrc + ".j2"
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

        log.error("Standard notebook {} does not exist".format(nbsrc))

import os
from caracal import log
from caracal.dispatch_crew import utils
from collections import OrderedDict
import itertools
import ruamel.yaml

yaml = ruamel.yaml.YAML(typ="rt")


def get_msinfo(pipeline, /, msname):
    """Returns info dict corresponding to an MS. Caches and reloads as needed"""
    msinfo_file = os.path.splitext(msname)[0] + "-summary.json"
    msinfo_path = os.path.join(pipeline.msdir, msinfo_file)
    msdict, mtime_cache = pipeline._msinfo_cache.get(msname, (None, 0))
    if not os.path.exists(msinfo_path):
        raise RuntimeError(f"MS summary file {msinfo_file} not found at expected location. This is a bug or "
                            "a misconfiguration. Was the MS transformed properly?")
    # reload cached dict if file on disk is newer
    mtime = os.path.getmtime(msinfo_path)
    if msdict is None or mtime > mtime_cache:
        with open(msinfo_path, 'r') as f:
            msdict = yaml.load(f)
        pipeline._msinfo_cache[msname] = msdict, mtime
    return msdict

def enable_task(pipeline, /, config, task):
    return task in config and config[task].get("enable")

def remove_output_products(files, directory=None, log=None):
    """
    Removes output products (given by a list of files), in a directory (if specified)
    """
    for fullpath in files:
        if directory:
            fullpath = os.path.join(directory, fullpath)
        if os.path.exists(fullpath):
            if log is not None:
                log.info(f'removing pre-existing {fullpath}')
            os.system(f'rm -rf {fullpath}')


def form_msname(pipeline, /, label=None, field=None):
    """
    Given a base MS name, an optional label, and an optional field name, return the full MS name
    """
    label = '' if not label else '-' + label
    field = '' if not field else '-' + utils.filter_name(field)
    return f'{pipeline.msbasename}{field}{label}.{pipeline.ms_extension}'


def get_mslist(pipeline, /, label="", target=False):
    """
    Given an MS number (0...nobs-1), and an optional label, returns list of corresponding MSs.
    If target is True, this will be one MS per each (split-out) target.
    If target is False, the list will contain just the single MS.
    Applies label in both cases.
    """
    if target:
        return [form_msname(pipeline, label, targ) for targ in pipeline.target]
    else:
        return [form_msname(pipeline, label)]


def get_target_mss(pipeline, /, label=None):
    """
    Given an MS label, returns a tuple of unique_targets, all_mss, mss_per_target
    Where all_mss is a list of all MSs to be processed for all targets, and mss_per_target maps target field
    to associated list of MSs
    """
    target_msfiles = OrderedDict()
    # self.target is a list of lists of targets, per each MS
    for target in pipeline.target:
        target_msfiles[target] = form_msname(pipeline, label, target)
    
    return target_msfiles


def get_callib_name(pipeline, /, name, ext="yml", extra_label=None):
    """Makes a callib name with the given extension. Replaces extension if needed. Adds callib- if needed."""
    if os.path.splitext(name)[-1] in ['.yml', '.txt']:
        name, _ = os.path.splitext(name)
    if not name.startswith("callib-"):
        name = f"callib-{name}"
    if extra_label:
        name = f"{name}-{extra_label}"
    return os.path.join(pipeline.caltables, f"{name}.{ext}")


def load_callib(pipeline, /, name):
    """Loads calibration library specified by name"""
    filename = get_callib_name(pipeline, name)
    if not os.path.exists(filename):
        raise IOError(f"Calibration library {filename} doesn't exist")
    with open(filename, 'r') as f:
        return ruamel.yaml.load(f, ruamel.yaml.RoundTripLoader)


def save_callib(pipeline, /, callib, name):
    """Dumps caldict to calibration library specified by name"""
    with open(get_callib_name(pipeline, name), 'w') as f:
        ruamel.yaml.dump(callib, f, ruamel.yaml.RoundTripDumper)
        
def parse_cabspec_dict(pipeline, /, cabspec_seq):
    """Turns sequence of cabspecs into a Stimela cabspec dict"""
    cabspecs = OrderedDict()
    speclists = OrderedDict()
    # collect all specs encountered, sort them by cab
    for spec in cabspec_seq:
        name, version, tag = spec["name"], spec.get("version") or None, spec.get("tag") or None
        if not version and not tag:
            log.warning(f"Neither version nor tag specified for cabspec {name}, ignoring")
            continue
        speclists.setdefault(name, []).append((version, tag))
    # now process each cab's list of specs.
    for name, speclist in speclists.items():
        if len(speclist) == 1:
            version, tag = speclist[0]
            if version is None:
                log.info(f"  {name}: forcing tag {tag} for all invocations")
                cabspecs[name] = dict(tag=tag, force=True)
                continue
            elif tag is None:
                log.info(f"  {name}: forcing version {version} for all invocations")
                cabspecs[name] = dict(version=version)
                continue
        # else make dict of version: tag pairs
        cabspecs[name] = dict(version={version: tag for version, tag in speclist}, force=True)
        for version, tag in speclist:
            log.info(f"  {name}: using tag {tag} for version {version}")
    return cabspecs


import stimela
import os.path

_MODES = dict(
    K       = "delay_cal", 
    B       = "bp_cal", 
    F       = "transfer_fluxscale", 
    G       = "gain_cal",
    Gpol    = "xcal_gain",
    Kcrs    = 'cross_delay',
    Xref    = 'cross_phase_ref',
    Xf      = 'cross_phase',
    Dref    = 'leakage_ref',
    Df      = 'leakage',
    Gxyamp  = 'cross_gain',
    Xfparang = 'cross_phase',
    Df0gen   = 'leakage'
    )


def add_callib_recipe(recipes, gt, interp, fldmap, field='', calwt=False):
    """Adds gaintable to a callin recipe"""
    # get extension of gain table, and strip off digits at end
    _, ext = os.path.splitext(gt)
    ext = ext and ext[1:].rstrip("0123456789")
    mode = _MODES.get(ext, "unknown")
    recipes[gt] = dict(mode=mode, caltable=gt, fldmap=fldmap, interp=interp, field=field, calwt=bool(calwt))

def resolve_calibration_library(pipeline, msprefix, config, cfg_callib, cfg_prefix, output_fields=None):
    """Unified method to export a dict into a .txt library corresponding to the yml file"""
    cal_lists = ([],)*5         # init 5 empty lists for output values

    # get name from callib name and/or from prefix
    cal_lib = config[cfg_callib]
    if not cal_lib and config[cfg_prefix]:
        cal_lib = f"{msprefix}-{config[cfg_prefix]}"
    else:
        return None, cal_lists

    caldict = pipeline.load_callib(cal_lib)
    outfile = pipeline.get_callib_name(cal_lib, "txt")
    with open(outfile, 'w') as stdw:
        for entry in caldict:
            field = entry.get('field', '')
            # if a field is specified, make intersection with target fields if these are specified
            if field and output_fields:
                field = set([x.strip() for x in entry['field'].split(',')])
                field = ",".join(field.intersection(output_fields))
            cal_lists[0].append(entry['caltable'])
            cal_lists[1].append(entry['fldmap'])
            cal_lists[2].append(entry['interp'])
            calwt = entry.get('calwt', False)
            cal_lists[3].append(calwt)
            cal_lists[4].append(field)

            filename = os.path.join(stimela.recipe.CONT_IO["output"], 'caltables', entry['caltable']) 
            stdw.write(f"""caltable="{filename}" calwt={calwt} tinterp='{entry['interp']}' """
                f"""finterp='linear' fldmap='{entry['fldmap']}' field='{field}' spwmap=0\n""")

    return outfile[len(pipeline.output)+1:], cal_lists

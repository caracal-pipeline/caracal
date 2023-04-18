# -*- coding: future_fstrings -*-
from caracal.workers.utils import manage_fields as manfields
from caracal.workers.utils import manage_flagsets as manflags
import os
from caracal.dispatch_crew import utils
import stimela.dismissable as sdm
import yaml
import re
import caracal
import sys
import glob
import fnmatch
import numpy as np
# import casacore.tables as tables

NAME = 'Flag'
LABEL = 'flag'


def worker(pipeline, recipe, config):
    label = config['label_in']
    wname = pipeline.CURRENT_WORKER
    flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
    flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)

    nobs = pipeline.nobs
    msiter = 0
    for i in range(nobs):
        prefix_msbase = pipeline.prefix_msbases[i]
        mslist = pipeline.get_mslist(i, label, target=(config['field'] == "target"))
        target_ls = pipeline.target[i] if config['field'] == "target" else []

        for j, msname in enumerate(mslist):
            msdict = pipeline.get_msinfo(msname)
            prefix = os.path.splitext(msname)[0]

            if not os.path.exists(os.path.join(pipeline.msdir, msname)):
                raise IOError("MS file {0:s} does not exist. Please check that is where it should be.".format(msname))

            # Write/rewind flag versions
            available_flagversions = manflags.get_flags(pipeline, msname)
            if config['rewind_flags']['enable']:
                if config['rewind_flags']['mode'] == 'reset_worker':
                    version = flags_before_worker
                    stop_if_missing = False
                elif config['rewind_flags']['mode'] == 'rewind_to_version':
                    version = config['rewind_flags']['version']
                    if version == 'auto':
                        version = flags_before_worker
                    stop_if_missing = True
                if version in available_flagversions:
                    if flags_before_worker in available_flagversions and available_flagversions.index(flags_before_worker) < available_flagversions.index(version) and not config['overwrite_flagvers']:
                        manflags.conflict('rewind_too_little', pipeline, wname, msname, config, flags_before_worker, flags_after_worker)
                    substep = 'version-{0:s}-ms{1:d}'.format(version, msiter)
                    manflags.restore_cflags(pipeline, recipe, version, msname, cab_name=substep)
                    if version != available_flagversions[-1]:
                        substep = 'delete-flag_versions-after-{0:s}-ms{1:d}'.format(version, msiter)
                        manflags.delete_cflags(pipeline, recipe,
                                               available_flagversions[available_flagversions.index(version) + 1],
                                               msname, cab_name=substep)
                    if version != flags_before_worker:
                        substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, msiter)
                        manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                            msname, cab_name=substep, overwrite=config['overwrite_flagvers'])
                elif stop_if_missing:
                    manflags.conflict('rewind_to_non_existing', pipeline, wname, msname, config, flags_before_worker, flags_after_worker)
                else:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, msiter)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                        msname, cab_name=substep, overwrite=config['overwrite_flagvers'])
            else:
                if flags_before_worker in available_flagversions and not config['overwrite_flagvers']:
                    manflags.conflict('would_overwrite_bw', pipeline, wname, msname, config, flags_before_worker, flags_after_worker)
                else:
                    substep = 'save-{0:s}-ms{1:d}'.format(flags_before_worker, msiter)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker,
                                        msname, cab_name=substep, overwrite=config['overwrite_flagvers'])

            # Define fields and field_ids to be used to only flag the fields selected with
            # flagging:field (either 'target' or 'calibrators') and with
            # flagging:calfields (for further selection among the calibrators)
            if config['field'] == 'target':
                fields = [target_ls[j]]
            else:
                fields = []
                fld_string = config['calfields']
                if fld_string == "auto":
                    iter_fields = "gcal bpcal xcal fcal".split()
                else:
                    iter_fields = fld_string.split(",")
                for item in iter_fields:
                    if hasattr(pipeline, item):
                        tfld = getattr(pipeline, item)[i]
                    else:
                        raise ValueError("Field given is invalid. Options are 'xcal bpcal gcal fcal'.")
                    if tfld:
                        fields += tfld
                fields = list(set(fields))
            field_ids = utils.get_field_id(msdict, fields)
            fields = ",".join(fields)

            if pipeline.enable_task(config, 'unflag'):
                step = '{0:s}-unflag-ms{1:d}'.format(wname, msiter)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'unflag',
                               "field": fields,
                               "flagbackup": False,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Unflag ms={1:s}'.format(step, msname))

            # flag antennas automatically based on drifts in the scan average of the
            # auto correlation spectra per field. This doesn't strictly require any calibration. It is also
            # not field structure dependent, since it is just based on the DC of the field
            # Compares scan to median power of scans per field per channel
            # Also compares antenna to median of the array per scan per field per channel
            # This should catch any antenna with severe temperature problems
            if pipeline.enable_task(config, 'flag_autopowerspec'):
                step = '{0:s}-autopowerspec-ms{1:d}'.format(wname, msiter)
                recipe.add("cab/politsiyakat_autocorr_amp", step,
                           {
                               "msname": msname,
                               "field": ",".join([str(id) for id in field_ids]),
                               "cal_field": ",".join([str(id) for id in field_ids]),
                               "scan_to_scan_threshold": config["flag_autopowerspec"]["scan_thr"],
                               "antenna_to_group_threshold": config["flag_autopowerspec"]["ant_group_thr"],
                               "dpi": 300,
                               "plot_size": 6,
                               "nproc_threads": config['flag_autopowerspec']['threads'],
                               "data_column": config['flag_autopowerspec']['col']
                           },
                           input=pipeline.input, output=pipeline.output,
                           label="{0:s}:: Flag out antennas with drifts in autocorrelation powerspectra ms={1:s}".format(step, msname))

            if pipeline.enable_task(config, 'flag_autocorr'):
                step = '{0:s}-autocorr-ms{1:d}'.format(wname, msiter)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'manual',
                               "autocorr": True,
                               "field": fields,
                               "flagbackup": False,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Flag auto-correlations ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_quack'):
                step = '{0:s}-quack-ms{1:d}'.format(wname, msiter)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'quack',
                               "quackinterval": config['flag_quack']['interval'],
                               "quackmode": config['flag_quack']['mode'],
                               "field": fields,
                               "flagbackup": False,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Quack flagging ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_elevation'):
                step = '{0:s}-elevation-ms{1:d}'.format(wname, msiter)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'elevation',
                               "lowerlimit": config['flag_elevation']['low'],
                               "upperlimit": config['flag_elevation']['high'],
                               "field": fields,
                               "flagbackup": False,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Flag elevation ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_shadow'):
                if config['flag_shadow']['full_mk64']:
                    addantennafile = '{0:s}/mk64.txt'.format(pipeline.input)
                    subarray = msdict['ANT']['NAME']
                    idleants = open(addantennafile, 'r').readlines()
                    for aa in subarray:
                        for kk in range(len(idleants)):
                            if aa in idleants[kk]:
                                del (idleants[kk:kk + 3])
                                break
                    addantennafile = 'idleants.txt'
                    with open('{0:s}/{1:s}'.format(pipeline.input, addantennafile), 'w') as ia:
                        for aa in idleants:
                            ia.write(aa)
                    addantennafile += ':input'
                else:
                    addantennafile = None
                step = '{0:s}-shadow-ms{1:d}'.format(wname, msiter)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'shadow',
                               "tolerance": config['flag_shadow']['tol'],
                               "addantenna": addantennafile,
                               "flagbackup": False,
                               "field": fields,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Flag shadowed antennas ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_spw'):
                step = '{0:s}-spw-ms{1:d}'.format(wname, msiter)
                flagspwselection = config['flag_spw']['chans']
                firsts = [min(ff) for ff in msdict['SPW']['CHAN_FREQ']]
                lasts = [max(ff) for ff in msdict['SPW']['CHAN_FREQ']]
                nrs = msdict['SPW']['NUM_CHAN']
                nspws = len(nrs)
                found_valid_data = 0
                if config['flag_spw']['ensure_valid']:
                    scalefactor, scalefactor_dict = 1, {
                        'GHz': 1e+9, 'MHz': 1e+6, 'kHz': 1e+3}
                    for ff in flagspwselection.split(','):
                        found_units = False
                        for dd in scalefactor_dict:
                            if dd.lower() in ff.lower():
                                ff, scalefactor = ff.lower().replace(
                                    dd.lower(), ''), scalefactor_dict[dd]
                                found_units = True
                        if 'hz' in ff.lower():
                            ff = ff.lower().replace('hz', '')
                            found_units = True
                        ff = ff.split(':')
                        if len(ff) > 1:
                            spws = ff[0]
                        else:
                            spws = '*'
                        edges = [
                            ii * scalefactor for ii in map(float, ff[-1].split('~'))]
                        if '*' in spws:
                            spws = list(range(nspws))
                        elif '~' in spws:
                            spws = list(
                                range(int(spws.split('~')[0]), int(spws.split('~')[1]) + 1))
                        else:
                            spws = [int(spws), ]
                        edges = [edges for uu in range(len(spws))]
                        for ss in spws:
                            if found_units and ss < nspws and min(edges[ss][1], lasts[ss]) - max(edges[ss][0], firsts[ss]) > 0:
                                found_valid_data = 1
                            elif not found_units and ss < nspws and edges[ss][0] >= 0 and edges[ss][1] < nrs[ss]:
                                found_valid_data = 1
                    if not found_valid_data:
                        caracal.log.warn(
                            'The following channel selection has been made in the flag_spw module of the flagging worker: "{1:s}". This selection would result in no valid data in {0:s}. This would lead to the FATAL error "No valid SPW & Chan combination found" in CASA/FLAGDATA. To avoid this error the corresponding cab {2:s} will not be added to the Stimela recipe of the flagging worker.'.format(msname, flagspwselection, step))

                if found_valid_data or not config['flag_spw']['ensure_valid']:
                    recipe.add('cab/casa_flagdata', step,
                               {
                                   "vis": msname,
                                   "mode": 'manual',
                                   "spw": flagspwselection,
                                   "field": fields,
                                   "flagbackup": False,
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}::Flag out channels ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_time'):
                step = '{0:s}-time-ms{1:d}'.format(wname, msiter)
                found_valid_data = 0
                if config['flag_time']['ensure_valid']:
                    if pipeline.startdate[i]:
                        start_flagrange, end_flagrange = config['flag_time']['timerange'].split('~')
                        flag_start = float(''.join(re.split('/|:', start_flagrange)))
                        flag_end = float(''.join(re.split('/|:', end_flagrange)))
                        if (flag_start <= pipeline.enddate[i]) and (pipeline.startdate[i] <= flag_end):
                            found_valid_data = 1
                    else:
                        raise ValueError("You wanted to ensure a valid time range but we could not find a start and end time")
                    if not found_valid_data:
                        caracal.log.warn(
                            'The following time selection has been made in the flag_time module of the flagging worker: "{1:s}". This selection would result in no valid data in {0:s}. This would lead to the FATAL error " The selected table has zero rows" in CASA/FLAGDATA. To avoid this error the corresponding cab {2:s} will not be added to the Stimela recipe of the flagging worker.'.format(msname, config['flag_time']['timerange'], step))
                if found_valid_data or not config['flag_time']['ensure_valid']:
                    recipe.add('cab/casa_flagdata', step,
                               {
                                   "vis": msname,
                                   "mode": 'manual',
                                   "timerange": config['flag_time']['timerange'],
                                   "flagbackup": False,
                                   "field": fields,
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}::Flag out channels ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_scan'):
                step = '{0:s}-scan-ms{1:d}'.format(wname, msiter)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'manual',
                               "scan": config['flag_scan']['scans'],
                               "flagbackup": False,
                               "field": fields,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}::Flag out channels ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_antennas'):
                # step = '{0:s}-antennas-ms{1:d}'.format(wname, msiter)
                antennas = [config['flag_antennas']['antennas']]
                times = [config['flag_antennas']['timerange']]
                found_valid_data = [0]
                ensure = config['flag_antennas']['ensure_valid']
                if times[0] == '':
                    ensure = False
                if ensure:
                    if pipeline.startdate[i]:
                        antennas = config['flag_antennas']['antennas'].split(',')
                        times = config['flag_antennas']['timerange'].split(',')
                        while len(times) < len(antennas):
                            times.append(times[-1])
                        while len(found_valid_data) < len(antennas):
                            found_valid_data.append(0)
                        for nn, time_range in enumerate(times):
                            start_flagrange, end_flagrange = time_range.split('~')
                            flag_start = float(''.join(re.split('/|:', start_flagrange)))
                            flag_end = float(''.join(re.split('/|:', end_flagrange)))
                            if (flag_start <= pipeline.enddate[i]) and (pipeline.startdate[i] <= flag_end):
                                found_valid_data[nn] = 1
                    else:
                        raise ValueError("You wanted to ensure a valid time range but we could not find a start and end time")
                for nn, antenna in enumerate(antennas):
                    antstep = 'ant-{0:s}-ms{1:d}-antsel{2:d}'.format(wname, i, nn)
                    if found_valid_data[nn] or not ensure:
                        recipe.add('cab/casa_flagdata', antstep,
                                   {
                                       "vis": msname,
                                       "mode": 'manual',
                                       "antenna": antenna,
                                       "timerange": times[nn],
                                       "field": fields,
                                       "flagbackup": False,
                                   },
                                   input=pipeline.input,
                                   output=pipeline.output,
                                   label='{0:s}:: Flagging bad antenna {2:s} ms={1:s}'.format(antstep, msname, antenna))
                    elif ensure and not found_valid_data[nn]:
                        caracal.log.warn(
                            'The following time selection has been made in the flag_antennas module of the flagging worker: "{1:s}". This selection would result in no valid data in {0:s}. This would lead to the FATAL error " The selected table has zero rows" in CASA/FLAGDATA. To avoid this error the corresponding cab {2:s} will not be added to the Stimela recipe of the flagging worker.'.format(msname, times[nn], antstep))

            if pipeline.enable_task(config, 'flag_mask'):
                step = '{0:s}-mask-ms{1:d}'.format(wname, msiter)
                recipe.add('cab/rfimasker', step,
                           {
                               "msname": msname,
                               "mask": config['flag_mask']['mask'],
                               "accumulation_mode": 'or',
                               "uvrange": sdm.dismissable(config['flag_mask']['uvrange'] or None),
                               "memory": 4096,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Apply flag mask ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_manual'):
                rules = config['flag_manual']['rules']
                for irule, rule in enumerate(rules):
                    # a manual flagging rule has a pattern to match the MS name, followed by key:value pairs
                    rule_elements = rule.split()
                    if len(rule_elements) < 2 or not all(':' in el for el in rule_elements[1:]):
                        raise ValueError(f"invalid flag_manual rule '{rule}'")
                    pattern = rule_elements[0]
                    keywords = {tuple(elem.split(":", 1)) for elem in rule_elements[1:]}
                    # end of parsing block. Replace this with file if you like
                    if not fnmatch.fnmatch(msname, pattern):
                        continue
                    caracal.log.info(f"adding manual flagging rule for {pattern}")
                    step = f'{wname}-manual-ms{msiter}-{irule}'
                    args = {
                        "vis": msname,
                        "mode": 'manual',
                        "flagbackup": False,
                        "field": fields,
                    }
                    args.update(keywords)
                    recipe.add('cab/casa_flagdata', step, args,
                               input=pipeline.input,
                               output=pipeline.output,
                               label=f'{step}::Flag ms={msname} using {rule}')

            if pipeline.enable_task(config, 'flag_rfi'):
                step = '{0:s}-rfi-ms{1:d}'.format(wname, msiter)
                if config['flag_rfi']["flagger"] == "aoflagger":
                    if config['flag_rfi']['aoflagger']['ensure_valid']:
                        ms_corr = msdict['CORR']['CORR_TYPE']
                        flag_corr = []
                        with open('{0:s}/{1:s}'.format(pipeline.input, config['flag_rfi']['aoflagger']['strategy'])) as stdr:
                            for ss in stdr.readlines():
                                for pp in 'xx,xy,yx,yy,stokes-i,stokes-q,stokes-u,stokes-v'.split(','):
                                    if '<on-{0:s}>1</on-{0:s}>'.format(pp) in ss:
                                        flag_corr.append(pp)
                        if ('stokes-u' in flag_corr and (('XY' not in ms_corr and 'RL' not in ms_corr) or ('YX' not in ms_corr and 'LR' not in ms_corr))) or\
                            ('stokes-v' in flag_corr and (('XY' not in ms_corr and 'RR' not in ms_corr) or ('YX' not in ms_corr and 'LL' not in ms_corr))) or\
                            ('stokes-i' in flag_corr and (('XX' not in ms_corr and 'RR' not in ms_corr) or ('YY' not in ms_corr and 'LL' not in ms_corr))) or\
                            ('stokes-q' in flag_corr and (('XX' not in ms_corr and 'RL' not in ms_corr) or ('YY' not in ms_corr and 'LR' not in ms_corr))) or\
                            ('xy' in flag_corr and ('XY' not in ms_corr and 'RL' not in ms_corr)) or\
                            ('yx' in flag_corr and ('YX' not in ms_corr and 'LR' not in ms_corr)) or\
                            ('xx' in flag_corr and ('XX' not in ms_corr and 'RR' not in ms_corr)) or\
                                ('yy' in flag_corr and ('YY' not in ms_corr and 'LL' not in ms_corr)):
                            raise ValueError("The selected flagging strategy {0:s}/{1:s} will attempt to flag on {2:} but this is"
                                             " not compatible with the {3:} correlations available in {4:s}. To proceed you can edit the flagging"
                                             " strategy or, if you know what you are doing, disable aoflagger: ensure_valid.".format(
                                                 pipeline.input, config['flag_rfi']['aoflagger']['strategy'], flag_corr, ms_corr, msname))

                    recipe.add('cab/autoflagger', step,
                               {
                                   "msname": msname,
                                   "column": config['flag_rfi']['col'],
                                   "fields": ",".join(map(str, field_ids)),
                                   "strategy": config['flag_rfi']['aoflagger']['strategy'],
                                   "indirect-read": True if config['flag_rfi']['aoflagger']['readmode'] == 'indirect' else False,
                                   "memory-read": True if config['flag_rfi']['aoflagger']['readmode'] == 'memory' else False,
                                   "auto-read-mode": True if config['flag_rfi']['aoflagger']['readmode'] == 'auto' else False,
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: AOFlagger auto-flagging flagging pass ms={1:s} fields={2:s}'.format(step, msname, fields))

                elif config['flag_rfi']["flagger"] == "tricolour":
                    tricolour_strat = config['flag_rfi']['tricolour']['strategy']
                    if config['flag_rfi']['tricolour']['mode'] == 'auto':
                        bandwidth = msdict['SPW']['TOTAL_BANDWIDTH'][0] / 10.0**6
                        caracal.log.info("Total Bandwidth = {0:} MHz".format(bandwidth))
                        if bandwidth <= 20.0:
                            caracal.log.info("Narrowband data detected, selecting appropriate flagging strategy")
                            tricolour_strat = config['flag_rfi']['tricolour']['strat_narrow']

                    caracal.log.info("Flagging strategy in use: {0:}".format(tricolour_strat))
                    recipe.add('cab/tricolour', step,
                               {
                                   "ms": msname,
                                   "data-column": config['flag_rfi']['col'],
                                   "window-backend": config['flag_rfi']['tricolour']['backend'],
                                   "field-names": fields,
                                   "flagging-strategy": 'polarisation',
                                   "config": tricolour_strat,
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: Tricolour auto-flagging flagging pass ms={1:s} fields={2:s}'.format(step, msname, fields))

                elif config['flag_rfi']["flagger"] == "tfcrop":
                    col = config['flag_rfi']['col'].split("_DATA")[0].lower()
                    recipe.add('cab/casa_flagdata', step,
                               {
                                   "vis": msname,
                                   "datacolumn": col,
                                   "mode": "tfcrop",
                                   "field": fields,
                                   "usewindowstats": config["flag_rfi"]["tfcrop"]["usewindowstats"],
                                   "combinescans": config["flag_rfi"]["tfcrop"]["combinescans"],
                                   "flagdimension": config["flag_rfi"]["tfcrop"]["flagdimension"],
                                   "flagbackup": False,
                                   "timecutoff": config["flag_rfi"]["tfcrop"]["timecutoff"],
                                   "freqcutoff": config["flag_rfi"]["tfcrop"]["freqcutoff"],
                                   "correlation": config["flag_rfi"]["tfcrop"]["correlation"],
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: Tfcrop auto-flagging flagging pass ms={1:s} fields={2:s}'.format(step, msname, fields))
                else:
                    raise RuntimeError(
                        "Flagger, {0:s} is not available. Options are 'aoflagger, tricolour, tfcrop'.")

            if pipeline.enable_task(config, 'inspect'):
                step = '{0:s}-inspect-ms{1:d}'.format(wname, msiter)
                if config['field'] == 'target':
                    field = '0'
                else:
                    field = ",".join(map(str, utils.get_field_id(msdict, manfields.get_field(
                        pipeline, i, config['inspect']['field']).split(","))))
                for f in field.split(','):
                    outlabel = '_{0:d}'.format(i) if len(field.split(',')) == 1 else '_{0:d}_{1:s}'.format(i, f)
                    recipe.add('cab/rfinder', step,
                               {
                                   "msname": msname,
                                   "field": int(f),
                                   "plot_noise": "noise",
                                   "RFInder_mode": "use_flags",
                                   "outlabel": outlabel,  # The output will be rfi_<pol>_<outlabel>
                                   "polarization": config['inspect']['polarization'],
                                   "spw_width": config['inspect']['spw_width'],
                                   "time_step": config['inspect']['time_step'],
                                   "time_enable": config['inspect']['time_enable'],
                                   "spw_enable": config['inspect']['spw_enable'],
                                   "1d_gif": config['inspect']['time_enable'],
                                   "2d_gif": config['inspect']['time_enable'],
                                   "altaz_gif": config['inspect']['spw_enable'],
                                   "movies_in_report": config['inspect']['time_enable'] or config['spw_enable']
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: Investigate presence of rfi in ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'summary'):
                __label = config['label_in']
                step = '{0:s}-summary-ms{1:d}'.format(wname, msiter)
                recipe.add('cab/flagstats', step, {
                           "msname": msname,
                           "plot": True,
                           "outfile": ('{0:s}-{1:s}-'
                                       'flagging-summary-{2:d}.json').format(
                               prefix, wname, i),
                           "htmlfile": ('{0:s}-{1:s}-'
                                        'flagging-summary-plots-{2:d}.html').format(
                               prefix, wname, i)
                           },
                           input=pipeline.input,
                           output=pipeline.diagnostic_plots,
                           label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))
                recipe.run()
                # Empty job que after execution
                recipe.jobs = []

            substep = 'save-{0:s}-ms{1:d}'.format(flags_after_worker, msiter)
            manflags.add_cflags(pipeline, recipe, flags_after_worker, msname, cab_name=substep, overwrite=config['overwrite_flagvers'])
            msiter += 1

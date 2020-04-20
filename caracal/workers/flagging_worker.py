from caracal.workers.utils import manage_fields as manfields
from caracal.workers.utils import manage_flagsets as manflags
import os
from caracal.dispatch_crew import utils
import stimela.dismissable as sdm
import yaml
import re
import caracal
import sys

NAME = 'Flagging'
LABEL = 'flagging'

def worker(pipeline, recipe, config):
    label = config['label_in']
    wname = pipeline.CURRENT_WORKER
    if pipeline.virtconcat:
        msnames = [pipeline.vmsname]
        prefixes = [pipeline.prefix]
        nobs = 1
    else:
        msnames = pipeline.msnames
        prefixes = pipeline.prefixes
        nobs = pipeline.nobs

    msiter=0
    for i in range(nobs):
        # loop over all input .MS files
        # the additional 'for loop' below loops over all single target .MS files
        #   produced by the pipeline (see "if config['field']" below)

        prefix = pipeline.prefixes[i]

        '''GET LIST OF INPUT MS'''
        mslist = []
        msn = pipeline.msnames[i][:-3]

        if config['field'] == 'target':
           target_ls = pipeline.target[i]
           for target in target_ls:
                field = utils.filter_name(target)
                mslist.append(pipeline.msnames[i] if label == \
                   '' else '{0:s}-{1:s}_{2:s}.ms'.format(msn, field, label))

        elif config['field'] == 'calibrators':
            mslist.append(pipeline.msnames[i] if label == \
                  '' else '{0:s}_{1:s}.ms'.format(msn, label))

        else:
            raise ValueError("Eligible values for 'field': 'target' or 'calibrators'. "\
                                 "User selected: '{}'".format(config['field']))

        for m in mslist:
            if not os.path.exists(os.path.join(pipeline.msdir, m)):
                raise IOError(
                    "MS file {0:s} does not exist. Please check that is where it should be.".format(m))

        for j, msname in enumerate(mslist):
            msinfo = '{0:s}/{1:s}-obsinfo.json'.format(
                pipeline.output, msname[:-3])

            if not os.path.exists(msinfo):
                raise IOError(
                    "MS info file {0:s} does not exist. Please check that is where it should be.".format(msinfo))

            # Proceed only if there are no conflicting flag versions or if conflicts are being dealt with
            flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
            flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
            available_flagversions = manflags.get_flags(pipeline,msname)
            if flags_before_worker in available_flagversions and not config['overwrite_flag_versions']:
                if not config['rewind_flags']["enable"]:
                    caracal.log.error('A worker named "{0:s}" was already run on the MS file "{1:s}" with pipeline prefix "{2:s}".'.format(wname,msname,pipeline.prefix))
                    ask_what_to_do = True
                else:
                    if available_flagversions.index(config['rewind_flags']["version"]) > available_flagversions.index(flags_before_worker) and not config['overwrite_flag_versions']:
                        caracal.log.error('A worker named "{0:s}" was already run on the MS file "{1:s}" with pipeline prefix "{2:s}"'.format(wname,msname,pipeline.prefix))
                        caracal.log.error('and you are rewinding to a later flag version: {0:s} .'.format(config['rewind_flags']["version"]))
                        ask_what_to_do = True
                    else: ask_what_to_do = False
            else: ask_what_to_do  = False
            if ask_what_to_do:
                caracal.log.error('Running "{0:s}" again will attempt to overwrite existing flag versions, it might get messy.'.format(wname))
                caracal.log.error('Caracal will not overwrite the "{0:s}" flag versions unless you explicitely request that.'.format(wname))
                caracal.log.error('The current flag versions of this MS are (from the oldest to the most recent):')
                for vv in  available_flagversions:
                    if vv == flags_before_worker:
                        caracal.log.error('       {0:s}        <-- (this worker)'.format(vv))
                    elif vv == flags_after_worker:
                        caracal.log.error('       {0:s}         <-- (this worker)'.format(vv))
                    elif config['rewind_flags']["enable"] and vv == config['rewind_flags']["version"]:
                        caracal.log.error('       {0:s}        <-- (rewinding to this version)'.format(vv))
                    else:
                        caracal.log.error('       {0:s}'.format(vv))
                caracal.log.error('You have the following options:')
                caracal.log.error('    1) If you are happy with the flags currently stored in the FLAG column of this MS and')
                caracal.log.error('       want to append new flags to them, change the name of this worker in the configuration')
                caracal.log.error('       file by appending "__n" to it (where n is an integer not already taken in the list')
                caracal.log.error('       above). The new flags will be appended to the FLAG column, and new flag versions will')
                caracal.log.error('       be added to the list above.')
                caracal.log.error('    2) If you want to discard the flags obtained during the previous run of "{0:s}" (and,'.format(wname))
                caracal.log.error('       necessarily, all flags obtained thereafter; see list above) rewind the flag versions')
                caracal.log.error('       by setting in the configuration file:')
                caracal.log.error('           {0:s}: rewind_flags: enable: true'.format(wname))
                caracal.log.error('           {0:s}: rewind_flags: version: {1:s}'.format(wname, flags_before_worker))
                caracal.log.error('       You could rewind to an even earlier flag version if necessary. You will lose all flags')
                caracal.log.error('       appended to the FLAG column after that version, and take it from there.')
                caracal.log.error('    3) If you really know what you are doing allow Caracal to overwrite flag versions by setting:')
                caracal.log.error('           {0:s}: overwrite_flag_versions: true'.format(wname))
                caracal.log.error('       The worker "{0:s}" will be run again; the new flags will be appended to the current'.format(wname))
                caracal.log.error('       FLAG column (or to whatever flag version you are rewinding to); the flag versions from')
                caracal.log.error('       the previous run of "{0:s}" will be overwritten and appended to the list above (or'.format(wname))
                caracal.log.error('       to that list truncated to the flag version you are rewinding to).')
                caracal.log.error('Your choice will be applied to all MS files being processed together in this run of Caracal.')
                raise RuntimeError()

            if config['rewind_flags']["enable"]:
                version = config['rewind_flags']["version"]
                substep = 'rewind_to_{0:s}_ms{1:d}'.format(version, msiter)
                manflags.restore_cflags(pipeline, recipe, version, msname, cab_name=substep)
                substep = 'delete_flag_versions_after_{0:s}_ms{1:d}'.format(version, msiter)
                manflags.delete_cflags(pipeline, recipe,
                    available_flagversions[available_flagversions.index(version)+1],
                    msname, cab_name=substep)
                if  version != flags_before_worker:
                    substep = 'save_{0:s}_ms{1:d}'.format(flags_before_worker, msiter)
                    manflags.add_cflags(pipeline, recipe, flags_before_worker, msname, cab_name=substep, overwrite=config['overwrite_flag_versions'])
            else:
                substep = 'save_{0:s}_ms{1:d}'.format(flags_before_worker, msiter)
                manflags.add_cflags(pipeline, recipe, flags_before_worker, msname, cab_name=substep, overwrite=config['overwrite_flag_versions'])

            # flag antennas automatically based on drifts in the scan average of the
            # auto correlation spectra per field. This doesn't strictly require any calibration. It is also
            # not field structure dependent, since it is just based on the DC of the field
            # Compares scan to median power of scans per field per channel
            # Also compares antenna to median of the array per scan per field per channel
            # This should catch any antenna with severe temperature problems
            if pipeline.enable_task(config, 'flag_autopowerspec'):
                step = '{0:s}_autopowerspec_ms{1:d}'.format(wname, msiter)
                def_fields = ",".join(map(str, utils.get_field_id(
                    msinfo, manfields.get_field(pipeline, i, "bpcal,gcal,target,xcal").split(","))))
                def_calfields = ",".join(map(str, utils.get_field_id(
                    msinfo, manfields.get_field(pipeline, i, "bpcal,gcal,xcal").split(","))))
                if config['flag_autopowerspec'].get('fields') != 'auto' and \
                   not set(config['flag_autopowerspec'].get('fields').split(',')) <= set(['gcal', 'bpcal', 'fcal', 'target']):
                    raise KeyError(
                        "autoflag on autocorrelations powerspectra fields can only be 'auto' or be a combination of 'gcal', 'bpcal', 'fcal' or 'target'")
                if config['flag_autopowerspec'].get('calibrator_fields') != 'auto' and \
                   not set(config['flag_autopowerspec'].get('calibrator_fields').split(',')) <= set(['gcal', 'bpcal', 'fcal']):
                    raise KeyError(
                        "autoflag on autocorrelations powerspectra calibrator fields can only be 'auto' or be a combination of 'gcal', 'bpcal', 'fcal'")

                fields = def_fields if config['flag_autopowerspec'].get('fields') == 'auto' else \
                    ",".join([getattr(pipeline, key + "_id")[i][0]
                              for key in config['flag_autopowerspec'].get('fields').split(',')])
                calfields = def_calfields if config['flag_autopowerspec'].get('calibrator_fields') == 'auto' else \
                    ",".join([getattr(pipeline, key + "_id")[i][0]
                              for key in config['flag_autopowerspec'].get('calibrator_fields').split(',')])

                fields = ",".join(set(fields.split(",")))
                calfields = ",".join(set(calfields.split(",")))

                recipe.add("cab/politsiyakat_autocorr_amp", step,
                           {
                               "msname": msname,
                               "field": fields,
                               "cal_field": calfields,
                               "scan_to_scan_threshold": config["flag_autopowerspec"].get("scan_to_scan_threshold"),
                               "antenna_to_group_threshold": config["flag_autopowerspec"].get("antenna_to_group_threshold"),
                               "dpi": 300,
                               "plot_size": 6,
                               "nproc_threads": config['flag_autopowerspec'].get('threads'),
                               "data_column": config['flag_autopowerspec'].get('column')
                           },
                           input=pipeline.input, output=pipeline.output,
                           label="{0:s}:: Flag out antennas with drifts in autocorrelation powerspectra ms={1:s}".format(step,msname))

            # Define fields and field_ids to be used to only flag the fields selected with
            # flagging:field (either 'target' or 'calibrators') and with
            # flagging:calibrator_fields (for further selection among the calibrators)
            if config['field'] == 'target':
                fields = [target_ls[j]]
            else:
                fields = []
                fld_string = config['calibrator_fields']
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
            field_ids = utils.get_field_id(msinfo, fields)
            fields = ",".join(fields)

            if pipeline.enable_task(config, 'flag_autocorr'):
                step = '{0:s}_autocorr_ms{1:d}'.format(wname, msiter)
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
                step = '{0:s}_quack_ms{1:d}'.format(wname, msiter)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'quack',
                               "quackinterval": config['flag_quack'].get('quackinterval'),
                               "quackmode": config['flag_quack'].get('quackmode'),
                               "field": fields,
                               "flagbackup": False,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Quack flagging ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_elevation'):
                step = '{0:s}_elevation_ms{1:d}'.format(wname, msiter)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'elevation',
                               "lowerlimit": config['flag_elevation'].get('low'),
                               "upperlimit": config['flag_elevation'].get('high'),
                               "field": fields,
                               "flagbackup": False,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Flag elevation ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_shadow'):
                if config['flag_shadow'].get('include_full_mk64'):
                    addantennafile = '{0:s}/mk64.txt'.format(pipeline.input)
                    with open(msinfo, 'r') as stdr:
                        subarray = yaml.load(stdr)['ANT']['NAME']
                    idleants = open(addantennafile, 'r').readlines()
                    for aa in subarray:
                        for kk in range(len(idleants)):
                            if aa in idleants[kk]:
                                del(idleants[kk:kk+3])
                                break
                    addantennafile = 'idleants.txt'
                    with open('{0:s}/{1:s}'.format(pipeline.input, addantennafile), 'w') as ia:
                        for aa in idleants:
                            ia.write(aa)
                    addantennafile += ':input'
                else:
                    addantennafile = None
                step = '{0:s}_shadow_ms{1:d}'.format(wname, msiter)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'shadow',
                               "tolerance": config['flag_shadow'].get('tolerance'),
                               "addantenna": addantennafile,
                               "flagbackup": False,
                               "field": fields,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Flag shadowed antennas ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_spw'):
                step = '{0:s}_spw_ms{1:d}'.format(wname, msiter)
                flagspwselection = config['flag_spw']['channels']
                found_valid_data = 0
                if config['flag_spw'].get('ensure_valid_selection'):
                    scalefactor, scalefactor_dict = 1, {
                        'GHz': 1e+9, 'MHz': 1e+6, 'kHz': 1e+3}
                    for ff in flagspwselection.split(','):
                        for dd in scalefactor_dict:
                            if dd.lower() in ff.lower():
                                ff, scalefactor = ff.lower().replace(
                                    dd.lower(), ''), scalefactor_dict[dd]
                        ff = ff.lower().replace('hz', '').split(':')
                        if len(ff) > 1:
                            spws = ff[0]
                        else:
                            spws = '*'
                        edges = [
                            ii*scalefactor for ii in map(float, ff[-1].split('~'))]
                        if spws == '*':
                            spws = list(range(len(pipeline.firstchanfreq[i])))
                        elif '~' in spws:
                            spws = list(
                                range(int(spws.split('~')[0]), int(spws.split('~')[1])+1))
                        else:
                            spws = [int(spws), ]
                        edges = [edges for uu in range(len(spws))]
                        for ss in spws:
                            if ss < len(pipeline.lastchanfreq[i]) and min(edges[ss][1], pipeline.lastchanfreq[i][ss])-max(edges[ss][0], pipeline.firstchanfreq[i][ss]) > 0:
                                found_valid_data = 1
                    if not found_valid_data:
                        caracal.log.warn(
                            'The following channel selection has been made in the flag_spw module of the flagging worker: "{1:s}". This selection would result in no valid data in {0:s}. This would lead to the FATAL error "No valid SPW & Chan combination found" in CASA/FLAGDATA. To avoid this error the corresponding cab {2:s} will not be added to the Stimela recipe of the flagging worker.'.format(msname, flagspwselection, step))

                if found_valid_data or not config['flag_spw'].get('ensure_valid_selection'):
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
                step = '{0:s}_time_ms{1:d}'.format(wname, msiter)
                found_valid_data = 0
                if config['flag_time'].get('ensure_valid_selection'):
                    if pipeline.startdate[i]:
                        start_flagrange,end_flagrange=config['flag_time']['timerange'].split('~')
                        flag_start = float(''.join(re.split('/|:', start_flagrange)))
                        flag_end  = float(''.join(re.split('/|:', end_flagrange)))
                        if (flag_start <= pipeline.enddate[i]) and (pipeline.startdate[i] <= flag_end):
                            found_valid_data = 1
                    else:
                        raise ValueError("You wanted to ensure a valid time range but we could not find a start and end time")
                    if not found_valid_data:
                        caracal.log.warn(
                            'The following time selection has been made in the flag_time module of the flagging worker: "{1:s}". This selection would result in no valid data in {0:s}. This would lead to the FATAL error " The selected table has zero rows" in CASA/FLAGDATA. To avoid this error the corresponding cab {2:s} will not be added to the Stimela recipe of the flagging worker.'.format(msname, config['flag_time']['timerange'], step))
                if found_valid_data or not config['flag_time'].get('ensure_valid_selection'):
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
                step = '{0:s}_scan_ms{1:d}'.format(wname, msiter)
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
                step = '{0:s}_antennas_ms{1:d}'.format(wname, msiter)
                antennas = [config['flag_antennas']['antennas']]
                times = [config['flag_antennas'].get('timerange')]
                found_valid_data = [0]
                ensure = config['flag_antennas'].get('ensure_valid_selection')
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
                        for nn,time_range in enumerate(times):
                            start_flagrange,end_flagrange=time_range.split('~')
                            flag_start = float(''.join(re.split('/|:', start_flagrange)))
                            flag_end  = float(''.join(re.split('/|:', end_flagrange)))
                            if (flag_start <= pipeline.enddate[i]) and (pipeline.startdate[i] <= flag_end):
                                found_valid_data[nn] = 1
                    else:
                        raise ValueError("You wanted to ensure a valid time range but we could not find a start and end time")
                for nn,antenna in enumerate(antennas):
                    antstep = 'flag_antennas_{0:s}_{1:d}_ant{2:s}'.format(wname, i,antenna.replace(',','_'))
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
                                    label='{0:s}:: Flagging bad antenna {2:s} ms={1:s}'.format(antstep, msname,antenna))
                    elif ensure and not found_valid_data[nn]:
                        caracal.log.warn(
                            'The following time selection has been made in the flag_antennas module of the flagging worker: "{1:s}". This selection would result in no valid data in {0:s}. This would lead to the FATAL error " The selected table has zero rows" in CASA/FLAGDATA. To avoid this error the corresponding cab {2:s} will not be added to the Stimela recipe of the flagging worker.'.format(msname, times[nn], antstep))

            if pipeline.enable_task(config, 'flag_mask'):
                step = '{0:s}_mask_ms{1:d}'.format(wname, msiter)
                recipe.add('cab/rfimasker', step,
                           {
                               "msname": msname,
                               "mask": config['flag_mask']['mask'],
                               "accumulation_mode": 'or',
                               "uvrange": sdm.dismissable(config['flag_mask'].get('uvrange')),
                               "memory": 4096,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Apply flag mask ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_rfi'):
                step = '{0:s}_rfi_ms{1:d}'.format(wname, msiter)
                if config['flag_rfi']["flagger"] == "aoflagger":
                    if config['flag_rfi']['aoflagger']['ensure_valid_strategy']:
                        with open(msinfo, 'r') as stdr:
                            ms_corr = yaml.load(stdr)['CORR']['CORR_TYPE']
                        flag_corr=[]
                        with open('{0:s}/{1:s}'.format(pipeline.input,config['flag_rfi']['aoflagger']['strategy'])) as stdr:
                            for ss in stdr.readlines():
                                for pp in 'xx,xy,yx,yy,stokes-i,stokes-q,stokes-u,stokes-v'.split(','):
                                    if '<on-{0:s}>1</on-{0:s}>'.format(pp) in ss: flag_corr.append(pp)
                        if (('stokes-u' in flag_corr or 'stokes-v' in flag_corr) and ('XY' not in ms_corr or 'YX' not in ms_corr)) or\
                             ('xy' in flag_corr and 'XY' not in ms_corr) or\
                             ('yx' in flag_corr and 'YX' not in ms_corr) or\
                             (('stokes-i' in flag_corr or 'stokes-q' in flag_corr) and ('XX' not in ms_corr or 'YY' not in ms_corr)) or\
                             ('xx' in flag_corr and 'XX' not in ms_corr) or\
                             ('yy' in flag_corr and 'YY' not in ms_corr):
                            raise ValueError("The selected flagging strategy {0:s}/{1:s} will attempt to flag on {2:} but this is"\
                                             " not compatible with the {3:} correlations available in {4:s}. To proceed you can edit the flagging"\
                                             " strategy or, if you know what you are doing, disable aoflagger: ensure_valid_strategy.".format(
                                             pipeline.input,config['flag_rfi']['aoflagger']['strategy'],flag_corr,ms_corr,msname))

                    recipe.add('cab/autoflagger', step,
                               {
                                   "msname": msname,
                                   "column": config['flag_rfi'].get('column'),
                                   "fields": ",".join(map(str, field_ids)),
                                   "strategy": config['flag_rfi']['aoflagger']['strategy'],
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: AOFlagger auto-flagging flagging pass ms={1:s} fields={2:s}'.format(step, msname, fields))

                elif config['flag_rfi']["flagger"] == "tricolour":
                    tricolour_strat=config['flag_rfi']['tricolour']['strategy']
                    if config['flag_rfi']['tricolour']['mode'] == 'auto':
                        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, msname[:-3])
                        with open(msinfo, 'r') as stdr:
                                  bandwidth = yaml.load(stdr)['SPW']['TOTAL_BANDWIDTH'][0]/10.0**6
                                  print("Total Bandwidth =", bandwidth, "MHz")
                                  if bandwidth <= 20.0:
                                      print("Narrowband data detected, selecting appropriate flagging strategy")
                                      tricolour_strat = config['flag_rfi']['tricolour']['strategy_narrowband']

                    print("Flagging strategy in use:", tricolour_strat)
                    recipe.add('cab/tricolour', step,
                               {
                                   "ms": msname,
                                   "data-column": config['flag_rfi'].get('column'),
                                   "window-backend": config['flag_rfi']['tricolour'].get('window_backend'),
                                   "field-names": fields,
                                   "flagging-strategy": 'polarisation',
                                   "config" : tricolour_strat,
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: Tricolour auto-flagging flagging pass ms={1:s} fields={2:s}'.format(step, msname, fields))

                elif config['flag_rfi']["flagger"] == "tfcrop":
                    column = config['flag_rfi'].get('column').split("_DATA")[0].lower()
                    recipe.add('cab/casa_flagdata', step,
                               {
                                   "vis" : msname,
                                   "datacolumn" : column,
                                   "mode" : "tfcrop",
                                   "field" : fields,
                                   "usewindowstats" : config["flag_rfi"]["tfcrop"]["usewindowstats"],
                                   "combinescans" : config["flag_rfi"]["tfcrop"]["combinescans"],
                                   "flagdimension" : config["flag_rfi"]["tfcrop"]["flagdimension"],
                                   "flagbackup" : False,
                                   "timecutoff" : config["flag_rfi"]["tfcrop"]["timecutoff"],
                                   "freqcutoff" : config["flag_rfi"]["tfcrop"]["freqcutoff"],
                                   "correlation" : config["flag_rfi"]["tfcrop"]["correlation"],
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: Tfcrop auto-flagging flagging pass ms={1:s} fields={2:s}'.format(step, msname, fields))
                else:
                    raise RuntimeError(
                        "Flagger, {0:s} is not available. Options are 'aoflagger, tricolour, tfcrop'.")

            if pipeline.enable_task(config, 'inspect'):
                step = '{0:s}_inspect_ms{1:d}'.format(wname,msiter)
                if config['field'] == 'target':
                    fieldName = utils.filter_name(target_ls[j])
                    field = '0'
                else:
                    field = ",".join(map(str, utils.get_field_id(msinfo, manfields.get_field(
                        pipeline, i, config['inspect'].get('field')).split(","))))
                for f in field.split(','):
                    outlabel = '_{0:d}'.format(i) if len(field.split(',')) == 1 else '_{0:d}_{1:s}'.format(i,f)
                    recipe.add('cab/rfinder', step,
                               {
                                   "msname": msname,
                                   "field": int(f),
                                   "plot_noise": "noise",
                                   "RFInder_mode": "use_flags",
                                   "outlabel": outlabel,  # The output will be rfi_<pol>_<outlabel>
                                   "polarization": config['inspect'].get('polarization'),
                                   "spw_width": config['inspect'].get('spw_width'),
                                   "time_step": config['inspect'].get('time_step'),
                                   "time_enable": config['inspect'].get('time_enable'),
                                   "spw_enable": config['inspect'].get('spw_enable'),
                                   "1d_gif": config['inspect'].get('time_enable'),
                                   "2d_gif": config['inspect'].get('time_enable'),
                                   "altaz_gif": config['inspect'].get('spw_enable'),
                                   "movies_in_report": config['inspect'].get('time_enable') or config.get('spw_enable')
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: Investigate presence of rfi in ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'summary'):
                __label = config.get('label_in', False)
                step = '{0:s}_summary_ms{1:d}'.format(wname,msiter)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'summary',
                               "field": fields,
                               "flagbackup": False,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))

            substep = 'save_{0:s}_ms{1:d}'.format(flags_after_worker, msiter)
            manflags.add_cflags(pipeline, recipe, flags_after_worker, msname, cab_name=substep, overwrite=config['overwrite_flag_versions'])
            msiter+=1

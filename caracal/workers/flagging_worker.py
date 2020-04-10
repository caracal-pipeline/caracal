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

FLAG_NAMES = "static automatic autocorr_spectrum".split()


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


    for i in range(nobs):
        # loop over all input .MS files
        # the additional 'for loop' below loops over all single target .MS files
        #   produced by the pipeline (see "if label" below)

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

        for m in mslist:  # check whether all ms files to be used exist
            if not os.path.exists(os.path.join(pipeline.msdir, m)):
                raise IOError(
                    "MS file {0:s} does not exist. Please check that is where it should be.".format(m))

        # flag antennas automatically based on drifts in the scan average of the
        # auto correlation spectra per field. This doesn't strictly require any calibration. It is also
        # not field structure dependent, since it is just based on the DC of the field
        # Compares scan to median power of scans per field per channel
        # Also compares antenna to median of the array per scan per field per channel
        # This should catch any antenna with severe temperature problems
        for j, msname in enumerate(mslist):
            msinfo = '{0:s}/{1:s}-obsinfo.json'.format(
                pipeline.output, msname[:-3])

            if not os.path.exists(msinfo):
                raise IOError(
                    "MS info file {0:s} does not exist. Please check that is where it should be.".format(msinfo))

            substep = 'save_flags_before_{0:s}_{1:d}_{2:d}'.format(wname, i, j)
            fversion = "before_%s" % wname
            _version = config['load_flags']["version"]
            manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, fversion]), msname, cab_name=substep)

            if config['load_flags']["enable"] and fversion!=_version:
                version = config['load_flags']["version"]
                merge = config['load_flags']["merge"]
                step = 'loading_flags_{0:s}_{1:s}'.format(wname, version)
                manflags.restore_cflags(pipeline, recipe, "_".join(
                    [wname, version]), msname, cab_name=step, merge=merge)

            if pipeline.enable_task(config, 'autoflag_autocorr_powerspectra'):

                substep = 'save_flags_before_autocorr_power_{0:s}_{1:d}_{2:d}'.format(wname, i, j)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "before_%s_autocorr_power" % wname]), msname, cab_name=substep)

                def_fields = ",".join(map(str, utils.get_field_id(
                    msinfo, manfields.get_field(pipeline, i, "bpcal,gcal,target,xcal").split(","))))
                def_calfields = ",".join(map(str, utils.get_field_id(
                    msinfo, manfields.get_field(pipeline, i, "bpcal,gcal,xcal").split(","))))
                if config['autoflag_autocorr_powerspectra'].get('fields') != 'auto' and \
                   not set(config['autoflag_autocorr_powerspectra'].get('fields').split(',')) <= set(['gcal', 'bpcal', 'fcal', 'target']):
                    raise KeyError(
                        "autoflag on powerspectra fields can only be 'auto' or be a combination of 'gcal', 'bpcal', 'fcal' or 'target'")
                if config['autoflag_autocorr_powerspectra'].get('calibrator_fields') != 'auto' and \
                   not set(config['autoflag_autocorr_powerspectra'].get('calibrator_fields').split(',')) <= set(['gcal', 'bpcal', 'fcal']):
                    raise KeyError(
                        "autoflag on powerspectra calibrator fields can only be 'auto' or be a combination of 'gcal', 'bpcal', 'fcal'")

                fields = def_fields if config['autoflag_autocorr_powerspectra'].get('fields') == 'auto' else \
                    ",".join([getattr(pipeline, key + "_id")[i][0]
                              for key in config['autoflag_autocorr_powerspectra'].get('fields').split(',')])
                calfields = def_calfields if config['autoflag_autocorr_powerspectra'].get('calibrator_fields') == 'auto' else \
                    ",".join([getattr(pipeline, key + "_id")[i][0]
                              for key in config['autoflag_autocorr_powerspectra'].get('calibrator_fields').split(',')])

                fields = ",".join(set(fields.split(",")))
                calfields = ",".join(set(calfields.split(",")))

                step = 'autoflag_autocorr_spectra_{0:s}_{1:d}_{2:d}'.format(wname, i, j)
                recipe.add("cab/politsiyakat_autocorr_amp", step,
                           {
                               "msname": msname,
                               "field": fields,
                               "cal_field": calfields,
                               "scan_to_scan_threshold": config["autoflag_autocorr_powerspectra"].get("scan_to_scan_threshold"),
                               "antenna_to_group_threshold": config["autoflag_autocorr_powerspectra"].get("antenna_to_group_threshold"),
                               "dpi": 300,
                               "plot_size": 6,
                               "nproc_threads": config['autoflag_autocorr_powerspectra'].get('threads'),
                               "data_column": config['autoflag_autocorr_powerspectra'].get('column')
                           },
                           input=pipeline.input, output=pipeline.output,
                           label="{0:s}: Flag out antennas with drifts in autocorrelation powerspectra")

                substep = 'save_flags_after_autocorr_power_{0:s}_{1:d}_{2:d}'.format(wname, i, j)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "after_%s_autocorr_power" % wname]), msname, cab_name=substep)


            if pipeline.enable_task(config, 'flag_autocorr'):
                static='autocorr'
                step = 'flag_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                substep = 'save_flags_before_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "before_%s" % (static)]), msname, cab_name=substep)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'manual',
                               "autocorr": True,
                               "flagbackup": False
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Flag auto-correlations ms={1:s}'.format(step, msname))
                substep = 'save_flags_after_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "after_%s" % (static)]), msname, cab_name=substep)


            if pipeline.enable_task(config, 'quack_flagging'):
                static='quack'
                step = 'flag_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                substep = 'save_flags_before_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "before_%s" % (static)]), msname, cab_name=substep)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'quack',
                               "quackinterval": config['quack_flagging'].get('quackinterval'),
                               "quackmode": config['quack_flagging'].get('quackmode'),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Quack flagging ms={1:s}'.format(step, msname))
                substep = 'save_flags_after_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "after_%s" % (static)]), msname, cab_name=substep)


            if pipeline.enable_task(config, 'flag_elevation'):
                static='elevation'
                step = 'flag_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                substep = 'save_flags_before_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "before_%s" % (static)]), msname, cab_name=substep)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'elevation',
                               "lowerlimit": config['flag_elevation'].get('low'),
                               "upperlimit": config['flag_elevation'].get('high'),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Flag elevation ms={1:s}'.format(step, msname))
                substep = 'save_flags_after_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "after_%s" % (static)]), msname, cab_name=substep)


            if pipeline.enable_task(config, 'flag_shadow'):
                static='shadow'
                if config['flag_shadow'].get('include_full_mk64'):
                    #                    msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, msname[:-3])
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
                step = 'flag_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                substep = 'save_flags_before_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "before_%s" % (static)]), msname, cab_name=substep)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'shadow',
                               "tolerance": config['flag_shadow'].get('tolerance'),
                               "addantenna": addantennafile,
                               "flagbackup": False
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Flag shadowed antennas ms={1:s}'.format(step, msname))
                substep = 'save_flags_after_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "after_%s" % (static)]), msname, cab_name=substep)


            if pipeline.enable_task(config, 'flag_spw'):
                static='spw'
                step = 'flag_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                substep = 'save_flags_before_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "before_%s" % (static)]), msname, cab_name=substep)
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
                                   "flagbackup": False,
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}::Flag out channels ms={1:s}'.format(step, msname))
                substep = 'save_flags_after_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "after_%s" % (static)]), msname, cab_name=substep)



            if pipeline.enable_task(config, 'flag_time'):
                static='time'
                step = 'flag_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                substep = 'save_flags_before_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "before_%s" % (static)]), msname, cab_name=substep)
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
                                },
                                input=pipeline.input,
                                output=pipeline.output,
                                label='{0:s}::Flag out channels ms={1:s}'.format(step, msname))
                substep = 'save_flags_after_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "after_%s" % (static)]), msname, cab_name=substep)


            if pipeline.enable_task(config, 'flag_scan'):
                static='scan'
                step = 'flag_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                substep = 'save_flags_before_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "before_%s" % (static)]), msname, cab_name=substep)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'manual',
                               "scan": config['flag_scan']['scans'],
                               "flagbackup": False,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}::Flag out channels ms={1:s}'.format(step, msname))
                substep = 'save_flags_after_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "after_%s" % (static)]), msname, cab_name=substep)



            if pipeline.enable_task(config, 'flag_antennas'):
                static='antennas'
                step = 'flag_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                substep = 'save_flags_before_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "before_%s" % (static)]), msname, cab_name=substep)
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
                    step = 'flag_antennas_{0:s}_{1:d}_ant{2:s}'.format(wname, i,antenna.replace(',','_'))
                    if found_valid_data[nn] or not ensure:
                        recipe.add('cab/casa_flagdata', step,
                                    {
                                        "vis": msname,
                                        "mode": 'manual',
                                        "antenna": antenna,
                                        "timerange": times[nn],
                                        "flagbackup": False,
                                    },
                                    input=pipeline.input,
                                    output=pipeline.output,
                                    label='{0:s}:: Flagging bad antenna {2:s} ms={1:s}'.format(step, msname,antenna))
                    elif ensure and not found_valid_data[nn]:
                        caracal.log.warn(
                            'The following time selection has been made in the flag_antennas module of the flagging worker: "{1:s}". This selection would result in no valid data in {0:s}. This would lead to the FATAL error " The selected table has zero rows" in CASA/FLAGDATA. To avoid this error the corresponding cab {2:s} will not be added to the Stimela recipe of the flagging worker.'.format(msname, times[nn], step))
                substep = 'save_flags_after_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "after_%s" % (static)]), msname, cab_name=substep)


            if pipeline.enable_task(config, 'static_mask'):
                static='static_mask'
                step = 'flag_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                substep = 'save_flags_before_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "before_%s" % (static)]), msname, cab_name=substep)

                recipe.add('cab/rfimasker', step,
                           {
                               "msname": msname,
                               "mask": config['static_mask']['mask'],
                               "accumulation_mode": 'or',
                               "uvrange": sdm.dismissable(config['static_mask'].get('uvrange')),
                               "memory": 4096,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Apply static mask ms={1:s}'.format(step, msname))

                substep = 'save_flags_after_{3:s}_{0:s}_{1:d}_{2:d}'.format(wname, i, j,static)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "after_%s" % (static)]), msname, cab_name=substep)


            if pipeline.enable_task(config, 'autoflag_rfi'):
                step = 'autoflag_{0:s}_{1:d}_{2:d}'.format(wname, i, j)
                # Clear autoflags if need be                substep = 'save_flags_before_automatic_{0:s}_{1:d}_{2:d}'.format(wname, i, j)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "before_%s_automatic" % wname]), msname, cab_name=substep)
                if config['field'] == 'target':
                    fields = [target_ls[j]]
                    tricolour_mode = 'polarisation'
                    tricolour_strat = 'mk_rfi_flagging_target_fields_firstpass.yaml'
                else:
                    fields = []
                    fld_string = config['autoflag_rfi']["fields"]
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
                    tricolour_mode = 'polarisation'
                    tricolour_strat = 'mk_rfi_flagging_target_fields_firstpass.yaml'

                field_ids = utils.get_field_id(msinfo, fields)
                fields = ",".join(fields)
                if config['autoflag_rfi']["flagger"] == "aoflagger":
                    recipe.add('cab/autoflagger', step,
                               {
                                   "msname": msname,
                                   "column": config['autoflag_rfi'].get('column'),
                                   # flag the calibrators for RFI and apply to target
                                   "fields": ",".join(map(str, field_ids)),
                                   # "bands"       : config['autoflag_rfi'].get('bands', "0"),
                                   "strategy": config['autoflag_rfi']['strategy'],
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: Auto-flagging flagging pass ms={1:s}'.format(step, msname))

                elif config['autoflag_rfi']["flagger"] == "tricolour":
                    if config['autoflag_rfi']['tricolour_mode'] == 'auto':
                        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, msname[:-3])
                        with open(msinfo, 'r') as stdr:
                                  bandwidth = yaml.load(stdr)['SPW']['TOTAL_BANDWIDTH'][0]/10.0**6
                                  print("Total Bandwidth =", bandwidth, "MHz")
                                  if bandwidth <= 20.0:
                                      print("Narrowband data detected, selecting appropriate flagging strategy")
                                      tricolour_strat = config['autoflag_rfi']['tricolour_calibrator_strat_narrowband']
                              
                    print("Flagging strategy in use:", tricolour_strat)
                    recipe.add('cab/tricolour', step,
                               {
                                   "ms": msname,
                                   "data-column": config['autoflag_rfi'].get('column'),
                                   "window-backend": config['autoflag_rfi'].get('window_backend'),
                                   "field-names": fields,
                                   "flagging-strategy": tricolour_mode,
                                   "config" : tricolour_strat,
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: Auto-flagging flagging pass ms={1:s}'.format(step, msname))

                elif config['autoflag_rfi']["flagger"] == "tfcrop":
                    column = config['autoflag_rfi'].get('column').split("_DATA")[0].lower()
                    recipe.add('cab/casa_flagdata', step,
                               {
                                   "vis" : msname,
                                   "datacolumn" : column,
                                   "mode" : "tfcrop",
                                   "field" : fields,
                                   "usewindowstats" : config["autoflag_rfi"]["usewindowstats"],
                                   "combinescans" : config["autoflag_rfi"]["combinescans"],
                                   "flagdimension" : config["autoflag_rfi"]["flagdimension"],
                                   "flagbackup" : False,
                                   "timecutoff" : config["autoflag_rfi"]["timecutoff"],
                                   "freqcutoff" : config["autoflag_rfi"]["freqcutoff"],
                                   "correlation" : config["autoflag_rfi"]["correlation"],
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: Auto-flagging flagging pass ms={1:s}'.format(step, msname))
                else:
                    raise RuntimeError(
                        "Flagger, {0:s} is not available. Options are 'aoflagger, tricolour'.")

                substep = 'save_flags_after_automatic_{0:s}_{1:d}_{2:d}'.format(
                    wname, i, j)
                manflags.add_cflags(pipeline, recipe, "_".join(
                    [wname, "after_%s_automatic" % wname]), msname, cab_name=substep)

            if pipeline.enable_task(config, 'rfinder'):
                step = 'rfinder_{0:s}_{1:d}_{2:d}'.format(wname, i, j)
                if config['field'] == 'target':
                    fieldName = utils.filter_name(target_ls[j])
                    field = '0'
                else:
                    field = ",".join(map(str, utils.get_field_id(msinfo, manfields.get_field(
                        pipeline, i, config['rfinder'].get('field')).split(","))))
                for f in field.split(','):
                    outlabel = '_{0:d}'.format(i) if len(field.split(',')) == 1 else '_{0:d}_{1:s}'.format(i,f)
                    recipe.add('cab/rfinder', step,
                               {
                                   "msname": msname,
                                   "field": int(f),
                                   "plot_noise": "noise",
                                   "RFInder_mode": "use_flags",
                                   "outlabel": outlabel,  # The output will be rfi_<pol>_<outlabel>
                                   "polarization": config['rfinder'].get('polarization'),
                                   "spw_width": config['rfinder'].get('spw_width'),
                                   "time_step": config['rfinder'].get('time_step'),
                                   "time_enable": config['rfinder'].get('time_enable'),
                                   "spw_enable": config['rfinder'].get('spw_enable'),
                                   "1d_gif": config['rfinder'].get('time_enable'),
                                   "2d_gif": config['rfinder'].get('time_enable'),
                                   "altaz_gif": config['rfinder'].get('spw_enable'),
                                   "movies_in_report": config['rfinder'].get('time_enable') or config.get('spw_enable')
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: Investigate presence of rfi in ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flagging_summary'):
                __label = config.get('label_in', False)
                step = 'flagging_summary_{0:s}_{1:d}{2:s}_{3:d}'.format(
                    wname, i, "_"+__label or "", j)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'summary',
                               "flagbackup": False,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}-{1:s}:: Flagging summary  ms={2:s}'.format(step, config.get('label_in'), msname))

            substep = 'save_flags_after_{0:s}_{1:d}_{2:d}'.format(
                wname, i, j)
            manflags.add_cflags(pipeline, recipe, "_".join(
                [wname, "after_%s" % wname]), msname, cab_name=substep)
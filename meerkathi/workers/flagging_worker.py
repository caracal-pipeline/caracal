from meerkathi.workers.utils import manage_fields as manfields
from meerkathi.workers.utils import manage_flagsets as manflags
import os
from meerkathi.dispatch_crew import utils
import stimela.dismissable as sdm
import yaml
import meerkathi
import sys
NAME = 'Pre-calibration flagging'


FLAGSETS_SUFFIX = "static automatic autocorr_spectrum".split()


def worker(pipeline, recipe, config):
    label = config['label']
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

        if label:
            target_ls = pipeline.target[i].split(',')
            for target in target_ls:
                field = utils.filter_name(target)
                mslist.append(
                    '{0:s}-{1:s}_{2:s}.ms'.format(pipeline.msnames[i][:-3], field, label))
        else:
            mslist.append(msnames[i])

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
            if label:
                fieldName = utils.filter_name(target_ls[j])
                msinfo = '{0:s}/{1:s}-{2:s}-obsinfo.json'.format(
                    pipeline.output, pipeline.prefix, msname[:-3])
            else:
                msinfo = '{0:s}/{1:s}-obsinfo.json'.format(
                    pipeline.output, prefix)

            if not os.path.exists(msinfo):
                raise IOError(
                    "MS info file {0:s} does not exist. Please check that is where it should be.".format(msinfo))

            if pipeline.enable_task(config, 'autoflag_autocorr_powerspectra'):
                step = 'autoflag_autocorr_spectra_{0:s}_{1:d}'.format(wname, i)
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

                # Clear autocorr_spectrum flags if they exist. Else, create the flagset
                substep = 'flagset_clear_autocorr_spectra_{0:s}_{1:d}'.format(
                    wname, i)
                manflags.clear_flagset(pipeline, recipe, "_".join(
                    [wname, "autocorr_spectrum"]), msname, cab_name=substep)

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

                substep = 'flagset_update_autocorr_spectra_{0:s}_{1:d}'.format(
                    wname, i)
                manflags.update_flagset(pipeline, recipe, "_".join(
                    [wname, "autocorr_spectrum"]), msname, cab_name=substep)

            # clear static flags if any of them are enabled
            static_flagging = True in [pipeline.enable_task(config, sflag) for sflag in ["flag_autocorr", "quack_flagging",
                                                                                         "flag_shadow", "flag_spw", "flag_time", "flag_scan", "flag_antennas", "static_mask"]]

            if static_flagging:
                substep = 'flagset_clear_static_{0:s}_{1:d}'.format(wname, i)
                manflags.clear_flagset(pipeline, recipe, "_".join(
                    [wname, "static"]), msname, cab_name=substep)

            if pipeline.enable_task(config, 'flag_autocorr'):
                step = 'flag_autocorr_{0:s}_{1:d}'.format(wname, i)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'manual',
                               "autocorr": True,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Flag auto-correlations ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'quack_flagging'):
                step = 'quack_flagging_{0:s}_{1:d}'.format(wname, i)
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

            if pipeline.enable_task(config, 'flag_shadow'):
                if config['flag_shadow'].get('include_full_mk64'):
                    #                    msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, prefix)
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
                step = 'flag_shadow_{0:s}_{1:d}'.format(wname, i)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'shadow',
                               "tolerance": config['flag_shadow'].get('tolerance'),
                               "addantenna": addantennafile,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Flag shadowed antennas ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_spw'):
                flagspwselection = config['flag_spw']['channels']
                step = 'flag_spw_{0:s}_{1:d}'.format(wname, i)
                found_valid_data = 0
                if config['flag_spw'].get('ensure_valid_selection'):
                    scalefactor, scalefactor_dict = 1, {
                        'GHz': 1e+9, 'MHz': 1e+6, 'kHz': 1e+3}
                    for ff in flagspwselection.split(','):
                        for dd in scalefactor_dict:
                            if dd in ff:
                                ff, scalefactor = ff.replace(
                                    dd, ''), scalefactor_dict[dd]
                        ff = ff.replace('Hz', '').split(':')
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
                            spws = [spws, ]
                        edges = [edges for uu in range(len(spws))]
                        for ss in spws:
                            if ss < len(pipeline.lastchanfreq[i]) and min(edges[ss][1], pipeline.lastchanfreq[i][ss])-max(edges[ss][0], pipeline.firstchanfreq[i][ss]) > 0:
                                found_valid_data = 1
                    if not found_valid_data:
                        meerkathi.log.warn(
                            'The following channel selection has been made in the flag_spw module of the flagging worker: "{1:s}". This selection would result in no valid data in {0:s}. This would lead to the FATAL error "No valid SPW & Chan combination found" in CASA/FLAGDATA. To avoid this error the corresponding cab {2:s} will not be added to the Stimela recipe of the flagging worker.'.format(msname, flagspwselection, step))

                if found_valid_data or not config['flag_spw'].get('ensure_valid_selection'):

                    recipe.add('cab/casa_flagdata', step,
                               {
                                   "vis": msname,
                                   "mode": 'manual',
                                   "spw": flagspwselection,
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}::Flag out channels ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_time'):
                step = 'flag_time_{0:s}_{1:d}'.format(wname, i)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'manual',
                               "timerange": config['flag_time']['timerange'],
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}::Flag out channels ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_scan'):
                step = 'flag_scan_{0:s}_{1:d}'.format(wname, i)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'manual',
                               "scan": config['flag_scan']['scans'],
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}::Flag out channels ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'flag_antennas'):
                step = 'flag_antennas_{0:s}_{1:d}'.format(wname, i)
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'manual',
                               "antenna": config['flag_antennas']['antennas'],
                               "timerange": config['flag_antennas'].get('timerange'),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Flagging bad antennas ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'static_mask'):
                step = 'static_mask_{0:s}_{1:d}'.format(wname, i)
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

            if static_flagging:
                substep = 'flagset_update_static_{0:s}_{1:d}'.format(wname, i)
                manflags.update_flagset(pipeline, recipe, "_".join(
                    [wname, "static"]), msname, cab_name=substep)

            if pipeline.enable_task(config, 'autoflag_rfi'):
                step = 'autoflag_{0:s}_{1:d}'.format(wname, i)
                # Clear autoflags if need be
                substep = 'flagset_clear_automatic_{0:s}_{1:d}'.format(
                    wname, i)
                manflags.clear_flagset(pipeline, recipe, "_".join(
                    [wname, "automatic"]), msname, cab_name=substep)

                if config['autoflag_rfi'].get('fields') != 'auto' and \
                   not set(config['autoflag_rfi'].get('fields', 'auto').split(',')) <= set(['xcal', 'gcal', 'bpcal', 'target', 'fcal']):
                    raise KeyError(
                        "autoflag rfi can only be 'auto' or be a combination of 'xcal', 'gcal', 'fcal', 'bpcal' or 'target'")
                if config['autoflag_rfi'].get('calibrator_fields') != 'auto' and \
                   not set(config['autoflag_rfi'].get('calibrator_fields').split(',')) <= set(['xcal', 'gcal', 'bpcal', 'fcal']):
                    raise KeyError(
                        "autoflag rfi fields can only be 'auto' or be a combination of 'xcal', 'gcal', 'bpcal', 'fcal'")

                if label:
                    fields = 'target'
                    field_names = manfields.get_field(pipeline, i, fields)
                    fields = ",".join(
                        map(str, utils.get_field_id(msinfo, field_names)))
                    tricolour_mode = 'polarisation'
                    tricolour_strat = 'mk_rfi_flagging_target_fields_firstpass.yaml'
                elif config['autoflag_rfi'].get('fields') == 'auto':
                    fields = 'target,bpcal,gcal,xcal'
                    tricolour_mode = 'polarisation'
                    tricolour_strat = 'mk_rfi_flagging_target_fields_firstpass.yaml'
                    field_names = manfields.get_field(pipeline, i, fields)
                    fields = ",".join(
                        map(str, utils.get_field_id(msinfo, field_names)))
                else:
                    field_names = manfields.get_field(
                        pipeline, i, config['autoflag_rfi'].get('fields')).split(",")
                    fields = ",".join(
                        map(str, utils.get_field_id(msinfo, field_names)))
                    if 'target' in fields:
                       tricolour_mode = 'polarisation'
                       tricolour_strt = 'mk_rfi_flagging_target_fields_firstpass.yaml'
                    else: 
                       tricolour_mode = 'total_power'
                       tricolour_strat = config['autoflag_rfi'].get('tricolour_calibrator_strat')
          

                field_names = list(set(field_names))

                # Make sure no field IDs are duplicated
                fields = ",".join(set(fields.split(",")))
                if config['autoflag_rfi']["flagger"] == "aoflagger":
                    recipe.add('cab/autoflagger', step,
                               {
                                   "msname": msname,
                                   "column": config['autoflag_rfi'].get('column'),
                                   # flag the calibrators for RFI and apply to target
                                   "fields": fields,
                                   # "bands"       : config['autoflag_rfi'].get('bands', "0"),
                                   "strategy": config['autoflag_rfi']['strategy'],
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: Auto-flagging flagging pass ms={1:s}'.format(step, msname))

                elif config['autoflag_rfi']["flagger"] == "tricolour":
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
                else:
                    raise RuntimeError(
                        "Flagger, {0:s} is not available. Options are 'aoflagger, tricolour'.")

                substep = 'flagset_update_automatic_{0:s}_{1:d}'.format(
                    wname, i)
                manflags.update_flagset(pipeline, recipe, "_".join(
                    [wname, "automatic"]), msname, cab_name=substep)

               # recipe.add('cab/autoflagger', step,
               #            {
               #                "msname": msname,
               #                "column": config['autoflag_rfi'].get('column'),
               #                # flag the calibrators for RFI and apply to target
               #                "fields": fields,
               #                # "bands"       : config['autoflag_rfi'].get('bands', "0"),
               #                "strategy": config['autoflag_rfi']['strategy'],
               #            #},
               #            input=pipeline.input,
               #            output=pipeline.output,
               #            label='{0:s}:: Aoflagger flagging pass ms={1:s}'.format(step, msname))

            if pipeline.enable_task(config, 'rfinder'):
                step = 'rfinder_{0:s}_{1:d}'.format(wname, i)
                if label:
                    field = '0'
                    outlabel = '_{0:s}_{1:d}'.format(fieldName, i)
                else:
                    field = ",".join(map(str, utils.get_field_id(msinfo, manfields.get_field(
                        pipeline, i, config['rfinder'].get('field')).split(","))))
                    outlabel = '_{0:s}'.format(i)
                recipe.add('cab/rfinder', step,
                           {
                               "msname": msname,
                               "field": int(field),
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

                __label = config.get('label', False)
                step = 'flagging_summary_{0:s}_{1:d}{2:s}'.format(
                    wname, i, "_"+__label or "")
                recipe.add('cab/casa_flagdata', step,
                           {
                               "vis": msname,
                               "mode": 'summary',
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}-{1:s}:: Flagging summary  ms={2:s}'.format(step, config.get('label'), msname))

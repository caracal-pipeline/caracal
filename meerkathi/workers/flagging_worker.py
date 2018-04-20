NAME = 'Pre-calibration flagging'

import sys
import meerkathi


def worker(pipeline, recipe, config):
    if pipeline.virtconcat:
        msnames = [pipeline.vmsname]
        nobs = 1
    else:
        msnames = pipeline.msnames
        nobs = pipeline.nobs
    if config['label']: msnames=[mm.replace('.ms','-{0:s}.ms'.format(config['label'])) for mm in msnames]
    for i in range(nobs):
        msname = msnames[i]
        # flag antennas automatically based on drifts in the scan average of the 
        # auto correlation spectra per field. This doesn't strictly require any calibration. It is also
        # not field structure dependent, since it is just based on the DC of the field
        # Compares scan to median power of scans per field per channel
        # Also compares antenna to median of the array per scan per field per channel
        # This should catch any antenna with severe temperature problems
        if pipeline.enable_task(config, 'autoflag_autocorr_powerspectra'):
            step = 'autoflag_autocorr_spectra_{0:d}'.format(i)
            def_fields = ','.join([pipeline.bpcal_id[i], pipeline.gcal_id[i], pipeline.target_id[i]])
            def_calfields = ','.join([pipeline.bpcal_id[i], pipeline.gcal_id[i]])
            if config['autoflag_autocorr_powerspectra'].get('fields', 'auto') != 'auto' and \
               not set(config['autoflag_autocorr_powerspectra'].get('fields', 'auto').split(',')) <= set(['gcal', 'bpcal', 'target']):
                raise KeyError("autoflag on powerspectra fields can only be 'auto' or be a combination of 'gcal', 'bpcal' or 'target'")
            if config['autoflag_autocorr_powerspectra'].get('calibrator_fields', 'auto') != 'auto' and \
               not set(config['autoflag_autocorr_powerspectra'].get('calibrator_fields', 'auto').split(',')) <= set(['gcal', 'bpcal']):
                raise KeyError("autoflag on powerspectra calibrator fields can only be 'auto' or be a combination of 'gcal', 'bpcal'")

            fields = def_fields if config['autoflag_autocorr_powerspectra'].get('fields', 'auto') == 'auto' else \
                     ",".join([getattr(pipeline, key + "_id")[i] for key in config['autoflag_autocorr_powerspectra'].get('fields').split(',')])
            calfields = def_calfields if config['autoflag_autocorr_powerspectra'].get('calibrator_fields', 'auto') == 'auto' else \
                     ",".join([getattr(pipeline, key + "_id")[i] for key in config['autoflag_autocorr_powerspectra'].get('calibrator_fields').split(',')])

            recipe.add("cab/politsiyakat_autocorr_amp", step,
                {
                    "msname": msname,
                    "field": fields,
                    "cal_field": calfields,
                    "scan_to_scan_threshold": config["autoflag_autocorr_powerspectra"]["scan_to_scan_threshold"],
                    "antenna_to_group_threshold": config["autoflag_autocorr_powerspectra"]["antenna_to_group_threshold"],

                    "dpi": config['autoflag_autocorr_powerspectra'].get('dpi', 300),
                    "plot_size": config['autoflag_autocorr_powerspectra'].get('plot_size', 6),
                    "nproc_threads": config['autoflag_autocorr_powerspectra'].get('threads', 8),
                    "data_column": config['autoflag_autocorr_powerspectra'].get('column', "DATA")
                },
                input=pipeline.input, output=pipeline.output,
                label="{0:s}: Flag out antennas with drifts in autocorrelation powerspectra")

        if pipeline.enable_task(config, 'flag_autocorr'):
            step = 'flag_autocorr_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata', step,
                {
                  "vis"         : msname,
                  "mode"        : 'manual',
                  "autocorr"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Flag auto-correlations ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'quack_flagging'):
            step = 'quack_flagging_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata', step,
                {
                  "vis"           : msname,
                  "mode"          : 'quack',
                  "quackinterval" : config['quack_flagging'].get('quackinterval', 10),
                  "quackmode"     : config['quack_flagging'].get('quackmode', 'beg'),
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Quack flagging ms={1:s}'.format(step, msname))


        if pipeline.enable_task(config, 'flag_spw'):
            flagspwselection=config['flag_spw']['channels']
            step = 'flag_spw_{0:d}'.format(i)
            found_valid_data=0
            if config['flag_spw'].get('ensure_valid_selection',False):
                scalefactor,scalefactor_dict=1,{'GHz':1e+9,'MHz':1e+6,'kHz':1e+3}
                for ff in flagspwselection.split(','):
                    for dd in scalefactor_dict:
                        if dd in ff: ff,scalefactor=ff.replace(dd,''),scalefactor_dict[dd]
                    ff=ff.replace('Hz','').split(':')
                    if len(ff)>1: spws=ff[0]
                    edges=[ii*scalefactor for ii in map(float,ff[-1].split('~'))]
                    if spws=='*': spws,edges=range(len(pipeline.firstchanfreq[i])),[edges for uu in range(len(pipeline.firstchanfreq[i]))]
                    else: spws,edges=[spws,],[edges,]
                    for ss in spws:
                        if min(edges[ss][1],pipeline.lastchanfreq[i][ss])-max(edges[ss][0],pipeline.firstchanfreq[i][ss])>0: found_valid_data=1
                if not found_valid_data: meerkathi.log.warn('The following channel selection has been made in the flag_spw module of the flagging worker: "{1:s}". This selection would result in no valid data in {0:s}. This would lead to the FATAL error "No valid SPW & Chan combination found" in CASA/FLAGDATA. To avoid this error the corresponding cab {2:s} will not be added to the Stimela recipe of the flagging worker.'.format(msname,flagspwselection,step))

            if found_valid_data or not config['flag_spw'].get('ensure_valid_selection',False):
                recipe.add('cab/casa_flagdata','flagspw_{:d}'.format(i),
                    {
                      "vis"     : msname,
                      "mode"    : 'manual',
                      "spw"     : flagspwselection,
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}::Flag out channels ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'flag_time'):
            step = 'flag_time_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata','flagtime_{:d}'.format(i),
                {
                  "vis"       : msname,
                  "mode"      : 'manual',
                  "timerange" : config['flag_time']['timerange'],
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}::Flag out channels ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'flag_scan'):
            step = 'flag_scan_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata', step,
                {
                  "vis"     : msname,
                  "mode"    : 'manual',
                  "scan"    : config['flag_scan']['scans'],
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}::Flag out channels ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'flag_antennas'):
            step = 'flag_antennas_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata', step,
                {
                  "vis"         : msname,
                  "mode"        : 'manual',
                  "antenna"     : config['flag_antennas']['antennas']
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Flagging bad antennas ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'static_mask'):
            step = 'static_mask_{0:d}'.format(i)
            recipe.add('cab/rfimasker', step,
                {
                    "msname"    : msname,
                    "mask"      : config['static_mask']['mask'],
                    "accumulation_mode" : 'or',
                    "uvrange"   : config['static_mask'].get('uvrange', "''"),
                    "memory"    : 4096,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Apply static mask ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'autoflag_rfi'):
            step = 'autoflag_{0:d}'.format(i)
            if config['autoflag_rfi'].get('fields', 'auto') != 'auto' and \
               not set(config['autoflag_rfi'].get('fields', 'auto').split(',')) <= set(['gcal', 'bpcal', 'target']):
                raise KeyError("autoflag rfi can only be 'auto' or be a combination of 'gcal', 'bpcal' or 'target'")
            if config['autoflag_rfi'].get('calibrator_fields', 'auto') != 'auto' and \
               not set(config['autoflag_rfi'].get('calibrator_fields', 'auto').split(',')) <= set(['gcal', 'bpcal']):
                raise KeyError("autoflag rfi fields can only be 'auto' or be a combination of 'gcal', 'bpcal'")
            def_fields = ','.join([pipeline.bpcal_id[i], pipeline.gcal_id[i]])
            fields = def_fields if config['autoflag_rfi'].get('fields', 'auto') == 'auto' else \
                     ",".join([getattr(pipeline, key + "_id")[i] for key in config['autoflag_rfi'].get('fields').split(',')])

            recipe.add('cab/autoflagger', step,
                {
                  "msname"      : msname,
                  "column"      : config['autoflag_rfi'].get('column', 'DATA'),
                  # flag the calibrators for RFI and apply to target
                  "fields"      : fields,
                  "strategy"    : config['autoflag_rfi']['strategy'],
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Aoflagger flagging pass ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'flagging_summary'):
            step = 'flagging_summary_flagging_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata', step,
                {
                  "vis"         : msname,
                  "mode"        : 'summary',
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))

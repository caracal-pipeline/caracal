NAME = 'Pre-calibration flagging'

def worker(pipeline, recipe, config):
    for i in range(pipeline.nobs):
        msname = pipeline.msnames[i]
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
            step = 'flag_spw_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata','flagspw_{:d}'.format(i),
                {
                  "vis"     : msname,
                  "mode"    : 'manual',
                  "spw"     : config['flag_spw']['channels'],
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
            def_fields = ','.join([pipeline.bpcal_id[i], pipeline.gcal_id[i], pipeline.target_id[i]])
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

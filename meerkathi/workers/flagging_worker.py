NAME = 'Pre-calibration flagging'

def worker(pipeline, recipe, config):
    for i in range(pipeline.nobs):
        msname = pipeline.msnames[i]

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
            recipe.add('cab/casa_flagdata','flagspw_{:d}'.format(i),
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
                    "memory"    : 1024,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Apply static mask ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'autoflag'):
            step = 'autoflag_{0:d}'.format(i)
            recipe.add('cab/autoflagger', step,
                {
                  "msname"      : msname,
                  "column"      : config['autoflag'].get('coumn', 'DATA'),
                  "fields"      : config['autoflag'].get('fields', pipeline.bpcal[i]),
                  "strategy"    : config['autoflag']['strategy'],
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Aoflagger flagging pass ms={1:s}'.format(step, msname))

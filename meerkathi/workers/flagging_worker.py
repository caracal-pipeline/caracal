NAME = 'Pre-calibration flagging'

def worker(pipeline, recipe, config):
    steps = []
    for i in range(pipeline.nobs):
        msname = pipeline.msnames[i]
        
        if config['flag_autocorr']['enable']:
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
            steps.append(step)

        if config['flag_milkyway']['enable']:
            step = 'flag_milkyway_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata','flagmw_{:d}'.format(i),
                {
                  "vis"     : msname,
                  "mode"    : 'manual',
                  "spw"     : config['flag_milkyway']['channels'],
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}::Flag out channels with HI emission from Milky Way ms={1:s}'.format(step, msname))
            steps.append(step)

        if config['flag_antennas']['enable']:
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
            steps.append(step)

        if config['flag_autocorr']['enable']:
            step = 'flag_autocorr_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata', step,
                {
                  "vis"         : msname,
                  "mode"        : 'manual',
                  "autocorr"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Flagging auto-correations ms={1:s}'.format(step, msname))
            steps.append(step)


        if config['static_mask']['enable']:
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
            steps.append(step)

        if config['autoflag']['enable']:
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
            steps.append(step)

    return steps

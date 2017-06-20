NAME = 'Pre-calibration flagging'

def worker(pipeline, config):

    for i in range(pipeline.nobs):
        msname = pipeline.msnames[i]
        
        if config['flag_autocorr']['enable']:
            step = 'flag_autocorr_{0:d}'.format(i)
            pipeline.preflag.add('cab/casa_flagdata', step,
                {
                  "vis"         : msname,
                  "mode"        : 'manual',
                  "autocorr"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Flag auto-correlations ms={1:s}'.format(step, msname))
            steps.append(step)

        if config['flag_milky_way']['enable']:
            step = 'flag_milky_way_{0:d}'.format(i)
            pipeline.preflag.add('cab/casa_flagdata','flagmw_{:d}'.format(i),
                {
                  "vis"     : msname,
                  "mode"    : 'manual',
                  "spw"     : config['flag_milky_way']['channels'],
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}::Flag out channels with HI emission from Milky Way ms={1:s}'.format(step, msname))
            steps.append(step)

        if config['autoflag']['enable']:
            step = 'autoflag_{0:d}'.format(i)
            pipeline.preflag.add('cab/autoflagger', step,
                {
                  "msname"      : msname,
                  "column"      : 'DATA',
                  "strategy"    : config['autoflag']['aoflag_strat1'],
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Aoflagger flagging pass 1 ms={1:s}'.format(step, msname))
            steps.append(step)

    return steps

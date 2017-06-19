def preflag(pipeline, config, label):

    for i in range(pipeline.nobs):
        msname = pipeline.msnames[i]
        
        if config['flag_autocorr']:
            step = 'flag_autocorr_{0:s}_{1:d}'.format(label, i)
            pipeline.preflag.add('cab/casa_flagdata', step,
                {
                  "vis"           :   msname,
                  "mode"          :   'manual',
                  "autocorr"      :   True,
                },
                input=INPUT,
                output=OUTPUT,
                label='{0:s}:: Flag out channels with emission from Milky Way'.format(step, msname))
            steps.append(step)

        if config['flag_milky_way']:
            step = 'flag_milky_way_{0:s}_{1:d}'.format(label, i)
            pipeline.preflag.add('cab/casa_flagdata','flagmw_{:d}'.format(i),
                {
                  "vis"           :   msname,
                  "mode"          :   'manual',
                  "spw"           :   config,
                },
                input=INPUT,
                output=OUTPUT,
                label='{0:s}::Flag out channels with HI emission from Milky Way'.format(step, msname))
            steps.append(step)

        if config['flag_milky_way']:
            step = 'flag_milky_way_{0:s}_{1:d}'.format(label, i)
            pipeline.preflag.add('cab/autoflagger', step,
                {
                  "msname"       :   msname,
                  "column"       :   'DATA',
                  "strategy"     :   config['aoflag_strat1'],
                },
                input=INPUT,
                output=OUTPUT,
                label='{:s}:: Aoflagger flagging pass 1'.format(step))
            steps.append(step)

    return steps

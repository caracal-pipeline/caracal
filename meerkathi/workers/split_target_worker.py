import os
import sys
import meerkathi
import stimela.dismissable as sdm

NAME = 'Split and average target data'

def worker(pipeline, recipe, config):
    label = config['label']
    pipeline.set_cal_msnames(label)
    if pipeline.enable_task(config, 'hires_split'):
       print("Setting Full Resolution Data Names...")
       hires_label = config['hires_split'].get('hires_label', 'hires')
       pipeline.set_hires_msnames(hires_label)
                                   

    for i in range(pipeline.nobs):
        msname = pipeline.msnames[i]
        target = pipeline.target[0]
        prefix = pipeline.prefixes[i]
        tms = pipeline.cal_msnames[i]
        flagv = tms + '.flagversions'
        if pipeline.enable_task(config, 'split_target'):
            step = 'split_target_{:d}'.format(i)
            if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, tms)) or \
                   os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, flagv)):

                os.system('rm -rf {0:s}/{1:s} {0:s}/{2:s}'.format(pipeline.msdir, tms, flagv))

            recipe.add('cab/casa_split', step,
                {
                    "vis"           : msname,
                    "outputvis"     : tms,
                    "timebin"       : config['split_target'].get('time_average', ''),
                    "width"         : config['split_target'].get('freq_average', 1),
                    "spw"           : config['split_target'].get('spw', ''),
                    "datacolumn"    : config['split_target'].get('column', 'corrected'),
                    "correlation"   : config['split_target'].get('correlation', ''),
                    "field"         : str(target),
                    "keepflags"     : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Split and average data ms={1:s}'.format(step, msname))
            
        if pipeline.enable_task(config, 'hires_split'):
            fms = pipeline.hires_msnames[i]
            flagf = fms + '.flagversions'
     
            if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, fms)) or \
                       os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, flagf)):
                   os.system('rm -rf {0:s}/{1:s} {0:s}/{2:s}'.format(pipeline.msdir, fms, flagv))   #Delet the previous split ms and flagversions.

            recipe.add('cab/casa_split', step,
                {
                    "vis"           : msname,
                    "outputvis"     : fms,
                    "timebin"       : sdm.dismissable(config['hires_split'].get('hires_tav', '')),
                    "width"         : sdm.dismissable(config['hires_split'].get('hires_fav', 1)),
                    "spw"           : sdm.dismissable(config['hires_split'].get('hires_spw', '')),
                    "datacolumn"    : 'corrected',
                    "correlation"   : config['split_target'].get('correlation', ''),
                    "field"         : str(target),
                    "keepflags"     : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Split and make a high res copy of data ms={1:s}'.format(step, msname))

            pipeline.hires_spw = sdm.dismissable(config['split_target'].get('hires_spw', ''))                ##Need to add this to the init file.



 
        if pipeline.enable_task(config, 'prepms'):
            step = 'prepms_{:d}'.format(i)
            recipe.add('cab/msutils', step,
                {
                  "msname"  : tms,
                  "command" : 'prep' ,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Add BITFLAG column ms={1:s}'.format(step, tms))

        if pipeline.enable_task(config, 'changecentre'):
            if config['changecentre'].get('ra','') == '' or config['changecentre'].get('dec','') == '':
                meerkathi.log.error('Wrong format for RA and/or Dec you want to change to. Check your settings of split_target:changecentre:ra and split_target:changecentre:dec')
                meerkathi.log.error('Current settings for ra,dec are {0:s},{1:s}'.format(config['changecentre'].get('ra',''),config['changecentre'].get('dec','')))
                sys.exit(1)
            step = 'changecentre_{:d}'.format(i)
            recipe.add('cab/casa_fixvis', step,
                {
                  "msname"  : tms,
                  "outputvis": tms,
                  "phasecenter" : 'J2000 {0:s} {1:s}'.format(config['changecentre'].get('ra',''),config['changecentre'].get('dec','')) ,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Change phase centre ms={1:s}'.format(step, tms))


        if (pipeline.enable_task(config, 'changecentre') and pipeline.enable_task(config, 'hires_split')):
            if config['changecentre'].get('ra','') == '' or config['changecentre'].get('dec','') == '':
                meerkathi.log.error('Wrong format for RA and/or Dec you want to change to. Check your settings of split_target:changecentre:ra and split_target:changecentre:dec')
                meerkathi.log.error('Current settings for ra,dec are {0:s},{1:s}'.format(config['changecentre'].get('ra',''),config['changecentre'].get('dec','')))
                sys.exit(1)
            step = 'changecentre_{:d}_hires'.format(i)
            recipe.add('cab/casa_fixvis', step,
                {
                  "msname"  : fms,
                  "outputvis": fms,
                  "phasecenter" : 'J2000 {0:s} {1:s}'.format(config['changecentre'].get('ra',''),config['changecentre'].get('dec','')) ,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Change phase centre ms={1:s}'.format(step, fms))


        if pipeline.enable_task(config, 'obsinfo'):
            if config['obsinfo'].get('listobs', True):
                step = 'listobs_{:d}'.format(i)
                recipe.add('cab/casa_listobs', step,
                    {
                      "vis"         : tms,
                      "listfile"    : '{0:s}-{1:s}-obsinfo.txt'.format(prefix, label),
                      "overwrite"   : True,
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Get observation information ms={1:s}'.format(step, tms))
    
            if config['obsinfo'].get('summary_json', True):
                 step = 'summary_json_{:d}'.format(i)
                 recipe.add('cab/msutils', step,
                    {
                      "msname"      : tms,
                      "command"     : 'summary',
                      "outfile"     : '{0:s}-{1:s}-obsinfo.json'.format(prefix, label),
                    },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Get observation information as a json file ms={1:s}'.format(step, tms))

        if (pipeline.enable_task(config, 'obsinfo') and pipeline.enable_task(config, 'hires_split')):
            if config['obsinfo'].get('listobs', True):
                step = 'listobs_{:d}_hires'.format(i)
                recipe.add('cab/casa_listobs', step,
                    {
                      "vis"         : fms,
                      "listfile"    : '{0:s}-{1:s}-obsinfo.txt'.format(prefix, hires_label),
                      "overwrite"   : True,
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Get observation information ms={1:s}'.format(step, tms))

            if (config['obsinfo'].get('summary_json', True) and pipeline.enable_task(config, 'hires_split')):
                 step = 'summary_json_{:d}_hires'.format(i)
                 recipe.add('cab/msutils', step,
                    {
                      "msname"      : fms,
                      "command"     : 'summary',
                      "outfile"     : '{0:s}-{1:s}-obsinfo.json'.format(prefix, hires_label),
                    },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Get observation information as a json file ms={1:s}'.format(step, fms))

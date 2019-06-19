import os
import sys
import meerkathi
import stimela.dismissable as sdm
import getpass
import stimela.recipe as stimela
import re

NAME = 'Split and average target data'
# Rules for interpolation mode to use when applying calibration solutions
applycal_interp_rules = {
   'target'   :  {
                  'delay_cal'      : 'linear',
                  'bp_cal'         : 'linear',
                  'transfer_fluxscale': 'linear',
                  'gain_cal_gain'  : 'linear',
                 },
}

get_dir_path = lambda string,pipeline : string.split(pipeline.output)[1][1:]

table_suffix = {
    "delay_cal"             : 'K0',
    "bp_cal"                : 'B0',
    "gain_cal_gain"         : 'G0',
    "gain_cal_flux"         : 'G0',
    "transfer_fluxscale"    : 'F0',
}

# Check if field was specified as known key, else return the
# same value.

def worker(pipeline, recipe, config):

    def get_field(field):
            """
                gets field ids parsed previously in the pipeline
                params:
                    field: list of ids or comma-seperated list of ids where
                           ids are in bpcal, gcal, target, fcal or an actual field name
            """
            return ','.join(filter(lambda s: s != "", map(lambda x: ','.join(getattr(pipeline, x)[i].split(',')
                                                if isinstance(getattr(pipeline, x)[i], str) and getattr(pipeline, x)[i] != "" else getattr(pipeline, x)[i])
                                              if x in ['bpcal', 'gcal', 'target', 'fcal', 'xcal']
                                              else x.split(','),
                                field.split(',') if isinstance(field, str) else field)))

    def get_gain_field(applyme, applyto=None):
            if applyme == 'delay_cal':
                return get_field(config['split_target']['otfcal']['apply_delay_cal'].get('field', ['bpcal','gcal','xcal']))
            if applyme == 'bp_cal':
                return get_field(config['split_target']['otfcal']['apply_bp_cal'].get('field', ['bpcal']))
            if applyme == 'gain_cal_flux':
                return get_field('fcal')
            if applyme == 'gain_cal_gain':
                return get_field('gcal')
            if applyme == 'transfer_fluxscale':
                return get_field('gcal')

    label_in = config['label_in']
    label_out = config['label_out']

    pipeline.set_hires_msnames(label_in)

    for i in range(pipeline.nobs):

        target_ls = pipeline.target[i].split(',')
        for field in target_ls:                                                     #replace non standard characters in field names with '_'
            field = re.sub('[^0-9a-zA-Z]+', '_', field)

        prefix = pipeline.prefixes[i]

        if pipeline.enable_task(config['split_target']	, 'otfcal'):                #write calibration library file for OTF cal in split_target_worker.py
          
    	    uname = getpass.getuser()
    	    gaintablelist,gainfieldlist,interplist = [],[],[]
            callabel = config['split_target']['otfcal'].get('callabel', '')
            calprefix = '{0:s}-{1:s}'.format(prefix, callabel)

    	    for applyme in 'delay_cal bp_cal gain_cal_flux gain_cal_gain transfer_fluxscale'.split():
                #meerkathi.log.info((applyme,pipeline.enable_task(config, 'apply_'+applyme)))
                if not pipeline.enable_task(config['split_target']['otfcal'], 'apply_'+applyme):
                   continue
                suffix = table_suffix[applyme]
                interp = applycal_interp_rules['target'][applyme]
                gainfield = get_gain_field(applyme, 'target')
                gaintablelist.append('{0:s}/{1:s}.{2:s}'.format(get_dir_path(pipeline.caltables, pipeline), calprefix, suffix))
                gainfieldlist.append(gainfield)
                interplist.append(interp)
            callib = 'callib_{0:s}_{1:s}.txt'.format(prefix,callabel)
            with open(os.path.join(pipeline.output, callib), 'w') as stdw:
                for j in range(len(gaintablelist)):
                    stdw.write('caltable="{0:s}/{1:s}"'.format(stimela.CONT_IO[recipe.JOB_TYPE]["output"], gaintablelist[j]))
                    stdw.write(' calwt=False')
                    stdw.write(' tinterp=\''+str(interplist[j])+'\'')
                    stdw.write(' finterp=\'linear\'')
                    stdw.write(' fldmap=\'' +str(gainfieldlist[j])+'\'\n')

            docallib = True
        else: docallib = False

        for field in target_ls:

            fms = pipeline.hires_msnames[i]
            tms = '{0:s}_{1:s}_{2:s}.ms'.format(fms[:-3],field,label_out)
            flagv = tms+'.flagversions'

            if pipeline.enable_task(config, 'split_target'):
                step = 'split_target_{:d}'.format(i)
                if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, tms)) or \
                       os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, flagv)):

                    os.system('rm -rf {0:s}/{1:s} {0:s}/{2:s}'.format(pipeline.msdir, tms, flagv))

                recipe.add('cab/casa_mstransform', step,
                    {
                        "vis"           : fms,
                        "outputvis"     : tms,
                        "timeaverage"   : True if (config['split_target'].get('time_average', '') != '' and config['split_target'].get('time_average', '') != '0s') else False,
                        "timebin"       : config['split_target'].get('time_average', ''),
                        "chanaverage"   : True if config['split_target'].get('freq_average', 1) > 1 else False,
                        "chanbin"       : config['split_target'].get('freq_average', 1),
                        "spw"           : config['split_target'].get('spw', ''),
                        "datacolumn"    : config['split_target'].get('column', 'data'),
                        "correlation"   : config['split_target'].get('correlation', ''),
                        "field"         : field,
                        "keepflags"     : True,
                        "docallib"      : docallib,
                        "callib"        : sdm.dismissable(callib if pipeline.enable_task(config['split_target']	, 'otfcal') else None),
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Split and average data ms={1:s}'.format(step, fms))


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

            if pipeline.enable_task(config, 'obsinfo'):
                if (config['obsinfo'].get('listobs', True)):
                    if pipeline.enable_task(config, 'split_target'):
                        listfile = '{0:s}-obsinfo.txt'.format(tms[:-3])
                    else: listfile = '{0:s}-obsinfo.txt'.format(fms[:-3])
                
                    step = 'listobs_{:d}'.format(i)
                    recipe.add('cab/casa_listobs', step,
                        {
                          "vis"         : tms,
                          "listfile"    : listfile,
                          "overwrite"   : True,
                        },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Get observation information ms={1:s}'.format(step, tms))
    
                if (config['obsinfo'].get('summary_json', True)):
                    if pipeline.enable_task(config, 'split_target'):
                        listfile = '{0:s}-obsinfo.json'.format(tms[:-3])
                    else: listfile = '{0:s}-obsinfo.json'.format(fms[:-3])
                
                    step = 'summary_json_{:d}'.format(i)
                    recipe.add('cab/msutils', step,
                        {
                          "msname"      : tms,
                          "command"     : 'summary',
                          "display"     : False,
                          "outfile"     : listfile
                        },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Get observation information as a json file ms={1:s}'.format(step, tms))

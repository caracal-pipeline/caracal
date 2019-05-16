NAME = 'Pre-calibration flagging'

import sys
import meerkathi
import yaml
import stimela.dismissable as sdm
from meerkathi.dispatch_crew import utils

def worker(pipeline, recipe, config):
    if pipeline.virtconcat:
        msnames = [pipeline.vmsname]
        prefixes = [pipeline.prefix]
        nobs = 1
    else:
        msnames = pipeline.msnames
        prefixes = pipeline.prefixes
        nobs = pipeline.nobs
    if config['label']:
        msnames=[mm.replace('.ms','-{0:s}.ms'.format(config['label'])) for mm in msnames]
        prefixes=['{0:s}-{1:s}'.format(prefix, config['label']) for prefix in prefixes]
    if config.get('hires_flag'): 
        print("Flagging Full Resolution Data")
        if config['label']:
            msnames.append(next('{0:s}'.format(mm.replace(config['label'], config['hires_label'])) for mm in msnames))
            prefixes.append(next('{0:s}'.format(prefix.replace(config['label'], config['hires_label'])) for prefix in prefixes))
        else:
            msnames = [mm.replace('.ms','-{0:s}.ms'.format(config['hires_label'])) for mm in msnames]
            prefixes = ['{0:s}-{1:s}'.format(prefix, config['hires_label']) for prefix in prefixes]
        nobs=len(msnames)

    def get_field(field):
        """
            gets field ids parsed previously in the pipeline 
            params:
                field: list of ids or comma-seperated list of ids where
                       ids are in bpcal, gcal, target, fcal or an actual field name
        """
        return ','.join(filter(lambda s: s != "", map(lambda x: ','.join(getattr(pipeline, x)[p_nob].split(',')
                                            if isinstance(getattr(pipeline, x)[p_nob], str) and getattr(pipeline, x)[p_nob] != "" else getattr(pipeline, x)[p_nob])
                                          if x in ['bpcal', 'gcal', 'target', 'fcal', 'xcal']
                                          else x.split(','),
                            field.split(',') if isinstance(field, str) else field)))


    for i in range(nobs):
        msname = msnames[i]
        prefix = prefixes[i]
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, prefix)
 
        # Since the nobs are now equal to the length of the msnames if hires flagging is activated
        # It is important to have a p_nob that will look-up sources based on the original unique ms names in `pipeline`
        # Note: Flagging is still perfomed on all msnames using index i
        if config['label'] and config['hires_flag']:
            p_prefix = pipeline.prefixes
            if config['label'] in prefix:
                p_nob = p_prefix.index(prefix.replace('-{0:s}'.format(config['label']), ''))
            elif config['hires_label'] in prefix:
                p_nob = p_prefix.index(prefix.replace('-{0:s}'.format(config['hires_label']), ''))
        else:
            p_nob = i


        # flag antennas automatically based on drifts in the scan average of the 
        # auto correlation spectra per field. This doesn't strictly require any calibration. It is also
        # not field structure dependent, since it is just based on the DC of the field
        # Compares scan to median power of scans per field per channel
        # Also compares antenna to median of the array per scan per field per channel
        # This should catch any antenna with severe temperature problems
        if pipeline.enable_task(config, 'autoflag_autocorr_powerspectra'):
            step = 'autoflag_autocorr_spectra_{0:d}'.format(i)
            def_fields = ",".join(map(str,utils.get_field_id(msinfo, get_field("bpcal,gcal,target,xcal").split(","))))
            def_calfields = ",".join(map(str, utils.get_field_id(msinfo, get_field("bpcal,gcal,xcal").split(","))))
            if config['autoflag_autocorr_powerspectra'].get('fields', 'auto') != 'auto' and \
               not set(config['autoflag_autocorr_powerspectra'].get('fields', 'auto').split(',')) <= set(['gcal', 'bpcal', 'fcal', 'target']):
                raise KeyError("autoflag on powerspectra fields can only be 'auto' or be a combination of 'gcal', 'bpcal', 'fcal' or 'target'")
            if config['autoflag_autocorr_powerspectra'].get('calibrator_fields', 'auto') != 'auto' and \
               not set(config['autoflag_autocorr_powerspectra'].get('calibrator_fields', 'auto').split(',')) <= set(['gcal', 'bpcal', 'fcal']):
                raise KeyError("autoflag on powerspectra calibrator fields can only be 'auto' or be a combination of 'gcal', 'bpcal', 'fcal'")

            fields = def_fields if config['autoflag_autocorr_powerspectra'].get('fields', 'auto') == 'auto' else \
                     ",".join([getattr(pipeline, key + "_id")[p_nob][0] for key in config['autoflag_autocorr_powerspectra'].get('fields').split(',')])
            calfields = def_calfields if config['autoflag_autocorr_powerspectra'].get('calibrator_fields', 'auto') == 'auto' else \
                     ",".join([getattr(pipeline, key + "_id")[p_nob][0] for key in config['autoflag_autocorr_powerspectra'].get('calibrator_fields').split(',')])

            
            fields = ",".join(set(fields.split(",")))
            calfields = ",".join(set(calfields.split(",")))

            recipe.add("cab/politsiyakat_autocorr_amp", step,
                {
                    "msname": msname,
                    "field": fields,
                    "cal_field": calfields,
                    "scan_to_scan_threshold": config["autoflag_autocorr_powerspectra"].get("scan_to_scan_threshold",3),
                    "antenna_to_group_threshold": config["autoflag_autocorr_powerspectra"].get("antenna_to_group_threshold",5),

                    "dpi": 300,
                    "plot_size": 6,
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


        if pipeline.enable_task(config, 'flag_shadow'):
            if config['flag_shadow'].get('include_full_mk64',False):
                msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, prefix)
                addantennafile = '{0:s}/mk64.txt'.format(pipeline.input)
                with open(msinfo, 'r') as stdr: subarray = yaml.load(stdr)['ANT']['NAME']
                idleants=open(addantennafile,'r').readlines()
                for aa in subarray:
                    for kk in range(len(idleants)):
                        if aa in idleants[kk]:
                            del(idleants[kk:kk+3])
                            break
                addantennafile='idleants.txt'
                with open('{0:s}/{1:s}'.format(pipeline.input,addantennafile),'w') as ia:
                    for aa in idleants: ia.write(aa)
                addantennafile+=':input'
            else: addantennafile = None
            step = 'flag_shadow_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata', step,
                {
                  "vis"         : msname,
                  "mode"        : 'shadow',
                  "tolerance"   : config['flag_shadow'].get('tolerance',0),
                  "addantenna"  : addantennafile,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Flag shadowed antennas ms={1:s}'.format(step, msname))

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
                    else: spws='*'
                    edges=[ii*scalefactor for ii in map(float,ff[-1].split('~'))]
                    if spws=='*': spws=range(len(pipeline.firstchanfreq[i]))
                    elif '~' in spws: spws=range(int(spws.split('~')[0]),int(spws.split('~')[1])+1)
                    else: spws=[spws,]
                    edges=[edges for uu in range(len(spws))]
                    for ss in spws:
                        if ss<len(pipeline.lastchanfreq[i]) and min(edges[ss][1],pipeline.lastchanfreq[i][ss])-max(edges[ss][0],pipeline.firstchanfreq[i][ss])>0: found_valid_data=1
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
                  "antenna"     : config['flag_antennas']['antennas'],
                  "timerange"   : config['flag_antennas'].get('timerange',""),
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
               not set(config['autoflag_rfi'].get('fields', 'auto').split(',')) <= set(['xcal', 'gcal', 'bpcal', 'target', 'fcal']):
                raise KeyError("autoflag rfi can only be 'auto' or be a combination of 'xcal', 'gcal', 'fcal', 'bpcal' or 'target'")
            if config['autoflag_rfi'].get('calibrator_fields', 'auto') != 'auto' and \
               not set(config['autoflag_rfi'].get('calibrator_fields', 'auto').split(',')) <= set(['xcal', 'gcal', 'bpcal', 'fcal']):
                raise KeyError("autoflag rfi fields can only be 'auto' or be a combination of 'xcal', 'gcal', 'bpcal', 'fcal'")

            if config['autoflag_rfi'].get('fields', 'auto') is 'auto':
                fields = ','.join([pipeline.bpcal_id[p_nob], pipeline.gcal_id[p_nob]])
            else:
                fields = ",".join(map(str, utils.get_field_id(msinfo, get_field(config['autoflag_rfi'].get('fields')).split(","))))

            # Make sure no field IDs are duplicated
            fields = ",".join(set(fields.split(",")))

            recipe.add('cab/autoflagger', step,
                {
                  "msname"      : msname,
                  "column"      : config['autoflag_rfi'].get('column', 'DATA'),
                  # flag the calibrators for RFI and apply to target
                  "fields"      : fields,
                  #"bands"       : config['autoflag_rfi'].get('bands', "0"),
                  "strategy"    : config['autoflag_rfi']['strategy'],
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Aoflagger flagging pass ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'rfinder'):
            step = 'rfinder'
            recipe.add('cab/rfinder', 'rfinder',
                {
                  "msname"             : msname,
                  "field"              : config[step].get('field', 1),
                  "plot_noise"         : "noise",
                  "RFInder_mode"       : "use_flags",
                  "outlabel"           : '_{}'.format(i),  # The output will be rfi_<pol>_<outlabel>
                  "polarization"       : config[step].get('polarization', 'Q'),
                  "spw_width"          : config[step].get('spw_width', 10),
                  "time_step"          : config[step].get('time_step', 10),
                  "time_enable"        : config[step].get('time_enable', True),
                  "spw_enable"         : config[step].get('spw_enable', True),
                  "1d_gif"             : config[step].get('time_enable', True),
                  "2d_gif"             : config[step].get('time_enable', True),
                  "altaz_gif"          : config[step].get('spw_enable', True),
                  "movies_in_report"   : config[step].get('time_enable', True) or config.get('spw_enable', True)
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Investigate presence of rfi in ms={1:s}'.format(step, msname))

        if pipeline.enable_task(config, 'flagging_summary'):
            step = 'flagging_summary_flagging_{0:d}_{1:s}'.format(i, config.get('label', '0gc'))
            recipe.add('cab/casa_flagdata', step,
                {
                  "vis"         : msname,
                  "mode"        : 'summary',
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}-{1:s}:: Flagging summary  ms={2:s}'.format(step, config.get('label', '0gc'), msname))

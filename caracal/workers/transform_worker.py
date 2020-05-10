# -*- coding: future_fstrings -*-
import os
import sys
import caracal
import stimela.dismissable as sdm
import getpass
import stimela.recipe
import re
import json
import numpy as np
from caracal.dispatch_crew import utils
from caracal.workers.utils import manage_fields as manfields
from caracal.workers.utils import manage_flagsets as manflags

NAME = 'Transform Data by Splitting/Average/Applying calibration'
LABEL = 'transform'

# Rules for interpolation mode to use when applying calibration solutions
applycal_interp_rules = {
    'target':  {
        'delay_cal': 'linear',
        'bp_cal': 'linear',
                  'transfer_fluxscale': 'linear',
                  'gain_cal_gain': 'linear',
    },
}


def get_dir_path(string, pipeline): return string.split(pipeline.output)[1][1:]


table_suffix = {
    "delay_cal": 'K0',
    "bp_cal": 'B0',
    "gain_cal_gain": 'G0',
    "gain_cal_flux": 'G0',
    "transfer_fluxscale": 'F0',
}

# Check if field was specified as known key, else return the
# same value.


def filter_name(string):
    string = string.replace('+', '_p_')
    return re.sub('[^0-9a-zA-Z]', '_', string)


def worker(pipeline, recipe, config):

    wname = pipeline.CURRENT_WORKER
    flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
    flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
    label_in = config['label_in']
    label_out = config['label_out']

    pipeline.set_hires_msnames(label_in)

    for i in range(pipeline.nobs):

        prefix = pipeline.prefixes[i]
        msname = pipeline.msnames[i][:-3]
        field_to_split = config['split_field']['field'].split(',')

        if 'calibrators' in field_to_split:
            field_to_split = ['fcal','bpcal','gcal']

        for fd in field_to_split:
            if fd not in ['target','fcal','bpcal','gcal']:
                raise ValueError("Eligible values for 'field': 'target', 'calibrators', 'fcal', 'bpcal' or 'gcal'. "\
                                 "User selected: {}".format(field_to_split))

        if any(x in field_to_split for x in ['fcal','bpcal','gcal']):
           calfields = []
           for fd in field_to_split:
               for elem in getattr(pipeline, fd)[i]:
                   calfields.append(elem)
           target_ls = [','.join(np.unique(np.array(calfields))),]
        else:
           target_ls = pipeline.target[i]

        #use existing calibration library if user gives one
        if pipeline.enable_task(config['split_field'], 'otfcal') and config['split_field']['otfcal']['callib']:
            callib = 'caltables/callibs/{}'.format(config['split_field']['otfcal']['callib'])

            if not os.path.exists(os.path.join(pipeline.output,callib)):
                raise IOError(
                    "Callib file {0:s} does not exist. Please check that it is where it should be.".format(callib))

            docallib = True

            if config['split_field']['col'] != 'corrected':
                caracal.log.info("Datacolumn was set to '{}'. by the user." \
                                   "Will be changed to 'corrected' for OTF calibration to work.".format(config['split_field']['col']))
            dcol = 'corrected'

        # write calibration library file for OTF cal
        elif pipeline.enable_task(config['split_field'], 'otfcal'):
            caltablelist, gainfieldlist, interplist = [], [], []
            calprefix = '{0:s}-{1:s}'.format(prefix,
                                             config['split_field']['otfcal']['label_cal'])
            callib = 'caltables/callibs/callib_{1:s}.txt'.format(prefix,calprefix)

            with open(os.path.join('{}/callibs'.format(pipeline.caltables),
                                  'callib_{0:s}-{1:s}.json'.format(prefix,
                                  config['split_field']['otfcal']['label_cal']))) as f:
                callib_dict = json.load(f)

            for applyme in callib_dict: 
                caltablelist.append(callib_dict[applyme]['caltable'])
                gainfieldlist.append(callib_dict[applyme]['fldmap'])
                interplist.append(callib_dict[applyme]['interp'])

            with open(os.path.join(pipeline.output, callib), 'w') as stdw:
                for j in range(len(caltablelist)):
                    stdw.write('caltable="{0:s}/{1:s}/{2:s}"'.format(
                        stimela.recipe.CONT_IO["output"], 'caltables',  caltablelist[j]))
                    stdw.write(' calwt=False')
                    stdw.write(' tinterp=\''+str(interplist[j])+'\'')
                    stdw.write(' finterp=\'linear\'')
                    stdw.write(' fldmap=\'' + str(gainfieldlist[j])+'\'')
                    stdw.write(' spwmap=0\n')

            docallib = True
            if config['split_field']['col'] != 'corrected':
                caracal.log.info("Datacolumn was set to '{}'. by the user." \
                                   "Will be changed to 'corrected' for OTF calibration to work.".format(config['split_field']['col']))
            dcol = 'corrected'

        else:
            docallib = False
            dcol = config['split_field']['col']

        target_iter=0
        for target in target_ls:
            field = utils.filter_name(target)

            if config['split_field']['field'] == 'target':
                fms = pipeline.hires_msnames[i] if label_in == \
                       '' else '{0:s}-{1:s}_{2:s}.ms'.format(msname, field, label_in)
                tms = '{0:s}-{1:s}_{2:s}.ms'.format(
                       msname, field, label_out)
            else:
                fms = pipeline.hires_msnames[i] if label_in == \
                       '' else '{0:s}_{1:s}.ms'.format(msname, label_in)
                tms = '{0:s}_{1:s}.ms'.format(
                       msname, label_out)

            # Rewind flags
            available_flagversions = manflags.get_flags(pipeline, fms)
            if config['rewind_flags']['enable']:
                version = config['rewind_flags']['version']
                if version in available_flagversions:
                    substep = 'rewind-{0:s}-ms{1:d}'.format(version, target_iter)
                    manflags.restore_cflags(pipeline, recipe, version, fms, cab_name=substep)
                    if available_flagversions[-1] != version:
                        substep = 'delete-flag_versions-after-{0:s}-ms{1:d}'.format(version, target_iter)
                        manflags.delete_cflags(pipeline, recipe,
                            available_flagversions[available_flagversions.index(version)+1],
                            fms, cab_name=substep)
                else:
                    manflags.conflict('rewind_to_non_existing', pipeline, wname, fms,
                        config, flags_before_worker, flags_after_worker)

            flagv = tms+'.flagversions'

            if pipeline.enable_task(config, 'split_field'):
                step = 'split_field-ms{0:d}-{1:d}'.format(i,target_iter)
                # If the output of this run of mstransform exists, delete it first
                if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, tms)) or \
                        os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, flagv)):
                    os.system(
                        'rm -rf {0:s}/{1:s} {0:s}/{2:s}'.format(pipeline.msdir, tms, flagv))

                recipe.add('cab/casa_mstransform', step,
                           {
                               "vis": fms,
                               "outputvis": tms,
                               "timeaverage": True if (config['split_field']['time_avg'] != '' and config['split_field']['time_avg'] != '0s') else False,
                               "timebin": config['split_field']['time_avg'],
                               "chanaverage": True if config['split_field']['chan_avg'] > 1 else False,
                               "chanbin": config['split_field']['chan_avg'],
                               "spw": config['split_field']['spw'],
                               "datacolumn": dcol,
                               "correlation": config['split_field']['correlation'],
                               "usewtspectrum": config['split_field']['create_specweights'],
                               "field": target,
                               "keepflags": True,
                               "docallib": docallib,
                               "callib": sdm.dismissable(callib+':output' if pipeline.enable_task(config['split_field'], 'otfcal') else None),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Split and average data ms={1:s}'.format(step, "".join(fms)))

                substep = 'save-{0:s}-ms{1:d}'.format(flags_after_worker, target_iter)
                manflags.add_cflags(pipeline, recipe, 'caracal_legacy', tms,
                    cab_name=substep, overwrite=False)

            obsinfo_msname = tms if pipeline.enable_task(
                config, 'split_field') else fms

            if pipeline.enable_task(config, 'changecentre'):
                if config['changecentre']['ra'] == '' or config['changecentre']['dec'] == '':
                    caracal.log.error(
                        'Wrong format for RA and/or Dec you want to change to. Check your settings of split_target:changecentre:ra and split_target:changecentre:dec')
                    caracal.log.error('Current settings for ra,dec are {0:s},{1:s}'.format(
                        config['changecentre']['ra'], config['changecentre']['dec']))
                    sys.exit(1)
                step = 'changecentre-ms{0:d}-{1:d}'.format(i,target_iter)
                recipe.add('cab/casa_fixvis', step,
                           {
                               "msname": tms,
                               "outputvis": tms,
                               "phasecenter": 'J2000 {0:s} {1:s}'.format(config['changecentre']['ra'], config['changecentre']['dec']),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Change phase centre ms={1:s}'.format(step, tms))

            if pipeline.enable_task(config, 'obsinfo'):
                if (config['obsinfo']['listobs']):
                    if pipeline.enable_task(config, 'split_field'):
                        listfile = '{0:s}-obsinfo.txt'.format(tms[:-3])
                    else:
                        listfile = '{0:s}-obsinfo.txt'.format(pipeline.dataid[i])

                    step = 'listobs-ms{0:d}-{1:d}'.format(i,target_iter)
                    recipe.add('cab/casa_listobs', step,
                               {
                                   "vis": obsinfo_msname,
                                   "listfile": listfile,
                                   "overwrite": True,
                               },
                               input=pipeline.input,
                               output=pipeline.obsinfo,
                               label='{0:s}:: Get observation information ms={1:s}'.format(step, obsinfo_msname))

                if (config['obsinfo']['summary_json']):
                    if pipeline.enable_task(config, 'split_field'):
                        listfile = '{0:s}-obsinfo.json'.format(tms[:-3])
                    else:
                        listfile = '{0:s}-obsinfo.json'.format(pipeline.dataid[i])

                    step = 'summary_json-ms{0:d}-{1:d}'.format(i,target_iter)
                    recipe.add('cab/msutils', step,
                               {
                                   "msname": obsinfo_msname,
                                   "command": 'summary',
                                   "display": False,
                                   "outfile": listfile
                               },
                               input=pipeline.input,
                               output=pipeline.obsinfo,
                               label='{0:s}:: Get observation information as a json file ms={1:s}'.format(step, obsinfo_msname))

            target_iter += 1

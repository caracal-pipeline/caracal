import os
import sys
import meerkathi
import stimela.dismissable as sdm
import getpass
import stimela.recipe as stimela
import re
import json
import numpy as np
from meerkathi.dispatch_crew import utils
from meerkathi.workers.utils import manage_flagsets
from meerkathi.workers.utils import manage_fields as manfields

NAME = 'Split and average target data'
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

# TODO(sphe) msutils incorrectly copies all intents from ms if there's just one field in the splitted dataset
    def fix_target_obsinfo(fname):
        if pipeline.enable_task(config, 'split_field'):
            with open(os.path.join(pipeline.output, fname), 'r') as stdr:
                d = json.load(stdr)
            d["FIELD"]["INTENTS"] = ['TARGET']
            with open(os.path.join(pipeline.output, fname), "w") as stdw:
                json.dump(d, stdw)

    def get_gain_field(applyme, applyto=None):
        if applyme == 'delay_cal':
            return manfields.get_field(pipeline, i, config['split_field']['otfcal']['apply_delay_cal'].get('field'))
        if applyme == 'bp_cal':
            return manfields.get_field(pipeline, i, config['split_field']['otfcal']['apply_bp_cal'].get('field'))
        if applyme == 'gain_cal_flux':
            return manfields.get_field(pipeline, i, 'fcal')
        if applyme == 'gain_cal_gain':
            return manfields.get_field(pipeline, i, 'gcal')
        if applyme == 'transfer_fluxscale':
            return manfields.get_field(pipeline, i, 'gcal')

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
        else: target_ls = pipeline.target[i]

        #use existing calibration library if user gives one
        if pipeline.enable_task(config['split_field'], 'otfcal') and config['split_field']['otfcal']['callib']:
            callib = 'caltables/callibs/{}'.format(config['split_field']['otfcal']['callib'])

            if not os.path.exists(os.path.join(pipeline.output,callib)):
                raise IOError(
                    "Callib file {0:s} does not exist. Please check that it is where it should be.".format(callib))

            docallib = True

            if config['split_field'].get('column') != 'corrected':
                meerkathi.log.info("Datacolumn was set to '{}'. by the user." \
                                   "Will be changed to 'corrected' for OTF calibration to work.".format(config['split_field'].get('column')))
            dcol = 'corrected'

        # write calibration library file for OTF cal
        elif pipeline.enable_task(config['split_field'], 'otfcal'):
            caltablelist, gainfieldlist, interplist = [], [], []
            calprefix = '{0:s}-{1:s}'.format(prefix,
                                             config['split_field']['otfcal'].get('label_cal'))
            callib = 'caltables/callibs/callib_{1:s}.txt'.format(prefix,calprefix)

            with open(os.path.join('{}/callibs'.format(pipeline.caltables),
                                  'callib_{0:s}_{1:s}.json'.format(prefix,
                                  config['split_field']['otfcal'].get('label_cal')))) as f:
                callib_dict = json.load(f)

            for applyme in 'delay_cal bp_cal gain_cal_flux gain_cal_gain transfer_fluxscale'.split():
                if not pipeline.enable_task(config['split_field']['otfcal'], 'apply_'+applyme):
                    continue
                caltablelist.append(callib_dict[applyme]['caltable'])
                gainfieldlist.append(callib_dict[applyme]['fldmap'])
                interplist.append(callib_dict[applyme]['interp'])

            with open(os.path.join(pipeline.output, callib), 'w') as stdw:
                for j in range(len(caltablelist)):
                    stdw.write('caltable="{0:s}/{1:s}/{2:s}"'.format(
                        stimela.CONT_IO[recipe.JOB_TYPE]["output"], 'caltables',  caltablelist[j]))
                    stdw.write(' calwt=False')
                    stdw.write(' tinterp=\''+str(interplist[j])+'\'')
                    stdw.write(' finterp=\'linear\'')
                    stdw.write(' fldmap=\'' + str(gainfieldlist[j])+'\'')
                    stdw.write(' spwmap=0\n')

            docallib = True
            if config['split_field'].get('column') != 'corrected':
                meerkathi.log.info("Datacolumn was set to '{}'. by the user." \
                                   "Will be changed to 'corrected' for OTF calibration to work.".format(config['split_field'].get('column')))
            dcol = 'corrected'

        else:
            docallib = False
            dcol = config['split_field'].get('column')

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

            flagv = tms+'.flagversions'

            if pipeline.enable_task(config, 'split_field'):
                step = 'split_field_{:d}'.format(i)
                if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, tms)) or \
                        os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, flagv)):

                    os.system(
                        'rm -rf {0:s}/{1:s} {0:s}/{2:s}'.format(pipeline.msdir, tms, flagv))
                recipe.add('cab/casa_mstransform', step,
                           {
                               "vis": fms,
                               "outputvis": tms,
                               "timeaverage": True if (config['split_field'].get('time_average') != '' and config['split_field'].get('time_average') != '0s') else False,
                               "timebin": config['split_field'].get('time_average'),
                               "chanaverage": True if config['split_field'].get('freq_average') > 1 else False,
                               "chanbin": config['split_field'].get('freq_average'),
                               "spw": config['split_field'].get('spw'),
                               "datacolumn": dcol,
                               "correlation": config['split_field'].get('correlation'),
                               "usewtspectrum": config['split_field'].get('usewtspectrum'),
                               "field": target,
                               "keepflags": True,
                               "docallib": docallib,
                               "callib": sdm.dismissable(callib+':output' if pipeline.enable_task(config['split_field'], 'otfcal') else None),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Split and average data ms={1:s}'.format(step, " ".join(fms)))

            msname = tms if pipeline.enable_task(
                config, 'split_field') else fms

            if pipeline.enable_task(config, 'changecentre'):
                if config['changecentre'].get('ra') == '' or config['changecentre'].get('dec') == '':
                    meerkathi.log.error(
                        'Wrong format for RA and/or Dec you want to change to. Check your settings of split_target:changecentre:ra and split_target:changecentre:dec')
                    meerkathi.log.error('Current settings for ra,dec are {0:s},{1:s}'.format(
                        config['changecentre'].get('ra'), config['changecentre'].get('dec')))
                    sys.exit(1)
                step = 'changecentre_{:d}'.format(i)
                recipe.add('cab/casa_fixvis', step,
                           {
                               "msname": tms,
                               "outputvis": tms,
                               "phasecenter": 'J2000 {0:s} {1:s}'.format(config['changecentre'].get('ra'), config['changecentre'].get('dec')),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Change phase centre ms={1:s}'.format(step, tms))

            if pipeline.enable_task(config, 'obsinfo'):
                if (config['obsinfo'].get('listobs')):
                    if pipeline.enable_task(config, 'split_field'):
                        listfile = '{0:s}-obsinfo.txt'.format(tms[:-3])
                    else:
                        listfile = '{0:s}-obsinfo.txt'.format(pipeline.dataid[i])

                    step = 'listobs_{:d}'.format(i)
                    recipe.add('cab/casa_listobs', step,
                               {
                                   "vis": msname,
                                   "listfile": listfile,
                                   "overwrite": True,
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: Get observation information ms={1:s}'.format(step, msname))

                if (config['obsinfo'].get('summary_json')):
                    if pipeline.enable_task(config, 'split_field'):
                        listfile = '{0:s}-obsinfo.json'.format(tms[:-3])
                    else:
                        listfile = '{0:s}-obsinfo.json'.format(pipeline.dataid[i])

                    step = 'summary_json_{:d}'.format(i)
                    recipe.add('cab/msutils', step,
                               {
                                   "msname": msname,
                                   "command": 'summary',
                                   "display": False,
                                   "outfile": listfile
                               },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='{0:s}:: Get observation information as a json file ms={1:s}'.format(step, msname))

            step = 'fix_target_obsinfo_{:d}'.format(i)  # set directories
            recipe.add(fix_target_obsinfo, step,
                       {
                           'fname': listfile,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Correct previously outputted obsinfo json: {0:s}'.format(listfile))

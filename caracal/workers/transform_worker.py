# -*- coding: future_fstrings -*-
import os
import sys
import caracal
import stimela.dismissable as sdm
import stimela.recipe
import json
import caracal
from caracal.workers.utils import manage_flagsets as manflags
from caracal import log
from caracal.workers.utils import remove_output_products

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

_target_fields = {'target'}
_cal_fields = set("fcal bpcal gcal xcal".split())

def get_fields_to_split(config, name):
    fields = config['field']
    if not fields:
        raise caracal.ConfigurationError(f"'{name}: field' cannot be empty")
    elif fields == 'calibrators':
        return _cal_fields
    elif fields == 'target':
        return _target_fields
    # else better be a combination of calibrator designators
    else:
        fields_to_split = set(fields.split(','))
        diff = fields_to_split.difference(_cal_fields)
        if diff:
            raise caracal.ConfigurationError(
                "'{}: field: expected 'target', 'calibrators', or one or more of {}. Got '{}'".format(name,
                    ', '.join([f"'{f}'" for f in _cal_fields]), ','.join(diff)
                ))
        return fields_to_split


def check_config(config, name):
    get_fields_to_split(config, name)


def worker(pipeline, recipe, config):
    wname = pipeline.CURRENT_WORKER
    flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
    flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
    label_in = config['label_in']
    label_out = config['label_out']
    from_target = True if label_in and config['field'] == 'target' else False
    field_to_split = get_fields_to_split(config, wname)
    # are we splitting calibrators
    splitting_cals = field_to_split.intersection(_cal_fields)
    if (pipeline.enable_task(config, 'split_field') or pipeline.enable_task(config, 'changecentre')) and pipeline.enable_task(config, 'concat'):
        raise ValueError("split_field/changecentre and concat cannot be enabled in the same run of the transform worker. The former need a single-valued label_in, the latter multiple comma-separated values.")
    if ',' in label_in:
        if pipeline.enable_task(config, 'split_field'):
            raise ValueError("split_field cannot be enabled with multiple (i.e., comma-separated) entries in label_in")
        if pipeline.enable_task(config, 'changecentre'):
            raise ValueError("changecentre cannot be enabled with multiple (i.e., comma-separated) entries in label_in")
        else: transform_mode = 'concat' # in this mode all .MS files from the same input .MS and with the same target, and with label inside the list label_in, are concatenated
    else:
        if pipeline.enable_task(config, 'concat'):
            raise ValueError("concat cannot be enabled with a single entry in label_in")
        else: transform_mode = 'split'

    for i, (msbase, prefix_msbase) in enumerate(zip(pipeline.msbasenames, pipeline.prefix_msbases)):
        # if splitting from target, we have multiple MSs to iterate over
        if transform_mode == 'split':
            from_mslist = pipeline.get_mslist(i, label_in, target=from_target)
        elif transform_mode == 'concat':
            from_mslist = pipeline.get_mslist(i, '', target=from_target)
        to_mslist  = pipeline.get_mslist(i, label_out, target=not splitting_cals)

        # if splitting cals, we'll split one (combined) target to one output MS
        if splitting_cals:
           calfields = set()
           for fd in field_to_split:
               for elem in getattr(pipeline, fd)[i]:
                   calfields.add(elem)
           target_ls = [','.join(calfields)]
        # else splitting target -- we'll split a list of targets to a list of output MSs
        else:
           target_ls = pipeline.target[i]
           # repeat the from-ms once per target, if not splitting from the target MS
           if not from_target:
               from_mslist = from_mslist * len(target_ls)

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
            calprefix = '{0:s}-{1:s}'.format(prefix_msbase,
                                             config['split_field']['otfcal']['label_cal'])
            callib = 'caltables/callibs/callib_{1:s}.txt'.format(prefix_msbase, calprefix)

            with open(os.path.join('{}/callibs'.format(pipeline.caltables),
                                  'callib_{0:s}-{1:s}.json'.format(prefix_msbase,
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

        for target_iter, (target, from_ms, to_ms) in enumerate(zip(target_ls, from_mslist, to_mslist)):
            # Rewind flags
            available_flagversions = manflags.get_flags(pipeline, from_ms)
            if config['rewind_flags']['enable'] and label_in and transform_mode == 'split':
                version = config['rewind_flags']['version']
                if version in available_flagversions:
                    substep = 'rewind-{0:s}-ms{1:d}'.format(version, target_iter)
                    manflags.restore_cflags(pipeline, recipe, version, from_ms, cab_name=substep)
                    if available_flagversions[-1] != version:
                        substep = 'delete-flag_versions-after-{0:s}-ms{1:d}'.format(version, target_iter)
                        manflags.delete_cflags(pipeline, recipe,
                            available_flagversions[available_flagversions.index(version)+1],
                            from_ms, cab_name=substep)
                else:
                    manflags.conflict('rewind_to_non_existing', pipeline, wname, from_ms,
                        config, flags_before_worker, flags_after_worker)

            flagv = to_ms + '.flagversions'
            if pipeline.enable_task(config, 'split_field'):
                msbase = os.path.splitext(to_ms)[0]
                obsinfo_msname = to_ms
            else:
                msbase = pipeline.msbasenames[i]
                obsinfo_msname = from_ms

            summary_file = f'{msbase}-summary.json'
            obsinfo_file = f'{msbase}-obsinfo.txt'

            if pipeline.enable_task(config, 'split_field'):
                step = 'split_field-ms{0:d}-{1:d}'.format(i, target_iter)
                # If the output of this run of mstransform exists, delete it first
                remove_output_products((to_ms, flagv, summary_file, obsinfo_file), directory=pipeline.msdir, log=log)

                recipe.add('cab/casa_mstransform', step,
                           {
                               "vis": from_ms if label_in else from_ms + ":input",
                               "outputvis": to_ms,
                               "timeaverage": True if (config['split_field']['time_avg'] != '' and config['split_field']['time_avg'] != '0s') else False,
                               "timebin": config['split_field']['time_avg'],
                               "chanaverage": True if config['split_field']['chan_avg'] > 1 else False,
                               "chanbin": config['split_field']['chan_avg'],
                               "spw": config['split_field']['spw'],
                               "datacolumn": dcol,
                               "correlation": config['split_field']['correlation'],
                               "scan": config['split_field']['scan'],
                               "usewtspectrum": config['split_field']['create_specweights'],
                               "field": target,
                               "keepflags": True,
                               "docallib": docallib,
                               "callib": sdm.dismissable(callib+':output' if pipeline.enable_task(config['split_field'], 'otfcal') else None),
                           },
                           input=pipeline.input if label_in else pipeline.rawdatadir,
                           output=pipeline.output,
                           label='{0:s}:: Split and average data ms={1:s}'.format(step, "".join(from_ms)))

                substep = 'save-{0:s}-ms{1:d}'.format(flags_after_worker, target_iter)
                manflags.add_cflags(pipeline, recipe, 'caracal_legacy', to_ms,
                    cab_name=substep, overwrite=False)

            obsinfo_msname = to_ms if pipeline.enable_task(config, 'split_field') else from_ms

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
                               "msname": to_ms,
                               "outputvis": to_ms,
                               "phasecenter": 'J2000 {0:s} {1:s}'.format(config['changecentre']['ra'], config['changecentre']['dec']),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Change phase centre ms={1:s}'.format(step, to_ms))


            if pipeline.enable_task(config, 'concat'):
                concat_labels = label_in.split(',')

                step = 'concat-ms{0:d}-{1:d}'.format(i,target_iter)
                concat_ms = [from_ms.replace('.ms','-{0:s}.ms'.format(cl)) for cl in concat_labels]
                recipe.add('cab/casa_concat', step,
                           {
                               "vis": concat_ms,
                               "concatvis": 'tobedeleted-'+to_ms,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Concatenate {1:}'.format(step, concat_ms))

                # If the output of this run of mstransform exists, delete it first
                if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, to_ms)) or \
                        os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, flagv)):
                    os.system(
                        'rm -rf {0:s}/{1:s} {0:s}/{2:s}'.format(pipeline.msdir, to_ms, flagv))

                step = 'singlespw-ms{0:d}-{1:d}'.format(i,target_iter)
                recipe.add('cab/casa_mstransform', step,
                           {
                               "vis": 'tobedeleted-'+to_ms,
                               "outputvis": to_ms,
                               "datacolumn": 'data',
                               "combinespws": True,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Single SPW {1:}'.format(step, concat_ms))

                substep = 'save-{0:s}-ms{1:d}'.format(flags_after_worker, target_iter)
                manflags.add_cflags(pipeline, recipe, 'caracal_legacy', to_ms,
                    cab_name=substep, overwrite=False)

                # Delete the tobedeleted file, but first we need to have created it, thus...
                recipe.run()
                # Empty job que after execution
                recipe.jobs = []
                os.system(
                    'rm -rf {0:s}/tobedeleted-{1:s}'.format(pipeline.msdir, to_ms))

                obsinfo_msname = to_ms


            if pipeline.enable_task(config, 'obsinfo'):
                if (config['obsinfo']['listobs']):

                    if pipeline.enable_task(config, 'split_field') or transform_mode == 'concat':
                        listfile = '{0:s}-obsinfo.txt'.format(os.path.splitext(to_ms)[0])
                    else:
                        listfile = '{0:s}-obsinfo.txt'.format(pipeline.msbasenames[i])

                    step = 'listobs-ms{0:d}-{1:d}'.format(i,target_iter)
                    recipe.add('cab/casa_listobs', step,
                               {
                                   "vis": obsinfo_msname,
                                   "listfile": listfile+":msfile",
                                   "overwrite": True,
                               },
                               input=pipeline.input,
                               output=pipeline.obsinfo,
                               label='{0:s}:: Get observation information ms={1:s}'.format(step, obsinfo_msname))

                if (config['obsinfo']['summary_json']):

                    if pipeline.enable_task(config, 'split_field') or transform_mode == 'concat':
                        listfile = '{0:s}-summary.json'.format(os.path.splitext(to_ms)[0])
                    else:
                        listfile = '{0:s}-summary.json'.format(pipeline.msbasenames[i])

                    step = 'summary_json-ms{0:d}-{1:d}'.format(i,target_iter)
                    recipe.add('cab/msutils', step,
                               {
                                   "msname": obsinfo_msname,
                                   "command": 'summary',
                                   "display": False,
                                   "outfile": listfile+":msfile"
                               },
                               input=pipeline.input,
                               output=pipeline.obsinfo,
                               label='{0:s}:: Get observation information as a json file ms={1:s}'.format(step, obsinfo_msname))

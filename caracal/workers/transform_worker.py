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
from caracal.workers.utils.callibs import resolve_calibration_library

NAME = 'Transform Data by Splitting/Average/Applying calibration'
LABEL = 'transform'

# Rules for interpolation mode to use when applying calibration solutions
applycal_interp_rules = {
    'target': {
        'delay_cal': 'linear',
        'bp_cal': 'linear',
        'transfer_fluxscale': 'linear',
        'gain_cal_gain': 'linear',
    },
}


def get_dir_path(string, pipeline):
    return string.split(pipeline.output)[1][1:]


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
            raise caracal.ConfigurationError("'{}: field: expected 'target', "
                                             "'calibrators', or one or more of {}. Got '{}'"
                                             "".format(name, ', '.join([f"'{f}'" for f in _cal_fields]), ','.join(diff)))
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
    if (pipeline.enable_task(config, 'split_field') or
            pipeline.enable_task(config, 'changecentre')) and pipeline.enable_task(config, 'concat'):
        raise ValueError(
            "split_field/changecentre and concat cannot be enabled in the same run of the transform worker. "
            "The former need a single-valued label_in, the latter multiple comma-separated values.")
    if ',' in label_in:
        if pipeline.enable_task(config, 'split_field'):
            raise ValueError("split_field cannot be enabled with multiple (i.e., comma-separated) entries in label_in")
        if pipeline.enable_task(config, 'changecentre'):
            raise ValueError("changecentre cannot be enabled with multiple (i.e., comma-separated) entries in label_in")
        else:
            transform_mode = 'concat'  # in this mode all .MS files from the same input .MS and with the same target, and with label inside the list label_in, are concatenated
    else:
        if pipeline.enable_task(config, 'concat'):
            raise ValueError("concat cannot be enabled with a single entry in label_in")
        else:
            transform_mode = 'split'

    for i, (msbase, prefix_msbase) in enumerate(zip(pipeline.msbasenames, pipeline.prefix_msbases)):
        # if splitting from target, we have multiple MSs to iterate over
        if transform_mode == 'split':
            from_mslist = pipeline.get_mslist(i, label_in, target=from_target)
        elif transform_mode == 'concat':
            from_mslist = pipeline.get_mslist(i, '', target=from_target)
        to_mslist = pipeline.get_mslist(i, label_out, target=not splitting_cals)

        # if splitting cals, we'll split one (combined) target to one output MS
        if splitting_cals:
            calfields = set()
            for fd in field_to_split:
                for elem in getattr(pipeline, fd)[i]:
                    calfields.add(elem)
            output_fields = calfields
            target_ls = [','.join(calfields)]
        # else splitting target -- we'll split a list of targets to a list of output MSs
        else:
            target_ls = pipeline.target[i]
            output_fields = [x.strip() for x in target_ls]
            # repeat the from-ms once per target, if not splitting from the target MS
            if not from_target:
                from_mslist = from_mslist * len(target_ls)

        dcol = config['split_field']['col']

        # if these are set to not None below, this means OTF is enabled and a valid library is to be applied
        polcal_lib = crosscal_lib = None
        pcaltablelist = pgainfieldlist = pinterplist = pcalwtlist = papplyfield = []

        if pipeline.enable_task(config['split_field'], 'otfcal'):
            if dcol != 'corrected':
                caracal.log.warning(
                    f"split_field: col set to '{dcol}' but OTF calibration is enabled. Forcing to 'corrected'")
                dcol = 'corrected'
            crosscal_lib, (caltablelist, gainfieldlist, interplist, calwtlist, applyfield) = \
                resolve_calibration_library(pipeline, prefix_msbase,
                                            config['split_field']['otfcal']['callib'],
                                            config['split_field']['otfcal']['label_cal'],
                                            output_fields=output_fields,
                                            default_interpolation_types=config['split_field']['otfcal'][
                                                'interpolation'])
            if crosscal_lib:
                caracal.log.info(f"applying OTF cross-cal from {os.path.basename(crosscal_lib)}")
            else:
                caracal.log.info(f"no cross-cal lib specified for OTF, ignoring")

            # load/export if specified -- otherwise will be empty lists. Also converts to full filename.
            polcal_lib, (pcaltablelist, pgainfieldlist, pinterplist, pcalwtlist, papplyfield) = \
                resolve_calibration_library(pipeline, prefix_msbase,
                                            config['split_field']['otfcal']['pol_callib'],
                                            config['split_field']['otfcal']['label_pcal'],
                                            output_fields=output_fields,
                                            default_interpolation_types=config['split_field']['otfcal'][
                                                'interpolation'])
            if polcal_lib:
                caracal.log.info(f"applying OTF polcal from {os.path.basename(polcal_lib)}")
            else:
                caracal.log.info(f"no polcal lib specified for OTF, ignoring")

        for target_iter, (target, from_ms, to_ms) in enumerate(zip(target_ls, from_mslist, to_mslist)):
            # Rewind flags
            available_flagversions = manflags.get_flags(pipeline, from_ms)
            if config['rewind_flags']['enable'] and label_in:
                version = config['rewind_flags']['version']
                if version in available_flagversions:
                    substep = 'rewind-{0:s}-ms{1:d}'.format(version, target_iter)
                    manflags.restore_cflags(pipeline, recipe, version, from_ms, cab_name=substep)
                    if available_flagversions[-1] != version:
                        substep = 'delete-flag_versions-after-{0:s}-ms{1:d}'.format(version, target_iter)
                        manflags.delete_cflags(pipeline, recipe,
                                               available_flagversions[available_flagversions.index(version) + 1],
                                               from_ms, cab_name=substep)
                else:
                    manflags.conflict('rewind_to_non_existing', pipeline, wname, from_ms,
                                      config, flags_before_worker, flags_after_worker)

            flagv = to_ms + '.flagversions'
            tmp_ms = 'tmp_' + to_ms
            tmpflagv = tmp_ms + '.flagversions'
            if pipeline.enable_task(config, 'split_field'):
                msbase = os.path.splitext(to_ms)[0]
            else:
                msbase = pipeline.msbasenames[i]

            summary_file = f'{msbase}-summary.json'
            obsinfo_file = f'{msbase}-obsinfo.txt'

            if pipeline.enable_task(config, 'split_field'):
                step = 'split_field-ms{0:d}-{1:d}'.format(i, target_iter)
                # If the output of this run of mstransform exists, delete it first
                remove_output_products((to_ms, tmp_ms, flagv, tmpflagv, summary_file, obsinfo_file),
                                       directory=pipeline.msdir, log=log)
                if not polcal_lib:
                    recipe.add('cab/casa_mstransform', step, {
                        "vis": from_ms if label_in else from_ms + ":input",
                        "outputvis": to_ms,
                        "timeaverage": config['split_field']['time_avg'] not in ('', '0s'),
                        "timebin": config['split_field']['time_avg'],
                        "chanaverage": config['split_field']['chan_avg'] > 1,
                        "chanbin": config['split_field']['chan_avg'],
                        "spw": config['split_field']['spw'],
                        "antenna": config['split_field']['antennas'],
                        "datacolumn": dcol,
                        "correlation": config['split_field']['correlation'],
                        "scan": config['split_field']['scan'],
                        "usewtspectrum": config['split_field']['create_specweights'],
                        "field": target,
                        "keepflags": True,
                        "docallib": bool(crosscal_lib),
                        "callib": sdm.dismissable(crosscal_lib and crosscal_lib + ':output'),
                        "nthreads": config['split_field']['nthreads'],
                    },
                        input=pipeline.input if label_in else pipeline.rawdatadir,
                        output=pipeline.output,
                        label=f'{step}:: Split and average data ms={"".join(from_ms)}')
                # workaround because mstransform does not accept the polcal gaintypes such as Xfparang
                else:
                    output_pcal_ms = config['split_field']['otfcal']['output_pcal_ms']
                    # in intermediate-output mode, do transform directly to the output MS
                    if output_pcal_ms == 'intermediate':
                        tmp_ms = to_ms
                        tmpflagv = flagv
                        log.warning(
                            "otfcal: output_pcal_ms is 'intermediate', output will be an intermediate MS only with DATA and CORRECTED_DATA columns. This is experimenatal.")

                    recipe.add('cab/casa_mstransform', step + '_tmp_split_crosscal_corrected', {
                        "vis": from_ms if label_in else from_ms + ":input",
                        "outputvis": tmp_ms,
                        "timeaverage": config['split_field']['time_avg'] not in ('', '0s'),
                        "timebin": config['split_field']['time_avg'],
                        "chanaverage": config['split_field']['chan_avg'] > 1,
                        "chanbin": config['split_field']['chan_avg'],
                        "spw": config['split_field']['spw'],
                        "datacolumn": sdm.dismissable('corrected' if crosscal_lib is not None else 'data'),
                        "correlation": config['split_field']['correlation'],
                        "scan": config['split_field']['scan'],
                        "antenna": config['split_field']['antennas'],
                        "usewtspectrum": config['split_field']['create_specweights'],
                        "field": target,
                        "keepflags": True,
                        "docallib": bool(crosscal_lib),
                        "callib": sdm.dismissable(crosscal_lib and crosscal_lib + ':output'),
                        "nthreads": config['split_field']['nthreads'],
                    },
                        input=pipeline.input if label_in else pipeline.rawdatadir,
                        output=pipeline.output,
                        label=f'{step}:: Split and average data ms={"".join(from_ms)}')

                    if any(papplyfield):
                        recipe.add('cab/casa_applycal', step + '_apply_polcal', {
                            "vis": tmp_ms,
                            "field": target,
                            "docallib": False,
                            "calwt": pcalwtlist,
                            "gaintable": [f"{ct}:output" for ct in pcaltablelist],
                            "gainfield": pgainfieldlist,
                            "interp": pinterplist,
                            "parang": config['split_field']['otfcal']['derotate_pa'],
                        },
                            input=pipeline.input,
                            output=pipeline.caltables,
                            label=f'{step}:: Apply pol callib ms={"".join(to_ms)}')
                    else:
                        trgt = [x.strip() for x in target.split(',')]
                        for ii, fld in enumerate(trgt):
                            pcal = []
                            pgain = []
                            pinter = []
                            pcalwt = []
                            for idx, f in enumerate(papplyfield):
                                if f == '' or f == fld:
                                    if pcaltablelist[idx] not in pcal:
                                        pcal.append(pcaltablelist[idx])
                                        pgain.append(pgainfieldlist[idx])
                                        pinter.append(pinterplist[idx])
                                        pcalwt.append(pcalwtlist[idx])
                            recipe.add('cab/casa_applycal', step + '_apply_polcal_' + str(ii), {
                                "vis": tmp_ms,
                                "field": fld,
                                "docallib": False,
                                "calwt": pcalwt,
                                "gaintable": ["%s:output" % ct for ct in pcal],
                                "gainfield": pgain,
                                "interp": pinter,
                                "parang": config['split_field']['otfcal']['derotate_pa'],
                            },
                                input=pipeline.input,
                                output=pipeline.caltables,
                                label=f'{step}:: Apply pol callib ms={"".join(to_ms)}, field={ii}')
                    recipe.run()
                    recipe.jobs = []
                    # generate final MS, unless we're only asked to produce the intermediate one
                    if tmp_ms != to_ms:
                        recipe.add('cab/casa_mstransform', step + '_split_polcal_corrected', {
                            "vis": tmp_ms,
                            "outputvis": to_ms,
                            "datacolumn": 'corrected',
                            "timeaverage": False,
                            "chanaverage": False,
                            "spw": '',
                            "correlation": '',
                            "usewtspectrum": config['split_field']['create_specweights'],
                            "field": '',
                            "keepflags": True,
                            "docallib": False,
                        },
                            input=pipeline.input if label_in else pipeline.rawdatadir,
                            output=pipeline.output,
                            label=f'{step}:: Split polcal corrected ms={"".join(to_ms)}')
                        recipe.run()
                        recipe.jobs = []

                        # Delete intermediate ms
                        if output_pcal_ms == 'final':
                            remove_output_products((tmp_ms, tmpflagv), directory=pipeline.msdir, log=log)

                substep = 'save-{0:s}-ms{1:d}'.format('caracal_legacy', target_iter)
                manflags.add_cflags(pipeline, recipe, 'caracal_legacy', to_ms,
                                    cab_name=substep, overwrite=False)

            obsinfo_msname = to_ms if pipeline.enable_task(config, 'split_field') else from_ms

            if pipeline.enable_task(config, 'changecentre'):
                if config['changecentre']['ra'] == '' or config['changecentre']['dec'] == '':
                    caracal.log.error(
                        'Wrong format for RA and/or Dec you want to change to. '
                        'Check your settings of split_target:changecentre:ra and split_target:changecentre:dec')
                    caracal.log.error('Current settings for ra,dec are {0:s},{1:s}'.format(
                        config['changecentre']['ra'], config['changecentre']['dec']))
                    sys.exit(1)
                step = 'changecentre-ms{0:d}-{1:d}'.format(i, target_iter)
                recipe.add('cab/casa_fixvis', step,
                           {
                               "msname": to_ms,
                               "outputvis": to_ms,
                               "phasecenter": 'J2000 {0:s} {1:s}'.format(config['changecentre']['ra'],
                                                                         config['changecentre']['dec']),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Change phase centre ms={1:s}'.format(step, to_ms))

            if pipeline.enable_task(config, 'concat'):
                concat_labels = label_in.split(',')

                step = 'concat-ms{0:d}-{1:d}'.format(i, target_iter)
                concat_ms = [from_ms.replace('.ms', '-{0:s}.ms'.format(cl)) for cl in concat_labels]
                recipe.add('cab/casa_concat', step,
                           {
                               "vis": concat_ms,
                               "concatvis": 'tobedeleted-' + to_ms,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Concatenate {1:}'.format(step, concat_ms))

                # If the output of this run of mstransform exists, delete it first
                if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, to_ms)) or \
                        os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, flagv)):
                    os.system(
                        'rm -rf {0:s}/{1:s} {0:s}/{2:s}'.format(pipeline.msdir, to_ms, flagv))

                step = 'singlespw-ms{0:d}-{1:d}'.format(i, target_iter)
                recipe.add('cab/casa_mstransform', step,
                           {
                               "vis": 'tobedeleted-' + to_ms,
                               "outputvis": to_ms,
                               "datacolumn": config['concat']['col'],
                               "combinespws": True,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Single SPW {1:}'.format(step, concat_ms))

                substep = 'save-{0:s}-ms{1:d}'.format('caracal_legacy', target_iter)
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
                if config['obsinfo']['listobs']:

                    if pipeline.enable_task(config, 'split_field') or transform_mode == 'concat':
                        listfile = '{0:s}-obsinfo.txt'.format(os.path.splitext(to_ms)[0])
                    else:
                        listfile = '{0:s}-obsinfo.txt'.format(pipeline.msbasenames[i])

                    step = 'listobs-ms{0:d}-{1:d}'.format(i, target_iter)
                    recipe.add('cab/casa_listobs', step,
                               {
                                   "vis": obsinfo_msname,
                                   "listfile": listfile + ":msfile",
                                   "overwrite": True,
                               },
                               input=pipeline.input,
                               output=pipeline.obsinfo,
                               label='{0:s}:: Get observation information ms={1:s}'.format(step, obsinfo_msname))

                if config['obsinfo']['summary_json']:

                    if pipeline.enable_task(config, 'split_field') or transform_mode == 'concat':
                        listfile = '{0:s}-summary.json'.format(os.path.splitext(to_ms)[0])
                    else:
                        listfile = '{0:s}-summary.json'.format(pipeline.msbasenames[i])

                    step = 'summary_json-ms{0:d}-{1:d}'.format(i, target_iter)
                    recipe.add('cab/msutils', step,
                               {
                                   "msname": obsinfo_msname,
                                   "command": 'summary',
                                   "display": False,
                                   "outfile": listfile + ":msfile"
                               },
                               input=pipeline.input,
                               output=pipeline.obsinfo,
                               label='{0:s}:: Get observation information as a json file ms={1:s}'.format(step,
                                                                                                          obsinfo_msname))

import caracal.dispatch_crew.utils as utils
import yaml
import caracal
import sys
import numpy as np
from os import path

NAME = 'Automatically categorize observed fields'
LABEL = 'observation_config'

def repeat_val(val, n):
    l = []
    for x in range(n):
        l.append(val)
    return l

def worker(pipeline, recipe, config):
    # Before doing anything let's check there are no conflicting instruction in the yml file
    # check self cal is defined
    if 'self_cal' in pipeline.config:
        #if  we are running self cal we want to check the following
        if pipeline.config['self_cal']['enable']:
            caracal.log.info(
                        "Checking the consistency of the Self_Cal input")



            # First let' check that we are not using transfer gains with meqtrees or not starting at the start with meqtrees
            if pipeline.config['self_cal']['calibrate_with'].lower() == 'meqtrees':
                if pipeline.config['self_cal']['transfer_apply_gains']['enable']:
                    raise caracal.UserInputError(
                        'Gains cannot be interpolated with MeqTrees, please switch to CubiCal. Exiting.')
                if int(pipeline.config['self_cal']['start_at_iter']) != 1:
                    raise caracal.UserInputError(
                        "We cannot reapply MeqTrees calibration at a given step. Hence you will need to do a full selfcal loop.")
                if int(pipeline.config['self_cal']['cal_channel_chunk']) != -1:
                        caracal.log.info("The channel chunk has no effect on MeqTrees.")
            else:
                if int(pipeline.config['self_cal']['start_at_iter']) != 1:
                    raise caracal.UserInputError(
                        "We cannot reapply Cubical calibration at a given step. Hence you will need to do a full selfcal loop.")
            # First check we are actually running a calibrate
            if pipeline.config['self_cal']['calibrate']['enable']:
                # Running with a model shorter than the output type is dengerous with 'CORR_RES'
                if 'CORR_RES' in  pipeline.config['self_cal']['calibrate']['output_data']:
                    if len(pipeline.config['self_cal']['calibrate']['model']) < pipeline.config['self_cal']['cal_niter']:
                        raise caracal.UserInputError(
                            "You did not set a model to use for every iteration while using residuals. This is too dangerous for CARACal to execute.")

                # Make sure we are not using two_step with CubiCal
                if pipeline.config['self_cal']['calibrate_with'].lower() == 'cubical' and pipeline.config['self_cal']['calibrate']['two_step']:
                    raise caracal.UserInputError(
                        "Two_Step calibration is an experimental mode only available for meqtrees at the moment.")
                #Then let's check that the solutions are reasonable and fit in our time chunks
                #!!!!!! Remainder solutions are not checked to be a full solution block!!!!!!!!
                #  we check there are enough solution
                if len(pipeline.config['self_cal']['calibrate']['Gsols_timeslots']) < int(pipeline.config['self_cal']['cal_niter']):
                    amount_sols = len(pipeline.config['self_cal']['calibrate']['Gsols_timeslots'])
                else:
                    amount_sols = int(pipeline.config['self_cal']['cal_niter'])
                #  we collect all time solutions
                solutions = pipeline.config['self_cal']['calibrate']['Gsols_timeslots'][:amount_sols]
                # if we do Bjones we add those
                if pipeline.config['self_cal']['calibrate']['Bjones']:
                    if len(pipeline.config['self_cal']['calibrate']['Bsols_timeslots']) < int(pipeline.config['self_cal']['cal_niter']):
                        amount_sols = len(pipeline.config['self_cal']['calibrate']['Bsols_timeslots'])
                    else:
                        amount_sols = int(pipeline.config['self_cal']['cal_niter'])
                    solutions.append(pipeline.config['self_cal']['calibrate']['Bsols_timeslots'][:amount_sols])
                # Same for GA solutions
                if len(pipeline.config['self_cal']['calibrate']['gain_matrix_type']) < int(pipeline.config['self_cal']['cal_niter']):
                    amount_matrix = len(pipeline.config['self_cal']['calibrate']['gain_matrix_type'])
                else:
                    amount_matrix = int(pipeline.config['self_cal']['cal_niter'])
                if 'GainDiag' in pipeline.config['self_cal']['calibrate']['gain_matrix_type'][:amount_matrix] or \
                    'Gain2x2' in pipeline.config['self_cal']['calibrate']['gain_matrix_type'][:amount_matrix]:
                    if len(pipeline.config['self_cal']['calibrate']['GAsols_timeslots']) < int(pipeline.config['self_cal']['cal_niter']):
                        amount_sols = len(pipeline.config['self_cal']['calibrate']['GAsols_timeslots'])
                    else:
                        amount_sols = int(pipeline.config['self_cal']['cal_niter'])
                    for i,val in enumerate(pipeline.config['self_cal']['calibrate']['GAsols_timeslots'][:amount_sols]):
                        if val >= 0:
                            solutions.append(val)
                # then we assign the timechunk
                if pipeline.config['self_cal']['cal_timeslots_chunk'] == -1:
                    if np.min(solutions) != 0.:
                        time_chunk = max(solutions)
                    else:
                        time_chunk = 0
                else:
                    time_chunk = pipeline.config['self_cal']['cal_timeslots_chunk']
                # if time_chunk is not 0 all solutions should fit in there.
                # if it is 0 then it does not matter as we are not checking remainder intervals
                if time_chunk != 0:
                    sol_int_array = float(time_chunk)/np.array(solutions,dtype=float)
                    for val in sol_int_array:
                        if val != int(val):
                            raise caracal.UserInputError(
                                "Not all applied time solutions fit in the timeslot_chunk. \n" +
                                "Your timeslot chunk = {} \n".format(time_chunk) +
                                "Your time solutions to be applied are {}".format(', '.join([str(x) for x in solutions])))
                # Then we repeat for the channels, as these arrays do not have to be the same length as the timeslots this can not be combined
                # This is not an option for meqtrees
                if pipeline.config['self_cal']['calibrate_with'].lower() == 'cubical':
                    if len(pipeline.config['self_cal']['calibrate']['Gsols_channel']) < int(pipeline.config['self_cal']['cal_niter']):
                        amount_sols = len(pipeline.config['self_cal']['calibrate']['Gsols_channel'])
                    else:
                        amount_sols = int(pipeline.config['self_cal']['cal_niter'])
                    #  we collect all time solutions
                    solutions = pipeline.config['self_cal']['calibrate']['Gsols_channel'][:amount_sols]
                    # if we do Bjones we add those
                    if pipeline.config['self_cal']['calibrate']['Bjones']:
                        if len(pipeline.config['self_cal']['calibrate']['Bsols_channel']) < int(pipeline.config['self_cal']['cal_niter']):
                            amount_sols = len(pipeline.config['self_cal']['calibrate']['Bsols_channel'])
                        else:
                            amount_sols = int(pipeline.config['self_cal']['cal_niter'])
                        solutions.append(pipeline.config['self_cal']['calibrate']['Bsols_channel'][:amount_sols])
                    # Same for GA solutions
                    if 'GainDiag' in pipeline.config['self_cal']['calibrate']['gain_matrix_type'][:amount_matrix] or \
                        'Gain2x2' in pipeline.config['self_cal']['calibrate']['gain_matrix_type'][:amount_matrix]:
                        if len(pipeline.config['self_cal']['calibrate']['GAsols_channel']) < int(pipeline.config['self_cal']['cal_niter']):
                            amount_sols = len(pipeline.config['self_cal']['calibrate']['GAsols_channel'])
                        else:
                            amount_sols = int(pipeline.config['self_cal']['cal_niter'])
                        for i,val in enumerate(pipeline.config['self_cal']['calibrate']['GAsols_channel'][:amount_sols]):
                            if val >= 0:
                                solutions.append(val)
                    # then we assign the timechunk
                    if pipeline.config['self_cal']['cal_channel_chunk'] == -1:
                        if np.min(solutions) != 0.:
                            channel_chunk = max(solutions)
                        else:
                            channel_chunk = 0
                    else:
                        channel_chunk = pipeline.config['self_cal']['cal_channel_chunk']
                    # if channel_chunk is not 0 all solutions should fit in there.
                    # if it is 0 then it does not matter as we are not checking remainder intervals
                    if channel_chunk != 0:
                        sol_int_array = float(channel_chunk)/np.array(solutions,dtype=float)
                        for val in sol_int_array:
                            if val != int(val):
                                caracal.UserInputError("Not all applied channel solutions fit in the channel_chunk. \n" +
                                               "Your channel chunk = {} \n".format(channel_chunk) +
                                               "Your channel solutions to be applied are {}".format(', '.join([str(x) for x in solutions])))
            # Check some imaging stuff
            if pipeline.config['self_cal']['image']['enable']:
                if pipeline.config['self_cal']['img_maxuv_l'] > 0. and  pipeline.config['self_cal']['taper'] > 0.:
                    caracal.UserInputError(
                        "You are trying to image with a Gaussian taper as well as a Tukey taper. Please remove one. ")

    if pipeline.virtconcat:
        msnames = [pipeline.vmsname]
        prefixes = [pipeline.prefix]
        nobs = 1
    else:
        msnames = pipeline.msnames
        prefixes = pipeline.prefixes
        nobs = pipeline.nobs

    for i in range(nobs):
        prefix = prefixes[i]
        msname = msnames[i]
        msroot = msname[:-3]

        if pipeline.enable_task(config, 'obsinfo'):
            if config['obsinfo'].get('listobs'):
                step = 'listobs_{:d}'.format(i)
                recipe.add('cab/casa_listobs', step,
                           {
                               "vis": msname,
                               "listfile": '{0:s}-obsinfo.txt'.format(msroot),
                               "overwrite": True,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Get observation information ms={1:s}'.format(step, msname))

            if config['obsinfo'].get('summary_json'):
                step = 'summary_json_{:d}'.format(i)
                recipe.add('cab/msutils', step,
                           {
                               "msname": msname,
                               "command": 'summary',
                               "display": False,
                               "outfile": '{0:s}-obsinfo.json'.format(msroot),
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{0:s}:: Get observation information as a json file ms={1:s}'.format(step, msname))

          #  if config['obsinfo']["plot_elevation_tracks"]:
          #      step = "elevation_plots_{:d}".format(i)
          #      recipe.add("cab/casa_plotms", step, {
          #              "vis" : msname,
          #              "xaxis" : "hourangle",
          #              "yaxis" : "elevation",
          #              "coloraxis" : "field",
          #              "plotfile": "{:s}_elevation-tracks_{:d}.png".format(prefix, i),
          #              "overwrite" : True,
          #          },
          #              input=pipeline.input,
          #              output=pipeline.diagnostic_plots,
          #              label="{:s}:: Plotting elevation tracks".format(step))

            if config['obsinfo'].get('vampirisms'):
                step = 'vampirisms_{0:d}'.format(i)
                recipe.add('cab/sunblocker', step,
                           {
                               "command": 'vampirisms',
                               "inset": msname,
                               "dryrun": True,
                               "nononsoleil": True,
                               "verb": True,
                           },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Note sunrise and sunset'.format(step))

            if pipeline.enable_task(config['obsinfo'], 'plot_elevation_tracks'):
                step = "elevation_plots_{:d}".format(i)
                if config['obsinfo']["plot_elevation_tracks"].get("plotter") in ["plotms"]:
                    recipe.add("cab/casa_plotms", step, {
                               "vis" : msname,
                               "xaxis" : "hourangle",
                               "yaxis" : "elevation",
                               "coloraxis" : "field",
                               "plotfile": "{:s}_elevation-tracks_{:d}.png".format(prefix, i),
                               "overwrite" : True,
                               },
                               input=pipeline.input,
                               output=pipeline.diagnostic_plots,
                               label="{:s}:: Plotting elevation tracks".format(step))
                elif config['obsinfo']["plot_elevation_tracks"].get("plotter") in ["owlcat"]:
                    recipe.add("cab/owlcat_plotelev", step, {
                               "msname" : msname,
                               "output-name" : "{:s}_elevation-tracks_{:d}.png".format(prefix, i)
                               },
                               input=pipeline.input,
                               output=pipeline.diagnostic_plots,
                               label="{:s}:: Plotting elevation tracks".format(step))

        recipe.run()
        recipe.jobs = []

    # initialse things
    for item in 'xcal fcal bpcal gcal target reference_antenna'.split():
        val = config.get(item)
        for attr in ["", "_ra", "_dec", "_id"]:
            setattr(pipeline, item+attr, repeat_val(val, pipeline.nobs))

    setattr(pipeline, 'nchans', repeat_val(None,pipeline.nobs))
    setattr(pipeline, 'firstchanfreq', repeat_val(None, pipeline.nobs))
    setattr(pipeline, 'lastchanfreq', repeat_val(None, pipeline.nobs))
    setattr(pipeline, 'chanwidth', repeat_val(None, pipeline.nobs))
    setattr(pipeline, 'specframe', repeat_val(None, pipeline.nobs))
    setattr(pipeline, 'startdate', repeat_val(None, pipeline.nobs))
    setattr(pipeline, 'enddate', repeat_val(None, pipeline.nobs))

    # Set antenna properties
    pipeline.Tsys_eta = config.get('Tsys_eta')
    pipeline.dish_diameter = config.get('dish_diameter')

    for i, prefix in enumerate(prefixes):
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, pipeline.dataid[i])
        caracal.log.info('Extracting info from {2:s} and (if present, and only for the purpose of automatically setting the reference antenna) the metadata file {0:s}/{1:s}-obsinfo.json'.format(
            pipeline.data_path, pipeline.dataid[i], msinfo))
        msname = msnames[i]
        # get the  actual date stamp for the start and end of the observations.
        # This info appears to not be present in the json file just the totals and start times (without slew times) so we'll get it from the txt file
        with open('{0:s}/{1:s}-obsinfo.txt'.format(pipeline.output, pipeline.dataid[i]), 'r') as stdr:
            content = stdr.readlines()
        for line in content:
            info_on_line = [x for x in line.split() if x != '']
            if len(info_on_line) > 2:
                if info_on_line[0].lower() == 'observed' and info_on_line[1].lower() == 'from':
                    calender_month_abbr = ['jan', 'feb', 'mar', 'apr', 'may','jun', 'jul', 'aug', 'sep', 'oct', 'nov',
                                           'dec']
                    startdate,starttime =info_on_line[2].split('/')
                    hr,min,sec = starttime.split(':')
                    day,month_abbr,year = startdate.split('-')
                    month_num = '{:02d}'.format(calender_month_abbr.index(month_abbr.lower())+1)
                    correct_date = ''.join([year,month_num,day,hr,min,sec])
                    pipeline.startdate[i] = float(correct_date)
                    enddate,endtime =info_on_line[4].split('/')
                    hr,min,sec = endtime.split(':')
                    day,month_abbr,year = enddate.split('-')
                    month_num = '{:02d}'.format(calender_month_abbr.index(month_abbr.lower())+1)
                    correct_date = ''.join([year,month_num,day,hr,min,sec])
                    pipeline.enddate[i] = float(correct_date)

        # get reference antenna
        if config.get('reference_antenna') == 'auto':
            msmeta = '{0:s}/{1:s}-obsinfo.json'.format(
                pipeline.data_path, pipeline.dataid[i])
            if path.exists(msmeta):
                pipeline.reference_antenna[i] = utils.meerkat_refant(msmeta)
                caracal.log.info('Auto selecting reference antenna as {:s}'.format(
                    pipeline.reference_antenna[i]))
            else:
                caracal.log.error(
                    'Cannot auto select reference antenna because the metadata file {0:s}, which should have been provided by the observatory, does not exist.'.format(msmeta))
                caracal.log.error(
                    'Note that this metadata file is generally available only for MeerKAT-16/ROACH2 data.')
                caracal.log.error(
                    'Please set the reference antenna manually in the config file and try again.')
                raise caracal.ConfigurationError("can't auto-select the reference antenna")

        # Get channels in MS
        with open(msinfo, 'r') as stdr:
            msdict = yaml.safe_load(stdr)
            spw = msdict['SPW']['NUM_CHAN']
            pipeline.nchans[i] = spw
        caracal.log.info('MS has {0:d} spectral windows, with NCHAN={1:s}'.format(
            len(spw), ','.join(map(str, spw))))

        # Get first chan, last chan, chan width
        chfr = msdict['SPW']['CHAN_FREQ']
        firstchanfreq = [ss[0] for ss in chfr]
        lastchanfreq = [ss[-1] for ss in chfr]
        chanwidth = [(ss[-1]-ss[0])/(len(ss)-1) for ss in chfr]
        pipeline.firstchanfreq[i] = firstchanfreq
        pipeline.lastchanfreq[i] = lastchanfreq
        pipeline.chanwidth[i] = chanwidth
        caracal.log.info('CHAN_FREQ from {0:s} Hz to {1:s} Hz with average channel width of {2:s} Hz'.format(
                ','.join(map(str, firstchanfreq)), ','.join(map(str, lastchanfreq)), ','.join(map(str, chanwidth))))
        if i == len(prefixes)-1 and np.max(pipeline.chanwidth) > 0 and np.min(pipeline.chanwidth) < 0:
            caracal.log.err('Some datasets have a positive channel increment, some negative. This will lead to errors. Exiting')
            raise caracal.BadDataError("MSs with mixed channel ordering not supported")

        # Get spectral frame
        with open(msinfo, 'r') as stdr:
            pipeline.specframe[i] = yaml.safe_load(
                stdr)['SPW']['MEAS_FREQ_REF']

        with open(msinfo, 'r') as stdr:
            targetinfo = yaml.safe_load(stdr)['FIELD']

        intents = utils.categorize_fields(msinfo)
        # Save all fields in a list
        all_fields = msdict["FIELD"]["NAME"]
        # The order of fields here is important
        for term in "target gcal fcal bpcal xcal".split():
            conf_fields = getattr(pipeline, term)[i]
            label, fields = intents[term]
            label = ",".join(label)
            # check if user set fields manually
            if set(all_fields).intersection(conf_fields):
                label = term
                if term == 'target':
                    pipeline.target[i] = [value for value in getattr(pipeline, term)[i] if value in all_fields]
            elif fields in [None, []]:
                getattr(pipeline, term)[i] = []
                continue
            elif "all" in conf_fields:
                getattr(pipeline, term)[i] = fields
            elif "longest" in conf_fields:
                f = utils.observed_longest(msinfo, fields)
                getattr(pipeline, term)[i] = [f]
            elif "nearest" in conf_fields:
                f = utils.set_gcal(msinfo, fields, mode="nearest")
                getattr(pipeline, term)[i] = [f]
            else:
                raise RuntimeError("Could not find field/selection {0}."\
                        " Please check the [observation_config.{1}] "\
                        "section of the config file".format(conf_fields, term))

            caracal.log.info("====================================")
            caracal.log.info(label)
            caracal.log.info(" ---------------------------------- ")
            _ra = []
            _dec = []
            _fid = []
            for f in getattr(pipeline, term)[i]:
                fid = utils.get_field_id(msinfo, f)[0]
                targetpos = targetinfo['REFERENCE_DIR'][fid][0]
                ra = targetpos[0]/np.pi*180
                dec = targetpos[1]/np.pi*180
                _ra.append(ra)
                _dec.append(dec)
                _fid.append(fid)
                tobs = utils.field_observation_length(msinfo, f)/60.0
                caracal.log.info(
                        '{0:s} (ID={1:d}) : {2:.2f} minutes | RA={3:.2f} deg, Dec={4:.2f} deg'.format(f, fid, tobs, ra, dec))
            getattr(pipeline, term+"_ra")[i] = _ra
            getattr(pipeline, term+"_dec")[i] = _dec
            getattr(pipeline, term+"_id")[i] = _fid

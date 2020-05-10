# -*- coding: future_fstrings -*-
import caracal.dispatch_crew.utils as utils
import yaml
import caracal
import sys
import numpy as np
from os import path

NAME = 'Automatically Categorize Observed Fields'
LABEL = 'obsconf'

def repeat_val(val, n):
    l = []
    for x in range(n):
        l.append(val)
    return l

def worker(pipeline, recipe, config):
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
            if config['obsinfo']['listobs']:
                step = 'listobs-ms{:d}'.format(i)
                recipe.add('cab/casa_listobs', step,
                           {
                               "vis": msname,
                               "listfile": '{0:s}-obsinfo.txt'.format(msroot),
                               "overwrite": True,
                           },
                           input=pipeline.input,
                           output=pipeline.obsinfo,
                           label='{0:s}:: Get observation information ms={1:s}'.format(step, msname))

            if config['obsinfo']['summary_json']:
                step = 'summary_json-ms{:d}'.format(i)
                recipe.add('cab/msutils', step,
                           {
                               "msname": msname,
                               "command": 'summary',
                               "display": False,
                               "outfile": '{0:s}-obsinfo.json'.format(msroot),
                           },
                           input=pipeline.input,
                           output=pipeline.obsinfo,
                           label='{0:s}:: Get observation information as a json file ms={1:s}'.format(step, msname))

            if config['obsinfo']['vampirisms']:
                step = 'vampirisms-ms{0:d}'.format(i)
                recipe.add('cab/sunblocker', step,
                           {
                               "command": 'vampirisms',
                               "inset": msname,
                               "dryrun": True,
                               "nononsoleil": True,
                               "verb": True,
                           },
                       input=pipeline.input,
                       output=pipeline.obsinfo,
                       label='{0:s}:: Note sunrise and sunset'.format(step))

            if pipeline.enable_task(config['obsinfo'], 'plotelev'):
                step = "elevation_plots-ms{:d}".format(i)
                if config['obsinfo']["plotelev"]["plotter"] in ["plotms"]:
                    recipe.add("cab/casa_plotms", step, {
                               "vis" : msname,
                               "xaxis" : "hourangle",
                               "yaxis" : "elevation",
                               "coloraxis" : "field",
                               "plotfile": "{:s}_elevation-tracks_{:d}.png".format(prefix, i),
                               "overwrite" : True,
                               },
                               input=pipeline.input,
                               output=pipeline.obsinfo,
                               label="{:s}:: Plotting elevation tracks".format(step))
                elif config['obsinfo']["plotelev"]["plotter"] in ["owlcat"]:
                    recipe.add("cab/owlcat_plotelev", step, {
                               "msname" : msname,
                               "output-name" : "{:s}_elevation-tracks_{:d}.png".format(prefix, i)
                               },
                               input=pipeline.input,
                               output=pipeline.obsinfo,
                               label="{:s}:: Plotting elevation tracks".format(step))

        recipe.run()
        recipe.jobs = []

    # initialse things
    for item in 'xcal fcal bpcal gcal target refant'.split():
        val = config[item]
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
    #pipeline.Tsys_eta = config['Tsys_eta']
    #pipeline.dish_diameter = config['dish_diameter']

    for i, prefix in enumerate(prefixes):
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.obsinfo, pipeline.dataid[i])
        caracal.log.info('Extracting MS info from {0:s} '.format(msinfo))
        msname = msnames[i]
        # get the  actual date stamp for the start and end of the observations.
        # This info appears to not be present in the json file just the totals and start times (without slew times) so we'll get it from the txt file
        with open(msinfo, 'r') as stdr:
            content = stdr.readlines()
        for line in content:
            info_on_line = [x for x in line.split() if x != '']
            if len(info_on_line) > 2:
                if info_on_line[0].lower() == 'observed' and info_on_line[1].lower() == 'from':
                    calender_month_abbr = ['jan', 'feb', 'mar', 'apr', 'may','jun', 'jul', 'aug', 'sep', 'oct', 'nov',
                                           'dec']
                    startdate,starttime =info_on_line[2].split('/')
                    hr,minute,sec = starttime.split(':')
                    day,month_abbr,year = startdate.split('-')
                    month_num = '{:02d}'.format(calender_month_abbr.index(month_abbr.lower())+1)
                    correct_date = ''.join([year,month_num,day,hr,minute,sec])
                    pipeline.startdate[i] = float(correct_date)
                    enddate,endtime =info_on_line[4].split('/')
                    hr,minute,sec = endtime.split(':')
                    day,month_abbr,year = enddate.split('-')
                    month_num = '{:02d}'.format(calender_month_abbr.index(month_abbr.lower())+1)
                    correct_date = ''.join([year,month_num,day,hr,minute,sec])
                    pipeline.enddate[i] = float(correct_date)

        # get reference antenna LEAVING THIS LINE HERE
        # FOR WHEN WE COME UP WITH A WAY TO AUTOSELECT
        #if config.get('refant') == 'auto':
        #    pipeline.refant[i] = '0'

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

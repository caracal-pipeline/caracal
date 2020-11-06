# -*- coding: future_fstrings -*-
import caracal.dispatch_crew.utils as utils
import caracal
import sys
import numpy as np
import os
import shutil

from caracal import log

NAME = 'Automatically Categorize Observed Fields'
LABEL = 'obsconf'

def repeat_val(val, n):
    l = []
    for x in range(n):
        l.append(val)
    return l

def worker(pipeline, recipe, config):
    recipe.msdir = pipeline.rawdatadir
    recipe.output = pipeline.msdir
    step = None

    for i, (msname, msroot, prefix) in enumerate(zip(pipeline.msnames, pipeline.msbasenames, pipeline.prefix_msbases)):
        # filenames generated
        obsinfo  = f'{msroot}-obsinfo.txt'
        summary  = f'{msroot}-summary.json'
        elevplot = f'{msroot}-elevation-tracks.png'

        if pipeline.enable_task(config, 'obsinfo'):
            if config['obsinfo']['listobs']:
                if os.path.exists(os.path.join(pipeline.msdir, obsinfo)):
                    caracal.log.info(f"obsinfo file {obsinfo} exists, not regenerating")
                else:
                    step = f'listobs-ms{i}'
                    recipe.add('cab/casa_listobs', step,
                               {
                                   "vis": msname,
                                   "listfile": obsinfo,
                                   "overwrite": True,
                               },
                               input=pipeline.input,
                               output=pipeline.msdir,
                               label='{0:s}:: Get observation information ms={1:s}'.format(step, msname))

            if config['obsinfo']['summary_json']:
                if os.path.exists(os.path.join(pipeline.msdir, summary)):
                    caracal.log.info(f"summary file {summary} exists, not regenerating")
                else:
                    step = f'summary_json-ms{i}'
                    recipe.add('cab/msutils', step,
                               {
                                   "msname": msname,
                                   "command": 'summary',
                                   "display": False,
                                   "outfile": summary,
                               },
                               input=pipeline.input,
                               output=pipeline.msdir,
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
                       output=pipeline.msdir,
                       label='{0:s}:: Note sunrise and sunset'.format(step))

            if pipeline.enable_task(config['obsinfo'], 'plotelev'):
                if os.path.exists(os.path.join(pipeline.msdir, elevplot)):
                    caracal.log.info(f"elevation plot {elevplot} exists, not regenerating")
                else:
                    step = "elevation-plots-ms{:d}".format(i)
                    if config['obsinfo']["plotelev"]["plotter"] in ["plotms"]:
                        recipe.add("cab/casa_plotms", step, {
                                   "vis" : msname,
                                   "xaxis" : "hourangle",
                                   "yaxis" : "elevation",
                                   "coloraxis" : "field",
                                   "plotfile": elevplot,
                                   "overwrite" : True,
                                   },
                                   input=pipeline.input,
                                   output=pipeline.msdir,
                                   label="{:s}:: Plotting elevation tracks".format(step))
                    elif config['obsinfo']["plotelev"]["plotter"] in ["owlcat"]:
                        recipe.add("cab/owlcat_plotelev", step, {
                                   "msname" : msname,
                                   "output-name" : elevplot
                                   },
                                   input=pipeline.input,
                                   output=pipeline.msdir,
                                   label="{:s}:: Plotting elevation tracks".format(step))

    # if any steps at all were inserted, run the recipe
    if step is not None:
        recipe.run()
        recipe.jobs = []

    # initialse things
    for item in 'xcal fcal bpcal gcal target refant minbase maxdist'.split():
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

    for i, (msname, msroot, prefix) in enumerate(zip(pipeline.msnames, pipeline.msbasenames, pipeline.prefix_msbases)):
        caracal.log.info(f"MS #{i}: {msname}")

        msdict = pipeline.get_msinfo(msname)
        obsinfo  = f'{msroot}-obsinfo.txt'
        summary  = f'{msroot}-summary.json'
        elevplot = f'{msroot}-elevation-tracks.png'

        # copy these to obsinfo dir if needed
        for filename in obsinfo, summary, elevplot:
            src, dest = os.path.join(pipeline.msdir, filename), os.path.join(pipeline.obsinfo, filename)
            if os.path.exists(src) and (not os.path.exists(dest) or os.path.getmtime(dest) < os.path.getmtime(src)):
                caracal.log.info(f"generated new obsinfo/{filename}")
                shutil.copy2(src, dest)

        # get the  actual date stamp for the start and end of the observations.
        # !!!!!!! This info appears to not be present in the json file just the totals and start times (without slew times) so we'll get it from the txt file
        with open(os.path.join(pipeline.msdir, obsinfo), 'r') as stdr:
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
        spw = msdict['SPW']['NUM_CHAN']
        pipeline.nchans[i] = spw
        caracal.log.info('  {0:d} spectral windows, with NCHAN={1:s}'.format(
            len(spw), ','.join(map(str, spw))))

        # Get first chan, last chan, chan width
        chfr = msdict['SPW']['CHAN_FREQ']
        firstchanfreq = [ss[0] for ss in chfr]
        lastchanfreq = [ss[-1] for ss in chfr]
        chanwidth = [(ss[-1]-ss[0])/(len(ss)-1) for ss in chfr]
        pipeline.firstchanfreq[i] = firstchanfreq
        pipeline.lastchanfreq[i] = lastchanfreq
        pipeline.chanwidth[i] = chanwidth
        caracal.log.info('  CHAN_FREQ from {0:s} Hz to {1:s} Hz with average channel width of {2:s} Hz'.format(
                ','.join(map(str, firstchanfreq)), ','.join(map(str, lastchanfreq)), ','.join(map(str, chanwidth))))
        if i == pipeline.nobs-1 and np.max(pipeline.chanwidth) > 0 and np.min(pipeline.chanwidth) < 0:
            caracal.log.err('Some datasets have a positive channel increment, some negative. This will lead to errors. Exiting')
            raise caracal.BadDataError("MSs with mixed channel ordering not supported")

        # Get spectral frame
        pipeline.specframe[i] = msdict['SPW']['MEAS_FREQ_REF']

        targetinfo = msdict['FIELD']

        intents = utils.categorize_fields(msdict)
        # Save all fields in a list
        all_fields = msdict["FIELD"]["NAME"]
        # The order of fields here is important
        for term in "target gcal fcal bpcal xcal".split():
            conf_fields = getattr(pipeline, term)[i]
            conf_fields_str = ','.join(conf_fields)
            label, fields = intents[term]
            label = ",".join(label) or term
            #  no fields set for this term -- make empty and continue
            if not conf_fields:
                found_fields = []
                # check if user set fields manually
            elif conf_fields_str == "all":
                found_fields = fields
            elif conf_fields_str == "longest":
                f = utils.observed_longest(msdict, fields)
                found_fields = [f] if f else []
            elif conf_fields_str == "nearest":
                f = utils.select_gcal(msdict, fields, mode="nearest")
                found_fields = [f] if f else []
            else:
                found_fields = set(all_fields).intersection(conf_fields)

            getattr(pipeline, term)[i] = list(found_fields)
            if not found_fields:
            # complain if not found, unless it's 'xcal', which is only for polcal, so let that worker complain
                if term != "xcal":
                    raise RuntimeError(f"Can't find an appropriate FIELD for obsinfo: {term}: {conf_fields_str}. "
                                        "Please check this config setting. It may also be that your MS scan intents "
                                        f"are not pupulated correctly, in which case you must set {term} to a list of explicit field names.")
                continue

#            caracal.log.info("    ====================================")
            caracal.log.info(f"  {term} ({label}):")
#            caracal.log.info("     ---------------------------------- ")
            _ra = []
            _dec = []
            _fid = []
            for f in found_fields:
                fid = utils.get_field_id(msdict, f)[0]
                targetpos = targetinfo['REFERENCE_DIR'][fid][0]
                ra = targetpos[0]/np.pi*180
                dec = targetpos[1]/np.pi*180
                _ra.append(ra)
                _dec.append(dec)
                _fid.append(fid)
                tobs = utils.field_observation_length(msdict, f)/60.0
                caracal.log.info(
                        '    {0:s} (ID={1:d}) : {2:.2f} minutes | RA={3:.2f} deg, Dec={4:.2f} deg'.format(f, fid, tobs, ra, dec))
            getattr(pipeline, term+"_ra")[i] = _ra
            getattr(pipeline, term+"_dec")[i] = _dec
            getattr(pipeline, term+"_id")[i] = _fid

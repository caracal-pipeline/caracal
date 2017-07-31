import stimela
import os
import sys
import glob
sys.path.append('.')
import scripts.sunblocker as sunblocker
#import byo



#################
### I/O SETUP ###
#################

INPUT = 'input'
OUTPUT = 'output'
MSDIR = 'msdir'



###########################
### READ PARAMETER FILE ###
###########################

f=open('meerkathi-parameters.par')
pars=f.readlines()
pars=[jj.strip().replace(' ','') for jj in pars]
jj=0
while jj<len(pars):
    if not len(pars[jj]): del(pars[jj])
    elif pars[jj][0]=='#': del(pars[jj])
    else:
        if pars[jj][-1]==';': pars[jj]=pars[jj][:-1]
        jj+=1
pars=[jj.replace("'","").replace('"','').split('=') for jj in pars]
pars={jj[0]:jj[1] for jj in pars}
f.close()



######################
### SET FILE NAMES ###
######################

dataids = pars['dataids'].split(',')
h5files = ['{:s}.h5'.format(dataid) for dataid in dataids]
msnames = ['{:s}.ms'.format(os.path.basename(dataid)) for dataid in dataids]
split_msnames = ['{:s}_split.ms'.format(os.path.basename(dataid)) for dataid in dataids]
cal_msnames = ['{:s}_cal.ms'.format(os.path.basename(dataid)) for dataid in dataids]
prefixes = ['meerkathi-{:s}'.format(os.path.basename(dataid)) for dataid in dataids]
combprefix = pars['combprefix']



############################################
### PRELIMINARY STEPS (TO BE CLEANED UP) ###
############################################

# This changes the value of variables to whatever was set through the 
# '-g/--globals' option in the 'stimela run' command
stimela.register_globals()

# Fields
target=pars['target']
bpcal=pars['bpcal']
gcal=pars['gcal']

# Initialise recipe
recipe = stimela.Recipe('MeerKATHI pipeline', ms_dir=MSDIR)
steps2run=[]

# Check that the MS has been or will be split if any of 2GC, CONTSUB or HI_IMAGING are switched on
if pars['RUN_2GC'].lower() in ['yes', 'true', '1'] or pars['RUN_CONTSUB'].lower() in ['yes', 'true', '1'] or pars['RUN_HI_IMAGING'].lower() in ['yes', 'true', '1']:
    split_error,missing_files=False,[]
    for split_msname in split_msnames:
        if not os.path.exists('{0:s}/{1:s}'.format(MSDIR, split_msname)):
            if not pars['RUN_1GC'].lower() in ['yes', 'true', '1'] or (pars['RUN_1GC'].lower() in ['yes', 'true', '1'] and not pars['SPLIT_AND_TAV'].lower() in ['yes', 'true', '1']):
                split_error=True
                missing_files.append('{0:s}/{1:s}'.format(MSDIR, split_msname))
    if split_error==True:
        print 
        print '#############'
        print '# FATAL ERROR'
        print '#   You have requested to run at least one of the modules: 2GC, CONTSUB, HI_IMAGING.'
        print '#   These modules work on .MS files containing only the target (optionally time-averaged)'
        print '#   Unfortunately, the following such files do not exist:'
        for ff in missing_files: print '#    ',ff
        print '#   To create them please switch on the 1GC module and, within it, the SPLIT_AND_TAV task.'
        sys.exit()



#print
#print '###########################'
#print '### Start BYO functions ###'
#print '###########################'
#print

#spectrum=byo.get_spec(msnames,msdir=MSDIR)
#print spectrum.shape

#print 
#print '#########################'
#print '### End BYO functions ###'
#print '#########################'
#print



##################
### PREPARE MS ###
##################

# This will:
# - delete old MS (optional)
# - convert HDF5 to MS (if MS file does not exist)
# - fix UVW (optional)
# - write listobs log to file (optional)
# - add corrected_data and model columns (optional)

if pars['RUN_PREPMS'].lower() in ['yes', 'true', '1']:

    # Delete MS if user wants to do this
    if pars['REMOVE_MS'].lower() in ['yes', 'true', '1']:
        print 
        print '######################################'
        print '# Deleting old MS files as requested #'
        print '######################################'
        print
        for msname in msnames:
            if os.path.exists('{0:s}/{1:s}'.format(MSDIR, msname)):
                print '{0:s}/{1:s}'.format(MSDIR, msname)
                os.system('rm -fr {0:s}/{1:s}'.format(MSDIR, msname))
            else: print '{0:s}/{1:s} does not exist'.format(MSDIR, msname)
        print
        print '########################'
        print '# Old MS files deleted #'
        print '########################'
        print 

    # Convert HDF5 to MS if MS file does not exist
    for i, (h5file,msname) in enumerate(zip(h5files, msnames)):
        if not os.path.exists('{0:s}/{1:s}'.format(MSDIR, msname)):
            chani,chanf=pars['channel_range'].split(',')
            print chani+','+chanf
            recipe.add('cab/h5toms', 'h5toms_{:d}'.format(i),
                {
                  "hdf5files"     :    h5file,
                  "channel-range" :    chani+','+chanf,
                  "no-auto"       :    False,
                  "output-ms"     :    msname,
                  "full-pol"      :    True,
                  "tar"           :    True,
                  "model-data"    :    True,
                },
                input='/var/kat/archive2/data/MeerKATAR1/telescope_products',
                output=MSDIR,
                label='h5toms_{0:d}:: Convert from h5 to ms={1:s}'.format(i, msname))
            steps2run.append('h5toms_{0:d}'.format(i))
    
    # Fix UVW coorindates
    if pars['FIX_UVW'].lower() in ['yes', 'true', '1']:
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_fixvis', 'fix_uvw_coords_{:d}'.format(i), 
                {
                  "vis"       :    msname,
                  "reuse"     :    False,
                  "outputvis" :    msname,
                },
                input=INPUT,
                output=OUTPUT,
                label='fix_uvw_{0:d}:: Fix UVW coordinates for ms={1:s}'.format(i, msname))
            steps2run.append('fix_uvw_{:d}'.format(i))

    # Write observational set up to log file
    if pars['WRITE_LISTOBS'].lower() in ['yes', 'true', '1']:
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_listobs', 'obsinfo_{:d}'.format(i), 
                {
                  "vis"       :    msname,
                  "listfile"  :    prefix+'-listobs.txt',
                  "overwrite" :  True,
                },
                input=INPUT,
                output=OUTPUT,
                label='get_obsinfo_{0:d}:: Get observation information ms={1:s}'.format(i, msname))
            steps2run.append('get_obsinfo_{:d}'.format(i))

    # Add corrected and model data columns
    if pars['ADD_COLUMNS'].lower() in ['yes', 'true', '1']:
        for i, msname in enumerate(msnames):
            recipe.add('cab/msutils', 'data2corrdata{:d}'.format(i),
                {
                  "msname"          :   msname,
                  "command"         :   'copycol',
                  "fromcol"         :   'DATA',
                  "tocol"           :   'CORRECTED_DATA',
                },
                input=INPUT,
                output=OUTPUT,
                label='data2corrdata_{0:d}:: Copy DATA to CORRECTED_DATA column ms={1:s}'.format(i, msname))
            steps2run.append('data2corrdata_{:d}'.format(i))

        for i, msname in enumerate(msnames):
            recipe.add('cab/msutils', 'data2modeldata{:d}'.format(i),
                {
                  "msname"          :   msname,
                  "command"         :   'copycol',
                  "fromcol"         :   'DATA',
                  "tocol"           :   'MODEL_DATA',
                },
                input=INPUT,
                output=OUTPUT,
                label='data2modeldata_{0:d}:: Copy DATA to MODEL_DATA column ms={1:s}'.format(i, msname))
            steps2run.append('data2modeldata_{:d}'.format(i))



#################
### FLAG DATA ###
#################

# This will:
# - flag autocorrelations (optional)
# - flag channels with Milky-Way HI emission (optional)
# - flag with AOflagger (optional)

if pars['RUN_FLAGGING'].lower() in ['yes', 'true', '1']:
    #Flag autocorrelations
    if pars['FLAG_AUTOCORR'].lower() in ['yes', 'true', '1']:
        for i, msname in enumerate(msnames):
            recipe.add('cab/casa_flagdata','flagautocorr_{:d}'.format(i),
                {
                  "vis"           :   msname,
                  "mode"          :   'manual',
                  "autocorr"      :   True,
                },
                input=INPUT,
                output=OUTPUT,
                label='flagautocorr_{0:d}::Flag out channels with emission from Milky Way'.format(i, msname))
            steps2run.append('flagautocorr_{0:d}'.format(i, msname))

    # Flag Milky Way HI channels
    if pars['FLAG_MWHI'].lower() in ['yes', 'true', '1']:
        for i, msname in enumerate(msnames):
            recipe.add('cab/casa_flagdata','flagmw_{:d}'.format(i),
                {
                  "vis"           :   msname,
                  "mode"          :   'manual',
                  "spw"           :   pars['mw_hi_channels'],
                },
                input=INPUT,
                output=OUTPUT,
                label='flagmw_{0:d}::Flag out channels with HI emission from Milky Way'.format(i, msname))
            steps2run.append('flagmw_{0:d}'.format(i, msname))

    # Flag with AOflagger
    if pars['AOFLAG'].lower() in ['yes', 'true', '1']:
        recipe.add('cab/autoflagger', 'aoflag_1',
            {
              "msname"       :   msnames,
              "column"       :   'DATA',
              "strategy"     :   pars['aoflag_strat1'],
            },
            input=INPUT,
            output=OUTPUT,
            label='aoflag_1:: Aoflagger flagging pass 1')
        steps2run.append('aoflag_1')



###############################
### 1GC (CROSS) CALIBRATION ###
###############################

# This will:
# - calibrate the delays (optional)
# - set bandpass model (optional)
# - calibrate bandpass and gains using the bandpass calibrator (optional)
# - calibrate the gains using the phase calibrator (optional)
# - transfer the flux scale (optional)
# - apply the calibration to all fields (optional)
# - split the target and time average (optional)
# - make several calibration diagnostic plots (optional)

if pars['RUN_1GC'].lower() in ['yes', 'true', '1']:
    #Delay calibration
    if pars['DELAY_CAL'].lower() in ['yes', 'true', '1']:
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_gaincal','delay_calibration_{:d}'.format(i),
               {
                 "vis"          :  msname,
                 "caltable"     :  prefix+".K0",
                 "field"        :  bpcal,
                 "refant"       :  pars['refant_delay'],
                 "solint"       :  "inf",
                 "gaintype"     :  "K",
                 "uvrange"      :  pars['uvrange'],
               },
               input=INPUT,
               output=OUTPUT,
               label='delay_calibration_{:d}:: Delay calibration'.format(i,msname))
            steps2run.append('delay_calibration_{:d}'.format(i,msname))

    # Setjansky
    if pars['SET_MODEL'].lower() in ['yes', 'true', '1']:
        for i, msname in enumerate(msnames):
            recipe.add('cab/casa_setjy','setjansky_{:d}'.format(i),
               {
                  "vis"         :  msname,
                  "field"       :  bpcal,
                  "standard"    :  "Perley-Taylor 99",
                  "usescratch"  :  False,
                  "scalebychan" :  True,
               },
               input=INPUT,
               output=OUTPUT,
               label='setjansky_{0:d}:: Set jansky'.format(i,msname))
            steps2run.append('setjansky_{0:d}'.format(i,msname))

    # Bandpass
    if pars['BP_CAL'].lower() in ['yes', 'true', '1']:
        #Set "Combine" to 'scan' for getting combining all scans for BP soln.
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_bandpass','bandpass_calibration_{:d}'.format(i),
               {
                 "vis"          :  msname,
                 "caltable"     :  prefix+".B0",
                 "field"        :  bpcal,
                 "refant"       :  pars['refant'],
                 "solint"       :  "inf",
                 "combine"      :  pars['combine'],
                 "bandtype"     :  "B",
                 "gaintable"    :  [prefix+".K0:output"],
                 "fillgaps"     :  70,
                 "uvrange"      :  pars['uvrange'],
                 "minsnr"       :  float(pars['minsnr']),
                 "minblperant"  :  int(pars['minnrbl']),
                 "solnorm"      :  pars['solnorm'].lower() in ['yes', 'true', '1'] ,
               },
               input=INPUT,
               output=OUTPUT,
               label='bandpass_calibration_{:d}:: Bandpass calibration'.format(i,msname)) 
            steps2run.append('bandpass_calibration_{:d}'.format(i,msname))

        # Gain calibration for Bandpass field
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_gaincal','gain_calibration_bp_{:d}'.format(i),
               {
                 "vis"          :  msname,
                 "caltable"     :  prefix+".G0:output",
                 "field"        :  bpcal,
                 "refant"       :  pars['refant'],
                 "solint"       :  "inf",
                 "combine"      :  pars['combine'],
                 "gaintype"     :  "G",
                 "calmode"      :   'ap',
                 "gaintable"    :  [prefix+".B0:output",prefix+".K0:output"],
                 "interp"       :  ['nearest','nearest'],
                 "uvrange"      :  pars['uvrange'],
                 "minsnr"       :  float(pars['minsnr']),
                 "minblperant"  :  int(pars['minnrbl']),
                 "append"       :  False
               },
               input=INPUT,
               output=OUTPUT,
               label='gain_calibration_bp_{:d}:: Gain calibration'.format(i,msname))
            steps2run.append('gain_calibration_bp_{:d}'.format(i,msname))

    # Gain calibration for Gaincal field
    if pars['G_CAL'].lower() in ['yes', 'true', '1']:
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_gaincal','gain_calibration_g_{:d}'.format(i),
               {
                 "vis"          :  msname,
                 "caltable"     :  prefix+".G0:output",
                 "field"        :  gcal,
                 "refant"       :  pars['refant'],
                 "solint"       :  "inf",
                 "gaintype"     :  "G",
                 "calmode"      :  'ap',
                 "minsnr"       :  5,
                 "gaintable"    :  [prefix+".B0:output",prefix+".K0:output"],
                 "interp"       :  ['linear','linear'],
                 "append"       :  True,
                 "uvrange"      :  pars['uvrange'],
                 "minsnr"       :  float(pars['minsnr']),
                 "minblperant"  :  int(pars['minnrbl']),
               },
               input=INPUT,
               output=OUTPUT,
               label='gain_calibration_g_{:d}:: Gain calibration'.format(i,msname))
            steps2run.append('gain_calibration_g_{:d}'.format(i,msname))

    #Flux scale transfer
    if pars['TRANSF_FLUX'].lower() in ['yes', 'true', '1']:
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_fluxscale','fluxscale_{:d}'.format(i),
               {
                 "vis"          :  msname,
                "caltable"      :   prefix+".G0:output",
                "fluxtable"     :   prefix+".F0:output",
                "reference"     :   [bpcal],
                "transfer"      :   [gcal],
               },
               input=INPUT,       
               output=OUTPUT,
               label='fluxscale_{:d}:: Flux scale transfer'.format(i,msname))
            steps2run.append('fluxscale_{:d}'.format(i,msname))

    # Apply calibration tables to all fields
    if pars['APPLY_CAL'].lower() in ['yes', 'true', '1']:
        # to Bandpass field
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            gaintablelist,gainfieldlist,interplist=[],[],[]
            if   pars['apply_delay'].lower() in ['yes', 'true', '1']:
                gaintablelist.append(prefix+".K0:output")
                gainfieldlist.append(bpcal)
                interplist.append('nearest')
            if   pars['apply_bpass'].lower() in ['yes', 'true', '1']:
                gaintablelist.append(prefix+".B0:output")
                gainfieldlist.append(bpcal)
                interplist.append('nearest')
            if   pars['apply_gains'].lower() in ['yes', 'true', '1']:
                gaintablelist.append(prefix+".F0:output")
                gainfieldlist.append(bpcal)
                interplist.append('nearest')
            elif pars['apply_bpass'].lower() in ['yes', 'true', '1']:
                gaintablelist.append(prefix+".G0:output")
                gainfieldlist.append(bpcal)
                interplist.append('nearest')
            recipe.add('cab/casa_applycal','applycal_bp_{:d}'.format(i),
               {
                "vis"       :    msname,
                "field"     :   bpcal,
                "gaintable" :   gaintablelist,
                "gainfield" :   gainfieldlist,
                "interp"    :   interplist,
                "calwt"     :   [False],
                "parang"    :   False,
                "applymode" :   pars['applymode'],
               },
               input=INPUT,
               output=OUTPUT,
               label='applycal_bp_{:d}:: Apply calibration to bandpass field'.format(i,msname))
            steps2run.append('applycal_bp_{:d}'.format(i,msname))

        # to Gaincal field
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            gaintablelist,gainfieldlist,interplist=[],[],[]
            if   pars['apply_delay'].lower() in ['yes', 'true', '1']:
                gaintablelist.append(prefix+".K0:output")
                gainfieldlist.append(bpcal)
                interplist.append('linear')
            if   pars['apply_bpass'].lower() in ['yes', 'true', '1']:
                gaintablelist.append(prefix+".B0:output")
                gainfieldlist.append(bpcal)
                interplist.append('linear')
            if   pars['apply_gains'].lower() in ['yes', 'true', '1']:
                gaintablelist.append(prefix+".F0:output")
                gainfieldlist.append(gcal)
                interplist.append('nearest')
            elif pars['apply_bpass'].lower() in ['yes', 'true', '1']:
                gaintablelist.append(prefix+".G0:output")
                gainfieldlist.append(bpcal)
                interplist.append('linear')
            recipe.add('cab/casa_applycal','applycal_g_{:d}'.format(i),
               {
                "vis"       :    msname,
                "field"     :   gcal,
                "gaintable" :   gaintablelist,
                "gainfield" :   gainfieldlist,
                "interp"    :   interplist,
                "calwt"     :   [False],
                "parang"    :   False,
                "applymode" :   pars['applymode'],
               },
               input=INPUT,
               output=OUTPUT,
               label='applycal_g_{:d}:: Apply calibration to gaincal field'.format(i,msname))
            steps2run.append('applycal_g_{:d}'.format(i,msname))

        # to Target Field
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            gaintablelist,gainfieldlist,interplist=[],[],[]
            if   pars['apply_delay'].lower() in ['yes', 'true', '1']:
                gaintablelist.append(prefix+".K0:output")
                gainfieldlist.append(bpcal)
                interplist.append('linear')
            if   pars['apply_bpass'].lower() in ['yes', 'true', '1']:
                gaintablelist.append(prefix+".B0:output")
                gainfieldlist.append(bpcal)
                interplist.append('linear')
            if   pars['apply_gains'].lower() in ['yes', 'true', '1']:
                gaintablelist.append(prefix+".F0:output")
                gainfieldlist.append(gcal)
                interplist.append('linear')
            elif pars['apply_bpass'].lower() in ['yes', 'true', '1']:
                gaintablelist.append(prefix+".G0:output")
                gainfieldlist.append(bpcal)
                interplist.append('linear')
            recipe.add('cab/casa_applycal','applycal_tar_{:d}'.format(i),
               {
                "vis"       :    msname,
                "field"     :   target,
                "gaintable" :   gaintablelist,
                "gainfield" :   gainfieldlist,
                "interp"    :   interplist,
                "calwt"     :   [False],
                "parang"    :   False,
                "applymode" :   pars['applymode'],
               },
               input=INPUT,
               output=OUTPUT,
               label='applycal_tar_{:d}:: Apply calibration to gaincal field'.format(i,msname))
            steps2run.append('applycal_tar_{:d}'.format(i,msname))

    # Split and time average the target
    if pars['SPLIT_AND_TAV'].lower() in ['yes', 'true', '1']:
        for i, (msname, split_msname) in enumerate(zip(msnames, split_msnames)):
            recipe.add('cab/casa_split','split_avtime_{:d}'.format(i),
               {
                 "msname"          :   msname,
                 "output-msname"   :   split_msname,
                 "datacolumn"      :   "CORRECTED",
                 "field"           :   target,
                 "timebin"         :   pars['timebin'],
                 "keepflags"       :   True,
                },
                input=INPUT,
                output=OUTPUT,
                label='split_avtime_{:d}:: Split and time average the target'.format(i))
            steps2run.append('split_avtime_{:d}'.format(i))

    # Make plots
    if pars['MAKE_PLOTS'].lower() in ['yes', 'true', '1']:
        # Plot bandpass amplitude
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_plotcal','plot_bandpassA_{:d}'.format(i),
               {
                "caltable"  :   prefix+".B0:output",
                "xaxis"     :   'chan',
                "yaxis"     :   'amp',
                "field"     :   bpcal,
                "iteration" :   'antenna',
                "subplot"   :   441,
                "plotsymbol":   ',',
                "figfile"   :   prefix+'-B0-amp.png',
                "showgui"   :   False,
               },
               input=INPUT,
               output=OUTPUT,
               label='plot_bandpassA_{:d}:: Plot bandpass amplitude'.format(i,msname))
            steps2run.append('plot_bandpassA_{:d}'.format(i,msname))

        # Plot bandpass phase
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_plotcal','plot_bandpassP_{:d}'.format(i),
               {
                "caltable"  :   prefix+".B0:output",
                "xaxis"     :   'chan',
                "yaxis"     :   'phase',
                "field"     :   bpcal,
                "iteration" :   'antenna',
                "subplot"   :   441,
                "plotsymbol":   ',',
                "figfile"   :   prefix+'-B0-phase.png',
                "showgui"   :   False,
               },
               input=INPUT,
               output=OUTPUT,
               label='plot_bandpassP_{:d}:: Plot bandpass phase'.format(i,msname))
            steps2run.append('plot_bandpassP_{:d}'.format(i,msname))

        # Plot gain phase vs time
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_plotcal','plot_gaincalP_{:d}'.format(i),
               {
                "caltable"  :   prefix+".F0:output",
                "xaxis"     :   'time',
                "yaxis"     :   'phase',
                "field"     :    bpcal,
                "iteration" :   'antenna',
                "subplot"   :   441,
                "plotsymbol":   'o',
                "figfile"   :   prefix+'-F0-phase.png',
                "showgui"   :   False,
               },
               input=INPUT,
               output=OUTPUT,
               label='plot_gaincalP_{:d}:: Plot gaincal phase'.format(i,msname))
            steps2run.append('plot_gaincalP_{:d}'.format(i,msname))

        # Plot pre-fluxcal gain phase vs time
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_plotcal','plot_gaincalPG_{:d}'.format(i),
               {
                "caltable"  :   prefix+".G0:output",
                "xaxis"     :   'time',
                "yaxis"     :   'phase',
                "field"     :    bpcal,
                "iteration" :   'antenna',
                "subplot"   :   441,
                "plotsymbol":   'o',
                "figfile"   :   prefix+'-G0-phase.png',
                "showgui"   :   False,
               },
               input=INPUT,
               output=OUTPUT,
               label='plot_gaincalPG_{:d}:: Plot gaincal phase'.format(i,msname))
            steps2run.append('plot_gaincalPG_{:d}'.format(i,msname))

        # Plot gain amplitude vs time
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_plotcal','plot_gaincalA_{:d}'.format(i),
               {
                "caltable"  :   prefix+".F0:output",
                "xaxis"     :   'time',
                "yaxis"     :   'amp',
                "field"     :    bpcal,
                "iteration" :   'antenna',
                "subplot"   :   441,
                "plotsymbol":   'o',
                "figfile"   :   prefix+'-F0-amp.png',
                "showgui"   :   False,
               },
               input=INPUT,
               output=OUTPUT,
               label='plot_gaincalA_{:d}:: Plot gaincal amplitude'.format(i,msname))
            steps2run.append('plot_gaincalA_{:d}'.format(i,msname))

        # Plot pre-fluxcal gain amplitude vs time
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_plotcal','plot_gaincalAG_{:d}'.format(i),
               {
                "caltable"  :   prefix+".G0:output",
                "xaxis"     :   'time',
                "yaxis"     :   'amp',
                "field"     :    bpcal,
                "iteration" :   'antenna',
                "subplot"   :   441,
                "plotsymbol":   'o',
                "figfile"   :   prefix+'-G0-amp.png',
                "showgui"   :   False,
               },
               input=INPUT,
               output=OUTPUT,
               label='plot_gaincalAG_{:d}:: Plot gaincal amplitude'.format(i,msname))
            steps2run.append('plot_gaincalAG_{:d}'.format(i,msname))

        # Plot corrected real vs imag for bandpass field
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_plotms','plot_realimag_bp_{:d}'.format(i),
               {
                "vis"           :   msname,
                "field"         :   bpcal,
                "correlation"   :   'XX,YY',
                "timerange"     :   '',
                "antenna"       :   '',
                "xaxis"         :   'imag',
                "xdatacolumn"   :   'corrected',
                "yaxis"         :   'real',
                "ydatacolumn"   :   'corrected',
                "coloraxis"     :   'corr',
                "plotfile"      :   prefix+'-bpcal-reim.png',
                 "uvrange"      :   pars['uvrange'],
                "overwrite"     :   True,
               },
               input=INPUT,
               output=OUTPUT,
               label='plot_realimag_{:d}:: Plot imag vs real for bandpass calibrator'.format(i,msname))
            steps2run.append('plot_realimag_{:d}'.format(i,msname))



##############################
### 2GC (SELF) CALIBRATION ###
##############################

# This will:
# - frequency average the target (optional)
# - make a continuum image and selfcalibrate the gain phases

npix_cont   = int(pars['npix_cont'])
cell_cont   = int(pars['cell_cont'])
trim        = int(pars['trim'])
robust_cont = float(pars['robust_cont'])

robust      = float(pars['robust'])
npix        = int(pars['npix'])
cell        = float(pars['cell'])

if pars['RUN_2GC'].lower() in ['yes', 'true', '1']:

    # Prepare MS files for image+selfcal loop
    if pars['PREPARE_CONT_MS'].lower() in ['yes', 'true', '1']:
        for i, (split_msname, cal_msname) in enumerate(zip(split_msnames, cal_msnames)):
            recipe.add('cab/casa_split','freqav_{:d}'.format(i),
               {
                 "msname"          :   split_msname,
                 "output-msname"   :   cal_msname,
                 "datacolumn"      :   "DATA",
                 "width"           :   int(pars['width'])
                },
                input=INPUT,
                output=OUTPUT,
                label='freqav_{:d}:: Frequency average the target'.format(i))
            steps2run.append('freqav_{:d}'.format(i))
     
        for i, (cal_msname) in enumerate(zip(cal_msnames)):
            recipe.add('cab/msutils', 'copydata2corrdata_{:d}'.format(i),
               {
                 "msname"          :   cal_msname,
                 "command"         :   'copycol',
                 "fromcol"         :   'DATA',
                 "tocol"           :   'CORRECTED_DATA',
               },
               input=INPUT,
               output=OUTPUT,
               label='copydata2corrdata_{:d}:: Copy DATA to CORRECTED_DATA column'.format(i))
            steps2run.append('copydata2corrdata_{:d}'.format(i))
         
        for i, (cal_msname) in enumerate(zip(cal_msnames)):
            recipe.add('cab/msutils', 'prepms_2gc_{:d}'.format(i),
               {
                 "msname"          :  cal_msname,
                 "command"         : 'prep',
               },
               input = INPUT,
               output= MSDIR,
               label = 'prepms_2gc_{:d}:: Add flagsets'.format(i))

    # Imaging+selfcal loop governed by the parameter nr_it (nr_selfcal_iter in the parameter file)
    # General procedure:

    # ITERATION 0
    # - Dirty image
    # - Mask at peak/2 of dirty image
    # - Clean with mask down to 1 sigma
    # - PyBDSM with detected_image = convolved_model

    # ITERATION 1
    # - Selfcal and write CORR_DATA
    # - Dirty image
    # - Mask at peak/2 of dirty image
    # - Clean with mask down to 1 sigma
    # - Mask at peak/2 of residual
    # - Continue cleaning with new mask down to 1 sigma
    # - PyBDSM with detected_image = convolved_model

    # ITERATION 2
    # - Selfcal and write CORR_DATA
    # - Dirty image
    # - Mask at peak/2 of dirty image
    # - Clean with mask down to 1 sigma
    # - Mask at peak/2 of residual
    # - Continue cleaning with new mask down to 1 sigma
    # - Mask at peak/2 of residual
    # - Continue cleaning with new mask down to 1 sigma
    # - PyBDSM with detected_image = convolved_model

    # ...

    # ITERATION N (LAST)
    # - Selfcal and write CORR_DATA
    # - Dirty image
    # - Mask at peak/2 of dirty image
    # - Clean with mask down to 1 sigma
    # - N x
    # -   Mask at peak/2 of residual
    # -   Continue cleaning with new mask down to 1 sigma
    # - Mask at 5 sigma
    # - Continue cleaning with new mask down to 1 sigma
    # - PyBDSM with detected_image = convolved_model

    # NOTES FOR FUTURE IMPROVEMENTS
    # - At the last iteration, we could experiment with cleaning with no mask, letting WSClean decide where to clean
    # - At the last iteration, we could run PyBDSM before the last, deep clean to model bright sources only, and add
    #   add the clean components of the last, deep clean to the model (as Sphe suggested).
    # - It might be OK to use the clean mask instead of the convolved model as detected_image in PyBDSM. In that case
    #   we would need to add all clean masks used in that particular iteration together.
    # - After the first few iterations we could speed up the first few steps. I.e., we don't need to start from scratch.
    #   We could for example start with a clean mask = sum of the clean masks of the previous iteration.

    if pars['SELFCAL_CONT_MS'].lower() in ['yes', 'true', '1']:
        nr_it=int(pars['nr_selfcal_iter'])
        for sc_it in range(nr_it):
            if sc_it:
                for i, (cal_msname) in enumerate(zip(cal_msnames)):
                    # Run selfcal and output corrected data
                    recipe.add('cab/calibrator','selfcal_{:02d}_msfile{:02d}'.format(sc_it,i),
                       {
                         "skymodel"     :  lsm,
                         "msname"       :  cal_msname,
                         "threads"      :  16,
                         "column"       :  "DATA",
                         "output-data"  :  "CORR_DATA",
                         "output-column":  "CORRECTED_DATA",
                         "Gjones"       :  True,
                         "Gjones-solution-intervals" : [2, 0],
                         "Gjones-matrix-type" : "GainDiagPhase",
                         "make-plots"         : False,
                         "tile-size"          : 512,
                       },
                       input=INPUT,
                       output=OUTPUT,
                       label='selfcal_{:02d}_msfile{:02d}:: Selfcal'.format(sc_it,i))
                    steps2run.append('selfcal_{:02d}_msfile{:02d}'.format(sc_it,i)) 

            combprefix_sc=combprefix+'_cont'
            for cl_it in range(sc_it+2):
                if not cl_it:
                    # Make dirty image
                    suff_img="_%02d_%02d"%(sc_it,cl_it)
                    recipe.add('cab/wsclean', 'cont_image'+suff_img,
                       {
                         "msname"         :    cal_msnames,
                         "prefix"         :    combprefix_sc+suff_img,
                         "nomfsweighting" :    False,
                         "mgain"          :    0.8,
                         "column"         :    "CORRECTED_DATA",
                         "auto-threshold" :    10,
                         "trim"           :    trim,
                         "stokes"         :    "I",
                         "npix"           :    npix_cont,
                         "cellsize"       :    cell_cont,
                         "niter"          :    0,
                         "weight"         :    '{0:s} {1:f}'.format(pars['weight_cont'], robust_cont),
                       },
                       input=INPUT,
                       output=OUTPUT,
                       label='cont_image'+suff_img+':: Make a combined continuum image of selfcaled data')
                    steps2run.append('cont_image'+suff_img)

                else:
                    # Make a peak/2 mask and clean
                    suff_msk="_%02d_%02d"%(sc_it,cl_it)
                    fitsmask = combprefix_sc+suff_msk+"-mask.fits:output"
                    if cl_it==1:
                        image2mask=combprefix_sc+suff_img+"-dirty.fits:output"
                        suff_img="_%02d_%02d"%(sc_it,1)
                        wscontinue=False
                    else:
                        suff_img="_%02d_%02d"%(sc_it,1)
                        image2mask=combprefix_sc+suff_img+"-residual.fits:output"
                        wscontinue=True

                    # Mask image2mask at peak/2
                    recipe.add('cab/cleanmask', 'cleanmask'+suff_msk,
                       {
                         "image"           :  image2mask,
                         "output"          :  fitsmask,
                         "dilate"          :  False,
                         "peak-fraction"   :  0.5,
                         "no-negative"     :  True,
                         "boxes"           :  1,
                         "log-level"       :  'DEBUG',
                       },
                       input=INPUT,
                       output=OUTPUT,
                       label='cleanmask'+suff_msk+':: Make a cleanmask from the dirty image')
                    steps2run.append('cleanmask'+suff_msk) 

                    # Clean with mask                    
                    recipe.add('cab/wsclean', 'cont_image'+suff_msk,
                       {
                         "msname"         :    cal_msnames,
                         "prefix"         :    combprefix_sc+suff_img,
                         "nomfsweighting" :    False,
                         "mgain"          :    0.8,
                         "column"         :    "CORRECTED_DATA",
                         "auto-threshold" :    1,
                         "trim"           :    trim,
                         "stokes"         :    "I",
                         "npix"           :    npix_cont,
                         "cellsize"       :    cell_cont,
                         "niter"          :    100000000,
                         "fitsmask"       :    fitsmask,
                         "weight"         :    '{0:s} {1:f}'.format(pars['weight_cont'], robust_cont),
                         "continue"       :    wscontinue,
                       },
                       input=INPUT,
                       output=OUTPUT,
                       label='cont_image'+suff_msk+':: Make a combined continuum image of selfcaled data')
                    steps2run.append('cont_image'+suff_msk)

            # At the last selfcal iteration make a final clean with a 5 sigma mask
            if sc_it==nr_it-1:
                suff_msk="_%02d_%02d"%(sc_it,cl_it+1)
                fitsmask = combprefix_sc+suff_msk+"-mask.fits:output"
                suff_img="_%02d_%02d"%(sc_it,1)
                image2mask=combprefix_sc+suff_img+"-residual.fits:output"
                wscontinue=True

                # Mask image2mask at 5 sigma
                recipe.add('cab/cleanmask', 'cleanmask'+suff_msk,
                   {
                     "image"           :  image2mask,
                     "output"          :  fitsmask,
                     "dilate"          :  False,
                     "sigma"           :  5,
                     "no-negative"     :  True,
                     "boxes"           :  1,
                     "log-level"       :  'DEBUG',
                   },
                   input=INPUT,
                   output=OUTPUT,
                   label='cleanmask'+suff_msk+':: Make a cleanmask from the dirty image')
                steps2run.append('cleanmask'+suff_msk) 

                # Clean with mask                    
                recipe.add('cab/wsclean', 'cont_image'+suff_msk,
                   {
                     "msname"         :    cal_msnames,
                     "prefix"         :    combprefix_sc+suff_img,
                     "nomfsweighting" :    False,
                     "mgain"          :    0.8,
                     "column"         :    "CORRECTED_DATA",
                     "auto-threshold" :    1,
                     "trim"           :    trim,
                     "stokes"         :    "I",
                     "npix"           :    npix_cont,
                     "cellsize"       :    cell_cont,
                     "niter"          :    100000000,
                     "fitsmask"       :    fitsmask,
                     "weight"         :    '{0:s} {1:f}'.format(pars['weight_cont'], robust_cont),
                     "continue"       :    wscontinue,
                   },
                   input=INPUT,
                   output=OUTPUT,
                   label='cont_image'+suff_msk+':: Make a combined continuum image of selfcaled data')
                steps2run.append('cont_image'+suff_msk)

            # Having made the last clean iteration, make a sky model with PyBDSM
            # We force PyBDSM to find sources only among those that have been cleaned

            # Make convolved model image
            recipe.add('cab/fitstool', 'diff_{:02d}'.format(sc_it),
               {
                 "image"  : ['{:s}-{:s}.fits:output'.format(combprefix_sc+suff_img,a) for a in ['image', 'residual']],
                 "diff"   : True,
                 "output" : '{:s}-convmodel.fits'.format(combprefix_sc+suff_img)
               },
               input=INPUT,
               output=OUTPUT,
               label='diff_{:02d}:: Difference'.format(sc_it))
            steps2run.append('diff_{:02d}'.format(sc_it))

            # Run PyBDSM
            lsmprefix=combprefix_sc+'%02d-LSM'%sc_it
            recipe.add('cab/pybdsm', 'sky_model_%02d'%sc_it,
               {
                 "image"             :  combprefix_sc+suff_img+'-image.fits:output',
                 "outfile"           :  '%s.fits:output'%(lsmprefix),
                 "rms_map"           :  False,
                 "clobber"           :  True,
                 "port2tigger"       :  True,
                 "detection_image"   :  '{:s}-convmodel.fits:output'.format(combprefix_sc+suff_img),
                 "blank_limit"       :  1e-5,
               },
               input=INPUT,
               output=OUTPUT,
               label='sky_model_%02d::make initial model'%sc_it)
            steps2run.append('sky_model_%02d'%sc_it) 
            lsm=lsmprefix+".lsm.html:output"


    if pars['SELFCAL_LINE_MS'].lower() in ['yes', 'true', '1']:

        if not pars['SELFCAL_CONT_MS'].lower() in ['yes', 'true', '1']:
            # If the LSM has not been derived during the current pipeline run, find latest LSM model compatible with current settings
            lsm=sorted(glob.glob(OUTPUT+'/*'+combprefix+'*LSM*html'))[-1].split('/')[-1]+':output'

        # Selfcal with the 'final' skymodel and the time-averaged only data. If we need to 'apply' previous gains, uncomment the parameters
        for i, (split_msname) in enumerate(zip(split_msnames)):
            recipe.add('cab/calibrator', 'final_selfcal_{:d}'.format(i),
               {
                 "skymodel"     :  lsm,
                 "msname"       :  split_msname,
                 "threads"      :  16,
                 "column"       :  "DATA",
                 "output-data"  :  "CORR_RES",
                 "output-column":  "CORRECTED_DATA",
                 "Gjones-solution-intervals" : [2, 0],
                 "Gjones"       : True,
            #    "Gjones-apply-only" : True,
                 "make-plots"         : True,
            #    "Gjones-gain-table"  : "final_table.cp:output",
                 "Gjones-matrix-type" : "GainDiagPhase",
                 "tile-size"          : 512,
               },
               input=INPUT,
               output=OUTPUT,
               label='final_selfcal_{:d}:: Selfcal t-avg data with complete skymodel'.format(i))
            steps2run.append('final_selfcal_{:d}'.format(i))

        # Image the residual of continuum subtraction with no cleaning (same imaging settings as for continuum)
        recipe.add('cab/wsclean', 'cont_sub_image',
            {
              "msname"         :    split_msnames,
              "prefix"         :    combprefix+"contsubtracted",
              "nomfsweighting" :    False,
              "mgain"          :    0.8,
              "column"         :    "CORRECTED_DATA",
              "auto-threshold" :    10,
              "trim"           :    trim,
              "stokes"         :    "I",
              "npix"           :    npix_cont,
              "cellsize"       :    cell_cont,
              "niter"          :    0,
              "weight"         :    '{0:s} {1:f}'.format(pars['weight'], robust_cont),
            },
            input=INPUT,
            output=OUTPUT,
            label='cont_sub_image:: Make a combined continuum image of t-avg data residuals after continuum subtraction')
        steps2run.append('cont_sub_image')



#############################
### CONTINUUM SUBTRACTION ###
#############################

# This will:
# - subtract the latest continuum model from the visibilities (optional)
# - subtract continuum emission through polynomial fitting in the UV plane

if pars['RUN_CONTSUB'].lower() in ['yes', 'true', '1']:
    # Subtract the continuum in the UV plane
    if pars['UV_CONTSUB'].lower() in ['yes', 'true', '1']:
        for i, msname in enumerate(split_msnames):
            recipe.add('cab/casa_uvcontsub','uvcontsub_{:d}'.format(i),
               {
                 "msname"         :    msname,
#                 "field"          :    target,
                 "fitorder"       :    1,
               },
               input=INPUT,
               output=OUTPUT,
               label='uvcontsub_{0:d}:: Subtract continuum in the UV plane'.format(i,msname))
            steps2run.append('uvcontsub_{0:d}'.format(i,msname))


#################################################
### Common to HI IMAGING_PRESB and HI IMAGING ###
#################################################

# This will:
# - make an HI cube

# Imaging settings
npix   = int(pars['npix'])
cell   = float(pars['cell'])
nchan  = int(pars['nchan'])
chan1  = int(pars['chan1'])
weight = pars['weight']
robust = float(pars['robust'])
restfreq = pars['restfreq']
sf_threshold = float(pars['sf_threshold'])
sf_flagregion=[map(int,jj.split(',')) for jj in pars['sf_flagregion'].split(';')]
sf_merge=pars['sf_merge'].lower() in ['yes','true','1']
sf_mergeX=int(pars['sf_mergeX'])
sf_mergeY=int(pars['sf_mergeY'])
sf_mergeZ=int(pars['sf_mergeZ'])
sf_minSizeX=int(pars['sf_minSizeX'])
sf_minSizeY=int(pars['sf_minSizeY'])
sf_minSizeZ=int(pars['sf_minSizeZ'])


if pars['USE_UVCONTSUB'].lower() in ['yes', 'true', '1']: msnames_cube = [ff+'.contsub' for ff in split_msnames]
else: msnames_cube = split_msnames

########################
### HI IMAGING_PRESB ###
########################

if pars['RUN_HI_IMAGING_PRESB'].lower() in ['yes', 'true', '1']:
    prefix_presb = combprefix+'_presb'
    # Make cube with CASA CLEAN
    if pars['hiimager']=='casa':
        recipe.add('cab/casa_clean', 'casa_clean_presb',
            {
                 "msname"         :    msnames_cube,
                 "prefix"         :    prefix_presb,
#                 "field"          :    target,
#                 "column"         :    "CORRECTED_DATA",
                 "mode"           :    'channel',
                 "nchan"          :    nchan,
                 "start"          :    chan1,
                 "interpolation"  :    'nearest',
                 "niter"          :    100,
                 "psfmode"        :    'hogbom',
                 "threshold"      :    '9mJy',
                 "npix"           :    npix,
                 "cellsize"       :    cell,
                 "weight"         :    weight,
                 "robust"         :    robust,
#                 "wprojplanes"    :    1,
                 "port2fits"      :    True,
                 "restfreq"       :    restfreq
            },
            input=INPUT,
            output=OUTPUT,
            label='casa_clean_presb:: Make a dirty cube with CASA CLEAN before sunblock')
        steps2run.append('casa_clean_presb')
    
    # Make dirty cube with WSCLEAN
    elif pars['hiimager']=='wsclean':
        recipe.add('cab/wsclean', 'wsclean_dirty_presb',
            {
                 "msname"         :    msnames_cube,
                 "prefix"         :    prefix_presb,
                 "nomfsweighting" :    True,
                 "npix"           :    npix,
                 "cellsize"       :    cell,
                 "channelsout"    :    nchan,
                 "channelrange"   :    [chan1,chan1+nchan],
#                 "field"          :    target,
#                 "column"         :    "DATA",
                 "niter"          :    0,
                 "weight"         :    '{0:s} {1:f}'.format(weight, robust),
#                 "nwlayers"       :    1,
            },
            input=INPUT,
            output=OUTPUT,
            label='wsclean_dirty_presb:: Make a WSCLEAN dirty image for each channel before sunblock')
        steps2run.append('wsclean_dirty_presb') 

        # Stack dirty channels into cube
        imagelist = ['{0:s}-{1:04d}-dirty.fits:output'.format(prefix_presb, jj) for jj in range(nchan)]
        recipe.add('cab/fitstool', 'stack_channels_presb',
            {
                 "stack"      :   True,
                 "image"      :   imagelist,
                 "fits-axis"  :   'FREQ',
                 "output"     :   '{:s}-cube.dirty.fits'.format(prefix_presb),
            },
            input=INPUT,
            output=OUTPUT,
            label='stack_channels_presb:: Stack individual channels made by WSClean before sunblock')
        steps2run.append('stack_channels_presb') 

################
### SUNBLOCK ###
################

# This will:
# Remove visibilities created by a remote, strong source
# CAUTION: Ideally this is run on line-free channels, which is currently not being done

if pars['RUN_SUNBLOCK'].lower() in ['yes', 'true', '1']:
    if pars['USE_UVCONTSUB'].lower() in ['yes', 'true', '1']: 
        pass
    else: 
        print '### Warning: ###'
        print 'Using sunblocker on continuum data.'
        print 'This will likely fail.'
        print 'Use USE_UVCONTSUB = 1.'
        print 'Unless you are sure that the continuum has been subtracted.'

    # Run sunblocker on all visibilities at once
    recipe.add(sunblocker.Sunblocker().phazer, 'sunblocker',
        {
            'inset'      : ['{0:s}/{1:s}'.format(MSDIR, n) for n in msnames_cube],
            'outset'     : ['{0:s}/{1:s}'.format(MSDIR, n) for n in msnames_cube],
            # channels : a, 
            'imsize'     : npix, 
            'cell'       : cell, 
            'pol'        : 'i', 
            'threshold'  : 4., 
            'mode'       : 'all', 
            'radrange'   : 0, 
            'angle'      : 0, 
            'showdir'    : OUTPUT,
            'show'       : 'sunblock.pdf', 
            'verb'       : True, 
            'dryrun'     : False,
        },
        input=INPUT,
        output=OUTPUT,
        label='sunblocker:: Block out the sun')
    steps2run.append('sunblocker')

##################
### HI IMAGING ###
##################

# This will:
# - make an HI cube (optional)
# - run SoFiA and make an HI image (optional)

# The following has already been done above
# Imaging settings
#npix   = int(pars['npix'])
#cell   = float(pars['cell'])
#nchan  = int(pars['nchan'])
#chan1  = int(pars['chan1'])
#weight = pars['weight']
#robust = float(pars['robust'])
#restfreq = pars['restfreq']
#sf_threshold = float(pars['sf_threshold'])
#sf_flagregion=[map(int,jj.split(',')) for jj in pars['sf_flagregion'].split(';')]
#sf_merge=pars['sf_merge'].lower() in ['yes','true','1']
#sf_mergeX=int(pars['sf_mergeX'])
#sf_mergeY=int(pars['sf_mergeY'])
#sf_mergeZ=int(pars['sf_mergeZ'])
#sf_minSizeX=int(pars['sf_minSizeX'])
#sf_minSizeY=int(pars['sf_minSizeY'])
#sf_minSizeZ=int(pars['sf_minSizeZ'])


#if pars['USE_UVCONTSUB'].lower() in ['yes', 'true', '1']: msnames_cube = [ff+'.contsub' for ff in split_msnames]
#else: msnames_cube = split_msnames

if pars['RUN_HI_IMAGING'].lower() in ['yes', 'true', '1']:
    # Make cube with CASA CLEAN
    if pars['MAKE_CUBE'].lower() in ['yes', 'true', '1'] and pars['hiimager']=='casa':
        recipe.add('cab/casa_clean', 'casa_clean',
            {
                 "msname"         :    msnames_cube,
                 "prefix"         :    combprefix,
#                 "field"          :    target,
#                 "column"         :    "CORRECTED_DATA",
                 "mode"           :    'channel',
                 "nchan"          :    nchan,
                 "start"          :    chan1,
                 "interpolation"  :    'nearest',
                 "niter"          :    100,
                 "psfmode"        :    'hogbom',
                 "threshold"      :    '9mJy',
                 "npix"           :    npix,
                 "cellsize"       :    cell,
                 "weight"         :    weight,
                 "robust"         :    robust,
#                 "wprojplanes"    :    1,
                 "port2fits"      :    True,
                 "restfreq"       :    restfreq
            },
            input=INPUT,
            output=OUTPUT,
            label='casa_clean:: Make a dirty cube with CASA CLEAN')
        steps2run.append('casa_clean')
    
    # Make dirty cube with WSCLEAN
    elif pars['MAKE_CUBE'].lower() in ['yes', 'true', '1'] and pars['hiimager']=='wsclean':
        recipe.add('cab/wsclean', 'wsclean_dirty',
            {
                 "msname"         :    msnames_cube,
                 "prefix"         :    combprefix,
                 "nomfsweighting" :    True,
                 "npix"           :    npix,
                 "cellsize"       :    cell,
                 "channelsout"    :    nchan,
                 "channelrange"   :    [chan1,chan1+nchan],
#                 "field"          :    target,
#                 "column"         :    "DATA",
                 "niter"          :    0,
                 "weight"         :    '{0:s} {1:f}'.format(weight, robust),
#                 "nwlayers"       :    1,
            },
            input=INPUT,
            output=OUTPUT,
            label='wsclean_dirty:: Make a WSCLEAN dirty image for each channel')
        steps2run.append('wsclean_dirty') 

        # Stack dirty channels into cube
        imagelist = ['{0:s}-{1:04d}-dirty.fits:output'.format(combprefix, jj) for jj in range(nchan)]
        recipe.add('cab/fitstool', 'stack_channels',
            {
                 "stack"      :   True,
                 "image"      :   imagelist,
                 "fits-axis"  :   'FREQ',
                 "output"     :   '{:s}-cube.dirty.fits'.format(combprefix),
            },
            input=INPUT,
            output=OUTPUT,
            label='stack_channels:: Stack individual channels made by WSClean')
        steps2run.append('stack_channels') 

    # Run SoFiA on stacked cube
    if pars['MAKE_IMAGE'].lower() in ['yes', 'true', '1']:
        recipe.add('cab/sofia', 'sofia',
            {
        #    USE THIS FOR THE WSCLEAN DIRTY CUBE
        #    "import.inFile"     :   '{:s}-cube.dirty.fits:output'.format(combprefix),
        #    USE THIS FOR THE CASA CLEAN CUBE
            "import.inFile"         :   '{:s}.image.fits:output'.format(combprefix),       # CASA CLEAN cube
            "steps.doMerge"         :   sf_merge,
            "steps.doMom0"          :   True,
            "steps.doMom1"          :   False,
            "steps.doParameterise"  :   False,
            "steps.doReliability"   :   False,
            "steps.doWriteCat"      :   False,
            "steps.doWriteMask"     :   True,
            "steps.doFlag"          :   True,
            "flag.regions"          :   sf_flagregion,
            "SCfind.threshold"      :   sf_threshold,
            "merge.radiusX"         :   sf_mergeX,
            "merge.radiusY"         :   sf_mergeY,
            "merge.radiusZ"         :   sf_mergeZ,
            "merge.minSizeX"        :   sf_minSizeX,
            "merge.minSizeY"        :   sf_minSizeY,
            "merge.minSizeZ"        :   sf_minSizeZ,
            },
            input=INPUT,
            output=OUTPUT,
            label='sofia:: Make SoFiA mask and images')
        steps2run.append('sofia') 



##################
### RUN RECIPE ###
##################

if not len(steps2run):
    print
    print '###################################'
    print '### STIMELA has got nothing to run!'
    print '###################################'
    print 
else:
    print 
    print '#########################################'
    print '### STIMELA will run the following steps:'
    print '###',steps2run
    print '#########################################'
    print
    if pars['DRY_RUN'].lower() in ['no', 'false', '0']:
        recipe.run(steps2run)

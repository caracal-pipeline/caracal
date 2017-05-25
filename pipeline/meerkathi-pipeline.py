import stimela
import os
import sys
#import byo

##### TODO 
#   :: The recipe is already getting a bit long. Maybe we should make 
#  different recipes for different stages of the reduction:
#       1. Data acquisitions and preliminaries ( h5toms, fixvis, etc.)		: Done
#       2. Precal Flagging 							: Done
#       3. 1GC Calibration (Bandpass, Gain/Phase calibration)			: TBD
#       4. SelfCal (Source finding, Calibration, Cleaning; continuum)		: TBD
#       5. Continuum Subtraction + Making final cube				: TBD
# 
#   :: Add SoFiA to stimela (TBD)
#       1. Use it to create multi-channel clean masks 
#       2. Make catalog from final cube
#####



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
# - make diagnostic plots (optional)

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
                 "combine"      :  "",                             
                 "bandtype"     :  "B",
                 "gaintable"    :  [prefix+".K0:output"],
                 "fillgaps"     :  70,
                 "uvrange"      :  pars['uvrange'],
                 "minsnr"       :  float(pars['minsnr']),
                 "minblperant"  :  int(pars['minnrbl']),
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
                 "gaintype"     :  "G",
                 "calmode"      :   'ap',
                 "gaintable"    :  [prefix+".B0:output",prefix+".K0:output"],
                 "interp"       :  ['nearest','nearest'],
                 "uvrange"      :  pars['uvrange'],
                 "minsnr"       :  float(pars['minsnr']),
                 "minblperant"  :  int(pars['minnrbl']),
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
            recipe.add('cab/casa_applycal','applycal_bp_{:d}'.format(i),
               {
                "vis"       :    msname,
                "field"     :   bpcal,
                "gaintable" :   [prefix+".K0:output", prefix+".B0:output", prefix+".F0:output"],
                "gainfield" :   ['','','',bpcal],
                "interp"    :   ['','','nearest','nearest'],
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
            recipe.add('cab/casa_applycal','applycal_g_{:d}'.format(i),
               {
                "vis"       :    msname,
                "field"     :   gcal,
                "gaintable" :   [prefix+".K0:output", prefix+".B0:output", prefix+".F0:output"],
                "gainfield" :   ['','','',gcal],
                "interp"    :   ['linear','linear','nearest'],
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
            recipe.add('cab/casa_applycal','applycal_tar_{:d}'.format(i),
               {
                "vis"       :    msname,
                "field"     :   target,
                "gaintable" :   [prefix+".K0:output", prefix+".B0:output", prefix+".F0:output"],
                "gainfield" :   ['','','',gcal],
                "interp"    :   ['linear','linear','nearest'],
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
                },
                input=INPUT,
                output=OUTPUT,
                label='split_avtime_{:d}:: Split and time average the target'.format(i))
            steps2run.append('split_avtime_{:d}'.format(i))

    # Make plots
    if pars['MAKE_PLOTS'].lower() in ['yes', 'true', '1']:
        # Plot bandpass
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_plotcal','plot_bandpass_{:d}'.format(i),
               {
                "caltable"  :   prefix+".B0:output",
                "xaxis"     :   'chan',
                "yaxis"     :   'amp',
                "field"     :    bpcal,
                "subplot"   :   221,
                "figfile"   :   prefix+'-B0-amp.png',
                "showgui"   :   False,
               },
               input=INPUT,
               output=OUTPUT,
               label='plot_bandpass_{:d}:: Plot bandpass'.format(i,msname))
            steps2run.append('plot_bandpass_{:d}'.format(i,msname))

        # Plot gains
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_plotcal','plot_gaincal_{:d}'.format(i),
               {
                "caltable"  :   prefix+".B0:output",
                "xaxis"     :   'time',
                "yaxis"     :   'amp',
                "field"     :    bpcal,
                "subplot"   :   221,
                "figfile"   :   prefix+'-G0-amp.png',
                "showgui"   :   False,
               },
               input=INPUT,
               output=OUTPUT,
               label='plot_gaincal_{:d}:: Plot gaincal'.format(i,msname))
            steps2run.append('plot_gaincal_{:d}'.format(i,msname))

        # Plot corrected phase vs amplitude for bandpass field
        for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
            recipe.add('cab/casa_plotms','plot_phaseamp_bp_{:d}'.format(i),
               {
                "vis"           :   msname,
                "field"         :   bpcal,
                "correlation"   :   'RR,LL',
                "timerange"     :   '',
                "antenna"       :   '',
                "xaxis"         :   'phase',
                "xdatacolumn"   :   'corrected',
                "yaxis"         :   'amp',
                "ydatacolumn"   :   'corrected',
                "coloraxis"     :   'corr',
                "plotfile"      :   prefix+'-bandpass-corrected-ampvsphase.png',
                "overwrite"     :   True,
               },
               input=INPUT,
               output=OUTPUT,
               label='plot_phaseamp_{:d}:: Plot phase vs amplitude for bandpass'.format(i,msname))
            steps2run.append('plot_phaseamp_{:d}'.format(i,msname))



##############################
### 2GC (SELF) CALIBRATION ###
##############################

# This will:
# - split and time average the target
# - make a continuum image and selfcalibrate the gain phases

if pars['RUN_2GC'].lower() in ['yes', 'true', '1']:
    kshitij_add_something = True



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



##################
### HI IMAGING ###
##################

# This will:
# - make an HI cube (optional)
# - run SoFiA and make an HI image (optional)

# Imaging settings
npix   = int(pars['npix'])
cell   = float(pars['cell'])
nchan  = int(pars['nchan'])
chan1  = int(pars['chan1'])
weight = pars['weight']
robust = float(pars['robust'])
sf_threshold = float(pars['sf_threshold'])
sf_flagregion=[map(int,jj.split(',')) for jj in pars['sf_flagregion'].split(';')]

if pars['USE_UVCONTSUB'].lower() in ['yes', 'true', '1']: msnames_cube = [ff+'.contsub' for ff in split_msnames]
else: msnames_cube = split_msnames

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
            "steps.doMerge"         :   False,
            "steps.doMom0"          :   True,
            "steps.doMom1"          :   False,
            "steps.doParameterise"  :   False,
            "steps.doReliability"   :   False,
            "steps.doWriteCat"      :   False,
            "steps.doWriteMask"     :   True,
            "steps.doFlag"          :   True,
            "flag.regions"          :   sf_flagregion,
            "SCfind.threshold"      :   sf_threshold
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
    recipe.run(steps2run)

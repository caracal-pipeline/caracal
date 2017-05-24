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

# I/O setup
INPUT = 'input'
OUTPUT = 'output'
MSDIR = 'msdir'

# Read parameter file
f=open('meerkathi-parameters.par')
pars=f.readlines()
pars=[jj.strip().replace(' ','') for jj in pars]
jj=0
while jj<len(pars):
    if not len(pars[jj]): del(pars[jj])
    elif pars[jj][0]=='#': del(pars[jj])
    else: jj+=1
pars=[jj.replace("'","").replace('"','').split('=') for jj in pars]
pars={jj[0]:jj[1] for jj in pars}
f.close()


# Set file names
dataids = pars['dataids'].split(',')
h5files = ['{:s}.h5'.format(dataid) for dataid in dataids]
msnames = ['{:s}.ms'.format(os.path.basename(dataid)) for dataid in dataids]
prefixes = ['meerkathi-{:s}'.format(os.path.basename(dataid)) for dataid in dataids]
combprefix = pars['combprefix']

# If MS exists, remove it before running 
REMOVE_MS = pars['REMOVE_MS']

# This changes the value of variables to whatever was set through the 
# '-g/--globals' option in the 'stimela run' command
stimela.register_globals()

REMOVE_MS = REMOVE_MS.lower() in ['yes', 'true', '1']
# Delete MS if user wants to do this (see comment above)
if REMOVE_MS:
    os.system('rm -fr {0:s}/{1:s}'.format(MSDIR, msname))

# Run UVCONTSUB?
RUN_UVCONTSUB = pars['RUN_UVCONTSUB']


# Use result of previous UVCONTSUB?
USE_UVCONTSUB = pars['USE_UVCONTSUB']

# Image continuum-subtracted files if UVCONTSUB is run
RUN_UVCONTSUB = RUN_UVCONTSUB.lower() in ['yes', 'true', '1']
USE_UVCONTSUB = USE_UVCONTSUB.lower() in ['yes', 'true', '1']
if RUN_UVCONTSUB or USE_UVCONTSUB: msnames_wsc = [ff+'.contsub' for ff in msnames]
else: msnames_wsc = msnames

# Fields
target=pars['target']
bpcal=pars['bpcal']
gcal=pars['gcal']

# Flagging strategies
aoflag_strat1=pars['aoflag_strat1']
mw_hi_channels=pars['mw_hi_channels']

# Imaging settings
npix   = int(pars['npix'])
cell   = float(pars['cell'])
nchan  = int(pars['nchan'])
chan1  = int(pars['chan1'])
weight = pars['weight']
robust = float(pars['robust'])


recipe = stimela.Recipe('MeerKATHI pipeline', ms_dir=MSDIR)

for i, (h5file,msname) in enumerate(zip(h5files, msnames)):

    recipe.add('cab/h5toms', 'h5toms_{:d}'.format(i),
        {
            "hdf5files"       :    h5file,
            "channel-range" :    "'20673,21673'",
            "no-auto"       :    False,
            "output-ms"     :    msname,
            "full-pol"      :    True,
            "tar"           :    True,
            "model-data"    :    True,
        },
        input='/var/kat/archive2/data/MeerKATAR1/telescope_products',
        output=MSDIR,
        label='h5toms_{0:d}:: Convert from h5 to ms={1:s}'.format(i, msname))


for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
    recipe.add('cab/casa_fixvis', 'fix_uvw_coords_{:d}'.format(i), 
        {
            "vis"    	:    msname,
            "reuse"     :    False,
            "outputvis" :    msname,
        },
        input=INPUT,
        output=OUTPUT,
        label='fix_uvw_{0:d}:: Fix UVW coordinates for ms={1:s}'.format(i, msname))

# List obs
for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
    recipe.add('cab/casa_listobs', 'obsinfo_{:d}'.format(i), 
        {
            "vis"    	:    msname,
            "listfile"      :    prefix+'-listobs.txt',
            "overwrite" :  True,
        },
        input=INPUT,
        output=OUTPUT,
        label='get_obsinfo_{0:d}:: Get observation information ms={1:s}'.format(i, msname))

#Flag autocorrelations
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



# Flag Milky Way HI channels

for i, msname in enumerate(msnames):
    recipe.add('cab/casa_flagdata','flagmw_{:d}'.format(i),
        {
            "vis"           :   msname,
            "mode"          :   'manual',
            "spw"           :   mw_hi_channels,
        },
        input=INPUT,
        output=OUTPUT,
        label='flagmw_{0:d}::Flag out channels with HI emission from Milky Way'.format(i, msname))

# Add corrected data

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

# Add model data 

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



# Flag with AOflagger

recipe.add('cab/autoflagger', 'aoflag_1',
    {
         "msname"       :   msnames,
         "column"       :   'DATA',
         "strategy"     :   aoflag_strat1,
    },
    input=INPUT,
    output=OUTPUT,
    label='aoflag_1:: Aoflagger flagging pass 1')

#Setjansky

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

#Delay calibration

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

#Bandpass
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

#Gain calibration for Bandpass field
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

#Gain calibration for Gaincal field
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

#Flux scale transfer
for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
    recipe.add('cab/casa_fluxscale','fluxscale_{:d}'.format(i),
       {
         "vis"           :  msname,
        "caltable"      :   prefix+".G0:output",
        "fluxtable"     :   prefix+".F0:output",
        "reference"     :   [bpcal],
        "transfer"      :   [gcal],
       },
       input=INPUT,       
       output=OUTPUT,
       label='fluxscale_{:d}:: Flux scale transfer'.format(i,msname))

#Apply calibration tables to Bandpass field
for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
    recipe.add('cab/casa_applycal','applycal_bp_{:d}'.format(i),
       {
        "vis"      :    msname,
        "field"     :   bpcal,
        "gaintable" :   [prefix+".K0:output", prefix+".B0:output", prefix+".F0:output"],
        "gainfield" :   ['','','',bpcal],
        "interp"    :   ['','','nearest','nearest'],
        "calwt"     :   [False],
        "parang"    :   False,
#        "applymode" :   'trial',
       },
       input=INPUT,
       output=OUTPUT,
       label='applycal_bp_{:d}:: Apply calibration to bandpass field'.format(i,msname))

#Apply calibration tables to Gaincal field
for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
    recipe.add('cab/casa_applycal','applycal_g_{:d}'.format(i),
       {
        "vis"      :    msname,
        "field"     :   gcal,
        "gaintable" :   [prefix+".K0:output", prefix+".B0:output", prefix+".F0:output"],
        "gainfield" :   ['','','',gcal],
        "interp"    :   ['linear','linear','nearest'],
        "calwt"     :   [False],
        "parang"    :   False,
#        "applymode" :   'trial',
       },
       input=INPUT,
       output=OUTPUT,
       label='applycal_g_{:d}:: Apply calibration to gaincal field'.format(i,msname))

#Apply calibration table to Target Field
for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
    recipe.add('cab/casa_applycal','applycal_tar_{:d}'.format(i),
       {
        "vis"      :    msname,
        "field"     :   target,
        "gaintable" :   [prefix+".K0:output", prefix+".B0:output", prefix+".F0:output"],
        "gainfield" :   ['','','',gcal],
        "interp"    :   ['linear','linear','nearest'],
        "calwt"     :   [False],
        "parang"    :   False,
#        "applymode" :   'trial',
       },
       input=INPUT,
       output=OUTPUT,
       label='applycal_tar_{:d}:: Apply calibration to gaincal field'.format(i,msname))

#Plot bandpass
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

#Plot gains
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





#Plot corrected phase vs amplitude for bandpass field
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


# Subtract the continuum in the UV plane

for i, msname in enumerate(msnames):
    recipe.add('cab/casa_uvcontsub','uvcontsub_{:d}'.format(i),
       {
          "msname"         :    msname,
          "field"          :    target,
          "fitorder"       :    1,
       },
       input=INPUT,
       output=OUTPUT,
       label='uvcontsub_{0:d}:: Subtract continuum in the UV plane'.format(i,msname))

# Make cube with CASA CLEAN
recipe.add('cab/casa_clean', 'casa_clean',
    {
         "msname"         :    msnames_wsc,
         "prefix"         :    combprefix,
#         "field"          :    target,
#         "column"         :    "CORRECTED_DATA",
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
#         "wprojplanes"    :    1,
         "port2fits"      :    True,
    },
    input=INPUT,
    output=OUTPUT,
    label='casa_clean:: Make a dirty cube with CASA CLEAN'.format(i,msname))
    
# Make dirty channels with WSClean
recipe.add('cab/wsclean', 'wsclean_dirty',
    {
         "msname"         :    msnames_wsc,
         "prefix"         :    combprefix,
         "nomfsweighting" :    True,
         "npix"           :    npix,
         "cellsize"       :    cell,
         "channelsout"    :    nchan,
         "channelrange"   :    [chan1,chan1+nchan],
#         "field"          :    target,
#         "column"         :    "DATA",
         "niter"          :    0,
         "weight"         :    '{0:s} {1:f}'.format(weight, robust),
#         "nwlayers"       :    1,
    },
    input=INPUT,
    output=OUTPUT,
    label='wsclean_dirty:: Make a WSCLEAN dirty image for each channel')

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

# Run SoFiA on stacked cube
recipe.add('cab/sofia', 'sofia',
    {
#    USE THIS FOR THE WSCLEAN DIRTY CUBE
#    "import.inFile"     :   '{:s}-cube.dirty.fits:output'.format(combprefix),
#    USE THIS FOR THE CASA CLEAN CUBE
    "import.inFile"     :   '{:s}.image.fits:output'.format(combprefix),       # CASA CLEAN cube
    "steps.doMerge"     :   False,
    "steps.doMom0"      :   True,
    "steps.doMom1"      :   False,
    "steps.doParameterise"  :   False,
    "steps.doReliability"   :   False,
    "steps.doWriteCat"      :   False,
    "steps.doWriteMask"     :   True,
    "steps.doFlag"          :   True,
    "flag.regions"          :   [[0,255,0,255,911,962],],
    "SCfind.threshold"      :   4
    },
    input=INPUT,
    output=OUTPUT,
    label='sofia:: Make SoFiA mask and images')


# If MS exists and REMOVE_MS==False, then h5toms step should not be added to recipe
h5toms = []
for i,msname in enumerate(msnames):
    if os.path.exists('{0:s}/{1:s}'.format(MSDIR, msname)) and REMOVE_MS==False:
        h5toms = []
    else:
        h5toms.append('h5toms_{:d}'.format(i))

# Fill in the uvcontsub list only if requested
if RUN_UVCONTSUB: uvcontsub=['uvcontsub_{:d}'.format(d) for d in range(len(msnames))]
else: uvcontsub = []

# Run it!
recipe.run(
     h5toms
    +['fix_uvw_{:d}'.format(d) for d in range(len(msnames))]
    +['get_obsinfo_{:d}'.format(d) for d in range(len(msnames))]
#    +['data2corrdata_{:d}'.format(d) for d in range(len(msnames))]
#    +['data2modeldata_{:d}'.format(d) for d in range(len(msnames))]
    +['flagautocorr_{:d}'.format(d) for d in range(len(msnames))]
    +['flagmw_{:d}'.format(d) for d in range(len(msnames))]
    +[ 'aoflag_1']
    +['setjansky_{:d}'.format(d) for d in range(len(msnames))]
    +['delay_calibration_{:d}'.format(d) for d in range(len(msnames))]
    +['bandpass_calibration_{:d}'.format(d) for d in range(len(msnames))]
    +['gain_calibration_bp_{:d}'.format(d) for d in range(len(msnames))]
    +['gain_calibration_g_{:d}'.format(d) for d in range(len(msnames))]
    +['fluxscale_{:d}'.format(d) for d in range(len(msnames))]
    +['applycal_bp_{:d}'.format(d) for d in range(len(msnames))]
    +['applycal_g_{:d}'.format(d) for d in range(len(msnames))]
    +['applycal_tar_{:d}'.format(d) for d in range(len(msnames))]
    +['plot_bandpass_{:d}'.format(d) for d in range(len(msnames))]
    +['plot_gaincal_{:d}'.format(d) for d in range(len(msnames))]
    +['plot_phaseamp_{:d}'.format(d) for d in range(len(msnames))]
    +uvcontsub
    +['casa_clean']
#    +['wsclean_dirty']
#    +['stack_channels']
    +['sofia']
)

print
print '###########################'
print '### Start BYO functions ###'
print '###########################'
print

#spectrum=byo.get_spec(msnames,msdir=MSDIR)
#print spectrum.shape

print 
print '#########################'
print '### End BYO functions ###'
print '#########################'
print

#recipe.run(
#    ['get_obsinfo_{:d}'.format(d) for d in range(len(msnames))]
#)

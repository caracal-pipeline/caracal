import stimela
import os
import sys


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




dataids = ['2017/04/06/1491463063', '2017/04/06/1491480644']

h5files = ['{:s}.h5'.format(dataid) for dataid in dataids]
msnames = ['{:s}.ms'.format(os.path.basename(dataid)) for dataid in dataids]
prefixes = ['meerkathi-{:s}'.format(os.path.basename(dataid)) for dataid in dataids]
PREFIX = 'meerkathi-combined-data'

# If MS exists, remove it before running 
REMOVE_MS = 'no'

# This changes the value of variables to whatever was set through the 
# '-g/--globals' option in the 'stimela run' command
stimela.register_globals()

REMOVE_MS = REMOVE_MS.lower() in ['yes', 'true', '1']
# Delete MS if user wants to do this (see comment above)
if REMOVE_MS:
    os.system('rm -fr {0:s}/{1:s}'.format(MSDIR, msname))

# Fields
target = '3'
bpcal = '0'
gcal = '2'

# Reference Antenna
REFANT = 'm006'

# Flagging strategies
aoflag_strat1 = "aoflagger_strategies/firstpass_HI_strat2.rfis"

# Imaging settings
npix = 256
cell = 20
nchan = 10
weight = 'briggs'
robust = 2


recipe = stimela.Recipe('MeerKATHI pipeline', ms_dir=MSDIR)

for i, (h5file,msname) in enumerate(zip(h5files, msnames)):

    recipe.add('cab/h5toms', 'h5toms_{:d}'.format(i),
        {
            "hdf5files"       :    h5file,
            "channel-range" :    "'20873,21639'",
            "no-auto"       :    False,
            "output-ms"     :    msname,
            "full-pol"      :    True,
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


for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
    recipe.add('cab/casa_listobs', 'obsinfo_{:d}'.format(i), 
        {
            "vis"    	:    msname,
            "listfile"      :    prefix+'-listobs.txt'
        },
        input=INPUT,
        output=OUTPUT,
        label='get_obsinfo_{0:d}:: Get observation information ms={1:s}'.format(i, msname))


for i, msname in enumerate(msnames):
    recipe.add('cab/casa_flagdata','flagmw_{:d}'.format(i),
        {
            "vis"           :   msname,
            "mode"          :   'manual',
            "spw"           :   "0:725~750",
        },
        input=INPUT,
        output=OUTPUT,
        label='flagmw_{0:d}::Flag out channels with emission from Milky Way'.format(i, msname))

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
         "refant"       :  REFANT,
         "solint"       :  "inf",
         "gaintype"     :  "K",
         "minsnr"    :   5,
       },
       input=INPUT,
       output=OUTPUT,
       label='delay_calibration_{:d}:: Delay calibration'.format(i,msname))

#Bandpass

for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
    recipe.add('cab/casa_bandpass','bandpass_calibration_{:d}'.format(i),
       {
         "vis"          :  msname,
         "caltable"     :  prefix+".B0",
         "field"        :  bpcal,
         "refant"       :  REFANT,
         "solint"       :  "inf",
         "bandtype"     :  "B",
         "gaintable"    :  [prefix+".K0:output"],
         "fillgaps"     :  26,
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
         "refant"       :  REFANT,
         "solint"       :  "inf",
         "gaintype"     :  "G",
         "calmode"   :   'ap',
         "minsnr"    :   5,
         "gaintable"  :  [prefix+".B0:output",prefix+".K0:output"],
         "interp"     :  ['nearest','nearest']
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
         "refant"       :  REFANT,
         "solint"       :  "inf",
         "gaintype"     :  "G",
         "calmode"   :   'ap',
         "minsnr"    :   5,
         "gaintable"  :  [prefix+".B0:output",prefix+".K0:output"],
         "interp"     :  ['linear','linear'],
         "append"    :   True,
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
       },
       input=INPUT,
       output=OUTPUT,
       label='applycal_bp_{:d}:: Apply calibration to bandpass field'.format(i,msname))

#Apply calibration tables to Gaincal field
for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
    recipe.add('cab/casa_applycal','applycal_g_{:d}'.format(i),
       {
        "vis"      :    msname,
        "field"     :   bpcal,
        "gaintable" :   [prefix+".K0:output", prefix+".B0:output", prefix+".F0:output"],
        "gainfield" :   ['','','',gcal],
        "interp"    :   ['linear','linear','nearest'],
        "calwt"     :   [False],
        "parang"    :   False,
       },
       input=INPUT,
       output=OUTPUT,
       label='applycal_g_{:d}:: Apply calibration to gaincal field'.format(i,msname))

#Apply calibration table to Target Field
for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
    recipe.add('cab/casa_applycal','applycal_tar_{:d}'.format(i),
       {
        "vis"      :    msname,
        "field"     :   bpcal,
        "gaintable" :   [prefix+".K0:output", prefix+".B0:output", prefix+".F0:output"],
        "gainfield" :   ['','','',gcal],
        "interp"    :   ['linear','linear','nearest'],
        "calwt"     :   [False],
        "parang"    :   False,
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
#Plot corrected phase vs amplitude for bandpass field
for i, (msname, prefix) in enumerate(zip(msnames, prefixes)):
      recipe.add('cab/casa_plotms','plot_phaseamp_bp_{:d}'.format(i),
       {
        "vis"           :   msname,
        "field"         :   bpcal,
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






imagelist = ['{0:s}-{1:04d}-dirty.fits:output'.format(PREFIX, jj) for jj in range(nchan)]

recipe.add('cab/wsclean', 'wsclean_dirty',
    {
         "msname"         :    msnames,
         "prefix"         :    PREFIX,
         "nomfsweighting" :    True,
         "npix"           :    npix,
         "cellsize"       :    cell,
         "channelsout"    :    nchan,
         "channelrange"   :    [61,70],
         "field"          :    target,
#         "column"         :    "DATA",
         "niter"          :    0,
         "weight"         :    '{0:s} {1:d}'.format(weight, robust),
    },
    input=INPUT,
    output=OUTPUT,
    label='wsclean_dirty:: Make a WSCLEAN dirty image for each channel')


recipe.add('cab/fitstool', 'stack_channels',
    {
         "stack"      :   True,
         "image"      :   imagelist,
         "fits-axis"  :   'FREQ',
         "output"     :   '{:s}-cube.dirty.fits'.format(PREFIX),
    },
    input=INPUT,
    output=OUTPUT,
    label='stack_channels:: Stack individual channels made by WSClean')


# If MS exists and REMOVE_MS==False, then h5toms step should not be added to recipe
h5toms = []
for i,msname in enumerate(msnames):
    if os.path.exists('{0:s}/{1:s}'.format(MSDIR, msname)) and REMOVE_MS==False:
        h5toms = []
    else:
        h5toms.append('h5toms_{:d}'.format(i))

recipe.run(
     h5toms +
    ['fix_uvw_{:d}'.format(d) for d in range(len(msnames))] +
    ['get_obsinfo_{:d}'.format(d) for d in range(len(msnames))] +
    ['data2corrdata_{:d}'.format(d) for d in range(len(msnames))] +
    ['flagmw_{:d}'.format(d) for d in range(len(msnames))] +
    [ 'aoflag_1']+
    ['setjansky_{:d}'.format(d) for d in range(len(msnames))]+
    ['delay_calibration_{:d}'.format(d) for d in range(len(msnames))]+
    ['bandpass_calibration_{:d}'.format(d) for d in range(len(msnames))] +
    ['gain_calibration_bp_{:d}'.format(d) for d in range(len(msnames))] +
    ['gain_calibration_g_{:d}'.format(d) for d in range(len(msnames))] +
    ['fluxscale_{:d}'.format(d) for d in range(len(msnames))] +
    ['applycal_bp_{:d}'.format(d) for d in range(len(msnames))] +
    ['applycal_g_{:d}'.format(d) for d in range(len(msnames))] +
    ['applycal_tar_{:d}'.format(d) for d in range(len(msnames))]+ 
    ['plot_bandpass_{:d}'.format(d) for d in range(len(msnames))]+
    ['plot_phaseamp_{:d}'.format(d) for d in range(len(msnames))]+
    ['wsclean_dirty'],
     'stack_channels',]
)

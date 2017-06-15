import stimela
import os
import sys

# I/O setup
INPUT = 'input'
OUTPUT = 'output'
MSDIR = 'msdir'

dataids = ['2017/04/06/1491463063', '2017/04/06/1491480644']

h5files = ['{:s}.h5'.format(dataid) for dataid in dataids]
msnames = ['{:s}.ms'.format(os.path.basename(dataid)) for dataid in dataids]
split_msnames = ['{:s}_split.ms'.format(os.path.basename(dataid)) for dataid in dataids]
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

# Run UVCONTSUB?
RUN_UVCONTSUB = 'no'

# Image continuum-subtracted files if UVCONTSUB is run
RUN_UVCONTSUB = RUN_UVCONTSUB.lower() in ['yes', 'true', '1']
if RUN_UVCONTSUB:
    msnames_wsc = ['{:s}.ms.contsub'.format(os.path.basename(dataid)) for dataid in dataids]
else:
    msnames_wsc = ['{:s}.ms'.format(os.path.basename(dataid)) for dataid in dataids]

# Fields
target = 'IC5264'
bpcal = 'PKS1934-638'
gcal = 'ATCA2259-375'

# Reference Antenna
REFANT = 'm006'

# Flagging strategies
aoflag_strat1 = "aoflagger_strategies/firstpass_HI_strat2.rfis"

# Continuum Imaging settings
trim   = 1024
cell   = 5
weight = 'briggs'
robust = -1.5
npix = 1180

recipe = stimela.Recipe('MeerKATHI 2GC pipeline', ms_dir=MSDIR)

#Split the MS to have only the target field and average the data.
for i, (msname, split_msname) in enumerate(zip(msnames, split_msnames)):

    recipe.add('cab/casa_split', 'splitandavg_{:d}'.format(i),
       {     
           "msname"          :   msname,
           "output-msname"   :   split_msname, 
           "datacolumn"      :   "CORRECTED",
           "field"           :   target,
           "timebin"         :   "32s"
       },
       input = INPUT,
       output= MSDIR,
       label='splitandavg_{:d}:: Split and average MSs'.format(i))

    recipe.add('cab/msutils', 'prepms_{:d}'.format(i),
       {
          "msname"          :  split_msname,
          'command'         : 'prep',
       },
       input = INPUT,
       output= MSDIR,
       label = 'prepms_{:d}:: Add flagsets'.format(i))

recipe.add('cab/wsclean', 'cont_image1',
    {
         "msname"         :    split_msnames,
         "prefix"         :    PREFIX+"cont_1",
         "nomfsweighting" :    False,
         "column"         :   "DATA",
         "mgain"          :    0.8,
         "auto-threshold" :    1,
         "auto-mask"      :    5,
         "rms-background" :    True,
         "trim"           :    trim,
         "stokes"         :    "I",
         "npix"           :    npix,
         "cellsize"       :    cell,
         "channelsout"    :    4,
         "joinchannels"	  :    True,
         "fit-spectral-pol":   True,
         "niter"          :    10000000,
         "weight"         :    '{0:s} {1:f}'.format(weight, robust),
    },
    input=INPUT,
    output=OUTPUT,
    label='cont_image1:: Make a combined continuum image')

lsmprefix=PREFIX+'-LSM0'
recipe.add('cab/pybdsm', 'init_model',
   {
        "image"             :   PREFIX+'cont_1-MFS-image.fits:output',
        "outfile"           :   '%s.fits:output'%(lsmprefix),
        "thresh_pix"        :  25,
        "thresh_isl"        :  15,
        "port2tigger"       :  True,
   },
   input=INPUT,
   output=OUTPUT,
   label='init_model::make initial model')
lsm=lsmprefix+".lsm.html:output"


#First Selfcal
for i, (split_msname) in enumerate(zip(split_msnames)):
    recipe.add('cab/calibrator','selfcal1_{:d}'.format(i),
        {
            "skymodel"     :  lsm,
            "msname"       :  split_msname,
            "threads"      :  16,
            "column"       :  "DATA",
            "output-data"  : "CORR_RES",
            "Gjones"       : True,
            "Gjones-solution-intervals" : [2, 10],
            "Gjones-matrix-type" : 'GainDiagPhase',
            "read-flags-from-ms" : True,
            "read-legacy-flags"  : True,
            "read-flagsets"      : '-stefcal', # ignore any existing stefcal flags
            "write-flags-ms"     : True,
            "write-flagset"      : 'stefcal',
            "write-flagset-policy": 'replace',
            "label"		 : 'cal1',
            "make-plots"         : True,
            "tile-size"          : 512,
        },
       input=INPUT,
       output=OUTPUT,
       label='selfcal1_{:d}:: First selfcal'.format(i)
       )     

recipe.add('cab/wsclean', 'cont_image2',
    {
         "msname"         :    split_msnames,
         "prefix"         :    PREFIX+"cont_2",
         "nomfsweighting" :    False,
         "mgain"          :    0.9,
         "column"         :    "CORRECTED_DATA",
         "auto-threshold" :    1,
         "auto-mask"      :    5,
         "rms-background" :    True,
         "trim"           :    trim,
         "stokes"         :    "I",
         "npix"           :    npix,
         "cellsize"       :    cell,
         "niter"          :    10000000,
         "weight"         :    '{0:s} {1:f}'.format(weight, robust),
    },
    input=INPUT,
    output=OUTPUT,
    label='cont_image2:: Make a combined continuum image of selfcaled data')


# Run it!
recipe.run(
   ['splitandavg_{:d}'.format(d) for d in range(len(msnames))]
  +['prepms_{:d}'.format(d) for d in range(len(split_msnames))]
  +['cont_image1']
  +['init_model']
  +['backup_initial_flags_{:d}'.format(d) for d in range(len(split_msnames))]
  +['selfcal1_{:d}'.format(d) for d in range(len(split_msnames))]
  +['cont_image2']
)

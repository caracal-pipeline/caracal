import stimela
import os
import sys

INPUT = 'input'
OUTPUT = 'output'
MSDIR = 'msdir'


dataids = ['2017/04/06/1491463063', '2017/04/06/1491480644']

h5files = ['{:s}.h5'.format(dataid) for dataid in dataids]
msnames = ['{:s}.ms'.format(os.path.basename(dataid)) for dataid in dataids]
prefixes = ['meerkathi-{:s}'.format(os.path.basename(dataid)) for dataid in dataids]

REMOVE_MS = 'no'

# This changes the value of variables to whatever was set through the 
# '-g/--globals' option in the 'stimela run' command
stimela.register_globals()

REMOVE_MS = REMOVE_MS.lower() in ['yes', 'true', '1']
# Delete MS if user wants to do this (see comment above)
if REMOVE_MS:
    os.system('rm -fr {0:s}/{1:s}'.format(MSDIR, msname))

# Flagging strategies
aoflag_strat1 = "aoflagger_strategies/firstpass_HI_strat2.rfis"



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
            "fromcol"         :   "DATA",
            "tocol"           :   "CORRECTED_DATA",
        },
        input=INPUT,
        output=OUTPUT,
        label='data2corrdata_{0:d}:: Copy DATA to CORRECTED_DATA column ms={1:s}'.format(i, msname))

recipe.add('cab/autoflagger', 'aoflag_1',
    {
         "msname"       :   msnames,
         "column"       :   "DATA",
         "strategy"     :   aoflag_strat1,
    },
    input=INPUT,
    output=OUTPUT,
    label='aoflag_1::Aoflagger flagging pass 1')


# If MS exists and REMOVE_MS==False, then h5toms step should not be added to recipe
h5toms = []
for i,msname in enumerate(msnames):
    if os.path.exists('{0:s}/{1:s}'.format(MSDIR, msname)) and REMOVE_MS==False:
        h5toms = []
    else:
        h5toms += 'h5toms_{:d}'.format(i)
        

recipe.run(
     h5toms +
    ['fix_uvw_{:d}'.format(d) for d in range(len(msnames))] +
    ['get_obsinfo_{:d}'.format(d) for d in range(len(msnames))] +
    ['data2corrdata_{:d}'.format(d) for d in range(len(msnames))] +
    ['flagmw_{:d}'.format(d) for d in range(len(msnames))] +
    [ 'aoflag_1',
])

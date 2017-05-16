import stimela
import os

INPUT = 'input'
OUTPUT = 'output'
MSDIR = 'msdir'


dataids = ['2017/04/06/1491463063', '2017/04/06/1491480644']

h5files = ['{:s}.h5'.format(dataid) for dataid in dataids]
msnames = ['{:s}.ms'.format(os.path.basename(dataid)) for dataid in dataids]
prefixes = ['meerkathi-{:s}'.format(os.path.basename(dataid)) for dataid in dataids]


recipe = stimela.Recipe('MeerKATHI pipeline', ms_dir=MSDIR)

for i, (h5file,msname) in enumerate(zip(h5files, msnames), 1):
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


for i, (msname, prefix) in enumerate(zip(msnames, prefixes), 1):
    recipe.add('cab/casa_lfixvis', 'fix_uvw_coords_{:d}'.format(i), 
        {
            "vis"    	:    msname,
            "reuse"     :    False,
        },
        input=INPUT,
        output=OUTPUT,
        label='fix_uvw_{0:d}:: Fix UVW coordinates for ms={1:s}'.format(i, msname))


for i, (msname, prefix) in enumerate(zip(msnames, prefixes), 1):
    recipe.add('cab/casa_listobs', 'obsinfo_{:d}'.format(i), 
        {
            "vis"    	:    msname,
            "listfile"      :    prefix+'-listobs.txt'
        },
        input=INPUT,
        output=OUTPUT,
        label='get_obsinfo_{0:d}:: Get observation information ms={1:s}'.format(i, msname))
    

recipe.run([
#    'h5toms_1', 
    'h5toms_2',
    'fix_uvw_1',
    'fix_uvw_1',
#    'get_obsinfo_1',
    'get_obsinfo_2',
])

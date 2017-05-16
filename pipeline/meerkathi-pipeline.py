import stimela

INPUT = 'input'
OUTPUT = 'output'
MSDIR = 'msdir'

h5file = '2017/04/06/1491463063.h5'
msname = '1491463063.ms'
prefix = 'meerkathi-1491463063'

recipe = stimela.Recipe('MeerKATHI pipeline', ms_dir=MSDIR)


recipe.add('cab/h5toms', 'h5toms', 
    {
        "hdf5files"    	:    h5file,
        "channel-range"	:    "'20873,21639'",
        "no-auto"       :    False,
        "output-ms"     :    msname,
        "full-pol"      :    True,
    },
    input='/var/kat/archive2/data/MeerKATAR1/telescope_products',
    output=MSDIR,
    label='h5toms:: Convert from h5 to MS')

recipe.add('cab/casa_listobs', 'obsinfo', 
    {
        "vis"    	:    msname,
        "listfile"      :    prefix+'-listobs.txt'
    },
    input=INPUT,
    output=OUTPUT,
    label='get_obsinfo:: Get observation information')


recipe.run([
    'h5toms',
    'get_obsinfo',
])


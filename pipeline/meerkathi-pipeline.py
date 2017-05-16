import stimela

INPUT = 'input'
OUTPUT = 'output'
MSDIR = 'msdir'

h5file = '2017/04/06/1491463063.h5'
msname = '1491463063.ms'
prefix = 'meerkathi-1491463063'
aoflag_strat1 = "aoflagger_strategies/firstpass_HI_strat.rfis"


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


recipe.add('cab/casa_flagdata','flagmw',
    {
        "vis"           :     msname,
        "mode"          :   'manual',
        "spw"           :   "0:725~750"
    },
    input=INPUT,
    output=OUTPUT,
    label='flagmw::Flag out channels with emission from Milky Way')


recipe.add('cab/autoflagger', 'aoflag_1',
    {
         "vis"          :    msname,
         "coulmn"       :    "DATA",
         "strategy"     :   aoflag_strat1,
    },
    input=INPUT,
    output=OUTPUT,
    label='aoflag_1::Aoflagger flagging pass 1')


recipe.run([
    'h5toms',
    'get_obsinfo',
    'flagmw',
    'aoflag1',
])


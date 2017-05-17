import stimela

INPUT = 'input'
OUTPUT = 'output'
MSDIR = 'msdir'

h5file = '2017/04/06/1491463063.h5'
msname = '1491480644.ms'
prefix = 'meerkathi-1491480644'
aoflag_strat1 = "aoflagger_strategies/firstpass_HI_strat2.rfis"
channelsout=10


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
         "msname"          :    msname,
         "column"       :    "DATA",
         "strategy"     :   aoflag_strat1,
    },
    input=INPUT,
    output=OUTPUT,
    label='aoflag_1::Aoflagger flagging pass 1')


recipe.add('cab/wsclean', 'wsclean_dirty',
    {
         "msname"         :    msname,
         "prefix"         :    prefix,
         "nomfsweighting" :    True,
         "npix"           :    256,
         "cellsize"       :    20,
         "channelsout"    :    channelsout,
         "channelrange"   :    [61,70],
         "field"          :    3,
         "column"         :    "DATA",
         "niter"          :    0,
         "weight"         :    "briggs 2"
    },
    input=INPUT,
    output=OUTPUT,
    label='wsclean_dirty::Make a WSCLEAN dirty image for each channel')


imagelist=["%s-%04d-dirty.fits:output"%(prefix,jj) for jj in range(channelsout)]

recipe.add('cab/fitstool', 'stack_channels',
    {
         "stack"      :   True,
         "image"      :   imagelist,
         "fits-axis"  :   "FREQ",
         "output"     :   "blabla.fits"
    },
    input=INPUT,
    output=OUTPUT,
    label='stack_channels::Stack individual channels made by WSClean')



recipe.run([
#    'h5toms',
#    'get_obsinfo',
#    'flagmw',
#    'aoflag_1',
#     'wsclean_dirty',
     'stack_channels',
])


import sys
import warnings


NAME = 'Make HI Cube'
def worker(pipeline, recipe, config):
    mslist = ['{0:s}-{1:s}.ms'.format(did, config['label']) for did in pipeline.dataid]
    prefix = pipeline.prefix

    for i, msname in enumerate(mslist):
        if pipeline.enable_task(config, 'uvcontsub'):
            prefix = '{0:s}_{1:d}'.format(pipeline.prefix, i)
            step = 'contsub_{:d}'.format(i)
            recipe.add('cab/casa_uvcontsub', step, 
                {
                    "msname"    : msname,
                    "fitorder"  : config['uvcontsub'].get('fitorder', 1)
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Subtract continuum'.format(step))

        if pipeline.enable_task(config, 'sunblocker'):

            try:
                from sunblocker.sunblocker import Sunblocker
                if config['sunblocker']['use_contsub']:
                    msname = msname+'.contsub'
                step = 'sunblocker_{0:d}'.format(i)
                recipe.add(Sunblocker().phazer, step, 
                    {
                        "inset"     : ['{0:s}/{1:s}'.format(pipeline.msdir, msname)],
                        "outset"    : ['{0:s}/{1:s}'.format(pipeline.msdir, msname)],
                        "imsize"    : config['image'].get('npix', 300),
                        "cell"      : config['image'].get('cell', 20),
                        "pol"       : 'i',
                        "threshold" : config['sunblocker'].get('threshold', 4),
                        "mode"      : 'all',
                        "radrange"  : 0,
                        "angle"     : 0,
                        "showdir"   : pipeline.output,
                        "show"      : prefix + '.sublocker.pdf',
                        "verb"      : True,
                        "dryrun"    : False,
                    },
                    label='{0:s}:: Block out sun'.format(step))

            except ImportError:
                warnings.warn('Sunblocker program not found. Will skip sublocking step')

            
    if pipeline.enable_task(config, 'image'):
        if config['image']['use_contsub']:
            mslist = ['{0:s}-{1:s}.ms.contsub'.format(did, config['label']) for did in pipeline.dataid]
	
        step = 'image_HI'
        recipe.add('cab/wsclean', step,
              {                       
                  "msname"    : mslist,
                  "weight"    : 'briggs {}'.format(config['image'].get('robust', 2)),
                  "npix"      : config['image'].get('npix', 300),
                  "trim"      : config['image'].get('trim', 256),
                  "scale"     : config['image'].get('cell', 20),
                  "prefix"    : pipeline.prefix+'_HI',
                  "niter"     : config['image'].get('niter', 1000000),
                  "mgain"     : config['image'].get('mgain', 0.90),
                  "channelsout"     : config['image'].get('nchans', pipeline.nchans[0]),
                  "auto-threshold"  : config['image'].get('autothreshold', 5),
                  #"auto-mask"  :   config['image'].get('automask', 3), # causes segfaults in channel mode. Will be fixed in wsclean 2.4
                  "channelrange" : config['image'].get('channelrange', [0, pipeline.nchans[0]]),
              },  
        input=pipeline.input,
        output=pipeline.output,
        label='{:s}:: Image HI'.format(step))

    if pipeline.enable_task(config, 'make_cube'):
        step = 'make_cube'
        recipe.add('cab/fitstool', step,
            {    
                "image"    : [pipeline.prefix+'_HI-{:04d}-image.fits:output'.format(d) for d in xrange(pipeline.nchans[0])],
                "output"   : pipeline.prefix+'_HI-cube.fits',
                "stack"    : True,
                "delete-files" : True,
                "fits-axis": 'FREQ',
            },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Make cube from wsclean channel images'.format(step))


    if pipeline.enable_task(config, 'sofia'):
        step = 'sofia_sources'
        recipe.add('cab/sofia', step,
            {
        #    USE THIS FOR THE WSCLEAN DIRTY CUBE
        #    "import.inFile"     :   '{:s}-cube.dirty.fits:output'.format(combprefix),
        #    USE THIS FOR THE CASA CLEAN CUBE
            "import.inFile"         : pipeline.prefix+'_HI-cube.fits:output',       # CASA CLEAN cube
            "steps.doMerge"         : config['sofia'].get('merge', True),
            "steps.doMom0"          : True,
            "steps.doMom1"          : False,
            "steps.doParameterise"  : False,
            "steps.doReliability"   : False,
            "steps.doWriteCat"      : False,
            "steps.doWriteMask"     : True,
            "steps.doFlag"          : True,
            "flag.regions"          : config['sofia'].get('flagregion', [0,255,0,255,881,937]),
            "SCfind.threshold"      : config['sofia'].get('threshold', 4),
            "merge.radiusX"         : config['sofia'].get('mergeX', 2),
            "merge.radiusY"         : config['sofia'].get('mergeY', 2),
            "merge.radiusZ"         : config['sofia'].get('mergeZ', 3),
            "merge.minSizeX"        : config['sofia'].get('minSizeX', 3),
            "merge.minSizeY"        : config['sofia'].get('minSizeY', 3),
            "merge.minSizeZ"        : config['sofia'].get('minSizeZ', 5),
            },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Make SoFiA mask and images'.format(step))

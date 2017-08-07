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
                  "prefix"    : prefix+'_HI',
                  "niter"     : config['image'].get('niter', 1000000),
                  "mgain"     : config['image'].get('mgain', 0.90),
                  "channelsout"     : config['image'].get('nchans', pipeline.nchans),
                  "auto-threshold"  : config['image'].get('autothreshold', 5),
                  #"auto-mask"  :   config['image'].get('automask', 3), # causes segfaults in channel mode. Will be fixed in wsclean 2.4
                  "channelrange" : config['image'].get('channelrange', [0,pipeline.nchans]),
              },  
        input=pipeline.input,
        output=pipeline.output,
        label='{:s}:: Image HI'.format(step))

    if pipeline.enable_task(config, 'make_cube'):
        step = 'make_cube'
        recipe.add('cab/fitstool', step,
            {    
                "image"    : [prefix+'_HI-{:04d}-image.fits:output'.format(d) for d in xrange(pipeline.nchans)],
                "output"   : prefix+'_HI-cube.fits',
                "stack"    : True,
                "fits-axis": 'FREQ',
            },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Make cube from wsclean channel images'.format(step))

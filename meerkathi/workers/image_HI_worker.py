import sys


NAME = 'Make HI Cube'
def worker(pipeline, recipe, config):
    mslist = ['{0:s}-{1:s}.ms'.format(did, config['label']) for did in pipeline.dataid]
    prefix = pipeline.prefix

    if pipeline.enable_task(config, 'uvcontsub'):
        for i, msname in enumerate(mslist):
            step = 'contsub_{:d}'.format(i)
            recipe.add('cab/casa_uvcontsub', step, 
                {
                    "msname"    : msname,
                    "fitorder"  : config['uvcontsub'].get('fitorder', 1)
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Subtract continuum'.format(step))
            
    if pipeline.enable_task(config, 'image'):
        if config['image']['use_contsub']:
            mslist = ['{0:s}-{1:s}.ms.contsub'.format(did, config['label']) for did in pipeline.dataid]

        step = 'image_HI'
        recipe.add('cab/wsclean', step,
              {                       
                  "msname"    : mslist,
                  "weight"    : 'briggs {}'.format(config['image']['weight']),
                  "npix"      : config['image']['npix'],
                  "trim"      : config['image']['trim'],
                  "prefix"    : prefix+'_HI',
                  "niter"     : config['image']['niter'],
                  "mgain"     : config['image']['mgain'],
                  "channelsout"     : pipeline.nchans,
                  "auto-threshold"  : config['image']['autothreshold'],
                  "auto-mask"  :   config['image']['automask'],
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

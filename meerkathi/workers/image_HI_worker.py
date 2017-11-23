import sys
import warnings
import stimela.dismissable as sdm

NAME = 'Make HI Cube'
def worker(pipeline, recipe, config):
    mslist = ['{0:s}-{1:s}.ms'.format(did, config['label']) for did in pipeline.dataid]
    prefix = pipeline.prefix
    restfreq = config.get('restfreq','1.420405752GHz')
    npix = config.get('npix', 1024)
    cell = config.get('cell', 7)
    weight = config.get('weight', 'natural')
    robust = config.get('robust', 0)


    for i, msname in enumerate(mslist):
        if pipeline.enable_task(config, 'uvcontsub'):
            prefix = '{0:s}_{1:d}'.format(pipeline.prefix, i)
            step = 'contsub_{:d}'.format(i)
            recipe.add('cab/casa_uvcontsub', step, 
                {
                    "msname"    : msname,
                    "fitorder"  : config['uvcontsub'].get('fitorder', 1),
                    "fitspw"    : sdm.dismissable(config['uvcontsub'].get('fitspw',None))
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Subtract continuum'.format(step))

        if pipeline.enable_task(config, 'sunblocker'):
            if config['sunblocker']['use_contsub']:
                msname = msname+'.contsub'
            step = 'sunblocker_{0:d}'.format(i)
            recipe.add("cab/sunblocker", step, 
                {
                    "command"   : "phazer",
                    "inset"     : msname,
                    "outset"    : msname,
                    "imsize"    : config['sunblocker'].get('imsize', npix),
                    "cell"      : config['sunblocker'].get('cell', cell),
                    "pol"       : 'i',
                    "threshold" : config['sunblocker'].get('threshold', 4),
                    "mode"      : 'all',
                    "radrange"  : 0,
                    "angle"     : 0,
                    "show"      : prefix + '.sunblocker.pdf',
                    "verb"      : True,
                    "dryrun"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Block out sun'.format(step))

            
    if pipeline.enable_task(config, 'wsclean_image'):
        if config['wsclean_image']['use_contsub']:
            mslist = ['{0:s}-{1:s}.ms.contsub'.format(did, config['label']) for did in pipeline.dataid]
        step = 'wsclean_image_HI'
        spwid = config['wsclean_image'].get('spwid', 0)
        nchans = config['wsclean_image'].get('nchans','all')
        # Construct weight specification
        if config['wsclean_image'].get('weight', 'natural') == 'briggs':
            weight = 'briggs {0:.3f}'.format( config['wsclean_image'].get('robust', robust))
        else:
            weight = config['wsclean_image'].get('weight', weight)
        if nchans=='all': nchans=pipeline.nchans[0][spwid]
        recipe.add('cab/wsclean', step,
              {                       
                  "msname"    : mslist,
                  "weight"    : weight,
                  "npix"      : config['wsclean_image'].get('npix', npix),
                  "trim"      : dsm(config['wsclean_image'].get('trim', None)),
                  "scale"     : config['wsclean_image'].get('cell', cell),
                  "prefix"    : pipeline.prefix+'_HI',
                  "niter"     : config['wsclean_image'].get('niter', 1000000),
                  "mgain"     : config['wsclean_image'].get('mgain', 0.90),
                  "channelsout"     : nchans,
                  "auto-threshold"  : config['wsclean_image'].get('autothreshold', 5),
                  "auto-mask"  :   config['wsclean_image'].get('automask', 3),
                  "channelrange" : config['wsclean_image'].get('channelrange', [0, pipeline.nchans[0][spwid]]),
              },  
        input=pipeline.input,
        output=pipeline.output,
        label='{:s}:: Image HI'.format(step))

        if config['wsclean_image']['make_cube']:
            #nchans = config['wsclean_image'].get('nchans', pipeline.nchans[0][spwid])
            if not config['wsclean_image'].get('niter', 1000000): imagetype=['image','dirty']
            else: imagetype=['image','dirty','psf','residual','first-residual','model']
            for mm in imagetype:
                step = 'make_{0:s}_cube'.format(mm.replace('-','_'))
                recipe.add('cab/fitstool', step,
                    {    
                        "image"    : [pipeline.prefix+'_HI-{0:04d}-{1:s}.fits:output'.format(d,mm) for d in xrange(nchans)],
                        "output"   : pipeline.prefix+'_HI-{0:s}-cube.fits'.format(mm),
                        "stack"    : True,
                        "delete-files" : True,
                        "fits-axis": 'FREQ',
                    },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Make {1:s} cube from wsclean {1:s} channels'.format(step,mm.replace('-','_')))

    if pipeline.enable_task(config, 'casa_image'):
        if config['casa_image']['use_contsub']:
            mslist = ['{0:s}-{1:s}.ms.contsub'.format(did, config['label']) for did in pipeline.dataid]
        step = 'casa_image_HI'
        spwid = config['casa_image'].get('spwid', 0)
        nchans = config['casa_image'].get('nchans','all')
        if nchans=='all': nchans=pipeline.nchans[0][spwid]
        recipe.add('cab/casa_clean', step,
            {
                 "msname"         :    mslist,
                 "prefix"         :    pipeline.prefix+'_HI',
#                 "field"          :    target,
                 "mode"           :    'channel',
                 "nchan"          :    nchans,
                 "start"          :    config['casa_image'].get('startchan', 0,),
                 "interpolation"  :    'nearest',
                 "niter"          :    config['casa_image'].get('niter', 1000000),
                 "psfmode"        :    'hogbom',
                 "threshold"      :    config['casa_image'].get('threshold', '10mJy'),
                 "npix"           :    config['casa_image'].get('npix', npix),
                 "cellsize"       :    config['casa_image'].get('cell', cell),
                 "weight"         :    config['casa_image'].get('weight', weight),
                 "robust"         :    config['casa_image'].get('robust', robust),
#                 "wprojplanes"    :    1,
                 "port2fits"      :    True,
                 "restfreq"       :    restfreq,
            },
            input=pipeline.input,
            output=pipeline.output,
            label='{:s}:: Image HI'.format(step))


    if pipeline.enable_task(config, 'sofia'):
        if config['sofia']['imager']=='casa': cubename=pipeline.prefix+'_HI.image.fits:output'
        elif config['sofia']['imager']=='wsclean': cubename=pipeline.prefix+'_HI-cube.fits:output'
        step = 'sofia_sources'
        recipe.add('cab/sofia', step,
            {
            "import.inFile"         : cubename,
            "steps.doFlag"          : config['sofia'].get('flag', False),
            "steps.doScaleNoise"    : True,
            "steps.doSCfind"        : True,
            "steps.doMerge"         : config['sofia'].get('merge', True),
            "steps.doReliability"   : False,
            "steps.doParameterise"  : False,
            "steps.doWriteMask"     : True,
            "steps.doMom0"          : True,
            "steps.doMom1"          : False,
            "steps.doWriteCat"      : False,
            "flag.regions"          : config['sofia'].get('flagregion', []),
            "scaleNoise.statistic"  : config['sofia'].get('rmsMode', 'mad'),
            "SCfind.threshold"      : config['sofia'].get('threshold', 4),
            "SCfind.rmsMode"        : config['sofia'].get('rmsMode', 'mad'),
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

    if pipeline.enable_task(config, 'flagging_summary'):
        for i,msname in enumerate(mslist):
            step = 'flagging_summary_image_HI_{0:d}'.format(i)
            recipe.add('cab/casa_flagdata', step,
                {
                  "vis"         : msname,
                  "mode"        : 'summary',
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))

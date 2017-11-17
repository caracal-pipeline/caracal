import sys
import warnings
import stimela.dismissable as sdm

NAME = 'Make HI Cube'
def worker(pipeline, recipe, config):
    mslist = ['{0:s}-{1:s}.ms'.format(did, config['label']) for did in pipeline.dataid]
    prefix = pipeline.prefix
    restfreq = config.get('restfreq','1.420405752GHz')

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
                        "show"      : prefix + '.sunblocker.pdf',
                        "verb"      : True,
                        "dryrun"    : False,
                    },
                    label='{0:s}:: Block out sun'.format(step))

            except ImportError:
                warnings.warn('Sunblocker program not found. Will skip sublocking step')

            
    if pipeline.enable_task(config, 'wsclean_image'):
        if config['wsclean_image']['use_contsub']:
            mslist = ['{0:s}-{1:s}.ms.contsub'.format(did, config['label']) for did in pipeline.dataid]
        step = 'wsclean_image_HI'
        spwid = config['wsclean_image'].get('spwid', 0)
        nchans = config['wsclean_image'].get('nchans','all')
        if nchans=='all': nchans=pipeline.nchans[0][spwid]
        recipe.add('cab/wsclean', step,
              {                       
                  "msname"    : mslist,
                  "weight"    : '{0} {1}'.format(config['wsclean_image'].get('weight', 
                                                  'natural'), config['wsclean_image'].get('robust', '')),
                  "npix"      : config['wsclean_image'].get('npix', 300),
                  "trim"      : config['wsclean_image'].get('trim', 256),
                  "scale"     : config['wsclean_image'].get('cell', 20),
                  "prefix"    : pipeline.prefix+'_HI',
                  "niter"     : config['wsclean_image'].get('niter', 1000000),
                  "mgain"     : config['wsclean_image'].get('mgain', 0.90),
                  "channelsout"     : nchans,
                  "auto-threshold"  : config['wsclean_image'].get('autothreshold', 5),
                  "auto-mask"  :   config['wsclean_image'].get('automask', 3), # causes segfaults in channel mode. Will be fixed in wsclean 2.4
                  "channelrange" : config['wsclean_image'].get('channelrange', [0, pipeline.nchans[0][spwid]]),
              },  
        input=pipeline.input,
        output=pipeline.output,
        label='{:s}:: Image HI'.format(step))

        if config['wsclean_image']['make_cube']:
            nchans = config['wsclean_image'].get('nchans', pipeline.nchans[0][spwid])
            step = 'make_cube'
            recipe.add('cab/fitstool', step,
                {    
                    "image"    : [pipeline.prefix+'_HI-{:04d}-image.fits:output'.format(d) for d in xrange(nchans)],
                    "output"   : pipeline.prefix+'_HI-cube.fits',
                    "stack"    : True,
                    "delete-files" : True,
                    "fits-axis": 'FREQ',
                },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Make cube from wsclean channel images'.format(step))

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
#                 "column"         :    "CORRECTED_DATA",
                 "mode"           :    'channel',
                 "nchan"          :    nchans,
                 "start"          :    config['casa_image'].get('startchan', 0,),
                 "interpolation"  :    'nearest',
                 "niter"          :    config['casa_image'].get('niter', 1000000),
                 "psfmode"        :    'hogbom',
                 "threshold"      :    config['casa_image'].get('threshold', '10mJy'),
                 "npix"           :    config['casa_image'].get('npix', 300),
                 "cellsize"       :    config['casa_image'].get('cell', 20),
                 "weight"         :    config['casa_image'].get('weight', 'briggs'),
                 "robust"         :    config['casa_image'].get('robust', 2.0),
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

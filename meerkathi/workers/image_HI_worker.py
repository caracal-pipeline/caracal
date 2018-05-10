import sys
import os
import warnings
import stimela.dismissable as sdm
from astropy.io import fits
import meerkathi
# Modules useful to calculate common barycentric frequency grid
from astropy.time import Time
from astropy.coordinates import SkyCoord
from astropy.coordinates import EarthLocation
from astropy import constants
import re, datetime
import numpy as np


def freq_to_vel(filename):
    C = 2.99792458e+8 # m/s
    HI = 1.4204057517667e+9 # Hz
    filename=filename.split(':')
    filename='{0:s}/{1:s}'.format(filename[1],filename[0])
    if not os.path.exists(filename): meerkathi.log.info('Skipping conversion for {0:s}. File does not exist.'.format(filename))
    else:
        with fits.open(filename, mode='update') as cube:
            headcube = cube[0].header
            if 'restfreq' in headcube: restfreq = float(headcube['restfreq'])
            else: restfreq = HI
            if 'FREQ' in headcube['ctype3']:
                headcube['cdelt3'] = -C * float(headcube['cdelt3'])/restfreq
                headcube['crval3'] =  C * (1-float(headcube['crval3'])/restfreq)
                headcube['ctype3'] = 'VELO-HEL'
                if 'cunit3' in headcube: del headcube['cunit3']
            else: meerkathi.log.info('Skipping conversion for {0:s}. Input cube not in frequency.'.format(filename))

NAME = 'Make HI Cube'
def worker(pipeline, recipe, config):
    mslist = ['{0:s}-{1:s}.ms'.format(did, config['label']) for did in pipeline.dataid]
    #prefix = pipeline.prefix
    prefixes = pipeline.prefixes
    restfreq = config.get('restfreq','1.420405752GHz')
    npix = config.get('npix', [1024])
    if len(npix) == 1:
        npix = [npix[0],npix[0]]
    cell = config.get('cell', 7)
    weight = config.get('weight', 'natural')
    robust = config.get('robust', 0)

    # Find common barycentric frequency grid for all input .MS
    firstchanfreq=pipeline.firstchanfreq
    chanw=pipeline.chanwidth
    lastchanfreq=pipeline.lastchanfreq
    RA=pipeline.TRA
    Dec=pipeline.TDec
    teldict={'meerkat':[21.4430,-30.7130],'gmrt':[73.9723, 19.1174],'vla':[-107.6183633,34.0783584],'wsrt':[52.908829698,6.601997592],'atca':[-30.307665436,149.550164466]}
    tellocation=teldict[config.get('telescope','meerkat')]
    telloc=EarthLocation.from_geodetic(tellocation[0],tellocation[1])
    firstchanfreq_dopp,chanw_dopp,lastchanfreq_dopp = firstchanfreq,chanw,lastchanfreq
    for i, prefix in enumerate(prefixes):
        msinfo = '{0:s}/{1:s}-obsinfo.txt'.format(pipeline.output, prefix)
        with open(msinfo, 'r') as searchfile:
            for longdatexp in searchfile:
               if "Observed from" in longdatexp:
                   dates   = longdatexp
                   matches = re.findall('(\d{2}[- ](\d{2}|January|Jan|February|Feb|March|Mar|April|Apr|May|May|June|Jun|July|Jul|August|Aug|September|Sep|October|Oct|November|Nov|December|Dec)[\- ]\d{2,4})', dates)
                   obsstartdate = str(matches[0][0])
                   obsdate = datetime.datetime.strptime(obsstartdate, '%d-%b-%Y').strftime('%Y-%m-%d')
                   targetpos = SkyCoord(RA[i], Dec[i], frame='icrs', unit='deg')
                   v=targetpos.radial_velocity_correction(kind='barycentric',obstime=Time(obsdate), location=telloc).to('km/s')
                   corr=np.sqrt((constants.c-v)/(constants.c+v))
                   firstchanfreq_dopp[i], chanw_dopp[i], lastchanfreq_dopp[i] = firstchanfreq_dopp[i]*corr, chanw_dopp[i]*corr, lastchanfreq_dopp[i]*corr  #Hz, Hz, Hz
    # WARNING: the following line assumes a single SPW for the HI line data being processed by this worker!
    comfreq0,comfreql,comchanw = np.max(firstchanfreq_dopp), np.min(lastchanfreq_dopp), np.max(chanw_dopp)
    nchan_dopp=int(np.floor(((comfreql - comfreq0)/comchanw)))+1
    meerkathi.log.info('Found common barycentric frequency grid for all input .MS: {0:d} channels starting at {1:.3f} Hz and with channel width {2:.3f} Hz.'.format(nchan_dopp,comfreq0,comchanw))

    for i, msname in enumerate(mslist):
        prefix = '{0:s}_{1:d}'.format(pipeline.prefix, i)

        if pipeline.enable_task(config, 'subtractmodelcol'):
            step = 'modelsub_{:d}'.format(i)
            recipe.add('cab/msutils', step,
                {
                    "command"  : 'sumcols',
                    "msname"   : msname,
                    "subtract" : True,
                    "col1"     : 'CORRECTED_DATA',
                    "col2"     : 'MODEL_DATA',
                    "column"   : 'CORRECTED_DATA'
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Subtract model column'.format(step))


        if pipeline.enable_task(config, 'uvcontsub'):
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
            if config['sunblocker']['use_contsub']: msnamesb = msname+'.contsub'
            else: msnamesb = msname
            step = 'sunblocker_{0:d}'.format(i)
            recipe.add("cab/sunblocker", step, 
                {
                    "command"   : "phazer",
                    "inset"     : msnamesb,
                    "outset"    : msnamesb,
                    "imsize"    : config['sunblocker'].get('imsize', max(npix)),
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
 

        if pipeline.enable_task(config, 'doppler_corr'):
           if os.path.exists('{1:s}/{0:s}.regrd'.format(msname,pipeline.msdir)): os.system('rm -r {1:s}/{0:s}.regrd'.format(msname,pipeline.msdir))
           if config['doppler_corr']['use_contsub']:
                msnamedc = msname+'.contsub'
                col='data'
                if os.path.exists('{1:s}/{0:s}.regrd'.format(msname,pipeline.msdir)): os.system('rm -r {1:s}/{0:s}.regrd'.format(msname,pipeline.msdir))
           else:
                msnamedc = msname
                col='corrected'

           if config['doppler_corr'].get('nchan', nchan_dopp)=='all': nchan_mstr=nchan_dopp
           else: nchan_mstr=min(nchan_dopp,config['doppler_corr'].get('nchan', nchan_dopp))

           step = 'doppler_corr_{:d}'.format(i)
           recipe.add('cab/casa_mstransform', step,
                {
                    "msname"    : msnamedc,
                    "outputvis" : msnamedc+'.regrd',
                    "regridms"  : config['doppler_corr'].get('regrid', True),
                    "mode"      : config['doppler_corr'].get('mode', 'frequency'),
                    "nchan"     : nchan_mstr,
                    "start"     : '{0:.3f}Hz'.format(comfreq0),
                    "width"     : '{0:.3f}Hz'.format(comchanw),
                    "interpolation" : 'nearest',
                    "datacolumn" : col,
                    "restfreq"  :restfreq,
                    "outframe"  : config['doppler_corr'].get('outframe', 'bary'),
                    "veltype"   : config['doppler_corr'].get('veltype', 'radio'),
                    "douvcontsub" : config['doppler_corr'].get('msuvcontsub', False),
                    "fitspw"    : sdm.dismissable(config['doppler_corr'].get('fitspw', None)),
                    "fitorder"  : config['doppler_corr'].get('fitorder', 1),
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Doppler tracking corrections'.format(step))


    if pipeline.enable_task(config, 'wsclean_image'):
        if not config['wsclean_image']['use_doppcorr'] and config['wsclean_image']['use_contsub']:
            mslist = ['{0:s}-{1:s}.ms.contsub'.format(did, config['label']) for did in pipeline.dataid]
        elif config['wsclean_image']['use_doppcorr'] and config['wsclean_image']['use_contsub']:
            mslist = ['{0:s}-{1:s}.ms.contsub.regrd'.format(did, config['label']) for did in pipeline.dataid]
        elif config['wsclean_image']['use_doppcorr'] and not config['wsclean_image']['use_contsub']:
            mslist = ['{0:s}-{1:s}.ms.regrd'.format(did, config['label']) for did in pipeline.dataid]
        else:
            mslist = ['{0:s}-{1:s}.ms'.format(did, config['label']) for did in pipeline.dataid]
        step = 'wsclean_image_HI'
        spwid = config['wsclean_image'].get('spwid', 0)
        nchans = config['wsclean_image'].get('nchans',0)
        if nchans == 0: nchans = 'all'
        if nchans=='all':
          if config['wsclean_image']['use_doppcorr']: nchans=nchan_dopp
          else: nchans=pipeline.nchans[0][spwid]
        firstchan = config['wsclean_image'].get('firstchan', 0)
        binchans  = config['wsclean_image'].get('binchans', 1)
        channelrange = [firstchan, firstchan+nchans*binchans]
        # Construct weight specification
        if config['wsclean_image'].get('weight', 'natural') == 'briggs':
            weight = 'briggs {0:.3f}'.format( config['wsclean_image'].get('robust', robust))
        else:
            weight = config['wsclean_image'].get('weight', weight)
        recipe.add('cab/wsclean', step,
              {                       
                  "msname"    : mslist,
                  "prefix"    : pipeline.prefix+'_HI',
                  "weight"    : weight,
                  "taper-gaussian" : str(config['wsclean_image'].get('taper',0)),
                  "pol"        : config['wsclean_image'].get('pol','I'),
                  "npix"      : config['wsclean_image'].get('npix', npix),
                  "padding"   : config['wsclean_image'].get('padding', 1.2),
                  "scale"     : config['wsclean_image'].get('cell', cell),
                  "channelsout"     : nchans,
                  "channelrange" : channelrange,
                  "niter"     : config['wsclean_image'].get('niter', 1000000),
                  "mgain"     : config['wsclean_image'].get('mgain', 1.0),
                  "auto-threshold"  : config['wsclean_image'].get('autothreshold', 5),
                  "auto-mask"  :   config['wsclean_image'].get('automask', 3),
                  "no-update-model-required": config['wsclean_image'].get('no-update-mod', True)
              },  
        input=pipeline.input,
        output=pipeline.output,
        label='{:s}:: Image HI'.format(step))

        if config['wsclean_image']['make_cube']:
            if not config['wsclean_image'].get('niter', 1000000): imagetype=['image','dirty']
            else:
                imagetype=['image','dirty','psf','residual','model']
                if config['wsclean_image'].get('mgain', 1.0)<1.0: imagetype.append('first-residual')
            for mm in imagetype:
                step = 'make_{0:s}_cube'.format(mm.replace('-','_'))
                recipe.add('cab/fitstool', step,
                    {    
                        "image"    : [pipeline.prefix+'_HI-{0:04d}-{1:s}.fits:output'.format(d,mm) for d in xrange(nchans)],
                        "output"   : pipeline.prefix+'_HI.{0:s}.fits'.format(mm),
                        "stack"    : True,
                        "delete-files" : True,
                        "fits-axis": 'FREQ',
                    },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Make {1:s} cube from wsclean {1:s} channels'.format(step,mm.replace('-','_')))

        if config['wsclean_image']['make_mask']:
           step = 'make_sofia_mask'
           cubename = pipeline.prefix+'_HI.image.fits:output'
           outmask = pipeline.prefix+'_HI.image_clean'
           recipe.add('cab/sofia', step,
               {
                "import.inFile"         : cubename,
                "steps.doFlag"          : False,
                "steps.doScaleNoise"    : True,
                "steps.doSCfind"        : True,
                "steps.doMerge"         : True,
                "steps.doReliability"   : False,
                "steps.doParameterise"  : False,
       	        "steps.doWriteMask"     : True,
                "steps.doMom0"          : False,
                "steps.doMom1"          : False,
                "steps.doWriteCat"      : False,
                "flag.regions"          : [], 
                "scaleNoise.statistic"  : 'mad' ,
                "SCfind.threshold"      : 4, 
                "SCfind.rmsMode"        : 'mad',
                "merge.radiusX"         : 2, 
                "merge.radiusY"         : 2,
                "merge.radiusZ"         : 2,
                "merge.minSizeX"        : 2,
                "merge.minSizeY"        : 2, 
                "merge.minSizeZ"        : 2,
                "writeCat.basename"     : outmask,
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Make SoFiA mask'.format(step))

 
    if pipeline.enable_task(config, 'rewsclean_image'):
        HIclean_mask=config['rewsclean_image'].get('fitsmask', 'sofia_mask') 
        if HIclean_mask=='sofia_mask':
          HIclean_mask=pipeline.prefix+'_HI.image_clean_mask.fits:output' 

        if not config['wsclean_image']['use_doppcorr'] and config['wsclean_image']['use_contsub']:
            mslist = ['{0:s}-{1:s}.ms.contsub'.format(did, config['label']) for did in pipeline.dataid]
        elif config['wsclean_image']['use_doppcorr'] and config['wsclean_image']['use_contsub']:
            mslist = ['{0:s}-{1:s}.ms.contsub.regrd'.format(did, config['label']) for did in pipeline.dataid]
        elif config['wsclean_image']['use_doppcorr'] and not config['wsclean_image']['use_contsub']:
            mslist = ['{0:s}-{1:s}.ms.regrd'.format(did, config['label']) for did in pipeline.dataid]
        else:
            mslist = ['{0:s}-{1:s}.ms'.format(did, config['label']) for did in pipeline.dataid]

        step = 'rewsclean_image_HI'
        
        spwid = config['wsclean_image'].get('spwid', 0)
        nchans = config['wsclean_image'].get('nchans',0)
        if nchans == 0: nchans = 'all'
        if nchans=='all':
          if config['wsclean_image']['use_doppcorr']: nchans=nchan_dopp
          else: nchans=pipeline.nchans[0][spwid]
        firstchan = config['wsclean_image'].get('firstchan', 0)
        binchans  = config['wsclean_image'].get('binchans', 1)
        channelrange = [firstchan, firstchan+nchans*binchans]
        if config['wsclean_image'].get('weight', 'natural') == 'briggs':
            weight = 'briggs {0:.3f}'.format( config['wsclean_image'].get('robust', robust))
        else:
            weight = config['wsclean_image'].get('weight', weight)
        recipe.add('cab/wsclean', step,
            {                       
                  "msname"    : mslist,
                  "prefix"    : pipeline.prefix+'_HI',
                  "weight"    : weight,
                  "taper-gaussian" : str(config['wsclean_image'].get('taper',0)),
                  "pol"        : config['wsclean_image'].get('pol','I'),
                  "npix"      : config['wsclean_image'].get('npix', npix),
                  "padding"   : config['wsclean_image'].get('padding', 1.2),
                  "scale"     : config['wsclean_image'].get('cell', cell),
                  "channelsout"     : nchans,
                  "channelrange" : channelrange,
                  "fitsmask"  : HIclean_mask, 
                  "niter"     : config['rewsclean_image'].get('niter', 1000000),
                  "mgain"     : config['wsclean_image'].get('mgain', 1.0),
                  "auto-threshold"  : config['rewsclean_image'].get('autothreshold', 5),
                  "no-update-model-required": config['wsclean_image'].get('no-update-mod', True)
            },  
        input=pipeline.input,
        output=pipeline.output,
        label='{:s}:: re-Image HI'.format(step))

        if config['wsclean_image']['make_cube']:
            if not config['rewsclean_image'].get('niter', 1000000): imagetype=['image','dirty']
            else:
                imagetype=['image','dirty','psf','residual','model']
                if config['wsclean_image'].get('mgain', 1.0)<1.0: imagetype.append('first-residual')
            for mm in imagetype:
                step = 'make_{0:s}_cube'.format(mm.replace('-','_'))
                recipe.add('cab/fitstool', step,
                    {    
                        "image"    : [pipeline.prefix+'_HI-{0:04d}-{1:s}.fits:output'.format(d,mm) for d in xrange(nchans)],
                        "output"   : pipeline.prefix+'_HI.{0:s}.fits'.format(mm),
                        "stack"    : True,
                        "delete-files" : True,
                        "fits-axis": 'FREQ',
                    },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Make {1:s} cube from rewsclean {1:s} channels'.format(step,mm.replace('-','_')))
            cubename = pipeline.prefix+'_HI-image.fits'

    if pipeline.enable_task(config, 'casa_image'):
        if config['casa_image']['use_contsub']:
            mslist = ['{0:s}-{1:s}.ms.contsub'.format(did, config['label']) for did in pipeline.dataid]
        step = 'casa_image_HI'
        spwid = config['casa_image'].get('spwid', 0)
        nchans = config['casa_image'].get('nchans', 0)
        if nchans == 0:
            nchans=pipeline.nchans[0][spwid]
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
                 "stokes"         :    config['casa_image'].get('pol','I'),
#                 "wprojplanes"    :    1,
                 "port2fits"      :    True,
                 "restfreq"       :    restfreq,
            },
            input=pipeline.input,
            output=pipeline.output,
            label='{:s}:: Image HI'.format(step))

    if pipeline.enable_task(config,'freq_to_vel'):
        for ss in ['dirty','psf','residual','model','image']:
            cubename=pipeline.prefix+'_HI.'+ss+'.fits:output'
            recipe.add(freq_to_vel, 'spectral_header_to_vel_radio_{0:s}_cube'.format(ss),
                       {
                           'filename' : cubename,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Convert spectral axis from frequency to radio velocity for cube {0:s}'.format(cubename))

    if pipeline.enable_task(config, 'sofia'):
        step = 'sofia_sources'
        recipe.add('cab/sofia', step,
            {
            "import.inFile"         : pipeline.prefix+'_HI.image.fits:output',
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

import sys
import os
import warnings
import stimela.dismissable as sdm
import astropy
from astropy.io import fits
import meerkathi
# Modules useful to calculate common barycentric frequency grid
from astropy.time import Time
from astropy.coordinates import SkyCoord
from astropy.coordinates import EarthLocation
from astropy import constants
import astropy.units as asunits
import re, datetime
import numpy as np
import yaml

def freq_to_vel(filename,reverse):
    C = 2.99792458e+8       # m/s
    HI = 1.4204057517667e+9 # Hz
    filename=filename.split(':')
    filename='{0:s}/{1:s}'.format(filename[1],filename[0])
    if not os.path.exists(filename): meerkathi.log.info('Skipping conversion for {0:s}. File does not exist.'.format(filename))
    else:
        with fits.open(filename, mode='update') as cube:
            headcube = cube[0].header
            if 'restfreq' in headcube: restfreq = float(headcube['restfreq'])
            else:
                restfreq = HI
                headcube['restfreq'] = restfreq                 # add rest frequency to FITS header

            if 'FREQ' in headcube['ctype3'] and not reverse:    # convert from frequency to radio velocity
                headcube['cdelt3'] = -C * float(headcube['cdelt3'])/restfreq
                headcube['crval3'] =  C * (1-float(headcube['crval3'])/restfreq)
                headcube['ctype3'] = 'VRAD'                     # FITS standard for radio velocity as per https://fits.gsfc.nasa.gov/standard40/fits_standard40aa-le.pdf
                if 'cunit3' in headcube: del headcube['cunit3'] # delete cunit3 because we adopt the default units = m/s

            elif 'VRAD' in headcube['ctype3'] and reverse:      # convert from radio velocity to frequency
                headcube['cdelt3'] = -restfreq * float(headcube['cdelt3']) / C
                headcube['crval3'] =  restfreq * (1-float(headcube['crval3'])/C)
                headcube['ctype3'] = 'FREQ'
                if 'cunit3' in headcube: del headcube['cunit3'] # delete cunit3 because we adopt the default units = Hz
            else:
                if not reverse: meerkathi.log.info('Skipping conversion for {0:s}. Input cube not in frequency.'.format(filename))
                else: meerkathi.log.info('Skipping conversion for {0:s}. Input cube not in velocity.'.format(filename))

def remove_stokes_axis(filename):
    filename=filename.split(':')
    filename='{0:s}/{1:s}'.format(filename[1],filename[0])
    if not os.path.exists(filename): meerkathi.log.info('Skipping Stokes axis removal for {0:s}. File does not exist.'.format(filename))
    else:
        with fits.open(filename, mode='update') as cube:
            headcube = cube[0].header
            if headcube['naxis'] == 4 and headcube['ctype4'] == 'STOKES':
                cube[0].data=cube[0].data[0]
                del headcube['cdelt4']
                del headcube['crpix4']
                del headcube['crval4']
                del headcube['ctype4']
                if 'cunit4' in headcube: del headcube['cunit4']
            else: meerkathi.log.info('Skipping Stokes axis removal for {0:s}. Input cube has less than 4 axis or the 4th axis type is not "STOKES".'.format(filename))

def fix_specsys(filename,specframe):
    # Reference frame codes below from from http://www.eso.org/~jagonzal/telcal/Juan-Ramon/SDMTables.pdf, Sec. 2.50 and 
    # FITS header notation from https://fits.gsfc.nasa.gov/standard40/fits_standard40aa-le.pdf
    specsys3 = {0:'LSRD', 1:'LSRK', 2:'GALACTOC', 3:'BARYCENT', 4:'GEOCENTR', 5:'TOPOCENT'}[np.unique(np.array(specframe))[0]]
    filename=filename.split(':')
    filename='{0:s}/{1:s}'.format(filename[1],filename[0])
    if not os.path.exists(filename): meerkathi.log.info('Skipping SPECSYS fix for {0:s}. File does not exist.'.format(filename))
    else:
        with fits.open(filename, mode='update') as cube:
            headcube = cube[0].header
            if 'specsys' in headcube: del headcube['specsys']
            headcube['specsys3']=specsys3

def make_pb_cube(filename):
    filename=filename.split(':')
    filename='{0:s}/{1:s}'.format(filename[1],filename[0])
    if not os.path.exists(filename): meerkathi.log.info('Skipping primary beam cube for {0:s}. File does not exist.'.format(filename))
    else:
        with fits.open(filename) as cube:
            headcube = cube[0].header
            datacube = np.indices((headcube['naxis2'],headcube['naxis1']),dtype=float32)
            datacube[0]-=(headcube['crpix2']-1)
            datacube[1]-=(headcube['crpix1']-1)
            datacube=np.sqrt((datacube**2).sum(axis=0))
            datacube.resize((1,datacube.shape[0],datacube.shape[1]))
            datacube=np.repeat(datacube,headcube['naxis3'],axis=0)*np.abs(headcube['cdelt1'])
            sigma_pb=17.52/(headcube['crval3']+headcube['cdelt3']*(np.arange(headcube['naxis3'])-headcube['crpix3']+1))*1e+9/13.5/2.355
            #sigma_pb=headcube['crval3']+headcube['cdelt3']*(np.arange(headcube['naxis3'])-headcube['crpix3']+1)
            sigma_pb.resize((sigma_pb.shape[0],1,1))
            #print sigma_pb
            datacube=np.exp(-datacube**2/2/sigma_pb**2)
            fits.writeto(filename.replace('image.fits','pb.fits'),datacube,header=headcube,overwrite=True)
            meerkathi.log.info('Created primary beam cube FITS {0:s}'.format(filename.replace('image.fits','pb.fits')))


def calc_rms(filename):
    #filename=filename.split(':')
    #filename='{0:s}/{1:s}'.format(filename[1],filename[0])
    if not os.path.exists(filename): meerkathi.log.info('Noise not determined in cube for {0:s}. File does not exist.'.format(filename))
    else:
        with fits.open(filename) as cube:
            headcube = cube[0].header
            datacube = cube[0].data
            y = datacube[~np.isnan(datacube)]
	    return np.sqrt(np.sum(y * y, dtype=np.float64) / y.size)

NAME = 'Make HI Cube'
def worker(pipeline, recipe, config):
    mslist = ['{0:s}-{1:s}.ms'.format(did, config['hires_label']) for did in pipeline.dataid]  if config.get('use_hires_data', True) else ['{0:s}-{1:s}.ms'.format(did, config['label'])for did in pipeline.dataid]
    pipeline.prefixes = ['meerkathi-{0:s}-{1:s}'.format(did,config['hires_label']) for did in pipeline.dataid] if config.get('use_hires_data', True) else  ['meerkathi-{0:s}-{1:s}'.format(did,config['label']) for did in pipeline.dataid]
    prefixes = pipeline.prefixes
    restfreq = config.get('restfreq','1.420405752GHz')
    npix = config.get('npix', [1024])
    if len(npix) == 1:
        npix = [npix[0],npix[0]]
    cell = config.get('cell', 7)
    weight = config.get('weight', 'natural')
    robust = config.get('robust', 0)

    # Upate pipeline attributes (useful if, e.g., channel averaging was performed by the split_data worker)
    for i, prefix in enumerate(prefixes):
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, prefix)
        if config.get('use_hires_data', True):
            msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, prefix)
        meerkathi.log.info('Updating info from {0:s}'.format(msinfo))
        with open(msinfo, 'r') as stdr:
            spw = yaml.load(stdr)['SPW']['NUM_CHAN']
            pipeline.nchans[i] = spw
        meerkathi.log.info('MS has {0:d} spectral windows, with NCHAN={1:s}'.format(len(spw), ','.join(map(str, spw))))

        # Get first chan, last chan, chan width
        with open(msinfo, 'r') as stdr:
            chfr = yaml.load(stdr)['SPW']['CHAN_FREQ']
            firstchanfreq = [ss[0] for ss in chfr]
            lastchanfreq  = [ss[-1] for ss in chfr]
            chanwidth     = [(ss[-1]-ss[0])/(len(ss)-1) for ss in chfr]
            pipeline.firstchanfreq[i] = firstchanfreq
            pipeline.lastchanfreq[i]  = lastchanfreq
            pipeline.chanwidth[i] = chanwidth
            meerkathi.log.info('CHAN_FREQ from {0:s} Hz to {1:s} Hz with average channel width of {2:s} Hz'.format(','.join(map(str,firstchanfreq)),','.join(map(str,lastchanfreq)),','.join(map(str,chanwidth))))


    # Find common barycentric frequency grid for all input .MS, or set it as requested in the config file
    if pipeline.enable_task(config, 'mstransform') and config['mstransform'].get('doppler', True) and config['mstransform'].get('outchangrid', 'auto')=='auto':
        firstchanfreq=pipeline.firstchanfreq
        chanw=pipeline.chanwidth
        lastchanfreq=pipeline.lastchanfreq
        RA=pipeline.TRA
        Dec=pipeline.TDec
        teldict={
            'meerkat':[21.4430,-30.7130],
            'gmrt':[73.9723, 19.1174],
            'vla':[-107.6183633,34.0783584],
            'wsrt':[52.908829698,6.601997592],
            'atca':[-30.307665436,149.550164466],
            'askap':[116.5333,-16.9833],
                }
        tellocation=teldict[config.get('telescope','meerkat')]
        telloc=EarthLocation.from_geodetic(tellocation[0],tellocation[1])
        firstchanfreq_dopp,chanw_dopp,lastchanfreq_dopp = firstchanfreq,chanw,lastchanfreq
        corr_order= False
        if len(chanw) > 1:
            if np.max(chanw) > 0 and np.min(chanw) < 0:
                corr_order = True
        for i, prefix in enumerate(prefixes):
            msinfo = '{0:s}/{1:s}-obsinfo.txt'.format(pipeline.output, prefix)
            with open(msinfo, 'r') as searchfile:
                for longdatexp in searchfile:
                   if "Observed from" in longdatexp:
                        dates   = longdatexp
                        matches = re.findall('(\d{2}[- ](\d{2}|January|Jan|February|Feb|March|Mar|April|Apr|May|May|June|Jun|July|Jul|August|Aug|September|Sep|October|Oct|November|Nov|Decem    ber|Dec)[\- ]\d{2,4})', dates)
                        obsstartdate = str(matches[0][0])
                        obsdate = datetime.datetime.strptime(obsstartdate, '%d-%b-%Y').strftime('%Y-%m-%d')
                        targetpos = SkyCoord(RA[i], Dec[i], frame='icrs', unit='deg')
                        v=targetpos.radial_velocity_correction(kind='barycentric',obstime=Time(obsdate), location=telloc).to('km/s')
                        corr=np.sqrt((constants.c-v)/(constants.c+v))
                        if corr_order:
                            if chanw_dopp[i][0] > 0.:
                                firstchanfreq_dopp[i], chanw_dopp[i], lastchanfreq_dopp[i] = lastchanfreq_dopp[i] * corr, chanw_dopp[i] * corr, firstchanfreq_dopp[i] * corr
                            else:
                                firstchanfreq_dopp[i], chanw_dopp[i], lastchanfreq_dopp[i] = firstchanfreq_dopp[i] * corr, chanw_dopp[i] * corr, lastchanfreq_dopp[i] * corr
                        else:
                            firstchanfreq_dopp[i], chanw_dopp[i], lastchanfreq_dopp[i] = firstchanfreq_dopp[i]*corr, chanw_dopp[i]*corr, lastchanfreq_dopp[i]*corr  #Hz, Hz, Hz
        # WARNING: the following line assumes a single SPW for the HI line data being processed by this worker!
        if np.min(chanw_dopp) < 0:
            comfreq0,comfreql,comchanw = np.min(firstchanfreq_dopp), np.max(lastchanfreq_dopp), -1*np.max(np.abs(chanw_dopp))
            comfreq0+=comchanw # safety measure to avoid wrong Doppler settings due to change of Doppler correction during a day
            comfreql-=comchanw # safety measure to avoid wrong Doppler settings due to change of Doppler correction during a day
        else:
            comfreq0,comfreql,comchanw = np.max(firstchanfreq_dopp), np.min(lastchanfreq_dopp), np.max(chanw_dopp)
            comfreq0+=comchanw # safety measure to avoid wrong Doppler settings due to change of Doppler correction during a day
            comfreql-=comchanw # safety measure to avoid wrong Doppler settings due to change of Doppler correction during a day
        nchan_dopp=int(np.floor(((comfreql - comfreq0)/comchanw)))+1
        comfreq0='{0:.3f}Hz'.format(comfreq0)
        comchanw='{0:.3f}Hz'.format(comchanw)
        meerkathi.log.info('Calculated common Doppler-corrected channel grid for all input .MS: {0:d} channels starting at {1:s} and with channel width {2:s}.'.format(nchan_dopp,comfreq0,comchanw))
        if pipeline.enable_task(config, 'wsclean_image') and corr_order:
            meerkathi.log.info('wsclean will not work when the input measurement sets are ordered in different directions. Use casa_image' )
            sys.exit(1)
    elif pipeline.enable_task(config, 'mstransform') and config['mstransform'].get('doppler', True) and config['mstransform'].get('outchangrid', 'auto')!='auto':
        if len(config['mstransform']['outchangrid'].split(','))!=3:
            meerkathi.log.error('Wrong format for mstransform:outchangrid in the .yml config file.')
            meerkathi.log.error('Current setting is mstransform:outchangrid:"{0:s}"'.format(config['mstransform']['outchangrid']))
            meerkathi.log.error('It must be "nchan,chan0,chanw" (note the commas) where nchan is an integer, and chan0 and chanw must include units appropriate for the chosen mstransform:mode')
            sys.exit(1)
        nchan_dopp,comfreq0,comchanw=config['mstransform']['outchangrid'].split(',')
        nchan_dopp=int(nchan_dopp)
        meerkathi.log.info('Set requested Doppler-corrected channel grid for all input .MS: {0:d} channels starting at {1:s} and with channel width {2:s}.'.format(nchan_dopp,comfreq0,comchanw))

    elif pipeline.enable_task(config, 'mstransform'):
        nchan_dopp,comfreq0,comchanw=None,None,None

    for i, msname in enumerate(mslist):
        prefix = '{0:s}_{1:d}'.format(pipeline.prefix, i)
        msname_mst=msname.replace('.ms','_mst.ms')

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


        if pipeline.enable_task(config, 'mstransform'):
            if os.path.exists('{1:s}/{0:s}'.format(msname_mst,pipeline.msdir)): os.system('rm -r {1:s}/{0:s}'.format(msname_mst,pipeline.msdir))
            col=config['mstransform'].get('column', 'corrected')
            step = 'mstransform_{:d}'.format(i)
            recipe.add('cab/casa_mstransform', step,
                {
                    "msname"    : msname,
                    "outputvis" : msname_mst,
                    "regridms"  : config['mstransform'].get('doppler', True),
                    "mode"      : config['mstransform'].get('mode', 'frequency'),
                    "nchan"     : sdm.dismissable(nchan_dopp),
                    "start"     : sdm.dismissable(comfreq0),
                    "width"     : sdm.dismissable(comchanw),
                    "interpolation" : 'nearest',
                    "datacolumn" : col,
                    "restfreq"  :restfreq,
                    "outframe"  : config['mstransform'].get('outframe', 'bary'),
                    "veltype"   : config['mstransform'].get('veltype', 'radio'),
                    "douvcontsub" : config['mstransform'].get('uvlin', False),
                    "fitspw"    : sdm.dismissable(config['mstransform'].get('fitspw', None)),
                    "fitorder"  : config['mstransform'].get('fitorder', 1),
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Doppler tracking corrections'.format(step))

            if config['mstransform'].get('obsinfo', True):
                step = 'listobs_{:d}'.format(i)
                recipe.add('cab/casa_listobs', step,
                    {
                      "vis"         : msname_mst,
                      "listfile"    : 'meerkathi-{0:s}-obsinfo.txt'.format(msname_mst.replace('.ms','')),
                      "overwrite"   : True,
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Get observation information ms={1:s}'.format(step, msname_mst))

                step = 'summary_json_{:d}'.format(i)
                recipe.add('cab/msutils', step,
                    {
                      "msname"      : msname_mst,
                      "command"     : 'summary',
                      "outfile"     : 'meerkathi-{0:s}-obsinfo.json'.format(msname_mst.replace('.ms','')),
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Get observation information as a json file ms={1:s}'.format(step, msname_mst))

        if pipeline.enable_task(config, 'sunblocker'):
            if config['sunblocker'].get('use_mstransform',True): msnamesb = msname_mst
            else: msnamesb = msname
            step = 'sunblocker_{0:d}'.format(i)
            recipe.add("cab/sunblocker", step,
                {
                    "command"    : "phazer",
                    "inset"      : msnamesb,
                    "outset"     : msnamesb,
                    "imsize"     : config['sunblocker'].get('imsize', max(npix)),
                    "cell"       : config['sunblocker'].get('cell', cell),
                    "pol"        : 'i',
                    "threshmode" : 'fit',
                    "threshold"  : config['sunblocker'].get('threshold', 3.),
                    "mode"       : 'all',
                    "radrange"   : 0,
                    "angle"      : 0,
                    "show"       : prefix + '.sunblocker.pdf',
                    "verb"       : True,
                    "dryrun"     : False,
                    "uvmax"      : config['sunblocker'].get('uvmax',2500.),
                    "uvmin"      : config['sunblocker'].get('uvmin',0.),
                    "vampirisms" : config['sunblocker'].get('vampirisms',True),
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Block out sun'.format(step))

    if pipeline.enable_task(config, 'wsclean_image'):
        if config['wsclean_image'].get('use_mstransform',True):
            mslist = ['{0:s}-{1:s}_mst.ms'.format(did, config['hires_label']) for did in pipeline.dataid]  if config.get('use_hires_data', True) else ['{0:s}-{1:s}_mst.ms'.format(did, config['label'])for did in pipeline.dataid]
            # Upate pipeline attributes (useful if, e.g., the channelisation was changed by mstransform during this or a previous pipeline run)
            for i, prefix in enumerate(prefixes):
                # If channelisation changed during a previous pipeline run as stored in the obsinfo.json file
                if not pipeline.enable_task(config, 'mstransform'):
                    msinfo = '{0:s}/{1:s}_mst-obsinfo.json'.format(pipeline.output, prefix)
                    meerkathi.log.info('Updating info from {0:s}'.format(msinfo))
                    with open(msinfo, 'r') as stdr:
                        spw = yaml.load(stdr)['SPW']['NUM_CHAN']
                        pipeline.nchans[i] = spw
                    meerkathi.log.info('MS has {0:d} spectral windows, with NCHAN={1:s}'.format(len(spw), ','.join(map(str, spw))))
                    # Get first chan, last chan, chan width
                    with open(msinfo, 'r') as stdr:
                        chfr = yaml.load(stdr)['SPW']['CHAN_FREQ']
                        firstchanfreq = [ss[0] for ss in chfr]
                        lastchanfreq  = [ss[-1] for ss in chfr]
                        chanwidth     = [(ss[-1]-ss[0])/(len(ss)-1) for ss in chfr]
                        pipeline.firstchanfreq[i] = firstchanfreq
                        pipeline.lastchanfreq[i]  = lastchanfreq
                        pipeline.chanwidth[i] = chanwidth
                    meerkathi.log.info('CHAN_FREQ from {0:s} Hz to {1:s} Hz with average channel width of {2:s} Hz'.format(','.join(map(str,firstchanfreq)),','.join(map(str,lastchanfreq)),','.join(map(str,chanwidth))))
                    with open(msinfo, 'r') as stdr:
                        pipeline.specframe[i] = yaml.load(stdr)['SPW']['MEAS_FREQ_REF']
                    meerkathi.log.info('The spectral reference frame is {0:}'.format(pipeline.specframe[i]))
                # If channelisation changed during this run
                elif config['mstransform'].get('doppler', True):
                    pipeline.nchans[i] = [nchan_dopp for kk in pipeline.nchans[i]]
                    pipeline.specframe[i]=[{'lsrd':0,'lsrk':1,'galacto':2,'bary':3,'geo':4,'topo':5}[config['mstransform'].get('outframe', 'bary')] for kk in pipeline.nchans[i]]

        else:
            mslist = ['{0:s}-{1:s}.ms'.format(did, config['hires_label']) for did in pipeline.dataid]  if config.get('use_hires_data', True) else ['{0:s}-{1:s}.ms'.format(did, config['label'])for did in pipeline.dataid]
        spwid = config['wsclean_image'].get('spwid', 0)
        nchans = config['wsclean_image'].get('nchans',0)
        if nchans == 0 or nchans == 'all': nchans=pipeline.nchans[0][spwid]
        firstchan = config['wsclean_image'].get('firstchan', 0)
        binchans  = config['wsclean_image'].get('binchans', 1)
        channelrange = [firstchan, firstchan+nchans*binchans]
        # Construct weight specification
        if config['wsclean_image'].get('weight', 'natural') == 'briggs':
            weight = 'briggs {0:.3f}'.format( config['wsclean_image'].get('robust', robust))
        else:
            weight = config['wsclean_image'].get('weight', weight)
        wscl_niter = config['wsclean_image'].get('wscl_niter', 2)
        j = 1
        tol = config['wsclean_image'].get('tol', 0.5)
        HIclean_mask=pipeline.prefix+'_HI_'+str(j-1)+'.image_clean_mask.fits:output'
        HIclean_mask_path=os.path.join(pipeline.output,pipeline.prefix+'_HI_'+str(j-1)+'.image_clean_mask.fits')
        
        
        while j<= wscl_niter:# and (os.path.exists(HIclean_mask_path)):
            if j==1:
                step = 'wsclean_image_HI_with_automasking'
        	ownHIclean_mask=config['wsclean_image'].get('ownfitsmask',)   
                recipe.add('cab/wsclean', step,
                   {
                  "msname"    : mslist,
                  "prefix"    : pipeline.prefix+'_HI_'+str(j),
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
                  "fitsmask"  : ownHIclean_mask,
                  "auto-mask"  :   config['wsclean_image'].get('automask', 3),
                  "multiscale" : config['wsclean_image'].get('multi_scale', False),
                  "multiscale-scales" : sdm.dismissable(config['wsclean_image'].get('multi_scale_scales', None)),
                  "no-update-model-required": config['wsclean_image'].get('no_update_mod', True)
                   },
                input=pipeline.input,
                output=pipeline.output,
                label='{:s}:: Image HI'.format(step))


            else:
                step = 'make_sofia_mask_'+str(j-1)
                HIclean_mask=pipeline.prefix+'_HI_'+str(j)+'.image_clean_mask.fits:output'
                HIclean_mask_path=os.path.join(pipeline.output,pipeline.prefix+'_HI_'+str(j)+'.image_clean_mask.fits')

                cubename = pipeline.prefix+'_HI_'+str(j-1)+'.image.fits:output'
                outmask = pipeline.prefix+'_HI_'+str(j)+'.image_clean'
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
                "SCfind.threshold"      : 5,
                "SCfind.rmsMode"        : 'mad',
                "merge.radiusX"         : 3,
                "merge.radiusY"         : 3,
                "merge.radiusZ"         : 3,
                "merge.minSizeX"        : 2,
                "merge.minSizeY"        : 2,
                "merge.minSizeZ"        : 2,
                "writeCat.basename"     : outmask,
                      },
                     input=pipeline.input,
                     output=pipeline.output,
                     label='{0:s}:: Make SoFiA mask'.format(step))

                recipe.run()
                recipe.jobs=[]

                if not os.path.exists(HIclean_mask_path):
                    meerkathi.log.info('Initial sofia mask_'+str(j)+' was not found. Exiting and saving the cube') 
                    break
                                
                step = 'Sofia_mask_found_running_wsclean_image_HI_loop_'+str(j)
                recipe.add('cab/wsclean', step,
                           {
                               "msname"    : mslist,
                               "prefix"    : pipeline.prefix+'_HI_'+str(j),
                               "weight"    : weight,
                               "taper-gaussian" : str(config['wsclean_image'].get('taper',0)),
                               "pol"        : config['wsclean_image'].get('pol','I'),
                               "npix"      : config['wsclean_image'].get('npix', npix),
                               "padding"   : config['wsclean_image'].get('padding', 1.2),
                               "scale"     : config['wsclean_image'].get('cell', cell),
                               "channelsout"     : nchans,
                               "channelrange" : channelrange,
                               "fitsmask"  : HIclean_mask,
                               "niter"     : config['wsclean_image'].get('niter', 1000000),
                               "mgain"     : config['wsclean_image'].get('mgain', 1.0),
                               "auto-threshold"  : config['wsclean_image'].get('autothreshold', 5),
                               "multiscale" : config['wsclean_image'].get('multi_scale', False),
                               "multiscale-scales" : sdm.dismissable(config['wsclean_image'].get('multi_scale_scales', None)),
                               "no-update-model-required": config['wsclean_image'].get('no_update_mod', True)
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='{:s}:: re-Image HI_'+str(j).format(step))
 
            if config['wsclean_image']['make_cube']:
                if not config['wsclean_image'].get('niter', 1000000): imagetype=['image','dirty']
                else:
                    imagetype=['image','dirty','psf','residual','model']
                    if config['wsclean_image'].get('mgain', 1.0)<1.0: imagetype.append('first-residual')
                for mm in imagetype:
                    step = 'make_{0:s}_cube'.format(mm.replace('-','_'))
                    recipe.add('cab/fitstool', step,
                        {
                        "image"    : [pipeline.prefix+'_HI_'+str(j)+'-{0:04d}-{1:s}.fits:output'.format(d,mm) for d in xrange(nchans)],
                        "output"   : pipeline.prefix+'_HI_'+str(j)+'.{0:s}.fits'.format(mm),
                        "stack"    : True,
                        "delete-files" : True,
                        "fits-axis": 'FREQ',
                        },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Make {1:s} cube from wsclean {1:s} channels'.format(step,mm.replace('-','_')))

            for ss in ['dirty','psf','residual','model','image']:
                cubename=pipeline.prefix+'_HI_'+str(j)+'.'+ss+'.fits:'+pipeline.output
                MFScubename=os.path.join(pipeline.output,pipeline.prefix+'_HI_'+str(j)+'-MFS-'+ss+'.fits')
                recipe.add(fix_specsys, 'fix_specsys_{0:s}_cube'.format(ss),
                          {
                              'filename' : cubename,
                              'specframe': pipeline.specframe,
                          },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Fix spectral reference frame for cube {0:s}'.format(cubename))
            
            if pipeline.enable_task(config,'freq_to_vel'):
                if not config['freq_to_vel'].get('reverse', False): meerkathi.log.info('Converting spectral axis of cubes from frequency to radio velocity')
                else: meerkathi.log.info('Converting spectral axis of cubes from radio velocity to frequency')
                for ss in ['dirty','psf','residual','model','image']:
            	    cubename=pipeline.prefix+'_HI_'+str(j)+'.'+ss+'.fits:'+pipeline.output
                    MFScubename=os.path.join(pipeline.output,pipeline.prefix+'_HI_'+str(j)+'-MFS-'+ss+'.fits')
                    recipe.add(freq_to_vel, 'spectral_header_to_vel_radio_{0:s}_cube'.format(ss),
                              {
                                  'filename' : cubename,
                                  'reverse'  : config['freq_to_vel'].get('reverse', False)
                              },
                               input=pipeline.input,
                               output=pipeline.output,
                               label='Convert spectral axis from frequency to radio velocity for cube {0:s}'.format(cubename))


            recipe.run()
            recipe.jobs=[] 
             
            cubename1=os.path.join(pipeline.output,pipeline.prefix+'_HI_1.image.fits')
            rms_old=calc_rms(cubename1)
            rms_new=rms_old
            print 'blah1',cubename1,rms_new, j

 #           recipe.run()
 #           recipe.jobs=[]               

            j+=1   

        j-=1
        for ss in ['dirty','psf','residual','model','image']:
            cubename=os.path.join(pipeline.output,pipeline.prefix+'_HI_'+str(j)+'.'+ss+'.fits')
            finalcubename=os.path.join(pipeline.output,pipeline.prefix+'_HI.'+ss+'.fits')
            HIclean_mask=os.path.join(pipeline.output,pipeline.prefix+'_HI_'+str(j)+'.image_clean_mask.fits')
            finalHIclean_mask=os.path.join(pipeline.output,pipeline.prefix+'_HI.image_clean_mask.fits')
            MFScubename=os.path.join(pipeline.output,pipeline.prefix+'_HI_'+str(j)+'-MFS-'+ss+'.fits')
            MFSfinalcubename=os.path.join(pipeline.output,pipeline.prefix+'_HI'+'-MFS-'+ss+'.fits')
            if os.path.exists(cubename):
                os.rename(cubename,finalcubename)
            if os.path.exists(HIclean_mask):
                os.rename(HIclean_mask,finalHIclean_mask)
            if os.path.exists(MFScubename):
                os.rename(MFScubename,MFSfinalcubename)


        for j in range(1,wscl_niter):               
            if config['wsclean_image'].get('rm_intcubes',True):
                for ss in ['dirty','psf','residual','model','image']:
                    cubename=os.path.join(pipeline.output,pipeline.prefix+'_HI_'+str(j)+'.'+ss+'.fits')
                    HIclean_mask=os.path.join(pipeline.output,pipeline.prefix+'_HI_'+str(j)+'.image_clean_mask.fits')
                    MFScubename=os.path.join(pipeline.output,pipeline.prefix+'_HI_'+str(j)+'-MFS-'+ss+'.fits')
                    if os.path.exists(cubename):
                        os.remove(cubename)
                    if os.path.exists(HIclean_mask):
                        os.remove(HIclean_mask)
                    if os.path.exists(MFScubename):
                        os.remove(MFScubename)


    if pipeline.enable_task(config, 'casa_image'):
        if config['casa_image']['use_mstransform']:
            mslist = ['{0:s}-{1:s}_mst.ms'.format(did, config['hires_label']) for did in pipeline.dataid]  if config.get('use_hires_data', True) else ['{0:s}-{1:s}_mst.ms'.format(did, config['label'])for did in pipeline.dataid]
        step = 'casa_image_HI'
        spwid = config['casa_image'].get('spwid', 0)
        nchans = config['casa_image'].get('nchans', 0)
        if nchans == 0:
            nchans=pipeline.nchans[0][spwid]
        image_opts = {
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
            }
        if config['casa_image'].get('taper', '') != '':
            image_opts.update({
                "uvtaper"         : True,
                "outertaper"      : config['casa_image'].get('taper', ''),
            })
        recipe.add('cab/casa_clean', step, image_opts,
            input=pipeline.input,
            output=pipeline.output,
            label='{:s}:: Image HI'.format(step))

    if pipeline.enable_task(config,'remove_stokes_axis'):
        for ss in ['dirty','psf','residual','model','image']:
            cubename=pipeline.prefix+'_HI.'+ss+'.fits:'+pipeline.output
            recipe.add(remove_stokes_axis, 'remove_stokes_axis_{0:s}_cube'.format(ss),
                       {
                           'filename' : cubename,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Remove Stokes axis for cube {0:s}'.format(cubename))

    if pipeline.enable_task(config,'pb_cube'):
        cubename=pipeline.prefix+'_HI.image.fits:'+pipeline.output
        recipe.add(make_pb_cube, 'pb_cube',
                   {
                       'filename': cubename,
                    },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='Make primary beam cube for {0:s}'.format(cubename))

    for ss in ['dirty','psf','residual','model','image','pb']:
        cubename=pipeline.prefix+'_HI_'+str(j)+'.'+ss+'.fits:'+pipeline.output
        MFScubename=os.path.join(pipeline.output,pipeline.prefix+'_HI_'+str(j)+'-MFS-'+ss+'.fits')
        recipe.add(fix_specsys, 'fix_specsys_{0:s}_cube'.format(ss),
                  {
                      'filename' : cubename,
                      'specframe': pipeline.specframe,
                  },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='Fix spectral reference frame for cube {0:s}'.format(cubename))
        
    if pipeline.enable_task(config,'freq_to_vel'):
         if not config['freq_to_vel'].get('reverse', False): meerkathi.log.info('Converting spectral axis of cubes from frequency to radio velocity')
         else: meerkathi.log.info('Converting spectral axis of cubes from radio velocity to frequency')
         for ss in ['dirty','psf','residual','model','image','pb']:
            cubename=pipeline.prefix+'_HI.'+ss+'.fits:'+pipeline.output
            recipe.add(freq_to_vel, 'spectral_header_to_vel_radio_{0:s}_cube'.format(ss),
                       {
                           'filename' : cubename,
                           'reverse'  : config['freq_to_vel'].get('reverse', False)
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
            "merge.radiusZ"         : config['sofia'].get('mergeZ', 2),
            "merge.minSizeX"        : config['sofia'].get('minSizeX', 2),
            "merge.minSizeY"        : config['sofia'].get('minSizeY', 2),
            "merge.minSizeZ"        : config['sofia'].get('minSizeZ', 2),
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

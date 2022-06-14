#!/bin/bash

import sys,os
import numpy as np
import yaml
import matplotlib
matplotlib.use("Agg")

import stimela.recipe 


import casacore.tables as tables

import casacore.images as images
import casacore.measures as measures
from casacore.measures import dq


from casatasks import mstransform as mstrans
from casatasks import flagmanager as flg
from casatasks import flagdata as flagger

from casatools import image


import astropy.io.ascii as astasc
from astropy.io import fits, ascii
from astropy.table import Table, Column
from astropy.wcs import WCS
import astropy.visualization as astviz
from astropy.coordinates import SkyCoord
from astropy import units as u

import datetime
from scipy import stats
import scipy.constants as scconstants

from matplotlib import pyplot as plt
from matplotlib import rc
from matplotlib import gridspec
import matplotlib.dates as mdat

# import bisect
from collections import OrderedDict

import time
import argparse
import shutil
import caracal

from caracal import log

import gc

ia = image()
dm = measures.measures()

timeInit = time.time()




def setDirs(output,config):

    config['flagUzeros']['stripeDir']=output+'/stripeAnalysis/'
    if not os.path.exists(config['flagUzeros']['stripeDir']):
        os.mkdir(config['flagUzeros']['stripeDir'])

    config['flagUzeros']['stripeLogDir']=config['flagUzeros']['stripeDir']+'logs/'
    if not os.path.exists(config['flagUzeros']['stripeLogDir']):
        os.mkdir(config['flagUzeros']['stripeLogDir'])


    config['flagUzeros']['stripeMSDir']=config['flagUzeros']['stripeDir']+'msdir/'
    if not os.path.exists(config['flagUzeros']['stripeMSDir']):
        os.mkdir(config['flagUzeros']['stripeMSDir'])

    config['flagUzeros']['stripeCubeDir']=config['flagUzeros']['stripeDir']+'cubes/'
    if not os.path.exists(config['flagUzeros']['stripeCubeDir']):
        os.mkdir(config['flagUzeros']['stripeCubeDir'])

    config['flagUzeros']['stripeFFTDir']=config['flagUzeros']['stripeDir']+'fft/'
    if not os.path.exists(config['flagUzeros']['stripeFFTDir']):
        os.mkdir(config['flagUzeros']['stripeFFTDir'])

    config['flagUzeros']['stripePlotDir']=config['flagUzeros']['stripeDir']+'plots/'
    if not os.path.exists(config['flagUzeros']['stripePlotDir']):
        os.mkdir(config['flagUzeros']['stripePlotDir'])

    config['flagUzeros']['stripeTableDir']=config['flagUzeros']['stripeDir']+'tables/'
    if not os.path.exists(config['flagUzeros']['stripeTableDir']):
        os.mkdir(config['flagUzeros']['stripeTableDir'])

    config['flagUzeros']['stripeSofiaDir']=config['flagUzeros']['stripeDir']+'sofiaOut/'
    if not os.path.exists(config['flagUzeros']['stripeSofiaDir']):
        os.mkdir(config['flagUzeros']['stripeSofiaDir'])


    return 

def splitScans(config,inVis,scanNums):

    scanVisList=[]
    scanVisNames=[]
    for scan in scanNums:
        baseVis=os.path.basename(inVis)
        outVis=config['flagUzeros']['stripeMSDir']+baseVis.split('.ms')[0]+'_scn'+str(scan)+'.ms'
        if os.path.exists(outVis):
            shutil.rmtree(outVis)
        if os.path.exists(outVis+'.flagversions'):
            shutil.rmtree(outVis+'.flagversions')
        mstrans(vis=inVis,outputvis=outVis,datacolumn='DATA',scan=str(scan))

        scanVisList.append(outVis)
        scanVisNames.append(baseVis.split('.ms')[0]+'_scn'+str(scan)+'.ms')


    caracal.log.info("All Scans splitted")


    return scanVisList, scanVisNames


def gaussian(x, cent, amp, sigma):
    """
    Gaussian function
    Input:
    cent (float): centre
    amp (float) : amplitude
    sigma (float) : sigma
    Return:
    gaussian() Gaussian
    """
    return amp*np.exp(-0.5*np.power((x-cent)/sigma,2))


def convToStokesI(data,flags):

    stflags = np.logical_not(flags).astype(float)

    # if polarisation is i, then take either average or single value, flag the rest
        # Calculate stokes i, reduce the number of polarizations to one, flag if not at least one pol is available
    with np.errstate(divide='ignore', invalid = 'ignore'):
        data = (data*np.logical_not(flags))/stflags #
    flags = stflags < 1.
    data[flags] = np.nan

    return data, flags


def makeCube(pipeline,msdir,inVis,outCubePrefix,config,kind='scan'):

    robust = config['flagUzeros']['robust']
    imsize = int(config['flagUzeros']['imsize'])
    cell = config['flagUzeros']['cell']
    chanMin = int(config['flagUzeros']['chanRange'][0])
    chanMax = int(config['flagUzeros']['chanRange'][1])
    taper = config['flagUzeros']['taper']

    print(inVis)
    recipe = stimela.Recipe('flagUzeros',
                                    ms_dir=msdir,
                                    singularity_image_dir=pipeline.singularity_image_dir,
                                    log_dir=config['flagUzeros']['stripeLogDir'],
                                    logfile=False, # no logfiles for recipes
                                    )
    recipe.JOB_TYPE = pipeline.container_tech
    # print(inVis,outCubePrefix)

    if kind=='scan':
        chMin=0
        chMax=chanMax-chanMin
    else:
        chMin=chanMin
        chMax=chanMax
    #imsize=400,scale=20.asec

    line_image_opts = {
        "msname": inVis,
        "prefix": outCubePrefix,
        "npix": imsize,
        "scale": cell,
        "weight": 'briggs {0:.3f}'.format(robust),
        "channelsout": 1,
        "channelrange": [chanMin,chanMax],
        "niter": 0,
        "gain": 0.2,
        "mgain": 0.85,
        "auto-threshold": 10.0,
        "multiscale":False,
        "multiscale-scale-bias": 0.6,
        "no-update-model-required": True,
        "auto-threshold": 0.5,
        "auto-mask": 10.0 ,
        "gain": 0.2,
            }

    if taper is not None:
        line_image_opts.update({"taper-gaussian": str(taper)})

    step='makeCube'
    recipe.add('cab/wsclean',
               step, line_image_opts,
               input=pipeline.input,
               output=config['flagUzeros']['stripeCubeDir'],
               label='{0:s}:: Image Line'.format(step))
    recipe.run()


#         cmd = """singularity exec /idia/software/containers/wsclean-v3.0.simg wsclean -name {outCubePrefix} -j 64 -mem 100 -no-update-model-required -weight briggs {robust} -taper-gaussian {taper} -size {imsize} {imsize} -scale {cell}asec -channels-out 1 -pol I -channel-range {chanmin} {chanmax} -niter 0 -auto-threshold 0.5 -auto-mask 10.0 -gain 0.2 -mgain 0.85 -multiscale-scale-bias 0.6 -padding 1.2 -quiet {inVis}""".format(
#                   outCubePrefix=outCubePrefix,robust=robust,
#                   taper=taper,imsize=imsize,cell=cell,chanmin=chMin,chanmax=chMax,inVis=inVis)
# #        os.system("singularity exec /idia/software/containers/wsclean-v3.0-idg.simg wsclean -name {outCubePrefix} -j 64 -mem 100 -no-update-model-required -weight briggs {robust} -taper-gaussian {taper} -size {imsize} {imsize} -scale {cell}asec -channels-out 1 -pol I -channel-range {chanmin} {chanmax} -niter 0 -auto-threshold 0.5 -auto-mask 10.0 -gain 0.2 -mgain 0.85 -multiscale-scale-bias 0.6 -padding 1.2 -quiet {inVis}".format(outCubePrefix=outCubePrefix,robust=robust,taper=taper,imsize=imsize,cell=cell,chanmin=chMin,chanmax=chMax,inVis=inVis))
#         caracal.log.info("\t-weight briggs {}  -taper-gaussian {} -size {} {} -scale {}asec -channels-out 1 -channel-range {} {}".format(robust,taper,imsize,imsize,cell,chMin,chMax))
#         os.system(cmd)
#     else:    #cell =2.asec imsize=3600
#         cmd = """singularity exec /idia/software/containers/wsclean-v3.0.simg wsclean -name {outCubePrefix} -j 64 -mem 100 -no-update-model-required -weight briggs {robust} -size {imsize} {imsize} -scale {cell}asec -channels-out 1 -pol I -channel-range {chanmin} {chanmax} -niter 0 -auto-threshold 0.5 -auto-mask 10.0 -gain 0.2 -mgain 0.85 -multiscale-scale-bias 0.6 -padding 1.2 -quiet {inVis}""".format(
#                  outCubePrefix=outCubePrefix,robust=robust,taper=taper,imsize=imsize,cell=cell,chanmin=chMin,chanmax=chMax,inVis=inVis)
#         caracal.log.info("\t-weight briggs {}  -taper-gaussian {} -size {} {} -scale {}asec -channels-out 1 -channel-range {} {}".format(robust,taper,imsize,imsize,cell,chMin,chMax))
#         os.system(cmd)
#        os.system("singularity exec /idia/software/containers/wsclean-v3.0-idg.simg wsclean -name {outCubePrefix} -j 64 -mem 100 -no-update-model-required -weight briggs {robust} -size {imsize} {imsize} -scale {cell}asec -channels-out 1 -pol I -channel-range {chanmin} {chanmax} -niter 0 -auto-threshold 0.5 -auto-mask 10.0 -gain 0.2 -mgain 0.85 -multiscale-scale-bias 0.6 -padding 1.2 -quiet {inVis}".format(outCubePrefix=outCubePrefix,robust=robust,taper=taper,imsize=imsize,cell=cell,chanmin=chanMin,chanmax=chanMax,inVis=inVis))

    caracal.log.info("Image Done")
    #caracal.log.info("----------------------------------------------------")

    return 0


def makeFFT(inCube,outFFT):
    ia.open(inCube)
    ia.fft(complex=outFFT)
    ia.close()
    imFFT=images.image(outFFT)
    dFFT=imFFT.getdata()
    dFFT=np.abs(np.squeeze(dFFT))
    headFFT=imFFT.info()

    hdr = fits.Header()
    hdr["CTYPE1"] = 'UU---SIN'
    hdr["CDELT1"] = headFFT['coordinates']['linear0']["cdelt"][0]
    hdr["CRVAL1"] = headFFT['coordinates']['linear0']["crval"][0]
    hdr["CRPIX1"] = headFFT['coordinates']['linear0']["crpix"][0]
    hdr["CUNIT1"] = headFFT['coordinates']['linear0']["units"][0]
    hdr["CTYPE2"] = 'VV---SIN'
    hdr["CDELT2"] = headFFT['coordinates']['linear0']["cdelt"][1]
    hdr["CRVAL2"] = headFFT['coordinates']['linear0']["crval"][1]
    hdr["CRPIX2"] = headFFT['coordinates']['linear0']["crpix"][1]
    hdr["CUNIT2"] = headFFT['coordinates']['linear0']["units"][1]
    caracal.log.info('\tFFT cell size = {0:.2f}'.format(hdr['cdelt2']))
    caracal.log.info("FFT Done")
    gc.collect()

    return dFFT,hdr

#


def plotAll(fig,gs,NS,kk,outCubeName,inFFTData,inFFTHeader,galaxy,track,scan,percent,common_vmax,ctff,type=None):

    fitsdata = fits.open(outCubeName)
    fitsim = fitsdata[0].data[0,0]
    fitshdr = fitsdata[0].header
    fitswcs = WCS(fitshdr).sub(2)
    rms1 = np.std(fitsim)


    ax = fig.add_subplot(gs[kk,0],projection=fitswcs)
    # ax.tick_params(bottom='on', top='on',left='on', right='on', which='major', direction='in')
    # ax.tick_params(bottom='on', top='on',left='on', right='on', which='minor', direction='in')
    ax.imshow(fitsim, cmap='Greys', vmin=-rms1, vmax=2*rms1)
    if scan!=0:
        ax.annotate("Scan: "+str(scan)+r" rms = "+str(np.round(rms1*1e6,3))+r" $\mu$Jyb$^{-1}$", xy=(0.05,0.95), xycoords='axes fraction', horizontalalignment='left', verticalalignment='top', backgroundcolor='w', fontsize=12)
    else:
        ax.annotate("rms = "+str(np.round(rms1*1e6,3))+r" $\mu$Jyb$^{-1}$", xy=(0.05,0.95), xycoords='axes fraction', horizontalalignment='left', verticalalignment='top', backgroundcolor='w', fontsize=12)
    if type=='postFlag':
        ax.annotate(r"Flags {percent} $\%$".format(percent=str(np.round(percent,2))), xy=(0.95,0.05), xycoords='axes fraction', horizontalalignment='right', verticalalignment='bottom', backgroundcolor='w', fontsize=12)

    #ax.annotate(r"rms = "+str(np.round(rms1*1e6,1))+r"$\mu$ Jyb$^{-1}$", xy=(0.1,0.1), xycoords='axes fraction', horizontalalignment='left', verticalalignment='bottom', backgroundcolor='w', fontsize=6)

    lon = ax.coords[0]
    lat = ax.coords[1]
    c = SkyCoord('00:02:00.','00:01:00.0',unit=(u.hourangle,u.deg))
    lon.set_ticks(spacing=c.ra.degree*u.degree)
    lat.set_ticks(spacing=c.ra.degree*u.degree)


    lon.set_auto_axislabel(False)
    lat.set_auto_axislabel(False)
    lon.set_ticklabel(exclude_overlapping=True)
    lat.set_ticklabel(exclude_overlapping=True)


    if kk==NS/2 or kk==NS/2+1 or (kk==0 and NS==1):
        lat.set_axislabel(r'Dec  (J2000)')
        lat.set_ticklabel_visible(True)
    else:
        lat.set_ticklabel_visible(True)
    if kk==NS-1:
        lon.set_axislabel(r'RA  (J2000)')
        lon.set_ticklabel_visible(True)
    else:
        lon.set_ticklabel_visible(False)

    udelt = inFFTHeader['CDELT1']
    vdelt = inFFTHeader['CDELT2']
    ax.set_autoscale_on(False)

    ax2 = fig.add_subplot(gs[kk,1])
    w = int(2000./vdelt)
    cx, cy = inFFTData.shape[0]//2, inFFTData.shape[1]//2
    extent = [-w*udelt, w*udelt, -w*vdelt, w*vdelt]
    if common_vmax == 0:
        common_vmax = np.nanpercentile(inFFTData[cx-w:cx+w+1, cy-w:cy+w+1], 99)
    fftim = ax2.imshow(inFFTData[cx-w:cx+w+1, cy-w:cy+w+1], vmin=0, vmax=common_vmax, extent=extent, origin='upper')
    if ctff: ax2.contour(inFFTData[cx-w:cx+w+1, cy-w:cy+w+1], levels=[ctff,], colors=['r'], linewidths=[1,], extent=extent, origin='upper')
    #fits.writeto('bla-{}-{}.fits'.format(kk,type),inFFTData[cx-w:cx+w+1, cy-w:cy+w+1], overwrite=True)

    ax2.yaxis.set_label_position("right")
    ax2.yaxis.tick_right()
    ax2.yaxis.set_ticks_position('right')

    if kk==NS-1 :
        ax2.set_xlabel(r'u [$\lambda$]')
        ax2.set_xticks([-1500,0,1500])
    else:
        ax2.set_xticks([])
    if kk==NS/2 or kk==NS/2+1 or (kk==0 and NS==1):
        ax2.set_ylabel(r'v [$\lambda$]')

    ax2.set_xlim(-2000,2000)
    ax2.set_ylim(-2000,2000)
    ax2.set_yticks([-1500,0,1500])

    ax2.set_autoscale_on(False)
    fig.subplots_adjust(left=0.05, bottom=0.05, right=0.97, top=0.97, wspace=0, hspace=0)

    return fig, common_vmax

def cleanUp(galaxy):

    caracal.log.info("====================================================")
    caracal.log.info("Cleanup")

    caracal.log.info("Deleting images")

    if os.path.exists(config['flagUzeros']['stripeCubeDir']):
        shutil.rmtree(config['flagUzeros']['stripeCubeDir'])

    caracal.log.info("Deleting FFTs")


    if os.path.exists(config['flagUzeros']['stripeFFTDir']):
        shutil.rmtree(config['flagUzeros']['stripeFFTDir'])

    caracal.log.info("Deleting .ms scans")

    if os.path.exists(config['flagUzeros']['stripeMSDir']):
        shutil.rmtree(config['flagUzeros']['stripeMSDir'])

    caracal.log.info("Cleanup done")

    return 0


def saveFFTTable(inFFT,inFFTHeader,visName, U, V, galaxy, msid, track, scan, el, az, method, threshold, dilateU, dilateV):

    xCol = np.zeros([len(U)*len(V)])
    yCol = np.zeros([len(U)*len(V)])
    BIN_ID =  np.arange(0,len(U)*len(V),1)
    tabGen=np.column_stack([BIN_ID,xCol,yCol])
    dt = np.dtype([('BIN_ID', np.int32), ('U', np.int32), ('V', np.int32)])
    tabGen = np.array(list(map(tuple, tabGen)), dtype=dt)

    namBins = tuple(['BIN_ID', 'U', 'V','Amp'])

    tabArr = np.zeros([len(tabGen)], dtype={'names':namBins,'formats':( 'i4', 'f8', 'f8', 'f8')})

    indexBin=0
    for i in range(0,len(U)):
        for j in range(0,len(V)):

            tabArr['BIN_ID'][indexBin] = indexBin
            tabArr['U'][indexBin] = U[i]
            tabArr['V'][indexBin] = V[j]
            tabArr['Amp'][indexBin] = inFFT[j,i]

            indexBin +=1

    hdr = fits.Header()
    hdr['COMMENT'] = "This is the table of the FFT"
    hdr['COMMENT'] = "Ext 1 = FFT table"

    if method=='sunblock':
        cutoff = sunBlockStats(inFFT,galaxy,msid,track,scan,'mad', threshold, ax = None, title = '', verb = True)
    else:
        if taper is not None:
            cutoff = np.nanpercentile(tabArr['Amp'], 99.99)
        else:
            cutoff = np.nanpercentile(tabArr['Amp'], 99.9999)

    empty_primary = fits.PrimaryHDU(header=hdr)

    inFFT1D= np.nansum(inFFT, axis=0)

    if cutoff>np.nanmax(tabArr['Amp']):
        willflag = False
        caracal.log.warn("Cutoff is larger than max amplitude. Notihng will be flagged.")
        #cutoff = tabArr['Amp'].max()
    else:
        willflag = True

    # This is where we decide where to flag
    index=np.where(tabArr['Amp']>=cutoff)[0]

    # And this is where we apply that flagging selection to the U,V,Amp arrays
    newtab = Table(names=['u', 'v', 'amp'], data=(tabArr['U'][index], tabArr['V'][index], tabArr['Amp'][index]))

    # Some stats ...
    if willflag:
        statsArray = [galaxy,track,scan,len(np.where(newtab['u']<=60.0)[0])/len(newtab['u']),cutoff,el,az]
    else:
        statsArray = [galaxy,track,scan,0.,cutoff,el,az]
    caracal.log.info("FFT Table saved")
    caracal.log.info("Flagging scan".format(scanNumber=str(scan), galaxy=galaxy, track=track))

    # the following scanFlags are the stripe flags for this scan
    scanFlags, percent = flagQuartile(visName,newtab,inFFTHeader,method,dilateU,dilateV,qrtdebug=False)

    return statsArray, scanFlags, percent, cutoff


def plotSunblocker(bin_centers,bin_edges,npoints,widthes,average,stdev,med,mad,popt,hist,threshold,galaxy,msid,track,scan,cut):

    caracal.log.info("\tPlotting Sunblocker stats")

    figS=plt.figure(figsize=(7.24409,7.24409), constrained_layout=False)
    figS.set_tight_layout(False)

    gsS = gridspec.GridSpec(nrows=1,ncols=1,figure=figS,hspace=0,wspace=0.0)


    showgouse = np.linspace(1.5*bin_centers[0]-0.5*bin_centers[1], 1.5*bin_centers[-1]-0.5*bin_centers[-2], 200)
    calculated = gaussian(showgouse, average, widthes[0]*npoints/(np.sqrt(2*np.pi)*stdev), stdev)

    # mad
    madded = gaussian(showgouse, med, widthes[0]*npoints/(np.sqrt(2*np.pi)*mad), mad)

    # In case of using only stats, this is right on top
    fitted = gaussian(showgouse, popt[0], popt[1], popt[2])

    ax = figS.add_subplot(gsS[0,0])
    ax.bar(bin_centers, hist, width=widthes, color = 'y', edgecolor = 'y')
    ax.plot(showgouse, calculated, 'g-')
    ax.plot(showgouse, fitted, 'r-')
    ax.plot(showgouse, madded, 'b-')
    ax.axvline(x = cut, linewidth=2, color='k')
    ax.set_xlim(min(bin_edges), max(bin_edges))
    plt.legend(['avg,std: {0:.1e}, {1:.1e}'.format(average, stdev),'fit:  {0:.1e}, {1:.1e}'.format(popt[0], popt[2]),'med,mad: {0:.1e}, {1:.1e}'.format(med, mad)], loc='upper right')
    ax.set_ylim(0.5,)
    plt.yscale('log')

    outPlot="{0}{1}_{2}_{3}_sblck.png".format(plotDir,galaxy,msid,scan)

    figS.savefig(outPlot,bbox_inches='tight',overwrite=True,dpi=200)   # save the figure to file
    plt.close(figS)


    caracal.log.info("\tPlot Done")

    return 0

def sunBlockStats(inFFTData,galaxy,msid,track,scan,threshmode = 'mad', threshold=300., ax = None, title = '', verb = True):

    av = np.copy(inFFTData)
    # Average data, then look for shape
    # av = np.nanmean(ampar,axis=1)
    npoints = inFFTData[np.isfinite(inFFTData)].size
    if verb:
        caracal.log.info('\tSunblocker: grid has {:d} nonzero points.'.format(npoints))
    if npoints < 3:
        caracal.log.info('\t Sunblocker: This is not sufficient for any statistics, returning no flags.')
        return np.zeros(av.shape, dtype = bool)

  # Find average and standard deviation
    average = np.nanmean(inFFTData)
    stdev  = np.nanstd(inFFTData)

    if average == np.nan:
        caracal.log.info('Sunblocker: cannot calculate average, returing no flags')
        return np.zeros(av.shape, dtype = bool)

    if stdev == np.nan:
        caracal.log.info('Sunblocker: cannot calculate standard deviation, returing no flags')
        return np.zeros(av.shape, dtype = bool)

    med = np.nanmedian(inFFTData)
    mad = stats.median_abs_deviation(inFFTData, scale='normal', nan_policy='omit',axis=None)
 #   if threshmode == 'fit' or ax != None:
    # Build a histogram
    hist, bin_edges = np.histogram(inFFTData[np.isfinite(inFFTData)], bins=int(np.sqrt(npoints))+1)
    bin_centers = bin_edges[:-1] + 0.5 * (bin_edges[1:] - bin_edges[:-1])
    widthes = bin_edges[1:] - bin_edges[:-1]
    # Find maximum in histogram
    maxhi = np.amax(hist)
    maxhiposval = bin_centers[np.argmax(hist)]

    # Fit a Gaussian
    try:
        gauss=gaussian()
        popt, pcov = opt.curve_fit(gauss, bin_centers, hist, p0 = [maxhiposval, maxhi, stdev/2.])
    except:
        popt = np.array([average, widthes[0]*npoints/(np.sqrt(2*np.pi)*stdev), stdev])

    if threshmode == 'abs':
        std = 1.
        ave = 0.
    if threshmode == 'std':
        std = stdev
        ave = average
    if threshmode == 'mad':
        std = mad
        ave = med
        stdev=mad
        average=med
    if threshmode == 'fit':
        std = popt[2]
        ave = popt[0]

    try:
        makeSunblockPlots
    except NameError:
        makeSunblockPlots = None 
    
    if makeSunblockPlots==True :
        plotSunblocker(bin_centers,bin_edges,npoints,widthes,average,stdev,med,mad,popt,hist,threshold,galaxy,msid,track,scan,ave+float(threshold)*std)

    caracal.log.info("FFT image flagging cutoff = median + {threshold} * mad = {cutoff:.5f}".format(threshold=float(threshold),cutoff=ave+float(threshold)*std))

    return ave+float(threshold)*std


def flagQuartile(inVis,tableFlags,inFFTHeader,method,dilateU,dilateV,qrtdebug=False):

    U=tableFlags['u']
    V=tableFlags['v']
    UV=np.array([U,V])

    t=tables.table(inVis,readonly=False,ack=False)
    # Take existing flags from MS of this scan to estimate flagged starting flagged fraction
    flags = t.getcol('FLAG')
    percTot=np.nansum(flags)/float(flags.shape[0]*flags.shape[1]*flags.shape[2])*100.
    # Reset to no flags and build up stripe flags
    flags = np.zeros(flags.shape, bool)
    caracal.log.info("Scan flags before stripe-flagging: {percent:.3f}%".format(percent=percTot))
    #uvw=np.array(t.getcol('UVW'),dtype=float) # This is never used
    spw=tables.table(inVis+'/SPECTRAL_WINDOW',ack=False)
    avspecchan = np.average(spw.getcol('CHAN_FREQ'))
    uv = t.getcol('UVW')[:,:2]*avspecchan/scconstants.c

    caracal.log.info('{0:d} UV cells in the FFT image selected for flagging'.format(U.shape[0]))

    if qrtdebug and U.shape[0]:
        caracal.log.info('\tamplitude of selected cells in range {0:.3f} - {1:.3f}'.format(np.nanmin(tableFlags['amp']),np.nanmax(tableFlags['amp'])))
        caracal.log.info('\t{0} total rows in scan MS'.format(flags.shape))

    if U.shape[0]:
        caracal.log.info('Finding MS rows within flagged cells +/- {0:d} U cell(s) and +/- {1:d} V cell(s)'.format(dilateU, dilateV))

    percent=0.
    for i in range(0,UV.shape[1]):
        indexU=np.where( np.logical_and( uv[:,0] > UV[0,i] - (1/2 + dilateU) * inFFTHeader['CDELT2'], uv[:,0]<=UV[0,i] + (1/2 + dilateU) * inFFTHeader['CDELT2'] ) )[0]
        indexV=np.where( np.logical_and( uv[:,1] > UV[1,i] - (1/2 + dilateV) * inFFTHeader['CDELT2'], uv[:,1]<=UV[1,i] + (1/2 + dilateV) * inFFTHeader['CDELT2'] ) )[0]

        if qrtdebug:
            caracal.log.info('\tcell {0:d}, [U,V] = {1}'.format(i,UV[:,i]))
            caracal.log.info('\t\tflagging u range = {0:.3f} - {1:.3f}'.format(UV[0,i] - (1/2 + dilateU) * inFFTHeader['CDELT2'], UV[0,i] + (1/2 + dilateU) * inFFTHeader['CDELT2']))
            caracal.log.info('\t\tflagging v range = {0:.3f} - {1:.3f}'.format(UV[1,i] - (1/2 + dilateV) * inFFTHeader['CDELT2'], UV[1,i] + (1/2 + dilateV) * inFFTHeader['CDELT2']))

        # Combine U and V selection into final selection
        indexTot= np.intersect1d(indexU,indexV)

        if qrtdebug:
            caracal.log.info('\t\t{0:d} rows found'.format(indexTot.shape[0]))
            if indexTot.shape[0]:
                caracal.log.info('\t\tSelected rows have uv in the following ranges')
                caracal.log.info('\t\tu: {0:.3f} - {1:.3f}'.format(np.nanmin(uv[indexTot,0]),np.nanmax(uv[indexTot,0])))
                caracal.log.info('\t\tv: {0:.3f} - {1:.3f}'.format(np.nanmin(uv[indexTot,1]),np.nanmax(uv[indexTot,1])))

        # Add to stripe flags of this scan
        flags[indexTot,:,:] = True
        percent+=float(len(indexTot))/float(flags.shape[0])*100.

    # Save modified flags to MS of this scan
    t.putcol('FLAG', t.getcol('FLAG') + flags)
    t.close()
    caracal.log.info("Flag scan done")
    return flags, percent


def putFlags(pf_inVis, pf_inVisName, pf_stripeFlags):
    caracal.log.info("Opening full MS file to add stripe flags".format(pf_inVisName))
    t=tables.table(pf_inVis,readonly=False,ack=False)
    flagOld = t.getcol('FLAG')
    percTotBefore = np.nansum(flagOld)/float(flagOld.shape[0]*flagOld.shape[1]*flagOld.shape[2])*100.
    caracal.log.info("Total Flags Before: {percent:.3f} %".format(percent=percTotBefore))
    flagNew = np.sum([pf_stripeFlags, flagOld], axis=0)
    percTotAfter=np.nansum(flagNew)/float(flagNew.shape[0]*flagNew.shape[1]*flagNew.shape[2])*100.
    caracal.log.info("Total Flags After: {percent:.3f} %".format(percent=percTotAfter))
    t.putcol('FLAG', flagNew)
    del flagOld
    del flagNew
    gc.collect()
    t.close()
    caracal.log.info("MS flagged")
    caracal.log.info("Before we close, save flag version 'stripe_flag_after'")
    flg(vis=pf_inVis, mode='save', versionname='stripe_flag_after')
    return 0


def run_flagUzeros(pipeline,targets,msname,config):

    method = config['flagUzeros']['method']
    makePlots=config['flagUzeros']['makePlots']

    makeSunblockPlots=config['flagUzeros']['makeSunblockPlots']
    
    doCleanUp =config['flagUzeros']['method']

    thresholds = config['flagUzeros']['thresholds']
    dilateU = config['flagUzeros']['dilateU']
    dilateV = config['flagUzeros']['dilateV']
    flagCmd = True

    galaxies = targets

    datapath=pipeline.output
    mfsOb = msname



    setDirs(pipeline.output,config)



    if makePlots ==True or makeSunblockPlots==True:
        font=16
        params = {'figure.autolayout' : True,
            'font.family'         :'serif',
            'figure.facecolor': 'white',
            'pdf.fonttype'        : 3,
            'font.serif'          :'times',
            'font.style'          : 'normal',
            'font.weight'         : 'book',
            'font.size'           : font,
            'axes.linewidth'      : 1.5,
            'lines.linewidth'     : 1,
            'xtick.labelsize'     : font,
            'ytick.labelsize'     : font,
            'legend.fontsize'     : font,
            'xtick.direction'     :'in',
            'ytick.direction'     :'in',
            'xtick.major.size'    : 3,
            'xtick.major.width'   : 1.5,
            'xtick.minor.size'    : 2.5,
            'xtick.minor.width'   : 1.,
            'ytick.major.size'    : 3,
            'ytick.major.width'   : 1.5,
            'ytick.minor.size'    : 2.5,
            'ytick.minor.width'   : 1.,
            'text.usetex' : False
         }
        plt.rcParams.update(params)


    ##### MAIN MAIN MAIN
    superArr = np.empty((0,7))
    for jj in range(0,len(galaxies)):
        galaxy = galaxies[jj]

        comvmax_tot, comvmax_scan = 0, 0
        runtime = time.strftime("%d-%m-%Y")+'_'+time.strftime("%H-%M")
        # logging.basicConfig(format='(%(asctime)s) [%(name)-17s] %(levelname)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S", filename='{datapath}/stripeAnalysis_{time}.log'.format(datapath=stripeDir, galaxy=galaxy, time=runtime),level=logging.DEBUG)
        # logging.captureWarnings(True)

        # logging.disable(logging.DEBUG)
        # caracal.log = logging.getcaracal.log(__name__)

        # consoleHandler = logging.StreamHandler(sys.stdout)
        # logFormatter = logging.Formatter("%(asctime)s [%(name)-17s] %(levelname)-5.5s:  %(message)s",datefmt="%Y-%m-%d %H:%M:%S")
        # consoleHandler.setFormatter(logFormatter)

        # caracal.log.addHandler(consoleHandler)
        caracal.log.warn(
                'Skipping Stokes axis removal for {0:s}. File does not exist.'.format(mfsOb))
        caracal.log.info("====================================================")
        # caracal.log.info("{galaxy}, lw(s): 'lw1'+ {track}".format(galaxy=galaxy, track=lws))

        obsIDs=[]
        rootMS = str.split(mfsOb,'.ms')[0]

        if config['flagUzeros']['transferFlags'] == True:
            lws = config['flagUzeros']['transferto'] 

            for lw in lws:
                obsIDs.append('{}{}.ms'.format(rootMS,lw))

            for obb in obsIDs:
                caracal.log.info("\t{}".format(obb))
        else:
            obsIDs.append(mfsOb)
            lws=['trk']


        for ii in range (0,len(obsIDs)):
            galNameVis=galaxy.replace('-','_')
            track = lws[ii]
            inVis=pipeline.msdir+'/'+obsIDs[ii]
            inVisName=obsIDs[ii]
            caracal.log.info("====================================================")
            caracal.log.info("\tWorking on {}".format(inVisName))
            caracal.log.info("====================================================")

            if os.path.exists(inVis+'.flagversions'):
                fvers = [ii.split(' :')[0] for ii in open(inVis+'.flagversions/FLAG_VERSION_LIST').readlines()]
                if 'stripe_flag_before' in fvers:
                    caracal.log.info("Before we start, restore existing flag version 'stripe_flag_before'")
                    flg(vis=inVis, mode='restore', versionname='stripe_flag_before')
                    while fvers[-1] != 'stripe_flag_before':
                        flg(vis=inVis, mode='delete', versionname=fvers[-1])
                        fvers = fvers[:-1]
                else:
                    caracal.log.info("Before we start, save flag version 'stripe_flag_before'")
                    flg(vis=inVis, mode='save', versionname='stripe_flag_before')
            else:
                caracal.log.info("Before we start, save flag version 'stripe_flag_before'")
                flg(vis=inVis, mode='save', versionname='stripe_flag_before')

            # For lw's other than the first one, just copy the flags and skip the rest of the for loop
            if ii != 0:
                putFlags(inVis, inVisName, stripeFlags)
                continue

            # For the first lw, do all that follows
            caracal.log.info("Opening full MS file".format(inVisName))
            t=tables.table(inVis,readonly=True,ack=False)
            scans=t.getcol('SCAN_NUMBER')
            FlagTot=t.getcol('FLAG')
            scanNums=np.unique(scans)
            timestamps = t.getcol("TIME")
            field_id = t.getcol("FIELD_ID")
            t.close()

            percTot=np.nansum(FlagTot)/float(FlagTot.shape[0]*FlagTot.shape[1]*FlagTot.shape[2])*100.
            caracal.log.info("Flagged visibilites so far: {percTot:.3f} %".format(percTot=percTot))

            anttab = tables.table(inVis+"::ANTENNA", ack=False)
            ant_xyz = anttab.getcol("POSITION", 0 , 1)[0]
            anttab.close()

            caracal.log.info("----------------------------------------------------")
            caracal.log.info("Imaging full MS for stripe analysis".format(track=track))
            outCubePrefix = galaxy+'_1'+track+'_tot'
            outCubeName=config['flagUzeros']['stripeCubeDir']+outCubePrefix+'-dirty.fits'
            if os.path.exists(outCubeName):
                os.remove(outCubeName)
            makeCube(pipeline,pipeline.msdir,inVisName,outCubePrefix,config)

            caracal.log.info("Making FFT of image")
            outFFT=config['flagUzeros']['stripeFFTDir']+galaxy+'_'+track+'_tot.im'
            if os.path.exists(outFFT):
                shutil.rmtree(outFFT)
            inFFTData,inFFTHeader = makeFFT(outCubeName,outFFT)

            ###U = ((np.linspace(1, inFFTData.shape[1], inFFTData.shape[1]) - inFFTHeader['CRPIX1']) * inFFTHeader['CDELT1'] + inFFTHeader['CRVAL1']) ############ Add this back?
            ###V = ((np.linspace(1, inFFTData.shape[1], inFFTData.shape[1]) - inFFTHeader['CRPIX2']-1) * inFFTHeader['CDELT2'] + inFFTHeader['CRVAL2']) ############ Add this back?

            scan=track

            if makePlots== True:
                if flagCmd ==True:
                    fig0=plt.figure(figsize=(7.24409,7.24409), constrained_layout=False)
                    fig0.set_tight_layout(False)
                    gs0 = gridspec.GridSpec(nrows=2,ncols=2,figure=fig0,hspace=0,wspace=0.0)
                    fig0, comvmax_tot = plotAll(fig0,gs0,2,0,outCubeName,inFFTData,inFFTHeader,galaxy,track,0,0,comvmax_tot,0,type=None)
                else:
                    outPlot="{0}{1}_{2}_tot.png".format(plotDir,galaxy,mfsOb)
                    fig0=plt.figure(figsize=(7.24409,7.24409), constrained_layout=False)
                    fig0.set_tight_layout(False)
                    gs0 = gridspec.GridSpec(nrows=1,ncols=2,figure=fig0,hspace=0,wspace=0.0)
                    fig0, comvmax_tot = plotAll(fig0,gs0,1,0,outCubeName,inFFTData,inFFTHeader,galaxy,track,0,0,comvmax_tot,0,type=None)
                    fig0.subplots_adjust(left=0.05, bottom=0.05, right=0.97, top=0.97, wspace=0, hspace=0)
                    fig0.savefig(outPlot,bbox_inches='tight',overwrite=True,dpi=200)   # save the figure to file
                    plt.close(fig0)

            caracal.log.info("----------------------------------------------------")

            caracal.log.info("Splitting scans".format(galaxy=galaxy, track=track))

            scanVisList,scanVisNames = splitScans(config,inVis,scanNums)

            arr = np.empty((0,7))
            NS = len(scanNums)
            if makePlots == True:
                fig1 = plt.figure(figsize=(8,21.73227), constrained_layout=False)
                fig1.set_tight_layout(False)
                fig2 = plt.figure(figsize=(8,21.73227), constrained_layout=False)
                fig2.set_tight_layout(False)

                gs1 = gridspec.GridSpec(nrows=NS,ncols=2,figure=fig1,hspace=0,wspace=0.0)
                gs2 = gridspec.GridSpec(nrows=NS,ncols=2,figure=fig2,hspace=0,wspace=0.0)

            # Initialising the stripeFlags array, to which scans will be added one by one
            stripeFlags=np.empty(((0),FlagTot.shape[1],FlagTot.shape[2]))
            percTotAv=[]

            del FlagTot
            gc.collect()

            for kk in range(len(scanNums)):

                scan=scanNums[kk]
                caracal.log.info("----------------------------------------------------")
                caracal.log.info("\tWorking on scan {}".format(str(scan)))
                visName=scanVisNames[kk]
                visAddress=scanVisList[kk]
                caracal.log.info("----------------------------------------------------")

                # Save flag version before start iterating over all thresholds
                flg(vis=visAddress, mode='save', versionname='scan_flags_start')

                caracal.log.info("Imaging scan for stripe analysis".format(scanNumber=str(scan), galaxy=galaxy, track=track))
                outCubePrefix_0 = galaxy+'_1'+track+'_scan'+str(scan)
                outCubeName_0 = config['flagUzeros']['stripeCubeDir']+outCubePrefix_0+'-dirty.fits'
                if os.path.exists(outCubeName_0):
                    os.remove(outCubeName_0)
                makeCube(pipeline,config['flagUzeros']['stripeMSDir'],visName,outCubePrefix_0,config)

                caracal.log.info("Making FFT of image")
                outFFT=config['flagUzeros']['stripeFFTDir']+galaxy+'_'+track+'_scan'+str(scan)+'.im'
                if os.path.exists(outFFT):
                    shutil.rmtree(outFFT)
                inFFTData,inFFTHeader = makeFFT(outCubeName_0,outFFT)

                U = ((np.linspace(1, inFFTData.shape[1], inFFTData.shape[1]) - inFFTHeader['CRPIX1']) * inFFTHeader['CDELT1'] + inFFTHeader['CRVAL1'])
                V = ((np.linspace(1, inFFTData.shape[1], inFFTData.shape[1]) - inFFTHeader['CRPIX2']-1) * inFFTHeader['CDELT2'] + inFFTHeader['CRVAL2'])

                el = 0
                az = 0

                outCubePrefix = galaxy+'_1'+track+'_scan'+str(scan)+'_stripeFlag'
                outCubeName = config['flagUzeros']['stripeCubeDir']+outCubePrefix+'-dirty.fits'

                rms_thresh = []

                if len(thresholds) > 1:
                    caracal.log.info('Start iterating over all requested thresholds {} to find the optimal one'.format(thresholds))
                # iterate over all thresholds
                for threshold in thresholds:
                    if len(thresholds) > 1:
                        caracal.log.info('New iter')
                    # Rewind flags of this scan to their initial state
                    fvers = [ii.split(' :')[0] for ii in open(visAddress+'.flagversions/FLAG_VERSION_LIST').readlines()]
                    flg(vis=visAddress, mode='restore', versionname='scan_flags_start')

                    while fvers[-1] != 'scan_flags_start':
                        flg(vis=visAddress, mode='delete', versionname=fvers[-1])
                        fvers = fvers[:-1]

                    caracal.log.info("Computing statistics on FFT and flagging scan for threshold {0}".format(threshold))
                    # scanFlags below are the stripe flags for this scan
                    statsArray, scanFlags, percent, cutoff_scan = saveFFTTable(inFFTData,inFFTHeader,visAddress, np.flip(U), V, galaxy, mfsOb, track, scan, el, az, method, threshold, dilateU, dilateV)
                    caracal.log.info("Scan flags from stripe-flagging: {percent:.3f}%".format(percent=percent))
                    caracal.log.info("Making post-flagging image")
                    
                    if os.path.exists(outCubeName):
                        os.remove(outCubeName)
                    makeCube(pipeline,config['flagUzeros']['stripeMSDir'],visName,outCubePrefix,config)
                    fitsdata = fits.open(outCubeName)
                    rms_thresh.append(np.std(fitsdata[0].data[0,0]))
                    caracal.log.info("Image noise = {0:.3e} Jy/beam".format(rms_thresh[-1]))
                    fitsdata.close()

                # Select best threshold (minimum noise), re-flag and re-image
                if len(thresholds) > 1:
                    caracal.log.info('Done iterating over all requested thresholds')
                    threshold = thresholds[rms_thresh.index(min(rms_thresh))]
                    caracal.log.info('\tThe threshold that minimises the image noise is {}'.format(threshold))
                    caracal.log.info('Repeating flagging and imaging steps with the selected threshold (yes, the must be a better way...)')
                    # Rewind flags of this scan to their initial state
                    fvers = [ii.split(' :')[0] for ii in open(visAddress+'.flagversions/FLAG_VERSION_LIST').readlines()]
                    flg(vis=visAddress, mode='restore', versionname='scan_flags_start')
                    while fvers[-1] != 'scan_flags_start':
                        flg(vis=visAddress, mode='delete', versionname=fvers[-1])
                        fvers = fvers[:-1]
                    # Re-flag with selected threshold
                    caracal.log.info("Computing statistics on FFT and flagging scan for threshold {0}".format(threshold))
                    statsArray, scanFlags, percent, cutoff_scan = saveFFTTable(inFFTData,inFFTHeader, visAddress, np.flip(U), V, galaxy, mfsOb, track, scan, el, az, method, threshold, dilateU, dilateV)
                    caracal.log.info("Scan flags from stripe-flagging: {percent:.3f}%".format(percent=percent))
                    # Re-image
                    caracal.log.info("Making post-flagging image")
                    if os.path.exists(outCubeName):
                        os.remove(outCubeName)
                    makeCube(pipeline,config['flagUzeros']['stripeMSDir'],visName,outCubePrefix,config)

                # Save stats for the selected threshold
                arr = np.vstack((arr, statsArray))
                percTotAv.append(percent)

                # Add the stripe flags of this scan to the stripe flags of all the scans done previously
                stripeFlags=np.concatenate([stripeFlags,scanFlags])

                if makePlots == True:
                    fig1, comvmax_scan = plotAll(fig1,gs1,NS,kk,outCubeName_0,inFFTData,inFFTHeader,galaxy,track,scan,None,comvmax_scan,cutoff_scan,type=None)

                caracal.log.info("Making FFT of post-flagging image")
                outFFT=config['flagUzeros']['stripeFFTDir']+galaxy+'_'+track+'_scan'+str(scan)+'_stripeFlag.im'
                if os.path.exists(outFFT):
                    shutil.rmtree(outFFT)
                inFFTData,inFFTHeader = makeFFT(outCubeName,outFFT)

                if makePlots == True:
                    fig2, comvmax_scan = plotAll(fig2,gs2,NS,kk,outCubeName,inFFTData,inFFTHeader,galaxy,track,scan,percent,comvmax_scan,0,type='postFlag')

            if makePlots == True:
                caracal.log.info("----------------------------------------------------")
                caracal.log.info("Saving scans diagnostic plots")
                outPlot="{0}{1}_{2}_perscan_preFlag.png".format(plotDir,galaxy,mfsOb)
                outPlotFlag="{0}{1}_{2}_perscan_postFlag.png".format(plotDir,galaxy,mfsOb)

                fig1.subplots_adjust(left=0.05, bottom=0.05, right=0.97, top=0.97, wspace=0, hspace=0)
                fig1.savefig(outPlot,bbox_inches='tight',overwrite=True,dpi=200)   # save the figure to file
                plt.close(fig1)
                fig2.subplots_adjust(left=0.05, bottom=0.05, right=0.97, top=0.97, wspace=0, hspace=0)
                fig2.savefig(outPlotFlag,bbox_inches='tight',overwrite=True,dpi=200)   # save the figure to file
                plt.close(fig2)

            superArr = np.vstack((superArr, arr))
            caracal.log.info("Saving stats table")
            newtab = Table(names=['galaxy','track','scan','perc', 'cutoff','el','az'], data=(superArr))
            outTablePercent="{tableDir}stats_{galaxy}_{track}.ecsv".format(tableDir=tableDir,galaxy=galaxy,track=track)
            ascii.write(newtab,outTablePercent, overwrite=True,format='ecsv')

            if flagCmd==True:
                caracal.log.info("====================================================")
                caracal.log.info("\tWorking on {}".format(inVisName))
                caracal.log.info("====================================================")

                putFlags(inVis, inVisName, stripeFlags)
                caracal.log.info("Making post-flagging image")

                outCubePrefix = galaxy+'_'+track+'_tot_stripeFlag'
                outCubeName=config['flagUzeros']['stripeCubeDir']+outCubePrefix+'-dirty.fits'

                if os.path.exists(outCubeName):
                    os.remove(outCubeName)
                makeCube(pipeline,pipeline.msdir,inVisName,outCubePrefix,config)

                caracal.log.info("Making FFT of post-flagging image")

                outFFT=config['flagUzeros']['stripeFFTDir']+galaxy+'_'+track+'_tot_stripeFlag.im'
                if os.path.exists(outFFT):
                    shutil.rmtree(outFFT)
                inFFTData,inFFTHeader = makeFFT(outCubeName,outFFT)

                U = ((np.linspace(1, inFFTData.shape[1], inFFTData.shape[1]) - inFFTHeader['CRPIX1']) * inFFTHeader['CDELT1'] + inFFTHeader['CRVAL1'])
                V = ((np.linspace(1, inFFTData.shape[1], inFFTData.shape[1]) - inFFTHeader['CRPIX2']-1) * inFFTHeader['CDELT2'] + inFFTHeader['CRVAL2'])

                caracal.log.info("Saving total stripe flagging diagnostic plots".format(galaxy=galaxy, track=track))

                percTotAfter=np.nansum(stripeFlags)/float(stripeFlags.shape[0]*stripeFlags.shape[1]*stripeFlags.shape[2])*100.
                caracal.log.info("Total stripe flags: {percent:.3f} %".format(percent=percTotAfter))
                percRel = percTotAfter-percTot
                caracal.log.info("Mean stripe flagging per scan: {percent:.3f}%".format(percent=np.nanmean(percTotAv)))

                if makePlots==True:
                    outPlot="{0}{1}_{2}_fullMS.png".format(plotDir,galaxy,mfsOb)
                    fig0, comvmax_tot = plotAll(fig0,gs0,2,1,outCubeName,inFFTData,inFFTHeader,galaxy,track,0,np.nanmean(percTotAv),comvmax_tot,0,type='postFlag')
                    fig0.subplots_adjust(left=0.05, bottom=0.05, right=0.97, top=0.97, wspace=0, hspace=0)
                    fig0.savefig(outPlot,bbox_inches='tight',overwrite=True,dpi=200)   # save the figure to file
                    plt.close(fig0)

                timeFlag = (time.time()-timeInit)/60.
                #caracal.log.info("\tTotal flagging time: {timeend:.1f} minutes".format(timeend=timeFlag))


    if doCleanUp is True:
        cleanUp(galaxy)

# timeEnd = (time.time()-timeInit)/60.
# #caracal.log.info("\tTotal processing time: {timeend} minutes".format(timeend=timeEnd))
# caracal.log.info("Done")

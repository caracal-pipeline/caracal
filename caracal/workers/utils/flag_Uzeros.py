#!/bin/bash

import gc
from caracal.workers.utils import remove_output_products
from caracal import log
import caracal
import shutil
import argparse
import time
from collections import OrderedDict
import matplotlib.dates as mdat
from matplotlib import gridspec
from matplotlib import rc
from matplotlib import pyplot as plt
import datetime
from casacore.measures import dq
import casacore.measures as measures
import casacore.images as images
import casacore.tables as tables
import stimela.recipe
import sys
import os
import numpy as np
import yaml
from caracal.utils.requires import extras


dm = measures.measures()

timeInit = time.time()

class UzeroFlagger:
    global u, SkyCoord, astviz, WCS, Table, Column, fits, astasc
    global optimize, scconstants, stats

    
    @extras(packages=["astropy", "scipy"])
    def __init__(self, config):
        from astropy import units as u
        from astropy.coordinates import SkyCoord
        import astropy.visualization as astviz
        from astropy.wcs import WCS
        from astropy.table import Table, Column
        from astropy.io import fits
        import astropy.io.ascii as astasc
        import scipy.optimize as optimize
        import scipy.constants as scconstants
        from scipy import stats

        self.config = config

    def setDirs(self, output):

        self.config['flag_u_zeros']['stripeDir'] = output + '/stripeAnalysis/'
        if not os.path.exists(self.config['flag_u_zeros']['stripeDir']):
            os.mkdir(self.config['flag_u_zeros']['stripeDir'])

        self.config['flag_u_zeros']['stripeLogDir'] = self.config['flag_u_zeros']['stripeDir'] + 'logs/'
        if not os.path.exists(self.config['flag_u_zeros']['stripeLogDir']):
            os.mkdir(self.config['flag_u_zeros']['stripeLogDir'])

        self.config['flag_u_zeros']['stripeMSDir'] = self.config['flag_u_zeros']['stripeDir'] + 'msdir/'
        if not os.path.exists(self.config['flag_u_zeros']['stripeMSDir']):
            os.mkdir(self.config['flag_u_zeros']['stripeMSDir'])

        self.config['flag_u_zeros']['stripeCubeDir'] = self.config['flag_u_zeros']['stripeDir'] + 'cubes/'
        if not os.path.exists(self.config['flag_u_zeros']['stripeCubeDir']):
            os.mkdir(self.config['flag_u_zeros']['stripeCubeDir'])

        self.config['flag_u_zeros']['stripeFFTDir'] = self.config['flag_u_zeros']['stripeDir'] + 'fft/'
        if not os.path.exists(self.config['flag_u_zeros']['stripeFFTDir']):
            os.mkdir(self.config['flag_u_zeros']['stripeFFTDir'])

        self.config['flag_u_zeros']['stripePlotDir'] = self.config['flag_u_zeros']['stripeDir'] + 'plots/'
        if not os.path.exists(self.config['flag_u_zeros']['stripePlotDir']):
            os.mkdir(self.config['flag_u_zeros']['stripePlotDir'])

        self.config['flag_u_zeros']['stripeTableDir'] = self.config['flag_u_zeros']['stripeDir'] + 'tables/'
        if not os.path.exists(self.config['flag_u_zeros']['stripeTableDir']):
            os.mkdir(self.config['flag_u_zeros']['stripeTableDir'])

        self.config['flag_u_zeros']['stripeSofiaDir'] = self.config['flag_u_zeros']['stripeDir'] + 'sofiaOut/'
        if not os.path.exists(self.config['flag_u_zeros']['stripeSofiaDir']):
            os.mkdir(self.config['flag_u_zeros']['stripeSofiaDir'])

        return

    def saveFlags(self, pipeline, inVis, msdir, flagname):

        recipe = stimela.Recipe('saveFlagZeros',
                                ms_dir=msdir,
                                singularity_image_dir=pipeline.singularity_image_dir,
                                log_dir=self.config['flag_u_zeros']['stripeLogDir'],
                                logfile=False,  # no logfiles for recipes
                                )
        recipe.JOB_TYPE = pipeline.container_tech

        step = 'saveFlag'

        recipe.add("cab/casa_flagmanager", step, {
            "vis": inVis,
            "mode": "save",
            "versionname": flagname,
        },
            input=pipeline.input,
            output=pipeline.output,
            label="{0:s}:: Save flag version")

        recipe.run()

    def deleteFlags(self, pipeline, inVis, msdir, flagname):

        recipe = stimela.Recipe('saveFlagZeros',
                                ms_dir=msdir,
                                singularity_image_dir=pipeline.singularity_image_dir,
                                log_dir=self.config['flag_u_zeros']['stripeLogDir'],
                                logfile=False,  # no logfiles for recipes
                                )
        recipe.JOB_TYPE = pipeline.container_tech

        step = 'deleteFlag'

        recipe.add("cab/casa_flagmanager", step, {
            "vis": inVis,
            "mode": "delete",
            "versionname": flagname,
        },
            input=pipeline.input,
            output=pipeline.output,
            label="Delete flag version")

        recipe.run()

    def restoreFlags(self, pipeline, inVis, msdir, flagname):

        recipe = stimela.Recipe('saveFlagZeros',
                                ms_dir=msdir,
                                singularity_image_dir=pipeline.singularity_image_dir,
                                log_dir=self.config['flag_u_zeros']['stripeLogDir'],
                                logfile=False,  # no logfiles for recipes
                                )
        recipe.JOB_TYPE = pipeline.container_tech

        step = 'restoreFlag'

        recipe.add("cab/casa_flagmanager", step, {
            "vis": inVis,
            "mode": "restore",
            "versionname": flagname,
        },
            input=pipeline.input,
            output=pipeline.output,
            label="Restore flag version")

        recipe.run()

    def splitScans(self, pipeline, msdir, inVis, scanNums):

        scanVisList = []
        scanVisNames = []

        for scan in scanNums:
            baseVis = os.path.basename(inVis)
            outVis = baseVis.split('.ms')[0] + '_scn' + str(scan) + '.ms'
            # if os.path.exists(self.config['flag_u_zeros']['stripeMSDir']+outVis):
            #     shutil.rmtree(self.config['flag_u_zeros']['stripeMSDir']+outVis)
            # if os.path.exists(self.config['flag_u_zeros']['stripeMSDir']+outVis+'.flagversions'):
            #     shutil.rmtree(self.config['flag_u_zeros']['stripeMSDir']+outVis+'.flagversions')
            remove_output_products((outVis, outVis + '.flagversions'), directory=self.config['flag_u_zeros']['stripeMSDir'])

            recipe = stimela.Recipe('flagUzerosMST',
                                    ms_dir=msdir,
                                    singularity_image_dir=pipeline.singularity_image_dir,
                                    log_dir=self.config['flag_u_zeros']['stripeLogDir'],
                                    logfile=False,  # no logfiles for recipes
                                    )
            recipe.JOB_TYPE = pipeline.container_tech

            step = 'splitScans'
            recipe.add('cab/casa_mstransform',
                       step,
                       {"msname": baseVis,
                        "outputvis": outVis + ":output",
                        "datacolumn": 'data',
                        "scan": str(scan),
                        },
                       input=msdir,
                       output=self.config['flag_u_zeros']['stripeMSDir'],
                       label='{0:s}:: Image Line'.format(step))
            recipe.run()

            scanVisList.append(self.config['flag_u_zeros']['stripeMSDir'] + outVis)
            scanVisNames.append(outVis)

        caracal.log.info("All Scans splitted")

        return scanVisList, scanVisNames

    def gaussian(self, x, cent, amp, sigma):
        """
        Gaussian function
        Input:
        cent (float): centre
        amp (float) : amplitude
        sigma (float) : sigma
        Return:
        gaussian() Gaussian
        """
        return amp * np.exp(-0.5 * np.power((x - cent) / sigma, 2))

    def convToStokesI(self, data, flags):

        stflags = np.logical_not(flags).astype(float)

        # if polarisation is i, then take either average or single value, flag the rest
        # Calculate stokes i, reduce the number of polarizations to one, flag if not at least one pol is available
        with np.errstate(divide='ignore', invalid='ignore'):
            data = (data * np.logical_not(flags)) / stflags
        flags = stflags < 1.
        data[flags] = np.nan

        return data, flags

    def makeCube(self, pipeline, msdir, inVis, outCubePrefix, kind='scan'):

        robust = self.config['flag_u_zeros']['robust']
        imsize = int(self.config['flag_u_zeros']['imsize'])
        cell = self.config['flag_u_zeros']['cell']
        chanMin = int(self.config['flag_u_zeros']['chans'][0])
        chanMax = int(self.config['flag_u_zeros']['chans'][1])

        recipe = stimela.Recipe('flagUzeros',
                                ms_dir=msdir,
                                singularity_image_dir=pipeline.singularity_image_dir,
                                log_dir=self.config['flag_u_zeros']['stripeLogDir'],
                                logfile=False,  # no logfiles for recipes
                                )
        recipe.JOB_TYPE = pipeline.container_tech
        # print(inVis,outCubePrefix)

        if kind == 'scan':
            chMin = 0
            chMax = chanMax - chanMin
        else:
            chMin = chanMin
            chMax = chanMax
        # imsize=400,scale=20.asec

        line_image_opts = {
            "msname": inVis,
            "prefix": outCubePrefix,
            "npix": imsize,
            "scale": cell,
            "weight": 'briggs {0:.3f}'.format(robust),
            "channelsout": 1,
            "channelrange": [chanMin, chanMax],
            "niter": 0,
            "gain": 0.2,
            "mgain": 0.85,
            "auto-threshold": 10.0,
            "multiscale": False,
            "multiscale-scale-bias": 0.6,
            "no-update-model-required": True,
            "auto-threshold": 0.5,
            "auto-mask": 10.0,
            "gain": 0.2,
        }

        if self.config['flag_u_zeros']['taper']:
            line_image_opts.update({"taper-gaussian": str(self.config['flag_u_zeros']['taper'])})

        step = 'makeCube'
        recipe.add('cab/wsclean',
                   step, line_image_opts,
                   input=pipeline.input,
                   output=self.config['flag_u_zeros']['stripeCubeDir'],
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
        # caracal.log.info("----------------------------------------------------")

        return 0

    # def makeFFT(self, inCube,outFFT):

    def makeFFT(self, inCube):

        with fits.open(inCube) as hdul:
            hdu = hdul[0]
            dFFT = np.abs(np.fft.fftshift(np.fft.fft2(np.squeeze(hdu.data))))
            hdr = fits.Header()
            hdr["CTYPE1"] = 'UU---SIN'
            hdr["CDELT1"] = 1 / (np.deg2rad(hdu.header["NAXIS1"] * hdu.header["CDELT1"]))
            hdr["CRVAL1"] = 0
            hdr["CRPIX1"] = hdu.header["NAXIS1"] / 2
            hdr["CUNIT1"] = 'lambda'
            hdr["CTYPE2"] = 'VV---SIN'
            hdr["CDELT2"] = 1 / (np.deg2rad(hdu.header["NAXIS2"] * hdu.header["CDELT2"]))
            hdr["CRVAL2"] = 0
            hdr["CRPIX2"] = hdu.header["NAXIS2"] / 2
            hdr["CUNIT2"] = 'lambda'

        caracal.log.info('\tFFT cell size = {0:.2f}'.format(hdr['CDELT2']))
        caracal.log.info("FFT Done")
        gc.collect()

        return dFFT, hdr

    def plotAll(self, fig, gs, NS, kk, outCubeName, inFFTData, inFFTHeader, galaxy, track, scan, percent, common_vmax, ctff, type=None):

        fitsdata = fits.open(outCubeName)
        fitsim = fitsdata[0].data[0, 0]
        fitshdr = fitsdata[0].header
        fitswcs = WCS(fitshdr).sub(2)
        rms1 = np.std(fitsim)

        ax = fig.add_subplot(gs[kk, 0], projection=fitswcs)
        ax.imshow(fitsim, cmap='Greys', vmin=-rms1, vmax=2 * rms1)
        if scan != 0:
            ax.annotate("Scan: " + str(scan) + r" rms = " + str(np.round(rms1 * 1e6, 3)) + r" $\mu$Jyb$^{-1}$", xy=(0.05, 0.95), xycoords='axes fraction', horizontalalignment='left', verticalalignment='top', backgroundcolor='w', fontsize=12)
        else:
            ax.annotate("rms = " + str(np.round(rms1 * 1e6, 3)) + r" $\mu$Jyb$^{-1}$", xy=(0.05, 0.95), xycoords='axes fraction', horizontalalignment='left', verticalalignment='top', backgroundcolor='w', fontsize=12)
        if type == 'postFlag':
            ax.annotate(r"Flags {percent} $\%$".format(percent=str(np.round(percent, 2))), xy=(0.95, 0.05), xycoords='axes fraction', horizontalalignment='right', verticalalignment='bottom', backgroundcolor='w', fontsize=12)

        lon = ax.coords[0]
        lat = ax.coords[1]
        c = SkyCoord('00:02:00.', '00:01:00.0', unit=(u.hourangle, u.deg))
        lon.set_ticks(spacing=c.ra.degree * u.degree)
        lat.set_ticks(spacing=c.ra.degree * u.degree)

        lon.set_auto_axislabel(False)
        lat.set_auto_axislabel(False)
        lon.set_ticklabel(exclude_overlapping=True)
        lat.set_ticklabel(exclude_overlapping=True)

        if kk == NS / 2 or kk == NS / 2 + 1 or (kk == 0 and NS == 1):
            lat.set_axislabel(r'Dec  (J2000)')
            lat.set_ticklabel_visible(True)
        else:
            lat.set_ticklabel_visible(True)
        if kk == NS - 1:
            lon.set_axislabel(r'RA  (J2000)')
            lon.set_ticklabel_visible(True)
        else:
            lon.set_ticklabel_visible(False)

        udelt = inFFTHeader['CDELT1']
        vdelt = inFFTHeader['CDELT2']
        ax.set_autoscale_on(False)

        ax2 = fig.add_subplot(gs[kk, 1])
        w = int(2000. / vdelt)
        cx, cy = inFFTData.shape[0] // 2, inFFTData.shape[1] // 2
        extent = [-w * udelt, w * udelt, -w * vdelt, w * vdelt]
        if common_vmax == 0:
            common_vmax = np.nanpercentile(inFFTData[cx - w:cx + w + 1, cy - w:cy + w + 1], 99)
        fftim = ax2.imshow(inFFTData[cx - w:cx + w + 1, cy - w:cy + w + 1], vmin=0, vmax=common_vmax, extent=extent, origin='upper')
        if ctff:
            ax2.contour(inFFTData[cx - w:cx + w + 1, cy - w:cy + w + 1], levels=[ctff,], colors=['r'], linewidths=[1,], extent=extent, origin='upper')

        ax2.yaxis.set_label_position("right")
        ax2.yaxis.tick_right()
        ax2.yaxis.set_ticks_position('right')

        if kk == NS - 1:
            ax2.set_xlabel(r'u [$\lambda$]')
            ax2.set_xticks([-1500, 0, 1500])
        else:
            ax2.set_xticks([])
        if kk == NS / 2 or kk == NS / 2 + 1 or (kk == 0 and NS == 1):
            ax2.set_ylabel(r'v [$\lambda$]')

        ax2.set_xlim(-2000, 2000)
        ax2.set_ylim(-2000, 2000)
        ax2.set_yticks([-1500, 0, 1500])

        ax2.set_autoscale_on(False)
        fig.subplots_adjust(left=0.05, bottom=0.05, right=0.97, top=0.97, wspace=0, hspace=0)

        return fig, common_vmax

    def baselineStats(self, galaxy, flags, uvw, avspecchan):

        lambdal = scconstants.c / avspecchan
        index = flags[:, 0, 0]
        flagCoords = uvw[index, :]
        baseFlags = np.sqrt(np.power(flagCoords[:, 0], 2) + np.power(flagCoords[:, 1], 2) + np.power(flagCoords[:, 2], 2)) * lambdal
        baseAll = np.sqrt(np.power(uvw[:, 0], 2) + np.power(uvw[:, 1], 2) + np.power(uvw[:, 2], 2)) * lambdal

        figBase = plt.figure(figsize=(7.24409, 7.24409), constrained_layout=False)
        gsBase = gridspec.GridSpec(nrows=1, ncols=1, figure=figBase, hspace=0, wspace=0.0)
        axBase = figBase.add_subplot(gsBase[0, 0])

        axBase.yaxis.set_label_position("left")
        axBase.yaxis.tick_right()
        axBase.yaxis.set_ticks_position('left')

        axBase.set_xlabel(r'Baseline Lenght [m]')
        axBase.set_ylabel(r'Percentage of u=0 flags')
        bins = [0, 5, 25, 50, 100, 250, 500, 1000, 8000]

        nFlags, binEdgesFlags = np.histogram(baseFlags, bins)
        nAll, binEdgesAll = np.histogram(baseAll, bins)
        nPerc = nFlags / nAll * 100.
        axBase.set_ylim(0, 100)

        np.save(self.config['flag_u_zeros']['stripePlotDir'] + 'baseflags.npy', baseFlags)
        np.save(self.config['flag_u_zeros']['stripePlotDir'] + 'baseAll.npy', baseAll)

        axBase.plot(bins[:-1], nPerc, 'k-', drawstyle='steps-pre')

        axBase.set_autoscale_on(False)
        outPlot = "{0}baselines_plot_{1}.png".format(self.config['flag_u_zeros']['stripePlotDir'], galaxy)
        figBase.savefig(outPlot, bbox_inches='tight', overwrite=True, dpi=200)   # save the figure to file
        plt.close(figBase)

    def cleanUp(self, galaxy):

        caracal.log.info("====================================================")
        caracal.log.info("Cleanup")

        caracal.log.info("Deleting images")

        if os.path.exists(self.config['flag_u_zeros']['stripeCubeDir']):
            shutil.rmtree(self.config['flag_u_zeros']['stripeCubeDir'])

        caracal.log.info("Deleting FFTs")

        if os.path.exists(self.config['flag_u_zeros']['stripeFFTDir']):
            shutil.rmtree(self.config['flag_u_zeros']['stripeFFTDir'])

        caracal.log.info("Deleting .ms scans")

        if os.path.exists(self.config['flag_u_zeros']['stripeMSDir']):
            shutil.rmtree(self.config['flag_u_zeros']['stripeMSDir'])

        caracal.log.info("Cleanup done")

        return 0

    def saveFFTTable(self, inFFT, inFFTHeader, visName, U, V, galaxy, msid, track, scan, el, az, method, threshold, dilateU, dilateV, makePlots):

        xCol = np.zeros([len(U) * len(V)])
        yCol = np.zeros([len(U) * len(V)])
        BIN_ID = np.arange(0, len(U) * len(V), 1)
        tabGen = np.column_stack([BIN_ID, xCol, yCol])
        dt = np.dtype([('BIN_ID', np.int32), ('U', np.int32), ('V', np.int32)])
        tabGen = np.array(list(map(tuple, tabGen)), dtype=dt)

        namBins = tuple(['BIN_ID', 'U', 'V', 'Amp'])

        tabArr = np.zeros([len(tabGen)], dtype={'names': namBins, 'formats': ('i4', 'f8', 'f8', 'f8')})

        indexBin = 0
        for i in range(0, len(U)):
            for j in range(0, len(V)):

                tabArr['BIN_ID'][indexBin] = indexBin
                tabArr['U'][indexBin] = U[i]
                tabArr['V'][indexBin] = V[j]
                tabArr['Amp'][indexBin] = inFFT[j, i]

                indexBin += 1

        hdr = fits.Header()
        hdr['COMMENT'] = "This is the table of the FFT"
        hdr['COMMENT'] = "Ext 1 = FFT table"

        if method == 'madThreshold':
            cutoff = self.sunBlockStats(inFFT, galaxy, msid, track, scan, makePlots, 'mad', threshold, ax=None, title='', verb=True)
        else:
            if self.config['flag_u_zeros']['taper']:
                cutoff = np.nanpercentile(tabArr['Amp'], 99.99)
            else:
                cutoff = np.nanpercentile(tabArr['Amp'], 99.9999)

        empty_primary = fits.PrimaryHDU(header=hdr)

        inFFT1D = np.nansum(inFFT, axis=0)

        if cutoff > np.nanmax(tabArr['Amp']):
            willflag = False
            caracal.log.warn("Cutoff is larger than max amplitude. Notihng will be flagged.")
            # cutoff = tabArr['Amp'].max()
        else:
            willflag = True

        # This is where we decide where to flag
        index = np.where(tabArr['Amp'] >= cutoff)[0]

        # And this is where we apply that flagging selection to the U,V,Amp arrays
        newtab = Table(names=['u', 'v', 'amp'], data=(tabArr['U'][index], tabArr['V'][index], tabArr['Amp'][index]))

        # Some stats ...
        if willflag:
            statsArray = [galaxy, track, scan, len(np.where(newtab['u'] <= 60.0)[0]) / len(newtab['u']), cutoff, el, az]
        else:
            statsArray = [galaxy, track, scan, 0., cutoff, el, az]
        caracal.log.info("FFT Table saved")
        caracal.log.info("Flagging scan".format(scanNumber=str(scan), galaxy=galaxy, track=track))

        # the following scanFlags are the stripe flags for this scan
        scanFlags, percent = self.flagQuartile(visName, newtab, inFFTHeader, method, dilateU, dilateV, qrtdebug=False)

        return statsArray, scanFlags, percent, cutoff

    def plotSunblocker(self, bin_centers, bin_edges, npoints, widthes, average, stdev, med, mad, popt, hist, threshold, galaxy, msid, track, scan, cut):

        caracal.log.info("\tPlotting stats")

        figS = plt.figure(figsize=(7.24409, 7.24409), constrained_layout=False)
        figS.set_tight_layout(False)

        gsS = gridspec.GridSpec(nrows=1, ncols=1, figure=figS, hspace=0, wspace=0.0)

        showgouse = np.linspace(1.5 * bin_centers[0] - 0.5 * bin_centers[1], 1.5 * bin_centers[-1] - 0.5 * bin_centers[-2], 200)
        calculated = self.gaussian(showgouse, average, widthes[0] * npoints / (np.sqrt(2 * np.pi) * stdev), stdev)

        # mad
        madded = self.gaussian(showgouse, med, widthes[0] * npoints / (np.sqrt(2 * np.pi) * mad), mad)

        # In case of using only stats, this is right on top
        fitted = self.gaussian(showgouse, popt[0], popt[1], popt[2])

        ax = figS.add_subplot(gsS[0, 0])
        ax.bar(bin_centers, hist, width=widthes, color='y', edgecolor='y')
        ax.plot(showgouse, calculated, 'g-')
        ax.plot(showgouse, fitted, 'r-')
        ax.plot(showgouse, madded, 'b-')
        ax.axvline(x=cut, linewidth=2, color='k')
        ax.set_xlim(min(bin_edges), max(bin_edges))
        plt.legend(['avg,std: {0:.1e}, {1:.1e}'.format(average, stdev), 'fit:  {0:.1e}, {1:.1e}'.format(popt[0], popt[2]), 'med,mad: {0:.1e}, {1:.1e}'.format(med, mad)], loc='upper right')
        ax.set_ylim(0.5,)
        plt.yscale('log')

        outPlot = "{0}{2}_{3}_fftstats.png".format(self.config['flag_u_zeros']['stripePlotDir'], galaxy, msid, scan)

        figS.savefig(outPlot, bbox_inches='tight', dpi=200)   # save the figure to file
        plt.close(figS)

        caracal.log.info("\tPlot Done")

        return 0

    def sunBlockStats(self, inFFTData, galaxy, msid, track, scan, makePlots, threshmode='mad', threshold=300., ax=None, title='', verb=True):

        av = np.copy(inFFTData)
        # Average data, then look for shape
        # av = np.nanmean(ampar,axis=1)
        npoints = inFFTData[np.isfinite(inFFTData)].size
        if verb:
            caracal.log.info('\tFFT grid has {:d} nonzero points.'.format(npoints))
        if npoints < 3:
            caracal.log.info('\tThis is not sufficient for any statistics, returning no flags.')
            return np.zeros(av.shape, dtype=bool)

      # Find average and standard deviation
        average = np.nanmean(inFFTData)
        stdev = np.nanstd(inFFTData)

        if average == np.nan:
            caracal.log.info('\tCannot calculate average, returing no flags.')
            return np.zeros(av.shape, dtype=bool)

        if stdev == np.nan:
            caracal.log.info('\tCannot calculate standard deviation, returing no flags.')
            return np.zeros(av.shape, dtype=bool)

        med = np.nanmedian(inFFTData)
        mad = stats.median_abs_deviation(inFFTData, scale='normal', nan_policy='omit', axis=None)
        # Build a histogram
        hist, bin_edges = np.histogram(inFFTData[np.isfinite(inFFTData)], bins=int(np.sqrt(npoints)) + 1)
        bin_centers = bin_edges[:-1] + 0.5 * (bin_edges[1:] - bin_edges[:-1])
        widthes = bin_edges[1:] - bin_edges[:-1]
        # Find maximum in histogram
        maxhi = np.amax(hist)
        maxhiposval = bin_centers[np.argmax(hist)]

        # Fit a Gaussian
        try:
            popt, pcov = optimize.curve_fit(self.gaussian, bin_centers, hist, p0=[maxhiposval, maxhi, stdev / 2.])
        except BaseException:
            popt = np.array([average, widthes[0] * npoints / (np.sqrt(2 * np.pi) * stdev), stdev])

        if threshmode == 'abs':
            std = 1.
            ave = 0.
        if threshmode == 'std':
            std = stdev
            ave = average
        if threshmode == 'mad':
            std = mad
            ave = med
            stdev = mad
            average = med
        if threshmode == 'fit':
            std = popt[2]
            ave = popt[0]

        try:
            makePlots
        except NameError:
            makePlots = None

        if makePlots:
            self.plotSunblocker(bin_centers, bin_edges, npoints, widthes, average, stdev, med, mad, popt, hist, threshold, galaxy, msid, track, scan, ave + float(threshold) * std)
        else:
            caracal.log.warn('For some reasons I am not making the fftstats plots!')

        caracal.log.info("FFT image flagging cutoff = median + {threshold} * mad = {cutoff:.5f}".format(threshold=float(threshold), cutoff=ave + float(threshold) * std))

        return ave + float(threshold) * std

    def flagQuartile(self, inVis, tableFlags, inFFTHeader, method, dilateU, dilateV, qrtdebug=False):

        U = tableFlags['u']
        V = tableFlags['v']
        UV = np.array([U, V])

        t = tables.table(inVis, readonly=False, ack=False)
        # Take existing flags from MS of this scan to estimate flagged starting flagged fraction
        flags = t.getcol('FLAG')
        percTot = np.nansum(flags) / float(flags.shape[0] * flags.shape[1] * flags.shape[2]) * 100.
        # Reset to no flags and build up stripe flags
        flags = np.zeros(flags.shape, bool)
        caracal.log.info("Scan flags before stripe-flagging: {percent:.3f}%".format(percent=percTot))
        # uvw=np.array(t.getcol('UVW'),dtype=float) # This is never used
        spw = tables.table(inVis + '/SPECTRAL_WINDOW', ack=False)
        avspecchan = np.average(spw.getcol('CHAN_FREQ'))
        uv = t.getcol('UVW')[:, :2] * avspecchan / scconstants.c

        caracal.log.info('{0:d} UV cells in the FFT image selected for flagging'.format(U.shape[0]))

        if qrtdebug and U.shape[0]:
            caracal.log.info('\tamplitude of selected cells in range {0:.3f} - {1:.3f}'.format(np.nanmin(tableFlags['amp']), np.nanmax(tableFlags['amp'])))
            caracal.log.info('\t{0} total rows in scan MS'.format(flags.shape))

        if U.shape[0]:
            caracal.log.info('Finding MS rows within flagged cells +/- {0:d} U cell(s) and +/- {1:d} V cell(s)'.format(dilateU, dilateV))

        percent = 0.
        for i in range(0, UV.shape[1]):
            indexU = np.where(np.logical_and(uv[:, 0] > UV[0, i] - (1 / 2 + dilateU) * inFFTHeader['CDELT2'], uv[:, 0] <= UV[0, i] + (1 / 2 + dilateU) * inFFTHeader['CDELT2']))[0]
            indexV = np.where(np.logical_and(uv[:, 1] > UV[1, i] - (1 / 2 + dilateV) * inFFTHeader['CDELT2'], uv[:, 1] <= UV[1, i] + (1 / 2 + dilateV) * inFFTHeader['CDELT2']))[0]

            if qrtdebug:
                caracal.log.info('\tcell {0:d}, [U,V] = {1}'.format(i, UV[:, i]))
                caracal.log.info('\t\tflagging u range = {0:.3f} - {1:.3f}'.format(UV[0, i] - (1 / 2 + dilateU) * inFFTHeader['CDELT2'], UV[0, i] + (1 / 2 + dilateU) * inFFTHeader['CDELT2']))
                caracal.log.info('\t\tflagging v range = {0:.3f} - {1:.3f}'.format(UV[1, i] - (1 / 2 + dilateV) * inFFTHeader['CDELT2'], UV[1, i] + (1 / 2 + dilateV) * inFFTHeader['CDELT2']))

            # Combine U and V selection into final selection
            indexTot = np.intersect1d(indexU, indexV)

            if qrtdebug:
                caracal.log.info('\t\t{0:d} rows found'.format(indexTot.shape[0]))
                if indexTot.shape[0]:
                    caracal.log.info('\t\tSelected rows have uv in the following ranges')
                    caracal.log.info('\t\tu: {0:.3f} - {1:.3f}'.format(np.nanmin(uv[indexTot, 0]), np.nanmax(uv[indexTot, 0])))
                    caracal.log.info('\t\tv: {0:.3f} - {1:.3f}'.format(np.nanmin(uv[indexTot, 1]), np.nanmax(uv[indexTot, 1])))

            # Add to stripe flags of this scan
            flags[indexTot, :, :] = True
            percent += float(len(indexTot)) / float(flags.shape[0]) * 100.

        # Save modified flags to MS of this scan
        t.putcol('FLAG', t.getcol('FLAG') + flags)
        t.close()
        caracal.log.info("Flag scan done")
        return flags, percent

    def putFlags(self, pipeline, pf_inVis, pf_inVisName, pf_stripeFlags):
        caracal.log.info("Opening full MS file to add stripe flags".format(pf_inVisName))
        t = tables.table(pf_inVis, readonly=False, ack=False)
        flagOld = t.getcol('FLAG')
        percTotBefore = np.nansum(flagOld) / float(flagOld.shape[0] * flagOld.shape[1] * flagOld.shape[2]) * 100.
        caracal.log.info("Total Flags Before: {percent:.3f} %".format(percent=percTotBefore))
        flagNew = np.sum([pf_stripeFlags, flagOld], axis=0)
        percTotAfter = np.nansum(flagNew) / float(flagNew.shape[0] * flagNew.shape[1] * flagNew.shape[2]) * 100.
        caracal.log.info("Total Flags After: {percent:.3f} %".format(percent=percTotAfter))
        t.putcol('FLAG', flagNew)
        del flagOld
        del flagNew
        gc.collect()
        t.close()
        caracal.log.info("MS flagged")
        caracal.log.info("Before we close, save flag version 'stripe_flag_after'")
        self.saveFlags(pipeline, pf_inVisName, msdir=pipeline.msdir, flagname='stripe_flag_after')

        return 0

    def run_flagUzeros(self, pipeline, targets, msname):

        method = self.config['flag_u_zeros']['method']
        makePlots = self.config['flag_u_zeros']['make_plots']

        doCleanUp = self.config['flag_u_zeros']['cleanup']

        thresholds = self.config['flag_u_zeros']['thresholds']
        dilateU = self.config['flag_u_zeros']['dilateU']
        dilateV = self.config['flag_u_zeros']['dilateV']
        flagCmd = True

        galaxies = targets

        datapath = pipeline.output
        mfsOb = msname

        self.setDirs(pipeline.output)

        if makePlots:
            font = 16
            params = {'figure.autolayout': True,
                      'font.family': 'serif',
                      'figure.facecolor': 'white',
                      'pdf.fonttype': 3,
                      'font.serif': 'times',
                      'font.style': 'normal',
                      'font.weight': 'book',
                      'font.size': font,
                      'axes.linewidth': 1.5,
                      'lines.linewidth': 1,
                      'xtick.labelsize': font,
                      'ytick.labelsize': font,
                      'legend.fontsize': font,
                      'xtick.direction': 'in',
                      'ytick.direction': 'in',
                      'xtick.major.size': 3,
                      'xtick.major.width': 1.5,
                      'xtick.minor.size': 2.5,
                      'xtick.minor.width': 1.,
                      'ytick.major.size': 3,
                      'ytick.major.width': 1.5,
                      'ytick.minor.size': 2.5,
                      'ytick.minor.width': 1.,
                      'text.usetex': False
                      }
            plt.rcParams.update(params)

        # MAIN MAIN MAIN
        superArr = np.empty((0, 7))
        galaxy = str.split(mfsOb, self.config['label_in'])[0]

        comvmax_tot, comvmax_scan = 0, 0
        runtime = time.strftime("%d-%m-%Y") + '_' + time.strftime("%H-%M")
        caracal.log.info("====================================================")
        caracal.log.info('Starting the flag_u_zeros segment')

        obsIDs = []

        rootMS = str.split(mfsOb, self.config['label_in'])[0]
        obsIDs.append(mfsOb)

        lws = self.config['flag_u_zeros']['transfer_flags']
        if lws == ['']:
            lws = []
        if len(lws):
            for lw in lws:
                #                obsIDs.append('{}{}.ms'.format(rootMS,lw))
                obsIDs.append(mfsOb.replace(self.config['label_in'], lw))


        lws = [self.config['label_in']] + lws

        stripeFlags = None
        for ii in range(0, len(obsIDs)):
            track = lws[ii]
            inVis = pipeline.msdir + '/' + obsIDs[ii]
            inVisName = obsIDs[ii]
            caracal.log.info("====================================================")
            caracal.log.info("\tWorking on {} ".format(inVisName))
            caracal.log.info("====================================================")

            if os.path.exists(inVis + '.flagversions'):
                fvers = [ii.split(' :')[0] for ii in open(inVis + '.flagversions/FLAG_VERSION_LIST').readlines()]
                if 'stripe_flag_before' in fvers:
                    caracal.log.info("Before we start, restore existing flag version 'stripe_flag_before'")
                    self.restoreFlags(pipeline, inVisName, msdir=pipeline.msdir, flagname='stripe_flag_before')
                    while fvers[-1] != 'stripe_flag_before':
                        self.deleteFlags(pipeline, inVisName, msdir=pipeline.msdir, flagname=fvers[-1])
                        fvers = fvers[:-1]
                else:
                    caracal.log.info("Before we start, save flag version 'stripe_flag_before'")
                    self.saveFlags(pipeline, inVisName, msdir=pipeline.msdir, flagname='stripe_flag_before')
            else:
                caracal.log.info("Before we start, save flag version 'stripe_flag_before'")
                self.saveFlags(pipeline, inVisName, msdir=pipeline.msdir, flagname='stripe_flag_before')

            # For lw's other than the first one, just copy the flags and skip the rest of the for loop
            if ii != 0 and stripeFlags is not None:
                self.putFlags(pipeline, inVis, inVisName, stripeFlags)
                continue

            # For the first lw, do all that follows
            caracal.log.info("Opening full MS file".format(inVisName))
            t = tables.table(inVis, readonly=True, ack=False)
            scans = t.getcol('SCAN_NUMBER')
            FlagTot = t.getcol('FLAG')
            scanNums = np.unique(scans)
            timestamps = t.getcol("TIME")
            field_id = t.getcol("FIELD_ID")
            spw = tables.table(inVis + '/SPECTRAL_WINDOW', ack=False)
            avspecchan = np.average(spw.getcol('CHAN_FREQ'))
            uvw = t.getcol("UVW")
            spw.close()
            t.close()

            percTot = np.nansum(FlagTot) / float(FlagTot.shape[0] * FlagTot.shape[1] * FlagTot.shape[2]) * 100.
            caracal.log.info("Flagged visibilites so far: {percTot:.3f} %".format(percTot=percTot))

            anttab = tables.table(inVis + "::ANTENNA", ack=False)
            ant_xyz = anttab.getcol("POSITION", 0, 1)[0]
            anttab.close()

            caracal.log.info("----------------------------------------------------")
            caracal.log.info("Imaging full MS for stripe analysis")
            outCubePrefix = galaxy + track + '_tot'
            outCubeName = self.config['flag_u_zeros']['stripeCubeDir'] + outCubePrefix + '-dirty.fits'
            if os.path.exists(outCubeName):
                os.remove(outCubeName)
            self.makeCube(pipeline, pipeline.msdir, inVisName, outCubePrefix)

            caracal.log.info("Making FFT of image")
            inFFTData, inFFTHeader = self.makeFFT(outCubeName)

            if makePlots:
                if flagCmd:
                    fig0 = plt.figure(figsize=(7.24409, 7.24409), constrained_layout=False)
                    fig0.set_tight_layout(False)
                    gs0 = gridspec.GridSpec(nrows=2, ncols=2, figure=fig0, hspace=0, wspace=0.0)
                    fig0, comvmax_tot = self.plotAll(fig0, gs0, 2, 0, outCubeName, inFFTData, inFFTHeader, galaxy, track, 0, 0, comvmax_tot, 0, type=None)
                else:
                    outPlot = "{0}{2}_tot.png".format(self.config['flag_u_zeros']['stripePlotDir'], galaxy, mfsOb)
                    fig0 = plt.figure(figsize=(7.24409, 7.24409), constrained_layout=False)
                    fig0.set_tight_layout(False)
                    gs0 = gridspec.GridSpec(nrows=1, ncols=2, figure=fig0, hspace=0, wspace=0.0)
                    fig0, comvmax_tot = self.plotAll(fig0, gs0, 1, 0, outCubeName, inFFTData, inFFTHeader, galaxy, track, 0, 0, comvmax_tot, 0, type=None)
                    fig0.subplots_adjust(left=0.05, bottom=0.05, right=0.97, top=0.97, wspace=0, hspace=0)
                    fig0.savefig(outPlot, bbox_inches='tight', dpi=200)   # save the figure to file
                    plt.close(fig0)

            caracal.log.info("----------------------------------------------------")

            caracal.log.info("Splitting scans".format(galaxy=galaxy, track=track))

            scanVisList, scanVisNames = self.splitScans(pipeline, pipeline.msdir, inVis, scanNums)

            arr = np.empty((0, 7))
            NS = len(scanNums)
            if makePlots:
                fig1 = plt.figure(figsize=(8, 21.73227), constrained_layout=False)
                fig1.set_tight_layout(False)
                fig2 = plt.figure(figsize=(8, 21.73227), constrained_layout=False)
                fig2.set_tight_layout(False)

                gs1 = gridspec.GridSpec(nrows=NS, ncols=2, figure=fig1, hspace=0, wspace=0.0)
                gs2 = gridspec.GridSpec(nrows=NS, ncols=2, figure=fig2, hspace=0, wspace=0.0)

            # Initialising the stripeFlags array, to which scans will be added one by one
            stripeFlags = np.empty(((0), FlagTot.shape[1], FlagTot.shape[2]))
            percTotAv = []

            del FlagTot
            gc.collect()

            for kk in range(len(scanNums)):

                scan = scanNums[kk]
                caracal.log.info("----------------------------------------------------")
                caracal.log.info("\tWorking on scan {}".format(str(scan)))
                visName = scanVisNames[kk]
                visAddress = scanVisList[kk]
                caracal.log.info("----------------------------------------------------")

                # Save flag version before start iterating over all thresholds
                self.saveFlags(pipeline, visName, msdir=self.config['flag_u_zeros']['stripeMSDir'], flagname='scan_flags_start')

                caracal.log.info("Imaging scan for stripe analysis".format(scanNumber=str(scan), galaxy=galaxy, track=track))
                outCubePrefix_0 = galaxy + track + '_scan' + str(scan)
                outCubeName_0 = self.config['flag_u_zeros']['stripeCubeDir'] + outCubePrefix_0 + '-dirty.fits'
                if os.path.exists(outCubeName_0):
                    os.remove(outCubeName_0)
                self.makeCube(pipeline, self.config['flag_u_zeros']['stripeMSDir'], visName, outCubePrefix_0)

                caracal.log.info("Making FFT of image")

                inFFTData, inFFTHeader = self.makeFFT(outCubeName_0)

                U = ((np.linspace(1, inFFTData.shape[1], inFFTData.shape[1]) - inFFTHeader['CRPIX1']) * inFFTHeader['CDELT1'] + inFFTHeader['CRVAL1'])
                V = ((np.linspace(1, inFFTData.shape[1], inFFTData.shape[1]) - inFFTHeader['CRPIX2'] - 1) * inFFTHeader['CDELT2'] + inFFTHeader['CRVAL2'])

                el = 0
                az = 0

                outCubePrefix = galaxy + track + '_scan' + str(scan) + '_stripeFlag'
                outCubeName = self.config['flag_u_zeros']['stripeCubeDir'] + outCubePrefix + '-dirty.fits'

                rms_thresh = []

                if len(thresholds) > 1:
                    caracal.log.info('Start iterating over all requested thresholds {} to find the optimal one'.format(thresholds))
                # iterate over all thresholds
                for threshold in thresholds:
                    if len(thresholds) > 1:
                        caracal.log.info('New iter')
                    # Rewind flags of this scan to their initial state
                    fvers = [ii.split(' :')[0] for ii in open(visAddress + '.flagversions/FLAG_VERSION_LIST').readlines()]
                    self.restoreFlags(pipeline, visName, msdir=self.config['flag_u_zeros']['stripeMSDir'], flagname='scan_flags_start')

                    while fvers[-1] != 'scan_flags_start':
                        self.deleteFlags(pipeline, visName, msdir=self.config['flag_u_zeros']['stripeMSDir'], flagname=fvers[-1])
                        fvers = fvers[:-1]

                    caracal.log.info("Computing statistics on FFT and flagging scan for threshold {0}".format(threshold))
                    # scanFlags below are the stripe flags for this scan
                    statsArray, scanFlags, percent, cutoff_scan = self.saveFFTTable(inFFTData, inFFTHeader, visAddress, np.flip(U), V, galaxy, mfsOb, track, scan, el, az, method, threshold, dilateU, dilateV, makePlots)
                    caracal.log.info("Scan flags from stripe-flagging: {percent:.3f}%".format(percent=percent))
                    caracal.log.info("Making post-flagging image")

                    if os.path.exists(outCubeName):
                        os.remove(outCubeName)
                    self.makeCube(pipeline, self.config['flag_u_zeros']['stripeMSDir'], visName, outCubePrefix)
                    fitsdata = fits.open(outCubeName)
                    rms_thresh.append(np.std(fitsdata[0].data[0, 0]))
                    caracal.log.info("Image noise = {0:.3e} Jy/beam".format(rms_thresh[-1]))
                    fitsdata.close()

                # Select best threshold (minimum noise), re-flag and re-image
                if len(thresholds) > 1:
                    caracal.log.info('Done iterating over all requested thresholds')
                    threshold = thresholds[rms_thresh.index(min(rms_thresh))]
                    caracal.log.info('\tThe threshold that minimises the image noise is {}'.format(threshold))
                    caracal.log.info('Repeating flagging and imaging steps with the selected threshold (yes, the must be a better way...)')
                    # Rewind flags of this scan to their initial state
                    fvers = [ii.split(' :')[0] for ii in open(visAddress + '.flagversions/FLAG_VERSION_LIST').readlines()]
                    self.restoreFlags(pipeline, visName, msdir=self.config['flag_u_zeros']['stripeMSDir'], flagname='scan_flags_start')

                    while fvers[-1] != 'scan_flags_start':
                        self.deleteFlags(pipeline, visName, msdir=self.config['flag_u_zeros']['stripeMSDir'], flagname=fvers[-1])

                        fvers = fvers[:-1]
                    # Re-flag with selected threshold
                    caracal.log.info("Computing statistics on FFT and flagging scan for threshold {0}".format(threshold))
                    statsArray, scanFlags, percent, cutoff_scan = self.saveFFTTable(inFFTData, inFFTHeader, visAddress, np.flip(U), V, galaxy, mfsOb, track, scan, el, az, method, threshold, dilateU, dilateV, makePlots)
                    caracal.log.info("Scan flags from stripe-flagging: {percent:.3f}%".format(percent=percent))
                    # Re-image
                    caracal.log.info("Making post-flagging image")
                    if os.path.exists(outCubeName):
                        os.remove(outCubeName)
                    self.makeCube(pipeline, self.config['flag_u_zeros']['stripeMSDir'], visName, outCubePrefix)

                # Save stats for the selected threshold
                arr = np.vstack((arr, statsArray))
                percTotAv.append(percent)

                # Add the stripe flags of this scan to the stripe flags of all the scans done previously
                stripeFlags = np.concatenate([stripeFlags, scanFlags])

                if makePlots:
                    fig1, comvmax_scan = self.plotAll(fig1, gs1, NS, kk, outCubeName_0, inFFTData, inFFTHeader, galaxy, track, scan, None, comvmax_scan, cutoff_scan, type=None)

                caracal.log.info("Making FFT of post-flagging image")

                inFFTData, inFFTHeader = self.makeFFT(outCubeName)

                if makePlots:
                    fig2, comvmax_scan = self.plotAll(fig2, gs2, NS, kk, outCubeName, inFFTData, inFFTHeader, galaxy, track, scan, percent, comvmax_scan, 0, type='postFlag')

            if makePlots:
                caracal.log.info("----------------------------------------------------")
                caracal.log.info("Saving scans diagnostic plots")
                outPlot = "{0}{2}_perscan_preFlag.png".format(self.config['flag_u_zeros']['stripePlotDir'], galaxy, mfsOb)
                outPlotFlag = "{0}{2}_perscan_postFlag.png".format(self.config['flag_u_zeros']['stripePlotDir'], galaxy, mfsOb)

                fig1.subplots_adjust(left=0.05, bottom=0.05, right=0.97, top=0.97, wspace=0, hspace=0)
                fig1.savefig(outPlot, bbox_inches='tight', dpi=200)   # save the figure to file
                plt.close(fig1)
                fig2.subplots_adjust(left=0.05, bottom=0.05, right=0.97, top=0.97, wspace=0, hspace=0)
                fig2.savefig(outPlotFlag, bbox_inches='tight', dpi=200)   # save the figure to file
                plt.close(fig2)

            superArr = np.vstack((superArr, arr))
            caracal.log.info("Saving stats table")
            newtab = Table(names=['galaxy', 'track', 'scan', 'perc', 'cutoff', 'el', 'az'], data=(superArr))
            outTablePercent = "{tableDir}stats_{galaxy}{track}.ecsv".format(tableDir=self.config['flag_u_zeros']['stripeTableDir'], galaxy=galaxy, track=track)
            astasc.write(newtab, outTablePercent, overwrite=True, format='ecsv')

            if flagCmd:
                caracal.log.info("====================================================")
                caracal.log.info("\tWorking on {}".format(inVisName))
                caracal.log.info("====================================================")
                self.putFlags(pipeline, inVis, inVisName, stripeFlags)
                caracal.log.info("Making post-flagging image")

                outCubePrefix = galaxy + track + '_tot_stripeFlag'
                outCubeName = self.config['flag_u_zeros']['stripeCubeDir'] + outCubePrefix + '-dirty.fits'

                if os.path.exists(outCubeName):
                    os.remove(outCubeName)
                self.makeCube(pipeline, pipeline.msdir, inVisName, outCubePrefix)

                caracal.log.info("Making FFT of post-flagging image")

                inFFTData, inFFTHeader = self.makeFFT(outCubeName)

                U = ((np.linspace(1, inFFTData.shape[1], inFFTData.shape[1]) - inFFTHeader['CRPIX1']) * inFFTHeader['CDELT1'] + inFFTHeader['CRVAL1'])
                V = ((np.linspace(1, inFFTData.shape[1], inFFTData.shape[1]) - inFFTHeader['CRPIX2'] - 1) * inFFTHeader['CDELT2'] + inFFTHeader['CRVAL2'])

                caracal.log.info("Saving total stripe flagging diagnostic plots".format(galaxy=galaxy, track=track))

                percTotAfter = np.nansum(stripeFlags) / float(stripeFlags.shape[0] * stripeFlags.shape[1] * stripeFlags.shape[2]) * 100.
                caracal.log.info("Total stripe flags: {percent:.3f} %".format(percent=percTotAfter))
                percRel = percTotAfter - percTot

                caracal.log.info("----------------------------------------------------")

                caracal.log.info("Mean stripe flagging per scan: {percent:.3f}%".format(percent=np.nanmean(percTotAv)))

                if makePlots:
                    caracal.log.info("----------------------------------------------------")
                    caracal.log.info("----------------------Plotting----------------------")

                    outPlot = "{0}{2}_fullMS_prepostFlag.png".format(self.config['flag_u_zeros']['stripePlotDir'], galaxy, mfsOb)
                    fig0, comvmax_tot = self.plotAll(fig0, gs0, 2, 1, outCubeName, inFFTData, inFFTHeader, galaxy, track, 0, np.nanmean(percTotAv), comvmax_tot, 0, type='postFlag')
                    fig0.subplots_adjust(left=0.05, bottom=0.05, right=0.97, top=0.97, wspace=0, hspace=0)
                    fig0.savefig(outPlot, bbox_inches='tight', dpi=200)   # save the figure to file
                    plt.close(fig0)

                timeFlag = (time.time() - timeInit) / 60.

        if doCleanUp is True:
            self.cleanUp(galaxy)

        return timeFlag

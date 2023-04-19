######################
### IMPORT MODULES ###
######################

import numpy as np
import pyrap.tables as tables
import sys
import os
import caracal


########################
### DEFINE FUNCTIONS ###
########################

# Get Tsys/eff (possibly from file)
def GetTsyseff(tsyseff):
    if os.path.exists(tsyseff):
        caracal.log.info(' ( Tsys/eff from file {0:s} )'.format(tsyseff))
        tsyseffFile, tsyseff = tsyseff, np.loadtxt(tsyseff)
    else:
        try:
            tsyseff = float(tsyseff)
        except ValueError:
            caracal.log.info('')
            caracal.log.info(' CATASTROPHE!')
            caracal.log.info(' You set Tsys/eff = {0:s}'.format(tsyseff))
            caracal.log.info(' This is either a file that cannot be found or a value that cannot be converted to float')
            caracal.log.info(' Correct any mistakes and try again')
            caracal.log.info(' Aborting ...')
            sys.exit()
        tsyseffFile = None
    return tsyseffFile, tsyseff

# Interpolate input Tsys/eff table to observed frequencies


def InterpolateTsyseff(tsyseff, chans):
    caracal.log.info('Interpolating Tsys/eff table to observed frequencies ...')
    return np.interp(np.ravel(chans), tsyseff[:, 0], tsyseff[:, 1])

# Get single-MS flags, intervals, channel widths, channel frequencies and calculate natural rms (ignoring flags)


def ProcessSingleMS(ms, kB, tsyseff, tsyseffFile, Aant, selectFieldName, verbose=0):
    if verbose > 1:
        caracal.log.info('    Processing MS file {0:s}'.format(ms))
    t = tables.table(ms, ack=False)
    fieldIDs = t.getcol('FIELD_ID')
    ant1 = t.getcol('ANTENNA1')
    ant2 = t.getcol('ANTENNA2')
    fieldNames = tables.table(ms + '/FIELD', ack=False).getcol('NAME')
    spw = tables.table(ms + '/SPECTRAL_WINDOW', ack=False)
    channelWidths = spw.getcol('CHAN_WIDTH')
    channelFreqs = spw.getcol('CHAN_FREQ')
    stokesdef = 'Undefined,I,Q,U,V,RR,RL,LR,LL,XX,XY,YX,YY,RX,RY,LX,LY,XR,XL,YR,YL,PP,PQ,QP,QQ,RCircular,LCircular,Linear,Ptotal,Plinear,PFtotal,PFlinear,Pangle'.split(',')
    corrs = [stokesdef[cc] for cc in tables.table(ms + '/POLARIZATION', ack=False).getcol('CORR_TYPE')[0]]  # taking the correlations of the first SPW

    if selectFieldName:
        try:
            selectFieldID = fieldNames.index(selectFieldName)
        except ValueError:
            caracal.log.info(' CATASTROPHE!')
            caracal.log.info(' Cannot find the field you want to process, {0:s}'.format(selectFieldName))
            caracal.log.info(' Available fields are {0:}'.format(fieldNames))
            caracal.log.info(' Aborting ...')
            sys.exit()
        if verbose > 1:
            caracal.log.info('      Successfully selected Field with name {0:s} (Field ID = {1:d})'.format(selectFieldName, selectFieldID))
        selection = fieldIDs == selectFieldID
    else:
        if verbose > 1:
            caracal.log.info('      Will process all available fields: {0:}'.format(fieldNames))
        selection = fieldIDs >= fieldIDs.min()

    autoCorr = ant1 == ant2
    if verbose > 1:
        if autoCorr.sum():
            caracal.log.info('      Successfully selected crosscorrelations only')
        else:
            caracal.log.info('      Found crosscorrelations only')
    selection *= ant1 != ant2
    nrAnt = np.unique(np.concatenate((ant1, ant2))).shape[0]
    nrBaseline = nrAnt * (nrAnt - 1) // 2
    if verbose > 1:
        caracal.log.info('      Number of antennas  = {0:d}'.format(nrAnt))
        caracal.log.info('      Number of baselines = {0:d}'.format(nrBaseline))
        caracal.log.info('      Frequency coverage  = {0:.5e} Hz - {1:.5e} Hz'.format(channelFreqs.min(), channelFreqs.max()))
        if np.unique(channelWidths).shape[0] == 1:
            caracal.log.info('      Channel width = {0:.5e} Hz'.format(np.unique(channelWidths)[0]))
        else:
            caracal.log.info('      The channel width takes the following unique values: {0:} Hz'.format(np.unique(channelWidths)))

    if verbose > 1:
        caracal.log.info('      Loading flags and intervals ...')
    flag = t.getcol('FLAG')[selection]          # flagged data have flag = True
    # select Stokes I-related corrs
    cc = 0
    while cc < len(corrs):
        if corrs[cc] not in 'I,RR,LL,XX,YY,'.split(','):
            if verbose > 1:
                caracal.log.info('      Discarding correlation {0:s} for predicting the Stokes I noise'.format(corrs[cc]))
            flag = np.delete(flag, cc, axis=2)
            del (corrs[cc])
        else:
            cc += 1
    if verbose > 1:
        caracal.log.info('      Retained correlations {0:}'.format(corrs))
    interval = t.getcol('INTERVAL')[selection]
    if verbose > 1:
        if np.unique(interval).shape[0] == 1:
            caracal.log.info('      Interval = {0:.5e} sec'.format(np.unique(interval)[0]))
        else:
            caracal.log.info('      The interval takes the following unique values: {0:} sec'.format(np.unique(interval)))
    t.close()

    if verbose > 1:
        caracal.log.info('      The *flag* array has shape (Nr_integrations, Nr_channels, Nr_polarisations) = {0:}'.format(flag.shape))
        caracal.log.info('      The *interval* array has shape (Nr_integrations) = {0:}'.format(interval.shape))
        caracal.log.info('      The *channel* width array has shape (-, Nr_channels) = {0:}'.format(channelWidths.shape))

    if verbose > 1:
        caracal.log.info('      Total Integration on selected field(s) = {0:.2f} h ({1:d} polarisations)'.format(interval.sum() / nrBaseline / 3600, flag.shape[2]))
    if tsyseffFile is not None:
        rms = np.sqrt(2) * kB * InterpolateTsyseff(tsyseff, channelFreqs) / Aant / np.sqrt(channelWidths * interval.sum() * flag.shape[2])
    else:
        rms = np.sqrt(2) * kB * tsyseff / Aant / np.sqrt(channelWidths * interval.sum() * flag.shape[2])
    if len(rms.shape) == 2 and rms.shape[0] == 1:
        rms = rms[0]

    if verbose > 1:
        caracal.log.info('      SINGLE MS median natural noise ignoring flags = {0:.3e} Jy/beam'.format(np.nanmedian(rms)))

    return flag, interval, channelWidths, channelFreqs, rms


# Predict natural rms for an arbitrary number of MS files (both ignoring and applying flags)
def PredictNoise(MS, tsyseff, diam, selectFieldName, verbose=0):

    # Get Tsys/eff either from table (col1 = frequency, col2 = Tsys/eff) or as a float values (frequency independent Tsys/eff value)
    tsyseffFile, tsyseff = GetTsyseff(tsyseff)

    # Derive quantities
    kB = 1380.6                                   # Boltzmann constant (Jy m^2 / K)
    Aant = np.pi * (diam / 2)**2                      # collecting area of 1 antenna (m^2)
    if tsyseffFile is None:
        SEFD = 2 * kB * tsyseff / Aant                  # frequency independent system equivalent flux density (Jy)
    else:
        SEFD = 2 * kB * np.median(tsyseff[:, 1]) / Aant  # median system equivalent flux density (Jy)

    # Read MS files to get the flags and calculate single-MS natural rms values (ignoring flags)

    # Start with first file ...
    flag0, interval0, channelWidths0, channelFreqs0, rms0 = ProcessSingleMS(MS[0], kB, tsyseff, tsyseffFile, Aant, selectFieldName, verbose=verbose)
    rmsAll = [rms0]

    # ... and do the same for all other MS's appending to the flag array, checking that the channelisation is the same
    for ii in range(1, len(MS)):
        flagi, intervali, channelWidthsi, channelFreqsi, rmsi = ProcessSingleMS(MS[ii], kB, tsyseff, tsyseffFile, Aant, selectFieldName, verbose=verbose)

        if channelWidths0.shape != channelWidthsi.shape or (channelWidths0 != channelWidthsi).sum() or (channelFreqs0 != channelFreqsi).sum():
            caracal.log.info('')
            caracal.log.info(' CATASTROPHE!')
            caracal.log.info(' The input .MS file {1:s} has different channelization than the first input .MS file {2:s}'.format(ii, MS[ii], MS[0]))
            caracal.log.info(' Cannot combine files to estimate their joint theoretical noise')
            caracal.log.info(' Aborting ...')
            sys.exit()
        else:
            flag0 = np.concatenate((flag0, flagi), axis=0)
            interval0 = np.concatenate((interval0, intervali), axis=0)
            rmsAll.append(rmsi)

    # Message concatenated files
    if verbose > 1 and len(MS) > 1:
        caracal.log.info('    Concatenating all {0:d} MS files ...'.format(len(MS)))
        caracal.log.info('      The concatenated *flag* array has shape (Nr_integrations, Nr_channels, Nr_polarisations) = {0:}'.format(flag0.shape))
        caracal.log.info('      The concatenated *interval* array has shape (Nr_integrations) = {0:}'.format(interval0.shape))
        caracal.log.info('      The concatenated *channel* width array has shape (-, Nr_channels) = {0:}'.format(channelWidths0.shape))

    # Reshape arrays
    if verbose > 1 and len(MS) > 1:
        caracal.log.info('      Reshaping arrays ...')
    interval0.resize((interval0.shape[0], 1, 1))
    channelWidths0.resize((channelWidths0.shape[1]))
    channelFreqs0.resize((channelFreqs0.shape[1]))

    # Interpolate Tsys
    if tsyseffFile is not None:
        tsyseff = InterpolateTsyseff(tsyseff, channelFreqs0)

    # Calculate theoretical natural rms
    rmsAll = np.array(rmsAll)
    rmsAll = 1. / np.sqrt((1. / rmsAll**2).sum(axis=0))
    unflaggedIntegration = (interval0 * (1 - flag0.astype(int))).sum(axis=(0, 2))  # total integration per channel adding up all UNFLAGGED integrations and polarisations (sec)
    unflaggedIntegration[unflaggedIntegration == 0] = np.nan
    rmsUnflagged = np.sqrt(2) * kB * tsyseff / Aant / np.sqrt(channelWidths0 * unflaggedIntegration)

    if verbose >= 1:
        caracal.log.info('    Natural noise ignoring flags: median = {0:.3e} Jy/beam, range = ({1:.3e} - {2:.3e}) Jy/beam'.format(np.nanmedian(rmsAll), np.nanmin(rmsAll), np.nanmax(rmsAll)))
        if not (~np.isnan(unflaggedIntegration)).sum():
            caracal.log.info('')
            caracal.log.info('    Natural noise applying flags: N/A, all data are flagged!')
        else:
            caracal.log.info('    Natural noise applying flags: median = {0:.3e} Jy/beam, range = ({1:.3e} - {2:.3e}) Jy/beam'.format(np.nanmedian(rmsUnflagged), np.nanmin(rmsUnflagged), np.nanmax(rmsUnflagged)))

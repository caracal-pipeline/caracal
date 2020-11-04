######################
### IMPORT MODULES ###
######################

import numpy as np
import pyrap.tables as tables
import sys,os
import caracal


########################
### DEFINE FUNCTIONS ###
########################

### Get Tsys/eff (possibly from file)
def GetTsyseff(tsyseff):
    if os.path.exists(tsyseff):
        caracal.log.info(' ( Tsys/eff from file {0:s} )'.format(tsyseff))
        tsyseffFile,tsyseff=tsyseff,np.loadtxt(tsyseff)
    else:
        try: tsyseff=float(tsyseff)
        except ValueError:
            caracal.log.info('')
            caracal.log.info(' CATASTROPHE!')
            caracal.log.info(' You set Tsys/eff = {0:s}'.format(tsyseff))
            caracal.log.info(' This is either a file that cannot be found or a value that cannot be converted to float')
            caracal.log.info(' Correct any mistakes and try again')
            caracal.log.info(' Aborting ...')
            sys.exit()
        tsyseffFile=None
    return tsyseffFile,tsyseff

### Interpolate input Tsys/eff table to observed frequencies
def InterpolateTsyseff(tsyseff,chans):
    caracal.log.info('Interpolating Tsys/eff table to observed frequencies ...')
    return np.interp(np.ravel(chans),tsyseff[:,0],tsyseff[:,1])

### Get single-MS flags, intervals, channel widths, channel frequencies and calculate natural rms (ignoring flags)
def ProcessSingleMS(ms,kB,tsyseff,tsyseffFile,Aant,selectFieldName,verbose=0):
    if verbose>1:
        caracal.log.info('--- Working on file {0:s} ---'.format(ms))
    t=tables.table(ms,ack=False)
    fieldIDs=t.getcol('FIELD_ID')
    ant1=t.getcol('ANTENNA1')
    ant2=t.getcol('ANTENNA2')
    fieldNames=tables.table(ms+'/FIELD',ack=False).getcol('NAME')
    spw=tables.table(ms+'/SPECTRAL_WINDOW',ack=False)
    channelWidths=spw.getcol('CHAN_WIDTH')
    channelFreqs=spw.getcol('CHAN_FREQ')

    if selectFieldName:
        try:
            selectFieldID=fieldNames.index(selectFieldName)
        except ValueError:
            caracal.log.info(' CATASTROPHE!')
            caracal.log.info(' Cannot find the field you want to process, {0:s}'.format(selectFieldName))
            caracal.log.info(' Available fields are {0:}'.format(fieldNames))
            caracal.log.info(' Aborting ...')
            sys.exit()
        if verbose>1:
            caracal.log.info('Successfully selected Field with name {0:s} (Field ID = {1:d})'.format(selectFieldName,selectFieldID))
        selection=fieldIDs==selectFieldID
    else:
        if verbose>1:
            caracal.log.info('Will process all available fields: {0:}'.format(fieldNames))
        selection=fieldIDs>=fieldIDs.min()

    autoCorr=ant1==ant2
    if verbose>1:
        if autoCorr.sum(): caracal.log.info('Successfully selected crosscorrelations only')
        else: caracal.log.info('Found crosscorrelations only')
    selection*=ant1!=ant2
    nrAnt=np.unique(np.concatenate((ant1,ant2))).shape[0]
    nrBaseline=nrAnt*(nrAnt-1)//2
    if verbose>1:
        caracal.log.info('Number of antennas  = {0:d}'.format(nrAnt))
        caracal.log.info('Number of baselines = {0:d}'.format(nrBaseline))
        caracal.log.info('Frequency coverage  = {0:.5e} Hz - {1:.5e} Hz'.format(channelFreqs.min(),channelFreqs.max()))
        if np.unique(channelWidths).shape[0]==1: caracal.log.info('Channel width = {0:.5e} Hz'.format(np.unique(channelWidths)[0]))
        else: caracal.log.info('The channel width takes the following unique values: {0:} Hz'.format(np.unique(channelWidths)))

    if verbose>1:
        caracal.log.info('Loading flags and intervals ...')
    flag=t.getcol('FLAG')[selection]          # flagged data have flag = True
    interval=t.getcol('INTERVAL')[selection]
    if verbose>1:
        if np.unique(interval).shape[0]==1: caracal.log.info('Interval = {0:.5e} sec'.format(np.unique(interval)[0]))
        else: caracal.log.info('The interval takes the following unique values: {0:} sec'.format(np.unique(interval)))
    t.close()

    if verbose>1:
        caracal.log.info('The *flag* array has shape (Nr_integrations, Nr_channels, Nr_polarisations) = {0:}'.format(flag.shape))
        caracal.log.info('The *interval* array has shape (Nr_integrations) = {0:}'.format(interval.shape))
        caracal.log.info('The *channel* width array has shape (-, Nr_channels) = {0:}'.format(channelWidths.shape))

    if verbose>1:
        caracal.log.info('Total Integration on selected field(s) = {0:.2f} h ({1:d} polarisations)'.format(interval.sum()/nrBaseline/3600,flag.shape[2]))
    if tsyseffFile!=None:
        rms=np.sqrt(2)*kB*InterpolateTsyseff(tsyseff,channelFreqs)/Aant/np.sqrt(channelWidths*interval.sum()*flag.shape[2])
    else:
        rms=np.sqrt(2)*kB*tsyseff/Aant/np.sqrt(channelWidths*interval.sum()*flag.shape[2])
    if len(rms.shape)==2 and rms.shape[0]==1: rms=rms[0]

    if verbose>1:
        caracal.log.info('The Stokes I theoretical natural rms ignoring flags has median and range:    *** {0:.3e} Jy/b, ({1:.3e} - {2:.3e}) Jy/b ***'.format(np.nanmedian(rms),np.nanmin(rms),np.nanmax(rms)))

    return flag,interval,channelWidths,channelFreqs,rms



### Predict natural rms for an arbitrary number of MS files (both ignoring and applying flags)
def PredictNoise(MS,tsyseff,diam,selectFieldName,verbose=0):

    # Get Tsys/eff either from table (col1 = frequency, col2 = Tsys/eff) or as a float values (frequency independent Tsys/eff value)
    tsyseffFile,tsyseff=GetTsyseff(tsyseff)

    # Derive quantities
    kB=1380.6                                   # Boltzmann constant (Jy m^2 / K)
    Aant=np.pi*(diam/2)**2                      # collecting area of 1 antenna (m^2)
    if tsyseffFile==None:
        SEFD=2*kB*tsyseff/Aant                  # frequency independent system equivalent flux density (Jy)
    else:
        SEFD=2*kB*np.median(tsyseff[:,1])/Aant  # median system equivalent flux density (Jy)

    # Print assumptions
    if verbose>1:
        caracal.log.info('--- Assumptions ---')
        if tsyseffFile==None:
            caracal.log.info('  Tsys/efficiency      = {0:.1f} K (frequency independent)'.format(tsyseff))
        else:
            caracal.log.info('  Tsys/efficiency      = ({0:.1f} - {1:.1f}) K (range over input table {2:s})'.format(tsyseff[:,1].min(),tsyseff[:,1].max(),tsyseffFile))
            caracal.log.info('                         (frequency range = ({0:.3e} - {1:.3e}) Hz'.format(tsyseff[:,0].min(),tsyseff[:,0].max()))
        caracal.log.info('  Dish diameter        = {0:.1f} m'.format(diam))
        if tsyseffFile==None: caracal.log.info('    and therefore SEFD = {0:.1f} Jy'.format(SEFD))
        else: caracal.log.info('    and therefore SEFD = {0:.1f} Jy (median over frequency)'.format(SEFD))

    # Read MS files to get the flags and calculate single-MS natural rms values (ignoring flags)

    # Start with first file ...
    flag0,interval0,channelWidths0,channelFreqs0,rms0=ProcessSingleMS(MS[0],kB,tsyseff,tsyseffFile,Aant,selectFieldName,verbose=verbose)
    rmsAll=[rms0]

    # ... and do the same for all other MS's appending to the flag array, checking that the channelisation is the same
    for ii in range(1,len(MS)):
        flagi,intervali,channelWidthsi,channelFreqsi,rmsi=ProcessSingleMS(MS[ii],kB,tsyseff,tsyseffFile,Aant,selectFieldName)

        if channelWidths0.shape!=channelWidthsi.shape or (channelWidths0!=channelWidthsi).sum() or (channelFreqs0!=channelFreqsi).sum():
            caracal.log.info('')
            caracal.log.info(' CATASTROPHE!')
            caracal.log.info(' The input .MS file {1:s} has different channelization than the first input .MS file {2:s}'.format(ii,MS[ii],MS[0]))
            caracal.log.info(' Cannot combine files to estimate their joint theoretical noise')
            caracal.log.info(' Aborting ...')
            sys.exit()
        else:
            flag0=np.concatenate((flag0,flagi),axis=0)
            interval0=np.concatenate((interval0,intervali),axis=0)
            rmsAll.append(rmsi)

    # Message concatenated files
    if verbose>1:
        caracal.log.info('--- All input tables concatenated ---')
        caracal.log.info('The concatenated *flag* array has shape (Nr_integrations, Nr_channels, Nr_polarisations) = {0:}'.format(flag0.shape))
        caracal.log.info('The concatenated *interval* array has shape (Nr_integrations) = {0:}'.format(interval0.shape))
        caracal.log.info('The concatenated *channel* width array has shape (-, Nr_channels) = {0:}'.format(channelWidths0.shape))
        caracal.log.info('')
        caracal.log.info('--- Result ---')

    # Reshape arrays
    if verbose>1:
        caracal.log.info('Reshaping arrays ...')
    interval0.resize((interval0.shape[0],1,1))
    channelWidths0.resize((channelWidths0.shape[1]))
    channelFreqs0.resize((channelFreqs0.shape[1]))

    # Interpolate Tsys
    if tsyseffFile!=None:
       tsyseff=InterpolateTsyseff(tsyseff,channelFreqs0)

    # Calculate theoretical natural rms
    if verbose>1:
        caracal.log.info('Calculating natural rms of all .MS files combined (with and without flags)...')
    rmsAll=np.array(rmsAll)
    rmsAll=1./np.sqrt( (1./rmsAll**2).sum(axis=0) )
    unflaggedIntegration=(interval0*(1-flag0.astype(int))).sum(axis=(0,2)) # total integration per channel adding up all UNFLAGGED integrations and polarisations (sec)
    unflaggedIntegration[unflaggedIntegration==0]=np.nan
    rmsUnflagged=np.sqrt(2)*kB*tsyseff/Aant/np.sqrt(channelWidths0*unflaggedIntegration)

    if verbose>=1:
        caracal.log.info('    ignoring flags: median = {0:.3e} Jy/beam, range = ({1:.3e} - {2:.3e}) Jy/beam'.format(np.nanmedian(rmsAll),np.nanmin(rmsAll),np.nanmax(rmsAll)))
        if not (~np.isnan(unflaggedIntegration)).sum():
            caracal.log.info('')
            caracal.log.info('    applying flags: N/A, all data are flagged!')
        else:  caracal.log.info('    applying flags: median = {0:.3e} Jy/beam, range = ({1:.3e} - {2:.3e}) Jy/beam'.format(np.nanmedian(rmsUnflagged),np.nanmin(rmsUnflagged),np.nanmax(rmsUnflagged)))

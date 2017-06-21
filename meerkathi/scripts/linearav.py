#!/usr/bin/env python
import numpy as np
import pyrap.tables as tables
import matplotlib.pyplot as plt
import scipy.optimize as opt
import sys

def gaussian(x, cent, amp, sigma):
    return amp*np.exp(-0.5*np.power((x-cent)/sigma,2))

def histoclip(data, mask, threshmode = 'fit', threshold = 5., show = False):
    """
    Measure sigma and return a mask indicating data at a distance larger than threshold sigma from the average 
    
    Input:
    data (ndarray, type = float): Input data, three dimensions, datapoint, frequency, polarization
    mask (ndarray, type = bool) : Mask indicating data points to ignore, same shape as data
    threshmode (string)         : Method to determine sigma, 'fit': fit Gaussian at the max to determine sigma, standard deviation otherwise
    threshold (float)           : Distance from average beyond which data are flagged in units of sigma
    show (bool)                 : Show histogram to monitor what is happening
    
    Output:
    histoclip (ndarray, type = bool): Mask indicating points with a distance of larger than threshold*sigma from average (or peak position)

    Calculates amplitude of all points in data and average along the
    frequency axis. For these data calculate average and standard
    deviation (rms) of all points in data whose indices are not
    flagged as True in mask. If mode == 'fit' create a histogram and
    attempt to fit a Gaussian, replacing the rms with the fitted
    dispersion and the average with the centre of the Gaussian. Then
    create an output mask histoclip, flagging all indices of data
    points in data, which are offset by more than threshold times the
    sigma (rms or fitted dispersion) from the mean (average or fitted
    centre of the Gaussian) as True, and expanding along the frequency
    axis (i.e. all data are flagged at a certain frequency).

    """
    global thefigure
    global theaxis
    
    # Calculate absolute, i.e. amplitude

    # This is somehow shit, as it calculates 
    ampar = np.copy(data)
    ampar[mask==True] = np.nan

    # Average data, then look for shape
    av = np.nanmean(ampar,axis=1)
    npoints = av[np.isfinite(av)].size

    # Find average and standard deviation 
    average = np.nanmean(av)
    stdev  = np.nanstd(av)

    if average == np.nan:
        return
    if stdev == np.nan:
        return
    
    if threshmode == 'fit' or show == True:
        # Build a histogram
        hist, bin_edges = np.histogram(av[np.isfinite(av)], bins=npoints/200+1)
        bin_centers = bin_edges[:-1] + 0.5 * (bin_edges[1:] - bin_edges[:-1])
        widthes = bin_edges[1:] - bin_edges[:-1]

        # Find maximum in histogram 
        maxhi = np.amax(hist)
        maxhiposval = bin_centers[np.argmax(hist)]
        
        # Fit a Gaussian
        try:
            popt, pcov = opt.curve_fit(gaussian, bin_centers, hist, p0 = [maxhiposval, maxhi, stdev/2.])
            if threshmode == 'fit':
            # Do this only if threshmode is fit
                average = popt[0]
                stdev = popt[2]
        except:
            popt = np.array([average, widthes[0]*npoints/(np.sqrt(2*np.pi)*stdev), stdev])
            pass
 
    # Build a new mask based on the statistics and return it
    select = av <= average-threshold*stdev
    select |= av >= average+threshold*stdev

    # Plot histogram and Gaussians
    if show == True:
        calculated = gaussian(bin_centers, average, widthes[0]*npoints/(np.sqrt(2*np.pi)*stdev), stdev)
        # In case of using only stats, this is right on top
        fitted = gaussian(bin_centers, popt[0], popt[1], popt[2])
        plt.close()
        plt.bar(bin_centers, hist, width=widthes, color = 'y', edgecolor = 'y')
        plt.plot(bin_centers, calculated, 'g-')
        plt.plot(bin_centers, fitted, 'r-')
        plt.axvline(x = average-threshold*stdev, linewidth=2, color='k')
        plt.axvline(x = average+threshold*stdev, linewidth=2, color='k')
        plt.xlim(min(bin_edges), max(bin_edges))
#        plt.show(block = False)
        plt.show()
         
    retmask = np.repeat(select,mask.shape[1],0).reshape(mask.shape)
    # or select = select.reshape(mask.shape[0],1,mask.shape[2])
    #    retmask = select*np.ones(mask.shape)
    return retmask

def flaglinav(inset, outset = None, col = 'DATA', fields = None, mode = 'all', pol = 'parallel', threshmode = 'fit', threshold = 5., show = False):
    """
    Flag based on scalarly averaged data

    Input:
    inset (str)       : Input data set
    outset (str)      : Name of output data set or None, in which case outset = inset
    col (str)         : Column to base flagging on
    fields = (int)    : Fields to select or None if all fields should be used 
    mode (str)        : Flagging based on 'all' data, repeated per 'antenna', or repeated per 'baseline'
    pol (str)         : Polarization selection, 'all' cross-and parallel-handed polarizations, only 'parallel'-handed, Stokes 'i', or Stokes 'q' 
    threshmode (str)  : Method to determine sigma, 'fit': fit Gaussian at the max to determine sigma, standard deviation otherwise
    threshold (float) : Distance from average beyond which data are flagged in units of sigma
    show (bool)       : Show histogram and cutoff line in a viewgraph

    Calculates amplitude of selected and converted points in a number
    of subsets of inset and averages along the frequency axis. The
    points are converted to polarisation products or Stokes parameters
    according to the parameter 'pol'. The selection is all baselines
    in one go, all antennas in one go (repeated for each antenna), all
    baselines (repeated per baseline). For these data calculate
    average and standard deviation (rms) of all points in data whose
    indices are not flagged as True in mask. If mode == 'fit' create a
    histogram and attempt to fit a Gaussian, replacing the rms with
    the fitted dispersion and the average with the centre of the
    Gaussian. Then create an output mask histoclip, flagging all
    indices of data points in data, which are offset by more than
    threshold times the sigma (rms or fitted dispersion) from the mean
    (average or fitted centre of the Gaussian) as True, and expanding
    along the frequency axis (i.e. all data are flagged at a certain
    frequency).

    """
    # Open data set as table
    t = tables.table(inset)
    if outset == None:
        tout = t
    else:
        if tables.tableexists(outset):
            tout = tables.table(outset, readonly=False)
        else:
            tout = t.copy(outset)
            tout.close()
            tout = tables.table(inset, readonly=False)

    # Read column (think, axes are by default ordered as time, frequency, polarization) and flags, which should have same dimension
    data = t.getcol(col)

    flags = t.getcol('FLAG')

    # uvw = t.getcol('UVW')/0.21
    
    # Convert into desired stokes parameters and adjust mask
    
    # If stokes is parallel, flag all that's not parallel
    if pol == 'all':
        pass
    else:
        if data.shape[2] == 4:
            flags[:,:,1:3] = True
            
        i = data.shape[2] - 1
        stflags = np.logical_not(flags[:,:,0]).astype(int) + np.logical_not(flags[:,:,i]).astype(int)

        if pol == 'i':
            # Calculate stokes i, put it in the first row, flag the rest
            with np.errstate(divide='ignore', invalid = 'ignore'):
                data[:,:,0] = (data[:,:,0]*np.logical_not(flags)[:,:,0]+data[:,:,i]*np.logical_not(flags)[:,:,i])/float(stflags)
        elif pol == 'q':
            # Calculate stokes i, put it in the first row, flag the rest
            with np.errstate(divide='ignore', invalid = 'ignore'):
                data[:,:,0] = (data[:,:,0]*np.logical_not(flags)[:,:,0]-data[:,:,i]*np.logical_not(flags)[:,:,i])/float(stflags)
#        if pol != 'parallel':
#            data[:,:,i] = data[:,:,0]

        flags[:,:,0] = np.logical_not((stflags.astype(bool)))

        
    # Also mask anything not listed in fields
    if fields != None:
        field = t.getcol('FIELD')
        select = np.ones(field.shape, dtype = bool)
        if type(fields) == list:
            for i in fields:
                select &= field == i
        else:
            select &= field == fields
            
        flags[select,:,:] = True

    # Calculate absolute values once
    data = np.absolute(data)

    # Now refine the mask, depending on the mode
    
    if mode == 'all':
        print 'Mode \'all\': filtering all data at once.'
        newflags = histoclip(data, flags, threshold = threshold, show = show)
    else:
        newflags = np.zeros(flags.shape, dtype = bool)
        antenna1 = t.getcol('ANTENNA1')
        antenna2 = t.getcol('ANTENNA2')
        if mode == 'antenna':
            antennas = np.unique(np.append(antenna1, antenna2))
            for antenna in antennas:
                print 'Filtering antenna %i' % antenna
                passedflags = np.copy(flags)
                select  = antenna1 != antenna
                select &= antenna2 != antenna
                passedflags[select,:,:] = True
                newflags |= histoclip(data, passedflags, threshold = threshold, show = show)
        else:
            antennas1 = np.unique(antenna1)
            antennas2 = np.unique(antenna2)
            for ant1 in antennas1:
                for ant2 in antennas2:
                    print 'Filtering baseline between antenna %i and %i' % (ant1,ant2)
                    passedflags = np.copy(flags)
                    select = antenna1 != ant1
                    select &= antenna2 != ant2
                    passedflags[select,:,:] = True
                    newflags |= histoclip(data, passedflags, threshold = threshold, show = show)

    # Now apply newflags to the data
    flags =  tout.getcol('FLAG') | newflags
    
    #print newflags.dtype
    #print newflags.sum(axis=(1,2))[0:100]
    #print np.min(uvw[newflags.sum(axis=(1,2)) > 0, 0])
    #print np.max(uvw[newflags.sum(axis=(1,2)) > 0, 0])
    #plt.plot(uvw[:, 0], uvw[:, 1], 'ko', zorder = 1)
    #plt.plot(uvw[newflags.sum(axis=(1,2)) > 0, 0], uvw[newflags.sum(axis=(1,2)) > 0, 1], 'ro', zorder = 2)
    #plt.xlim([3000,-3000])
    #plt.show()
    
    tout.putcol('FLAG', flags)
    tout.close()

    if outset != None:
        t.close
    return
    
if __name__ == '__main__':
    flaglinav('yoyo.ms', outset = 'yoyout.ms', pol = 'parallel', threshold = 5, mode = 'all', show = False)

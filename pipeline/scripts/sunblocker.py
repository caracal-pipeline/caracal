
# Copyright (c) 2017 Gyula Istvan Geza Jozsa, Paolo Serra, Kshitij Thorat, Sphesihle Makhatini, NRF (Square Kilometre Array South Africa) - All Rights Reserved

"""
Class Sunblocker

Taylored towards method phazer, aimed at removing solar interference from interferometric Measurement Set (MS) data. See description therein. All other methods are helpers.

Methods:
    opensilent          - Opening inset with pyrap as a table suppressing any feedback from pyrap
    gaussian            - Gaussian function
    wedge_around_centre - Return a boolean array selecting points in a 2-D wedge
    histoclip           - Measure sigma and return a mask indicating data at a distance larger than threshold times sigma from the average
    readdata            - Open a data set inset and return a few tables
    phazer              - Flag Measurement Set based on scalarly averaged data
"""
import numpy as np
import pyrap.tables as tables
import matplotlib.pyplot as plt
import scipy.optimize as opt
import scipy.constants as scconstants
import sys
import os
from matplotlib.path import Path
from matplotlib.patches import PathPatch
import types


class Sunblocker:
    def __init__(self):
        pass
    
    def opensilent(self, inset, readonly = True):
        """
        Opening inset with pyrap as a table suppressing any feedback from pyrap

        Input:
        inset (string): Input data set

        Output (pyrap table object): opensilent
        """
        old_stdout = sys.stdout
        sys.stdout = open(os.devnull, "w")
        t = tables.table(inset, readonly = readonly)
        sys.stdout.close()
        sys.stdout = old_stdout
        return t

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
        return amp*np.exp(-0.5*np.power((x-cent)/sigma,2))

    def wedge_around_centre(self, coord, radrange, angle):
        '''
        Return a boolean array selecting points in a 2-D wedge

        Input:
        coord (array-like)    : coordinates of centre
        radrange (float)      : radial range of wedge
        angle (float)         : width of the wedge in degrees    

        The radial range of the wedge is the radius of the centre plus and
        minus half of radrange, the angular range is given by the
        direction of the centre plus and minus angle/2. A boolean array is
        returned, True for all data points with uvcoords inside the wedge,
        False elsewhere.

        '''

        # Number of points in an arc
        npoints = 100 # This should be enough

        alpha_min = np.arctan2(coord[0],coord[1])-np.pi*angle/180./2.
        alpha_max = alpha_min+np.pi*angle/180.
        rmin = np.sqrt(np.power(coord[0],2)+np.power(coord[1],2))-radrange/2.
        rmax = rmin+radrange

        # Generate arcs and the polygon
        a = np.linspace(alpha_min,alpha_max,npoints)
        s = np.sin(a)
        c = np.cos(a)
        arc1 = zip(rmax*s,rmax*c)
        arc2 = zip(rmin*np.flipud(s),rmin*np.flipud(c))
        polygon = arc1+arc2+[arc1[0]]
        path = Path(polygon)
        return path

    def selwith_path(self, path, uvcoords):
        '''
        Return a boolean array indicating coordinates (pairs) inside a polygon path

        path                  : a path as returned by matplotlib.path.Path describing a polygon
        uvcoords (array-like) : nx2 array-like of coordinates of points

        Output:
        array-like(dtype = bool) selwith_wedge_around_centre: 

        Returns a boolean array flagging points inside the polygon as True
        '''

        # Create array of tuples
        points = np.array(zip(uvcoords[:,0], uvcoords[:,1]))
        boolarray = path.contains_points(points)
        return boolarray

    def histoclip(self, data, mask, gruvcoord, threshmode = 'fit', threshold = 5., ax = None, title = '', verb = True):
        """Measure sigma and return a mask indicating data at a distance larger than threshold times sigma from the average 

        Input:
        data (ndarray, type = float): Input data, one dimension
        mask (ndarray, type = bool) : Mask indicating data points to ignore, same shape as data
        gruvcoord (nx2 ndarray, type = float): Array of the same size of data x 2 denoting grid positions of the data
        threshmode (string)         : Method to determine sigma, 'fit': fit Gaussian at the max to determine sigma, 'fixed': threshold is in absolute units (sigma is 1.), otherwise standard deviation 
        threshold (float)           : Distance from average beyond which data are flagged in units of sigma
        show (bool)                 : Show histogram to monitor what is happening
        title (string)              : title of histogram
        verb (bool)                 : show info during processing?

        Output:
        histoclip (ndarray, type = bool): Mask indicating points with a distance of larger than threshold*sigma from average (or peak position)

        Calculates amplitude of all points in data and average along the
        frequency axis. For these data calculate average and standard
        deviation (rms) of all points in data whose indices are not
        flagged as True in mask. If mode == 'fit' create a histogram and
        attempt to fit a Gaussian, replacing the rms with the fitted
        dispersion and the average with the centre of the Gaussian. Then
        create an output mask histoclip, flagging all indices of data
        points in data, which have a positive offset by more than
        threshold times the sigma (rms or fitted dispersion) from the mean
        (average or fitted centre of the Gaussian) as True. Sigma means an
        absolute number if threshmode == 'fixed', the fitted sigma if
        threshmode == 'fitted', the standard deviation otherwise. Mean in
        this context means 0 if threshmode == 'fixed', the fitted centre
        if threshmode == 'fitted', the average otherwise.

        """
        # Copy data
        av = np.copy(data)
        # av[mask==True] = np.nan

        # Make a grid
        ugrid = np.unique(gruvcoord[:,0])
        vgrid = np.unique(gruvcoord[:,1])

        # Do this again. We really want a histogram only with values related to the unflagged visibilities
        uvgridded = np.zeros((ugrid.size*vgrid.size), dtype = float)
        i = 0
        for uu in ugrid:
            for vv in vgrid:
                active_visibs = av[(gruvcoord[:,0] == uu) * (gruvcoord[:,1] == vv) * (mask != True)]
                if active_visibs.size == 0:
                    uvgridded[i] = np.nan
                else:
                    uvgridded[i] = active_visibs[0]
                i = i + 1

        # Average data, then look for shape
        # av = np.nanmean(ampar,axis=1)
        npoints = uvgridded[np.isfinite(uvgridded)].size
        if verb:
            print 'Phazer: grid has %i nonzero points.' %  npoints

        # Find average and standard deviation 
        average = np.nanmean(uvgridded)
        stdev  = np.nanstd(uvgridded)

    #    print 'average: %f, stdev: %f' % (average, stdev)

        if average == np.nan:
            return
        if stdev == np.nan:
            return

        if threshmode == 'fit' or show != None:
            # Build a histogram
            hist, bin_edges = np.histogram(uvgridded[np.isfinite(uvgridded)], bins=int(np.sqrt(npoints))+1)
            bin_centers = bin_edges[:-1] + 0.5 * (bin_edges[1:] - bin_edges[:-1])
            widthes = bin_edges[1:] - bin_edges[:-1]

            # Find maximum in histogram 
            maxhi = np.amax(hist)
            maxhiposval = bin_centers[np.argmax(hist)]

            # Fit a Gaussian
            try:
                popt, pcov = opt.curve_fit(self.gaussian, bin_centers, hist, p0 = [maxhiposval, maxhi, stdev/2.])
            except:
                popt = np.array([average, widthes[0]*npoints/(np.sqrt(2*np.pi)*stdev), stdev])

        if threshmode == 'abs':
            std = 1.
            ave = 0.
        if threshmode == 'std':
            std = stdev
            ave = average
        if threshmode == 'fit':
            std = popt[2]
            ave = popt[0]

        # Build a new mask based on the statistics and return it
    #    select = av <= average-threshold*stdev
        select = av >= ave+threshold*std

        # Plot histogram and Gaussians
        if ax != None:
            # Calculating overplotted visibilities
            showgouse = np.linspace(1.5*bin_centers[0]-0.5*bin_centers[1], 1.5*bin_centers[-1]-0.5*bin_centers[-2], 200)
            calculated = self.gaussian(showgouse, average, widthes[0]*npoints/(np.sqrt(2*np.pi)*stdev), stdev)

            # In case of using only stats, this is right on top
            fitted = self.gaussian(showgouse, popt[0], popt[1], popt[2])

            ax.bar(bin_centers, hist, width=widthes, color = 'y', edgecolor = 'y')
            ax.plot(showgouse, calculated, 'g-')
            ax.plot(showgouse, fitted, 'r-')
            ax.axvline(x = average-threshold*stdev, linewidth=2, color='k')
            ax.axvline(x = average+threshold*stdev, linewidth=2, color='k')
            ax.set_xlim(min(bin_edges), max(bin_edges))
            ax.set_title(title)

        return select

    def readdata(self, inset, col = 'DATA', fields = None, channels = None, baselines = None, pol = 'i', verb = False):
        """Open a data set inset and return a few tables

        Input:
        inset (str)            : Input data set
        col (str)              : Column name to base flagging on (e.g. 'DATA' or 'CORRECTED')
        fields (int)           : Fields to select or None if all fields should be used
        channels (bool array)  : dtype = bool array with True for channels to base the analysis on "False" channels will be ignored
        baselines (array)      : nx2 array with antenna pairs for baselines to base the analysis on
        pol (str)              : Polarization selection, Stokes 'i', or Stokes 'q'
        verb (bool)            : Switch commenting on and off

        Output:
        readdate: data (complex array, array of single visibilities, Stokes I or Q per frequency), flags (bool array), uv (float array, uv coordinates), antenna1 (int array), antenna2 (int array), antennanames (string array)

        Will read in data set inset and read data column col (can be
        'DATA', 'CORRECTED', etc.). Apply flags, then calculate Stokes I
        or Q. Change flags to True for any visibility not in the specified
        fields (None means use everything). Change flags to True for
        anything not True in specified channels. Change flags to True for
        anything not contained in the baselines array. Then set all data
        read to numpy.nan if flags == True. Return data, flags,
        uv-coordinates, antenna1, antenna2, antennanames.
        """

        # We really don't want to hear about this
        t = self.opensilent(inset)

        # Read column (think, axes are by default ordered as time, frequency, polarization) and flags, which should have same dimension
        if verb:
            print 'Phazer: reading visibilities.'
        data = t.getcol(col)
        if verb:
            print 'Phazer: reading original flags.'
        flags = t.getcol('FLAG')

        ### The following two lines belong to test 2
        #print '1: shape'
        #print data.shape
        ###

        # Divide uv coordinates by wavelength, for this use average frequencies in Hz
        # If bandwidth becomes large, we have to come up with something better
        if verb:
            print 'Phazer: acquiring spectral information.'
        avspecchan = np.average(t.SPECTRAL_WINDOW.getcol('CHAN_FREQ'))
        if verb:
            print 'Phazer: average wavelength is %.3f m.' % (scconstants.c/avspecchan) # This is for testing: should be ~0.21 if local HI

        if verb:
            print 'Phazer: reading and calculating approximate uv coordinates.'
        uv = t.getcol('UVW')[:,:2]*avspecchan/scconstants.c

        # Convert into desired stokes parameters and adjust mask
        i = data.shape[2] - 1
        stflags = np.logical_not(flags[:,:,0]).astype(float) + np.logical_not(flags[:,:,i]).astype(float)

        # if polarisation is i, then take either average or single value, flag the rest
        if pol == 'i':
            if verb:
                print 'Phazer: calculating Stokes I.'

            # Calculate stokes i, reduce the number of polarizations to one, flag if not at least one pol is available
            with np.errstate(divide='ignore', invalid = 'ignore'):
                data = (data[:,:,0]*np.logical_not(flags)[:,:,0]+data[:,:,i]*np.logical_not(flags)[:,:,i])/stflags
            flags = stflags < 1.
        elif pol == 'q':
            if verb:
                print 'Phazer: calculating Stokes Q.'

            # Calculate stokes q, reduce the number of polarizations to one, flag everything if not both pols are available
            with np.errstate(divide='ignore', invalid = 'ignore'):
                data = (data[:,:,0]*np.logical_not(flags)[:,:,0]-data[:,:,i]*np.logical_not(flags)[:,:,i])/stflags
            flags = stflags < 2.
        else:
            raise('Polarisation must be i or q.')

        ### The following two lines belong to test 2
        #print '2: shape'
        #print data.shape
        ###

        #        flags[:,:,0] = np.logical_not((stflags.astype(bool)))

        # Also mask anything not listed in fields
        if type(fields) != types.NoneType:
            if verb:
                print 'Selecting specified fields.'
            field = t.getcol('FIELD')
            select = np.zeros(field.shape, dtype = bool)
            if type(fields) == list:
                for i in fields:
                    select |= field == i
            else:
                select |= field == fields

            flags[np.logical_not(select),:] = True

        # Flag autocorrelations
        # print t.ANTENNA.getcol('NAME')[0]
        if verb:
            print 'Phazer: reading antenna information.'
        antenna1 = t.getcol('ANTENNA1')
        antenna2 = t.getcol('ANTENNA2')

        if verb:
            print 'Phazer: de-selecting autocorrelations (if any).'
        flags[antenna1 == antenna2] = True

        # Select channels and flag everything outside provided channels
        if type(channels) != types.NoneType:
            if verb:
                print 'Phazer: selecting specified channels.'
            flags[:,np.logical_not(channels)] = True

        # Select baselines and select everything outside provided baselines
        if type(baselines) != types.NoneType:
            if verb:
                print 'Phazer: selecting specified baselines.'
            flags[np.logical_not([i in zip(np.array(baselines)[:,0],np.array(baselines)[:,1]) or i in zip(np.array(baselines)[:,1],np.array(baselines)[:,0]) for i in zip(antenna1, antenna2)])] = True

        # Now put all flagged data to nan:
        if verb:
            print 'Phazer: applying selections to data.'
        data[flags] = np.nan

        antennanames = t.ANTENNA.getcol('NAME')
        t.close()

        return data, flags, uv, antenna1, antenna2, antennanames


    def phazer(self, inset = None, outset = None, col = 'DATA', channels = None, baselines = None, fields = None, imsize = 512, cell = 4, mode = 'all', pol = 'parallel', threshmode = 'fit', threshold = 5., radrange = 0., angle = 0., show = None, showdir = '.', dryrun = True, verb = False):
        """Flag Measurement Set based on scalarly averaged data

        Input:
        inset (str)       : Input data set
        outset (str)      : Name of output data set or None, in which case outset = inset
        col (str)         : Column name to base flagging on (e.g. 'DATA' or 'CORRECTED')
        channels (array)  : dtype = bool array with True for channels to base the analysis on "False" channels will be ignored
        baselines (array) : nx2 array with antenna pairs for baselines to base the analysis on
        fields (int)      : Fields to select or None if all fields should be used
        imsize (int)      : Size of image in pixels
        cell (float)      : Size of cell in arcsec
        mode (str)        : Flagging based on 'all' data, repeated per 'antenna', or repeated per 'baseline'
        pol (str)         : Polarization selection, Stokes 'i', or Stokes 'q' 
        threshmode (str)  : Method to determine sigma, 'fit': fit Gaussian at the max to determine sigma, standard deviation otherwise
        threshold (float) : Distance from average beyond which data are flagged in units of sigma
        radrange (float)  : Each selected point is expanded in a wedge with this radial range
        angle (float)     : Each selected point is expanded in a wedge with this angular
        show (bool)       : Show histogram and cutoff line in a viewgraph
        showdir (str)     : Directory to put viewgraphs in
        dryrun (bool)     : Do not apply flags, but (e.g. produce viewgraphs only)
        verb (bool)       : Switch commenting on and off

        Takes a number of input visibilities (column given by col) and
        selects a sub-set using the selection criteria col, channels
        (selecting channel ranges), baselines (a list of selected
        baselines), and fields (list of selected fields). Then grids
        the visibilities according to the corresponding image
        dimensions, where imsize is the size of the image in pixels
        and cell is the size of the cell in arcsec ( uv cell in lambda
        is 1./(imsize*cell*np.pi/(3600.*180.)) ). In this process the
        assumed frequency to express uv coordinates in units of
        wavelength is the average frequency in the data set. Notice
        that this is not precise for a large bandwith. The
        visibilities are converted to Stokes parameters according to
        the parameter 'pol', then vectorially gridded onto the
        calculated grid, then the absolutes are calculated
        (alternatively the PHAses are set to ZERo) and then the
        visibilities are averaged along the frequency axis. Then from
        this data product, some visibilities are flagged using a
        clipping technique. This can be done using all baselines in
        one go, all antennas in one go (repeated for each antenna),
        all baselines (repeated per baseline). For these data average
        and standard deviation (rms) of all points in data are
        calculated whose indices are not flagged as True in mask. If
        mode == 'fit' a histogram is created and attempt to fit a
        Gaussian, replacing the rms with the fitted dispersion and the
        average with the centre of the Gaussian. Then all data with a
        positive distance of greater than threshold times sigma
        (either fitted or standard deviation) from the mean (or
        position of fitted Gaussian) are flagged (lipped). If mode ==
        'absolute', all data above the absolute value given in
        threshold are clipped instead. Each such flagged data point
        can be extended by a wedge, inside which all data points are
        flagged.  The radial range of the wedge is the radius of the
        point (distance from origin) plus and minus half of radrange,
        the angular range is given by the direction of the centre
        (position angle with respect to the centre) plus and minus
        angle/2.  Finally the flags are expanded in the polarisation
        and frequency direction and applied to the output data, which
        have to have the dimension of the input data. If the output
        data are of None type, it is assumed that the flags should be
        applied to the input data. If the output data have a name,
        then they are copies of the input data sets if they do not
        exist, otherwise the flags will be applied to the output data
        sets instead of the input data sets. If show is set to None,
        no plots will be produced. If show is a string type, hardcopy
        output plots (hist_$show and select_$show) will be produced,
        one showing the histograms, two Gaussians (red: fitted, green:
        according to mean and standard deviation), and the threshold
        position, and the other the uv coverage with the gridded
        values and the flagged data (red) and flagged data before
        wedge expansion (green).

        """
        if verb:
            print 'Phazer: start.'

        # Open data set as table
        if verb:
            print 'Phazer: opening input files.'

        if inset == None:
            if verb:
                print 'Phazer: No input. Stopping.'
                print 'Phazer: exiting (successfully).'

        if type(inset) == str:
            inset = [inset]

        if verb:
            if len(inset) == 1:
                print 'Phazer: reading one data set.'
            else:
                print 'Phazer: reading %i data sets.' % len(inset)


        # Let's do this the primitive way, first read a data set then append everything else
        if verb:
            print 'Phazer: reading %s.' % inset[0]

        nrows=[0,]
        data, flags, uv, antenna1, antenna2, antennanames = self.readdata(inset[0], col = col, fields = fields, channels = channels, baselines = baselines, pol = pol, verb = verb)
        nrows.append(data.shape[0])

        for i in range(1,len(inset)):
            if verb:
                print 'Phazer: reading %s.' % inset[i]
            dataplus, flagsplus, uvplus, antenna1plus, antenna2plus, antennanamesplus = self.readdata(inset[i], col = col, fields = fields, channels = channels, pol = pol, verb = verb)
            data     = np.concatenate((data,  dataplus ), axis = 0)
            flags    = np.concatenate((flags, flagsplus), axis = 0)
            uv       = np.concatenate((uv,    uvplus   ), axis = 0)
            antenna1 = np.concatenate((antenna1, antenna1plus), axis = 0)
            antenna2 = np.concatenate((antenna2, antenna2plus), axis = 0)

            # Antenna names is different. Just check if they are the same
            if np.all(antennanames == antennanamesplus):
                pass
            else:
                print 'Phazer: !!!WARNING!!!'
                print 'Phazer: It appears that the antennas in data sets differ.'
                print 'Phazer: This means that baseline selection (using parameter baselines) should not be used.'
                print 'Phazer: This means that only model \'all\' should not be used.'
                print ''
            nrows.append(data.shape[0])
        print data.shape
        sys.exit
        if verb:
            print 'Phazer: gridding visibilities (vector sum) and then building'
            print 'Phazer: scalar average of amplitudes along velocity axis.'
        duv = 1./(imsize*cell*np.pi/(3600.*180.)) # UV cell in lambda
        u = uv[:,0]
        v = uv[:,1]
        umin = u.min() # Minimum in u
        vmin = v.min() # Minimum in v
        umax = u.max() # Maximum in u
        vmax = v.max() # Maximum in v

        if verb:
            print 'Phazer: approximate minimum u is %.0f and \nPhazer: the maximum u is %.0f vminmax %.1f %.1f.' % (umin,umax,vmin,vmax)
        umin, umax = np.floor(umin), np.ceil(umax) # Make sure that all visibilities are included in grid in the next step
        vmin, vmax = np.floor(vmin), np.ceil(vmax) # Make sure that all visibilities are included in grid in the next step
        ugrid=np.arange(umin,umax,duv) # Notice that umax is not necessarily contained in an array like this, hence the step before
        vgrid=np.arange(vmin,vmax,duv) # Notice that vmax is not necessarily contained in an array like this, hence the step before

        # Check if all uv coordinates are somewhere in the grid
        #print umin, ugrid[0], umax, ugrid[-1]
        #print vmin, vgrid[0], vmax, vgrid[-1]

        # Griddedvis are for the viewgraph
        if show != None:
            griddedvis = np.zeros((ugrid.size, vgrid.size),dtype=float)

        # For the sake of efficiency create an array that replaces data
        nmdata = np.ones(data[:,0].size, dtype = float)

        # Keeping track of central uv coordinates, required for histogram later on
        gruvcoord = np.append(np.copy(nmdata),np.copy(nmdata)).reshape((2,nmdata.shape[0])).transpose()
        # Caution!!! The following would not create an independent copy but is equivalent to grucoord = nmdata. This also works for sub-arrays.
        # grucoord = nmdata[:]

        ### The following three lines belong to test 1
        #testdata = np.zeros(data[:,0].size, dtype = bool)
        #print 'Is any value of the boolean array testdata True (should be False)?'
        #print np.any(testdata)
        ###

        ### The following line belongs to test 3: plot gridded visibs
        k = 0
        ###

        for uu in ugrid:
            for vv in vgrid:
                active_visibs = (u > uu)*(u <= (uu+duv))*(v > vv)*(v <= (vv+duv))
                if np.any(active_visibs):

                    #gruvcoord[active_visibs,:] = uu, vv # Central pixel coordinate
                    # Central pixel becomes important when doing wedges
                    gruvcoord[active_visibs,:] = uu+duv/2., vv+duv/2 # Central pixel coordinate

                    ### The following line belongs to test 1
                    #testdata[active_visibs] = True
                    ###
                    scav = np.nanmean(np.abs(np.nansum(data[active_visibs],axis=0))) # Scalar average of amplitude of vectorial sum of visibilities in cell
                    nmdata[active_visibs] = scav # set all visibilities in that cell to same cell value

                    ### The following 4 lines belong to test 3: plot gridded visibs
                    #if k > 100:
                    #    print 'Please find and confirm pixel coordinates and values in the greyscale plot: u: %f v: %f value: %f' % (uu+duv/2, vv+duv/2, scav)
                    #    k = 0
                    #k = k+1
                    ###

                    if show != None:
                        griddedvis[ugrid == uu, vgrid == vv] = scav # For plotting

        # Scalar average in frequency
        data = nmdata

        ### The following two lines belong to test 2
        #print '3: shape'
        #print data.shape
        ###

        ### The following 6 lines belong to test 1
        #print 'Have all visibilities been addressed?'
        #print np.all(testdata)
        #x = (gruvcoord == 0.)
        #print 'Has any visibility been assigned a cell coordinate of 0 (Possible but unlikely)?'
        #print np.any(x)
        #sys.exit()
        ###

        if show != None:
            if verb:
                print 'Plotting gridded scalar average of amplitudes along velocity axis.'
            plt.imshow(np.flip(griddedvis,axis=0).transpose(),vmin=np.nanmin(griddedvis),vmax=np.nanmax(griddedvis),cmap='Greys', origin=('low#er'),interpolation='nearest', extent = [ugrid.max()+duv, ugrid.min(), vgrid.min(), vgrid.max()+duv])
            plt.xlabel('u')
            plt.ylabel('v')
            if type(show) == str:
                plt.savefig(showdir+'griddedvis_'+show)
                plt.close()
            else:
                plt.show()
                plt.close()

        # Now build the mask, depending on the mode
        if verb:
            print 'Phazer: clipping data based on scalar averaging'
        flags = np.zeros(data.shape, dtype = bool)

        if mode == 'all':
            if verb:
                print 'Phazer: mode \'all\', filtering all data at once.'
            if show == None:
                ax = None
            else:
                ax = plt.subplot(1,1,1)
            newflags = self.histoclip(data, flags, gruvcoord, threshmode = threshmode, threshold = threshold, ax = ax, verb = verb)
        else:
            newflags = np.zeros(data.shape, dtype = bool)
            antennas = np.unique(np.append(antenna1, antenna2))
            if mode == 'antenna':
                if verb:
                    print 'Phazer, mode \'antenna\', filtering data per antenna.'
                if show != None:
                    nplotsx = int(np.ceil(np.sqrt(antennas.size)))
                    i = 0
                for antenna in antennas:
                    if verb:
                        print 'Phazer: filtering antenna %i: %s' % (antenna, t.ANTENNA.getcol('NAME')[antenna])
                    passedflags = np.zeros(data.shape,dtype = bool)
                    select  = antenna1 != antenna
                    select &= antenna2 != antenna
                    passedflags[select,:] |= True
                    if show != None:
                        title = 'Ant '+antennanames[antenna]
                        ax = plt.subplot(nplotsx,nplotsx, i)
                    else:
                        ax = None
                    newflags |= self.histoclip(data, passedflags, gruvcoord, threshmode = threshmode, threshold = threshold, ax = ax, title = title, verb = verb)
                    i = i + 1
            else:
                if verb:
                    print 'Phazer: mode \'baseline\', filtering data per antenna.'
                antennas1 = np.unique(antenna1)
                antennas2 = np.unique(antenna2)
                pairs = np.unique(np.column_stack((antenna1,antenna2)))
                # Let's guess this
                if show != None:
                    nplotsx = int(np.ceil(np.sqrt(pairs.size)))
                    i = 0
                for pair in pairs:
                    if pair[0] != pair[1]:
                        if verb:
                            print 'Filtering baseline between antenna %i: %s and %i: %s' % (pair[0], antennanames[pair[0]], pair[1], antennanames[pair[1]])
                        passedflags = np.zeros(data.shape,dtype = bool)
                        select = antenna1 != pair[0]
                        select &= antenna2 != pair[1]
                        passedflags[select,:] |= True
                        if show != None:
                            title = 'Pair '+antennanames[pair[0]]+','+antennanames[pair[1]]
                            ax = plt.subplot(nplotsx,nplotsy,i)
                        else:
                            ax = None
                        newflags |= self.histoclip(data, passedflags, grucoord, grvcoord, threshmode = threshmode, threshold = threshold, ax = ax, title = title, verb = verb)
                    i = i+1
        if show != None:
            if type(show) == str:
                plt.savefig(showdir+'/'+'histo_'+show)
                plt.close()
            else:
                plt.show()
                plt.close()

        if show != None:
            patches = []

        # Extend the new flags, first make a copy of the flags
        if radrange > 0. and angle > 0.:
            if verb:
                print 'Phazer: extending flags to nearby pixels in the uv-plane using radrange: %.0f and angle: %.0f' % (radrange, angle)
            flaggeduv = uv[np.column_stack((newflags,newflags))]
            flaggeduv = flaggeduv.reshape(flaggeduv.size/2, 2)
            befflaggeduv = flaggeduv.copy()

            if verb:
                print 'Phazer: processing %i points.' % (flaggeduv.size/2)
            for i in range(flaggeduv.size/2):
                if i%500 == 0:
                    print 'Phazer: extended %i points.' % i
                thepath = self.wedge_around_centre(flaggeduv[i,:], radrange, angle)
                newflags[self.selwith_path(thepath, uv)] = True
                if show != None:
                    patches.append(PathPatch(thepath, facecolor='orange', lw=0, alpha = 0.1))

        # Plot data and flags
        if show != None:
            if verb:
                print 'Phazer: plotting flagged data positions onto gridded and averaged visibilities.'
            #ax = plt.imshow(np.flip(griddedvis,axis=0).transpose(),vmin=np.nanmin(griddedvis),vmax=np.nanmax(griddedvis),cmap='Greys', origin=('lower'),interpolation='nearest', extent = [ugrid.max()+duv, ugrid.min(), vgrid.min(), vgrid.max()+duv])
            average = np.nanmean(griddedvis)
            stdev  = np.nanstd(griddedvis)
            ax = plt.subplot(1,1,1)
            #plt.imshow(np.flip(griddedvis,axis=0).transpose(),vmin=np.maximum(np.nanmin(griddedvis),average-threshold*stdev),vmax=np.minimum(np.nanmax(griddedvis),average+threshold*stdev),cmap='Greys', origin=('lower'),interpolation='nearest', extent = [ugrid.max()+duv, ugrid.min(), vgrid.min(), vgrid.max()+duv])
            #plt.xlabel('u')
            #plt.ylabel('v')
            ax.imshow(np.flip(griddedvis,axis=0).transpose(),vmin=np.maximum(np.nanmin(griddedvis),average-threshold*stdev),vmax=np.minimum(np.nanmax(griddedvis),average+threshold*stdev),cmap='Greys', origin=('lower'),interpolation='nearest', extent = [ugrid.max()+duv, ugrid.min(), vgrid.min(), vgrid.max()+duv])
            ax.set_xlabel('u')
            ax.set_ylabel('v')
            for patch in patches:
                ax.add_patch(patch)
            flaggeduv = uv[np.column_stack((newflags,newflags))]
            flaggeduv = flaggeduv.reshape(flaggeduv.size/2, 2)
            ax.plot(flaggeduv[:,0], flaggeduv[:,1], '.r', markersize = 0.3)
            if radrange > 0. and angle > 0.:
                ax.plot(befflaggeduv[:,0], befflaggeduv[:,1], '.g', markersize = 0.3)
            notflaggeduv = uv[np.column_stack((np.logical_not(newflags),np.logical_not(newflags)))]
            notflaggeduv = notflaggeduv.reshape(notflaggeduv.size/2, 2)
            ax.plot(notflaggeduv[:,0], notflaggeduv[:,1], '.b', markersize = 0.3)
            if type(show) == str:
                plt.savefig(showdir+'/'+'select_'+show)
                plt.close()
            else:
                plt.show()
                plt.close()

        if type(outset) == str:
            outset = [outset]

        if outset == None:
            outset = inset
        else:
            if len(outset) != len(inset):
                raise('Phazer: number of outsets is not equal to number of insets, cowardly stopping application of flags.')

        for i in range(len(outset)):
            if tables.tableexists(outset[i]):
                if verb:
                    print 'Phazer: opening data set %s' % outset[i]
                if not dryrun:
                    tout = self.opensilent(outset[i], readonly=False)
                else:
                    print 'Phazer: it\'s a simulation (dry run)'
            else:
                if verb:
                    print 'Phazer: data set %s does not exist. Copying it from data set %s' % (outset[i], inset[i])
                if not dryrun:
                    t = self.opensilent(inset[i])
                    tout = t.copy(outset[i])
                    tout.close()
                    t.close()
                    tout = self.opensilent(outset[i], readonly=False)
                else:
                    if verb:
                        print 'Phazer: it\'s a simulation (dry run)'

            # Now apply newflags to the data
            if verb:
                print 'Phazer: applying new flags.'
            if not dryrun:
                flags =  tout.getcol('FLAG')
                flags[newflags[nrows[i]:nrows[i+1]],:,:] = True
            else:
                if verb:
                    print 'Phazer: it\'s a simulation (dry run)'

            if verb:
                print 'Phazer: writing flags.'
            if not dryrun:
                tout.putcol('FLAG', flags)
                tout.close()
            else:
                if verb:
                    print 'Phazer: it\'s a simulation (dry run).'
        if verb:
            print 'Phazer: exiting (successfully).'
        return

#if __name__ == '__main__':
#    a = np.zeros((767), dtype=bool)
#    a[1:35] = True
#    mysb = Sunblocker()
#    mysb.phazer(['yoyo.ms'], outset = ['yoyout.ms'], channels = a, imsize = 512, cell = 4, pol = 'i', threshold = 4., mode = 'all', radrange = 0, angle = 0, show = 'test.pdf', verb = True, dryrun = False)

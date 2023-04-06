#! /usr/bin/env python

import sys
from datetime import datetime
import numpy as np
import astropy.io.fits as astropy_io_fits
import scipy
import scipy.signal as scipy_signal
import argparse
import textwrap
from astropy.wcs import WCS
import astropy.units as u
from abc import ABC, abstractmethod


version = '1.0.2'


class FitFunc(ABC):
    """
    abstract class for writing fitting functions
    """
    def __init__(self):
        pass
    
    @abstractmethod
    def prepare(self, x, data, mask, weight):
        pass
    
    @abstractmethod
    def fit(self, x, data, mask, weight):
        pass
    

class FitBSpline(FitFunc):
    """
    BSpline fitting function based on `splev`, `splrep` in `scipy.interpolate` 
    """
    def __init__(self, order, velWidth):
        """
        needs to know the order of the spline and the number of knots
        """
        self._order = order
        self._velwid = velWidth
        
    def prepare(self, x, data = None, mask = None, weight = None):
        msort = np.argpartition(x, -2)
        m1l, m2l = msort[-2:]
        m1h, m2h = msort[:2]
        if np.abs(m1l - m2l) == 1 and np.abs(m1h - m2h) == 1:
            dvl = np.abs(x[m1l]-x[m2l])/np.mean([x[m1l],x[m2l]])*3e5
            dvh = np.abs(x[m1h]-x[m2h])/np.mean([x[m1h],x[m2h]])*3e5
            dv = (dvl+dvh)/2
            self._imax = int(len(x)/(self._velwid//dv))+1
            print('len(x) = {}, dv = {}, {}km/s in chans: {}, max order spline = {}'.format(len(x), dv, self._velwid, self._velwid//dv, self._imax))
        else:
            log.debug('probably x values are not changing monotonically, aborting')
            sys.exit(1)
            
        knotind = np.linspace(0, len(x), self._imax, dtype = int)[1:-1]
        chwid = (len(x)//self._imax)//6
        self._knots = lambda: np.random.randint(-chwid, chwid, size = knotind.shape)+knotind
    
    def fit(self, x, data, mask, weight):
        """
        returns the spline fit and the residuals from the fit
        
        x : x values for the fit
        y : values to be fit by spline
        mask : a mask (not implemented really)
        weight : weights for fitting the Spline
        """
        inds = self._knots()
        # log.info(f'inds: {inds}')
        splCfs = splrep(x, data, task = -1, w = weight, t = x[inds], k = self._order)
        spl = splev(x, splCfs)
        return spl, data-spl

class fitMedFilter(FitFunc):
    """
    Median filtering class for continuum subtraction 
    """
    def __init__(self, velWidth):
        """
        needs to know the order of the spline and the number of knots
        """
        self._velwid = velWidth
        
    def prepare(self, x, data = None, mask = None, weight = None):
        msort = np.argpartition(x, -2)
        m1l, m2l = msort[-2:]
        m1h, m2h = msort[:2]
        if np.abs(m1l - m2l) == 1 and np.abs(m1h - m2h) == 1:
            dvl = np.abs(x[m1l]-x[m2l])/np.mean([x[m1l],x[m2l]])*3e5
            dvh = np.abs(x[m1h]-x[m2h])/np.mean([x[m1h],x[m2h]])*3e5
            dv = (dvl+dvh)/2
            self._imax = int(self._velwid//dv)
            if self._imax %2 == 0:
                self._imax += 1
            print('len(x) = {}, dv = {}, {}km/s in chans: {}'.format(len(x), dv, self._velwid, self._velwid//dv))
        else:
            log.debug('probably x values are not changing monotonically, aborting')
            sys.exit(1)
            
    
    def fit(self, x, data, mask, weight):
        """
        returns the median filtered data as line emission
        
        x : x values for the fit
        y : values to be fit
        mask : a mask (not implemented really)
        weight : weights
        """
        nandata = np.hstack((np.full(self._imax//2, np.nan), data, np.full(self._imax//2, np.nan)))
        nanMed = np.nanmedian(np.lib.stride_tricks.sliding_window_view(nandata,self._imax), axis = 1)
        # resMed = nanMed[~np.isnan(nanMed)]
        resMed = nanMed
        return resMed, data-resMed


class ContSub():
    """
    a class for performing continuum subtraction on data
    """
    def __init__(self, x, cube, function, mask):
        """
        each object can be initiliazed by passing a data cube, a fitting function, and a mask
        cube : a fits cube containing the data
        function : a fitting function should be built on FitFunc class
        mask : a fitting mask where the pixels that should be used for fitting has a `True` value
        """
        self.cube = cube
        self.function = function
        self.mask = mask
        self.x = x
        
    def fitContinuum(self):
        """
        fits the data with the desired function and returns the continuum and the line
        """
        dimy, dimx = self.cube.shape[-2:]
        cont = np.zeros(self.cube.shape)
        line = np.zeros(self.cube.shape)
        self.function.prepare(self.x)

        if self.mask is None:
            for i in range(dimx):
                for j in range(dimy):
                    cont[:,j,i], line[:,j,i] = self.function.fit(self.x, self.cube[:,j,i], mask = None, weight = None)
        else:
            print('here')
            for i in range(dimx):
                for j in range(dimy):
                    cont[:,j,i], line[:,j,i] = self.function.fit(self.x, self.cube[:,j,i], mask = None, weight = self.mask[:,j,i])
                print(i)
            # log.info(f'row {i} is done')
            
        return cont, line
                

def retFreq(header):
    """
    Extract the part of the cube name that will be used in the name of
    the averaged cube

    Parameters
    ----------
    header : `~astropy.io.fits.Header`
        header object from the fits file

    Returns
    -------
    frequency
        a 1D numpy array of channel frequencies in MHz  
    """
    
    if not ('TIMESYS' in header):
        header['TIMESYS'] = 'utc'
    elif header['TIMESYS'] != 'utc':
        header['TIMESYS'] = 'utc'
    freqDim = header['NAXIS3']
    wcs3d=WCS(header)
    try:
        wcsfreq = wcs3d.spectral
    except:
        wcsfreq = wcs3d.sub(['spectral'])   
    return np.around(wcsfreq.pixel_to_world(np.arange(0,freqDim)).to(u.MHz).value, decimals = 7)


def printime(string):
    now = datetime.now().strftime("%H:%M:%S")
    print('{} {}'.format(now, string))


def imcontsub(
    incubus, outcubus=None, fitmode='median', length=0,
    polyorder=None, mask=None, sgiters=0, kertyp='gauss', kersiz=0,
        fitted=None, confit=None, clobber=False):
    """Continuum subtraction in a fits data cube

    Parameters:
        incubus (str):  Input cube
        outcubus (str): Name of continuum-subtracted output data cube
        fitmode (str):  Type of fit ('poly' or 'savgol')
        length (int): Length of the sliding window in channels (must be
            odd)
        polyorder (int): Polynomial order
        mask (string): Mask cube indicating regions to be excluded;
            excluded are voxels where the mask cube is not 0
        sgiters (int): Number of Savitzky-Golay filter iterations
        kertyp (str): Kernel type to convolve the polynomial fit with
            ('gauss'  'tophat')
        kersiz (int): Kernel size to convolve the polynomial fit with (pixel)
        fitted (str): Name of fitted continuum cube (optinal output)
        confit (str): Name of fitted and convolved continuum cube (optional
            output)
        clobber (bool): Overwrite output if set

    Returns:
        None

    Takes input cube incubus, which is expected to have velocity or
    frequency along the third axis. A fourth axis is allowed but
    expected to have a length of 1 ("Stokes I"). The script fits a
    function or filter along the third axis, subtracts it from the
    original, and writes the result onto disk (outcubus). Possible is
    a polynomial fit (fitmode = 'poly') or a Savitzky-Golay filter. In
    case of the Savitzky-Golay filter the window length is given by the
    parameter length. The polynomial order of either polynomial or the
    filter is specified with the parameter polyorder. A Savitzky-Golay
    filter with polynomial order 0 is a median filter. Optionally a
    mask data cube with the same dimensions of the input data cube can
    be provided.  Voxels for which the mask data cube is not equal to
    zero are ignored. For the polynomial fit the voxels are simply
    ignored. In case of the Savitzky Golay filter, an iterative
    process is started.  All masked voxels are set to zero and a
    median filter is run along the frequency axis. After that the
    Savitzky-Golay filter is run sgiters times. If the parameter
    sgiters is set to 0, only one Savitzky-Golay filter is
    applied. After the fitting procedure, the fitted data cube can
    optionally be convolved in the spatial domain (axes 1 and 2)
    before it gets subtracted from the original cube.  The type of the
    convolving kernel is given by the parameter kertyp ('gauss' or
    'tophat') and the size of the kernel in pixels is given by the
    parameter kersiz. With the parameter fitted the user can
    optionally supply the name of the output fitted data cube and with
    the parameter confit the user specifies the name of the fitted and
    convolved output data cube. The parameter clobber determines
    whether the output will be overwritten (if True).
    """
    # Read cube
    begin = datetime.now()
    print('')
    print('Welcome to image_contsub.py')

    if isinstance(incubus, type('')):
        printime('Reading input cube {}'.format(incubus))
        hdul_incubus = astropy_io_fits.open(incubus)
    else:
        hdul_incubus = incubus

    incubus_data = hdul_incubus[0].data

    # Reduce to 3 dims if necessary
    stokes = False
    if len(incubus_data.shape) == 4:
        stokes = True
        incubus_data = incubus_data[0, :]

    if not isinstance(mask, type(None)):

        if isinstance(mask, type('')):
            printime('Reading and applying mask {}'.format(mask))
            hdul_mask = astropy_io_fits.open(mask)
        else:
            hdul_mask = mask

        mask_data = hdul_mask[0].data

        # Reduce to 3 dims if necessary
        if len(mask_data.shape) == 4:
            mask_data = mask_data[0, :]

        # Create a masked cube
        # incubus_data_masked = np.ma.masked_array(incubus_data, mask_data > 0)
        incubus_data_masked = np.ma.masked_array(
            incubus_data, (mask_data > 0)+np.isnan(incubus_data))
        hdul_mask.close()
    else:
        incubus_data_masked = np.ma.masked_array(
            incubus_data, np.isnan(incubus_data))
        # incubus_data_masked = incubus_data


    if fitmode == 'spline':
        
        freqs =  retFreq(hdul_incubus[0].header)    
        methds = [FitBSpline(*fa) for fa in [[3, 1750], [3, 1675], [3, 1500]]]

        #run the first round of continuum subtraction
        constsub = ContSub(freqs, incubus_data, methds[0], ~mask_data.astype(bool))
        cont, subtracted = constsub.fitContinuum()

    else:
        # Read mask and apply


        if fitmode == 'poly':

            if isinstance(polyorder, type(None)):
                polyorder = length

            # Flatten and fit
            incubus_data_flat = incubus_data_masked.reshape(
                (incubus_data.shape[0], incubus_data.shape[1]
                    * incubus_data.shape[2]))
            x = np.ma.masked_array(np.arange(incubus_data.shape[0]), False)

            printime('Fitting polynomial of order {}'.format(polyorder))
            fitpars = np.array(
                np.flip(np.ma.polyfit(x, incubus_data_flat, polyorder)))

            printime('Creating continuum cube')
            fit = np.flip(np.polynomial.polynomial.polyval(
                np.flip(np.array(x), 0), fitpars).transpose()).reshape(
                    (incubus_data.shape[0], incubus_data.shape[1],
                        incubus_data.shape[2]))
            # Make sure that the fit cube can be convolved
            if np.nanmax(incubus_data) > 0.:
                maxincube = np.nanmax(incubus_data)*10.
            else:
                maxincube = 0
            if np.nanmin(incubus_data) < 0.:
                minincube = np.nanmin(incubus_data)*10.
            else:
                minincube = 0.

            fit[fit > maxincube] = maxincube
            fit[fit < minincube] = minincube

            # To be completely sure
            fit[np.logical_not(np.isfinite(fit))] = 0.

        elif fitmode == 'median':
            if length == 0:
                printime('Length is 0, no median-filtering.')
            else:
                printime('Median-filtering cube')

                fit = scipy.ndimage.median_filter(
                    incubus_data, (length, 1, 1))


        elif fitmode == 'savgol':
            if isinstance(polyorder, type(None)):
                polyorder = 0

            if length == 0:
                printime('Length is 0, no Savitzky-Golay-filtering.')
            else:
                printime('Savitzky-Golay-filtering cube (order {})'.format(polyorder))

                sgmask = np.ma.getmask(incubus_data_masked)
                sgincubus = incubus_data.copy()
                sgincubus[sgmask==True] = 0.0

                # First stitch holes in the data
                if sgiters > 0:
                    fit = scipy.ndimage.median_filter(
                        sgincubus, (length, 1, 1))

                    # Then iterate n times with better guesses for the
                    # stitched data
                    for i in range(sgiters):
                        print('Iteration {}'.format(i))

                        sgincubus = fit

                        fit = scipy_signal.savgol_filter(
                            sgincubus, length, polyorder, axis=0, mode='interp')
                else:
                    fit = scipy_signal.savgol_filter(
                        sgincubus, length, polyorder, axis=0, mode='interp')

        else:
            printime('No valid filter chosen, not filtering.')
            fit = incubus_data_masked*0.

        if not isinstance(fitted, type(None)):
            printime('Writing continuum cube')
            if stokes:
                hdul_incubus[0].data = fit.astype('float32').reshape(
                    (1, fit.shape[0], fit.shape[1], fit.shape[2]))
            else:
                hdul_incubus[0].data = fit.astype('float32')
            hdul_incubus[0].header['DATAMAX'] = np.nanmax(hdul_incubus[0].data)
            hdul_incubus[0].header['DATAMIN'] = np.nanmax(hdul_incubus[0].data)
            hdul_incubus.writeto(fitted, overwrite=clobber)

        if kersiz > 0:
            printime('Spatially convolving continuum cube')
            if kertyp == 'gauss':
                kernel = scipy_signal.gaussian(
                    int(10.*kersiz/np.sqrt(np.log(256.)))//2*2+1,
                    kersiz/np.sqrt(np.log(256.)))
            else:
                klength = int(10.*kersiz)//2*2+1
                coordinates = np.arange(klength, dtype=int)-int(klength)//2
                kernel = (np.fabs(coordinates) < (kersiz//2+1))

            kernel = np.outer(
                kernel, kernel).reshape((1, kernel.size, kernel.size))
            kernel = np.repeat(kernel, fit.shape[0], axis=0)
            fitmask = np.isnan(fit)
            fit[fitmask] = 0.
            convolved = scipy_signal.fftconvolve(
                fit, kernel, mode='same', axes=(1, 2))/kernel[0].sum()
            convolved[fitmask] = np.nan
        else:
            convolved = fit

        if not isinstance(confit, type(None)):
            printime('Writing convolved continuum cube')
            if stokes:
                hdul_incubus[0].data = convolved.astype('float32').reshape(
                    (1, convolved.shape[0], convolved.shape[1],
                        convolved.shape[2]))
            else:
                hdul_incubus[0].data = convolved.astype('float32')
            hdul_incubus[0].header['DATAMAX'] = np.nanmax(hdul_incubus[0].data)
            hdul_incubus[0].header['DATAMIN'] = np.nanmax(hdul_incubus[0].data)
            hdul_incubus.writeto(confit, overwrite=clobber)

        printime('Subtracting continuum.')
        subtracted = np.subtract(incubus_data-convolved)

    if stokes:
        hdul_incubus[0].data = subtracted.astype('float32').reshape(
            (1, subtracted.shape[0], subtracted.shape[1], subtracted.shape[2]))
    else:
        hdul_incubus[0].data = subtracted.astype('float32')
    printime('Writing subtracted cube.')
    hdul_incubus[0].header['DATAMAX'] = np.nanmax(hdul_incubus[0].data)
    hdul_incubus[0].header['DATAMIN'] = np.nanmax(hdul_incubus[0].data)
    hdul_incubus.writeto(outcubus, overwrite=clobber)
    hdul_incubus.close()
    
    now = datetime.now()
    printime(
        'Time elapsed: {:.1f} minutes'.format((now-begin).total_seconds()/60.))
    print('')


def description():
    """
    Verbose description of the module
    """
    return textwrap.fill(
        'Takes input cube incubus, which is expected to have velocity'
        'or frequency along the third axis. A fourth axis is allowed but'
        'expected to have a length of 1 ("Stokes I"). The script fits a'
        'function or filter along the third axis, subtracts it from the'
        'original, and writes the result onto disk (outcubus). Possible is'
        "a polynomial fit (fitmode = 'poly') or a Savitzky-Golay filter. In"
        'case of the Savitzky-Golay filter the window length is given by the'
        'parameter length. The polynomial order of either polynomial or the'
        'filter is specified with the parameter polyorder. A Savitzky-Golay'
        'filter with polynomial order 0 is a median filter. Optionally a'
        'mask data cube with the same dimensions of the input data cube can'
        'be provided.  Voxels for which the mask data cube is not equal to'
        'zero are ignored. For the polynomial fit the voxels are simply'
        'ignored. In case of the Savitzky Golay filter, an iterative'
        'process is started.  All masked voxels are set to zero and a'
        'median filter is run along the frequency axis. After that the'
        'Savitzky-Golay filter is run sgiters times. If the parameter'
        'sgiters is set to 0, only one Savitzky-Golay filter is'
        'applied. After the fitting procedure, the fitted data cube can'
        'optionally be convolved in the spatial domain (axes 1 and 2)'
        'before it gets subtracted from the original cube.  The type of the'
        "convolving kernel is given by the parameter kertyp ('gauss' or"
        "'tophat') and the size of the kernel in pixels is given by the"
        'parameter kersiz. With the parameter fitted the user can'
        'optionally supply the name of the output fitted data cube and with'
        'the parameter confit the user specifies the name of the fitted and'
        'convolved output data cube. The parameter clobber determines'
        'whether the output will be overwritten (if True).')


def parsing():
    if '-v' in sys.argv or '--verb' in sys.argv:
        epilog = description()
    else:
        epilog = 'Use \'equolver -h -v\' for verbose description.'
    parser = argparse.ArgumentParser(
        description='Continuum subtraction of a fits data cube',
        formatter_class=argparse.RawTextHelpFormatter,
        prog='image_contsub.py',
        usage='%(prog)s [options]', epilog=epilog,
        fromfile_prefix_chars='@',
        argument_default=argparse.SUPPRESS)

    # Common
    parser.add_argument(
        '--incubus', '-i', help='Name of input data cube.', type=str)
    parser.add_argument(
        '--outcubus', '-o',
        help='Name of continuum-subtracted output data cube', type=str)
    parser.add_argument(
        '--fitmode', '-f',
        help='Type of fit (\'poly\' or \'savgol\')', type=str)
    parser.add_argument(
        '--length', '-l',
        help='Length of the sliding window in channels', type=str)
    parser.add_argument(
        '--polyorder', '-p', help='Polynomial order', type=str)
    parser.add_argument(
        '--mask', '-m',
        help='Mask cube indicating regions to be excluded; excluded are voxels'
        ' where the mask cube is not 0', type=str)
    parser.add_argument(
        '--sgiters', '-s',
        help='Number of Savitzky-Golay filter iterations', type=int)
    parser.add_argument(
        '--kertyp', '-t',
        help="Kernel type to convolve the polynomial fit with ('gauss',"
        " 'tophat\')", type=str)
    parser.add_argument(
        '--kersiz', '-k',
        help='Kernel size to convolve the polynomial fit with (pixel)',
        type=str)
    parser.add_argument(
        '--fitted', help='Name of fitted continuum cube (optinal output)',
        type=str)
    parser.add_argument(
        '--confit', help='Name of fitted and convolved continuum cube '
        '(optional output)', type=str)
    parser.add_argument(
        '--clobber', '-c', help='overwrite output if set', default=False,
        action='store_true')

    whatnot = parser.parse_args()
    inpars = vars(whatnot)

    for key in list(inpars.keys()):
        try:
            result = eval(inpars[key])
        except Exception:
            result = inpars[key]
        inpars[key] = result

    # if 'inc_cubes' in inpars.keys():
    #     print(inpars['inc_cubes'])
    #     if inpars['inc_cubes'] == True:
    #         print('yo')
    # sys.exit()
    return inpars


if __name__ == "__main__":
    kwargs = parsing()
    for argument in ['help', 'version']:
        if argument in kwargs.keys():
            sys.exit()
    imcontsub(**kwargs)

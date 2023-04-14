#! /usr/bin/env python

import sys
from datetime import datetime
import numpy as np
import argparse
import textwrap
from caracal.utils.requires import extras

version = '1.0.2'


def printime(string):
    now = datetime.now().strftime("%H:%M:%S")
    print('{} {}'.format(now, string))

@extras(packages=["scipy", "astropy"])
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

    import astropy.io.fits as astropy_io_fits
    import scipy
    import scipy.signal as scipy_signal
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

    # Read mask and apply
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
            incubus_data, (mask_data > 0) + np.isnan(incubus_data))
        hdul_mask.close()
    else:
        incubus_data_masked = np.ma.masked_array(
            incubus_data, np.isnan(incubus_data))
        # incubus_data_masked = incubus_data

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
            maxincube = np.nanmax(incubus_data) * 10.
        else:
            maxincube = 0
        if np.nanmin(incubus_data) < 0.:
            minincube = np.nanmin(incubus_data) * 10.
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
            sgincubus[sgmask] = 0.0

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
        fit = incubus_data_masked * 0.

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
                int(10. * kersiz / np.sqrt(np.log(256.))) // 2 * 2 + 1,
                kersiz / np.sqrt(np.log(256.)))
        else:
            klength = int(10. * kersiz) // 2 * 2 + 1
            coordinates = np.arange(klength, dtype=int) - int(klength) // 2
            kernel = (np.fabs(coordinates) < (kersiz // 2 + 1))

        kernel = np.outer(
            kernel, kernel).reshape((1, kernel.size, kernel.size))
        kernel = np.repeat(kernel, fit.shape[0], axis=0)
        fitmask = np.isnan(fit)
        fit[fitmask] = 0.
        convolved = scipy_signal.fftconvolve(
            fit, kernel, mode='same', axes=(1, 2)) / kernel[0].sum()
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
    subtracted = incubus_data - convolved

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
        'Time elapsed: {:.1f} minutes'.format((now - begin).total_seconds() / 60.))
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

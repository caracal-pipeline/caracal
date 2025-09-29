import os
import shutil
import numpy as np
from astropy import units as u
import astropy.coordinates as coord
from astropy.wcs import WCS
from astropy.io import fits, ascii
from astropy.table import Table, Column
from astroquery.vizier import Vizier


def ra2deg(ra_hms):
    '''
    Converts right ascension in hms coordinates to degrees and radians
    INPUT
        rahms: ra in HH:MM:SS format (str)
    OUTPUT
        conv_units.radeg: ra in degrees
        conv_units.rarad: ra in radians
    '''

    ra = ra_hms.split(':')

    hh = float(ra[0]) * 15
    mm = (float(ra[1]) / 60) * 15
    ss = (float(ra[2]) / 3600) * 15

    return hh + mm + ss

def dec2deg(dec_dms):
    '''
    Converts right ascension in hms coordinates to degrees and radians
    INPUT
        rahms: ra in HH:MM:SS format (str)
    OUTPUT
        conv_units.radeg: ra in degrees
        conv_units.rarad: ra in radians
    '''

    dec = dec_dms.split(':')

    hh = abs(float(dec[0]))
    mm = float(dec[1]) / 60
    ss = float(dec[2]) / 3600

    if float(dec[0]) >= 0:
        return hh + mm + ss
    else:
        return -(hh + mm + ss)


def nvss_pbcorr(ra_deg, dec_deg, centre, cell, imsize, obs_freq, flux):
    '''

    Module to locate sources on continuum image/mask
    '''

    # I load the WCS coordinate system:
    # open file

    wcs = WCS(naxis=2)

    centre = coord.SkyCoord(centre[0], centre[1], unit=(
        u.hourangle, u.deg), frame='icrs')
    cell /= 3600.

    pb_fwhm = 1.02 * (2.99792458E8) / obs_freq / 13.5 / np.pi * 180.
    pb_fwhm_pix = pb_fwhm / cell
    sigma, mu = pb_fwhm_pix / 2.35482, 0.0

    wcs.crpix = [imsize / 2, imsize / 2]
    wcs.cdelt = np.array([-cell, cell])
    wcs.crval = [centre.ra.deg, centre.dec.deg]
    wcs.ctype = ["RA---SIN", "DEC--SIN"]

    px, py = wcs.wcs_world2pix(ra_deg, dec_deg, 0)

    d = np.sqrt(px * px + py * py)

    gauss = np.exp(-((d - mu)**2 / (2.0 * sigma**2)))

    new_flux = flux * 1e-3 * gauss

    return new_flux, px, py

def query_catalog_sumss(catalog_table, centre, width_im, cat_name):

    Vizier.ROW_LIMIT = -1

    p = Vizier.query_region(coord.SkyCoord(centre[0], centre[1], unit=(u.hourangle, u.deg), frame='icrs'),
                            width=width_im, catalog=cat_name)
    tab = p[0]

    ascii.write(tab, catalog_table, overwrite=True)

def query_catalog_nvss(catalog_table, centre, width_im, cell, imsize, obs_freq, cat_name, thresh):
    Vizier.ROW_LIMIT = -1
    p = Vizier.query_region(coord.SkyCoord(centre[0], centre[1], unit=(u.hourangle, u.deg), frame='icrs'),
                            width=width_im, catalog=cat_name)
    tab = p[0]

    ascii.write(tab, catalog_table, overwrite=True)

    tab = Table(tab, masked=True)

    ra_deg = np.empty([len(tab['RAJ2000'])])
    dec_deg = np.empty([len(tab['RAJ2000'])])
    flux_corr = np.empty([len(tab['RAJ2000'])])
    pix_x = np.empty([len(tab['RAJ2000'])])
    pix_y = np.empty([len(tab['RAJ2000'])])

    for i in range(0, len(tab['RAJ2000'])):
        tab['RAJ2000'][i] = tab['RAJ2000'][i].replace(' ', ':')
        ra_deg[i] = ra2deg(tab['RAJ2000'][i])
        tab['DEJ2000'][i] = tab['DEJ2000'][i].replace(' ', ':')
        dec_deg[i] = dec2deg(tab['DEJ2000'][i])
        flux_corr[i], pix_x[i], pix_y[i] = nvss_pbcorr(
            ra_deg[i], dec_deg[i], centre, cell, imsize, obs_freq, tab['S1.4'][i])

    ra_deg = Column(ra_deg)
    dec_deg = Column(dec_deg)
    pix_x = Column(pix_x)
    pix_y = Column(pix_y)
    flux_corr = Column(flux_corr)

    tab.add_column(pix_x, name='PixX')
    tab.add_column(pix_y, name='PixY')
    tab.add_column(flux_corr, name='Flux_pbcorr')
    tab.add_column(ra_deg, name='RADEG')
    tab.add_column(dec_deg, name='DECDEG')

    flux14 = np.array(flux_corr, dtype=float)
    below_thresh = flux14 < thresh

    for i in range(1, len(tab.colnames)):
        tab[tab.colnames[i]][below_thresh] = np.nan

    tab = tab[~np.isnan(tab['Flux_pbcorr'])]

    tab = Table(tab, masked=True)

    ascii.write(tab, catalog_table, overwrite=True)

def set_mosaic_files(catalog_table, mask_dir, fields_dir):

    tab = ascii.read(catalog_table)
    unique, _ = np.unique(tab['Mosaic'], return_counts=True)

    mosaic_tmpdir = mask_dir + '/formosaic/'
    mosaic_outdir = mask_dir + '/mosaic/'

    if os.path.exists(mosaic_tmpdir) == False:
        os.mkdir(mosaic_tmpdir)
    if os.path.exists(mosaic_outdir):
        shutil.rmtree(mosaic_outdir)

    for i in range(0, len(unique)):
        summsfield = fields_dir + str(unique[i]) + '.FITS'
        outfield = mosaic_tmpdir + str(unique[i]) + '.FITS'
        shutil.copy(summsfield, outfield)


def build_beam(obs_freq, centre, cell, imsize, out_beam):

    wcs = WCS(naxis=2)

    centre = coord.SkyCoord(centre[0], centre[1], unit=(
        u.hourangle, u.deg), frame='icrs')
    cell /= 3600.

    wcs.crpix = [imsize / 2, imsize / 2]
    wcs.cdelt = np.array([-cell, cell])
    wcs.crval = [centre.ra.deg, centre.dec.deg]
    wcs.ctype = ["RA---SIN", "DEC--SIN"]

    hdr = wcs.to_header()
    hdr['SIMPLE'] = 'T'
    hdr['BITPIX'] = -32
    hdr['NAXIS'] = 2
    hdr.set('NAXIS1', imsize, after='NAXIS')
    hdr.set('NAXIS2', imsize, after='NAXIS1')

    if 'CUNIT1' in hdr:
        del hdr['CUNIT1']
    if 'CUNIT2' in hdr:
        del hdr['CUNIT2']

    pb_fwhm = 1.02 * (2.99792458E8) / obs_freq / 13.5 / np.pi * 180.
    pb_fwhm_pix = pb_fwhm / hdr['CDELT2']
    x, y = np.meshgrid(np.linspace(-hdr['NAXIS2'] / 2., hdr['NAXIS2'] / 2., hdr['NAXIS2']),
                       np.linspace(-hdr['NAXIS1'] / 2., hdr['NAXIS1'] / 2., hdr['NAXIS1']))
    d = np.sqrt(x * x + y * y)
    sigma, mu = pb_fwhm_pix / 2.35482, 0.0
    gauss = np.exp(-((d - mu)**2 / (2.0 * sigma**2)))

    fits.writeto(out_beam, gauss, hdr, overwrite=True)

def pbcorr(beam, mosaic_regrid, mosaic_pbcorr):

    pblist = fits.open(beam)
    pbdata = pblist[0].data

    mlist = fits.open(mosaic_regrid)
    mdata = mlist[0].data
    mhead = mlist[0].header
    mhead['EPOCH'] = 2000

    if 'LONPOLE' in mhead:
        del mhead['LONPOLE']
    if 'LATPOLE' in mhead:
        del mhead['LATPOLE']
    if 'RADESYS' in mhead:
        del mhead['RADESYS']

    pbcorr = np.multiply(mdata, pbdata)
    fits.writeto(mosaic_pbcorr, pbcorr, mhead, overwrite=True)
    mlist.close()
    pblist.close()

def change_header(filename, copy_head, headfile):

    pblist = fits.open(filename)
    dat = pblist[0].data
    print('CHANGE THEE HEADER')
    if copy_head:
        hdrfile = fits.open(headfile)
        head = hdrfile[0].header

        if 'NAXIS3' in head:
            del head['NAXIS3']

        head['NAXIS'] = 2
        print(head['NAXIS'])
        dat = np.squeeze(dat)
        print(dat.shape)

    elif copy_head == False:

        head = pblist[0].header

        if 'ORIGIN' in head:
            del head['ORIGIN']
        if 'CUNIT1' in head:
            del head['CUNIT1']
        if 'CUNIT2' in head:
            del head['CUNIT2']

    fits.writeto(filename, dat, head, overwrite=True)

def make_mask(mosaic_pbcorr, mask, contour):

    moslist = fits.open(mosaic_pbcorr)
    mosdata = moslist[0].data
    moshead = moslist[0].header

    mosdata[np.isnan(mosdata)] = 0.0

    index_in = np.where(mosdata >= contour)
    index_out = np.where(mosdata <= contour)
    mosdata[index_in] = 1.0
    mosdata[index_out] = 0.0

    fits.writeto(mask, mosdata, moshead, overwrite=True)

def make_mask_nvss(catalog_table, centre, imsize, cell, mask):

    wcs = WCS(naxis=2)

    centre = coord.SkyCoord(centre[0], centre[1], unit=(
        u.hourangle, u.deg), frame='icrs')
    cell /= 3600.

    wcs.crpix = [imsize / 2, imsize / 2]
    wcs.cdelt = np.array([-cell, cell])
    wcs.crval = [centre.ra.deg, centre.dec.deg]
    wcs.ctype = ["RA---SIN", "DEC--SIN"]

    hdr = wcs.to_header()
    hdr['SIMPLE'] = 'T'
    hdr['BITPIX'] = -32
    hdr['NAXIS'] = 2
    hdr['EQUINOX'] = 2000.
    hdr.set('NAXIS1', imsize, after='NAXIS')
    hdr.set('NAXIS2', imsize, after='NAXIS1')

    if 'CUNIT1' in hdr:
        del hdr['CUNIT1']
    if 'CUNIT2' in hdr:
        del hdr['CUNIT2']

    data = np.zeros([hdr['NAXIS2'], hdr['NAXIS1']])

    tab = ascii.read(catalog_table)

    major = tab['MajAxis']
    minor = tab['MinAxis']
    minor = major

    pix_x = tab['PixX']
    pix_y = tab['PixY']

    xnum = np.linspace(0, hdr['NAXIS1'], hdr['NAXIS1'])
    ynum = np.linspace(0, hdr['NAXIS2'], hdr['NAXIS2'])
    x, y = np.meshgrid(xnum, ynum)

    for i in range(0, len(pix_x)):

        xc = pix_x[i]
        yc = pix_y[i]

        if minor[i] / 3600. >= float(hdr['CDELT2']) and major[i] / 3600. >= float(hdr['CDELT2']):
            a = major[i] / 3600. / float(hdr['CDELT2']) / 2.
            b = minor[i] / 3600. / float(hdr['CDELT2']) / 2.
            ell = np.power(x - xc, 2) / np.power(a, 2) + \
                np.power(y - yc, 2) / np.power(b, 2)
            index_ell = np.where(np.less_equal(ell, 1))
            data[index_ell] = 1
        else:
            data[int(yc), int(xc)] = 1

    fits.writeto(mask, data, hdr, overwrite=True)

def merge_masks(extended_mask, catalog_mask, end_mask):

    catlist = fits.open(catalog_mask)
    catdata = catlist[0].data
    cathead = catlist[0].header

    forlist = fits.open(extended_mask)
    fordata = forlist[0].data
    fordata = np.squeeze(fordata)
    fordata = np.squeeze(fordata)

    index_fornan = np.isnan(fordata)
    fordata[index_fornan] = catdata[index_fornan]
    index_forzero = np.where(fordata < 1)
    fordata[index_forzero] = catdata[index_forzero]

    fits.writeto(end_mask, fordata, cathead, overwrite=True)

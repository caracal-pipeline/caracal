# -*- coding: future_fstrings -*-
import os
import glob
import sys
import caracal
import numpy as np
from caracal.dispatch_crew import utils
from caracal.utils.requires import extras

NAME = "Mosaic 2D-images or cubes"
LABEL = 'mosaic'


@extras(packages="astropy")
def worker(pipeline, recipe, config):

    from astropy import units as u
    import astropy.coordinates as coord
    from astropy.io import fits
    from astropy import wcs

    wname = pipeline.CURRENT_WORKER

    ##########################################
    # Defining functions for the worker
    ##########################################

    # Not using anymore, but might need later
    def identify_last_selfcal_image(directory_to_check, prefix, field, mfsprefix):

        # Doing this because convergence may have been reached before the user-specified number of iterations
        matching_files = glob.glob(directory_to_check + '/{0:s}_{1:s}_*{2:s}-image.fits'.format(
            prefix, field, mfsprefix))  # '*' to pick up the number
        max_num = 0  # Initialisation

        for filename in matching_files:
            split_filename = filename.split('_')
            number = split_filename[-1].split('-')[0]
            num = int(number)
            if num > max_num:
                max_num = num

        filename_of_last_selfcal_image = '{0:s}_{1:s}_{2:s}{3:s}-image.fits'.format(
            prefix, field, str(max_num), mfsprefix)
        return filename_of_last_selfcal_image

    def identify_last_subdirectory(mosaictype):

        max_num = 0  # Initialisation

        # Subdirectory prefix depends on whether we are looking in pipeline.continuum or pipeline.cubes
        if mosaictype == 'continuum':
            directory_to_check = pipeline.continuum
            subdirectory_prefix = 'image_'
        else:  # i.e.  mosaictype == 'spectral'
            directory_to_check = pipeline.cubes
            subdirectory_prefix = 'cube_'

        matching_subdirectories = glob.glob(
            directory_to_check + '/' + subdirectory_prefix + '*')  # '*' to pick up the number

        for subdirectory in matching_subdirectories:
            split_subdirectory = subdirectory.split('_')
            # In case there is one or more '_' in the directory name, want to get the last portion
            number = split_subdirectory[-1]
            num = int(number)
            if num > max_num:
                max_num = num

        last_subdirectory = subdirectory_prefix + str(max_num)
        return max_num, last_subdirectory

    # Copied from masking_worker.py and edited. This is to get a Gaussian beam.
    def build_beam(obs_freq, centre, cell, imsize, out_beam):

        # if copy_head == True:
        #    hdrfile = fits.open(headfile)
        #    hdr = hdrfile[0].header
        # elif copy_head == False:

        w = wcs.WCS(naxis=2)

        # Using u.deg for both due to using 'CRVAL1' and 'CRVAL2' to set the centre
        centre = coord.SkyCoord(
            centre[0], centre[1], unit=(u.deg, u.deg), frame='icrs')
        # cell /= 3600.0  # Am assuming that cell was passed to the function in units of arcsec, so this is converting it to units of deg.
        # Commenting the above out as 'CDELT2' from the corresponding image will be passed to the function, and this is already in deg.

        # The '+ 1's are needed to avoid a shape mismatch later on
        w.wcs.crpix = [(imsize / 2) + 1, (imsize / 2) + 1]
        w.wcs.cdelt = np.array([-cell, cell])
        w.wcs.crval = [centre.ra.deg, centre.dec.deg]
        w.wcs.ctype = ["RA---SIN", "DEC--SIN"]

        hdr = w.to_header()
        hdr['SIMPLE'] = 'T'
        hdr['BITPIX'] = -32
        hdr['NAXIS'] = 2
        hdr.set('NAXIS1', imsize, after='NAXIS')
        hdr.set('NAXIS2', imsize, after='NAXIS1')

        if 'CUNIT1' in hdr:
            del hdr['CUNIT1']
        if 'CUNIT2' in hdr:
            del hdr['CUNIT2']

        # Units of m. The default assumes that MeerKAT data is being processed
        dish_diameter = config['dish_diameter']
        pb_fwhm_radians = 1.02 * (2.99792458E8 / obs_freq) / dish_diameter
        pb_fwhm = 180.0 * pb_fwhm_radians / np.pi   # Now in units of deg
        pb_fwhm_pix = pb_fwhm / hdr['CDELT2']
        x, y = np.meshgrid(np.linspace(-hdr['NAXIS2'] / 2.0, hdr['NAXIS2'] / 2.0, hdr['NAXIS2']),
                           np.linspace(-hdr['NAXIS1'] / 2.0, hdr['NAXIS1'] / 2.0, hdr['NAXIS1']))
        d = np.sqrt(x * x + y * y)
        sigma, mu = pb_fwhm_pix / 2.35482, 0.0  # sigma = FWHM/sqrt(8ln2)
        gaussian = np.exp(-((d - mu)**2 / (2.0 * sigma**2)))

        fits.writeto(out_beam, gaussian, hdr, overwrite=True)

    # Copied from line_worker.py and edited. This is to get a Mauchian beam.
    # The original version makes the build_beam function above redundant but I do not want to change too many things at once.
    def make_mauchian_pb(filename, freq):  # pbtype):
        with fits.open(filename) as image:
            headimage = image[0].header
            ang_offset = np.indices(
                (headimage['naxis2'], headimage['naxis1']), dtype=np.float32)
            ang_offset[0] -= (headimage['crpix2'] - 1)
            ang_offset[1] -= (headimage['crpix1'] - 1)
            ang_offset = np.sqrt((ang_offset**2).sum(axis=0))  # Using offset in x and y direction to calculate the total offset from the pointing centre
            ang_offset = ang_offset * np.abs(headimage['cdelt1'])  # Now offset is in units of deg
            # if pbtype == 'gaussian':
            #    sigma_pb = 17.52 / (freq / 1e+9) / dish_size / 2.355
            #    sigma_pb.resize((sigma_pb.shape[0], 1, 1))
            #    datacube = np.exp(-datacube**2 / 2 / sigma_pb**2)
            # elif pbtype == 'mauchian':
            FWHM_pb = (57.5 / 60) * (freq / 1.5e9)**-1  # Eqn 4 of Mauch et al. (2020), but in deg   # freq is just a float for the 2D case
            pb_image = (np.cos(1.189 * np.pi * (ang_offset / FWHM_pb)) / (
                1 - 4 * (1.189 * ang_offset / FWHM_pb)**2))**2  # Eqn 3 of Mauch et al. (2020)
            fits.writeto(filename.replace('image.fits', 'pb.fits'),
                         pb_image, header=headimage, overwrite=True)
            caracal.log.info('Created Mauchian primary-beam  FITS {0:s}'.format(
                filename.replace('image.fits', 'pb.fits')))

    def consistent_cdelt3(image_filenames, input_directory, nrdecimals):
        cdelt3s = []
        for ff in image_filenames:
            cc = fits.getval(ff, 'cdelt3')
            if cc not in cdelt3s:
                cdelt3s.append(cc)
        if len(cdelt3s) > 1:
            if nrdecimals:
                caracal.log.warn('Not all input cubes have the same CDELT3. Values found:')
                caracal.log.warn('    {0:}'.format(cdelt3s))
                caracal.log.warn('Rounding up the CDELT3 values to {0:d} decimals:'.format(nrdecimals))
                cdelt3s_r = []
                for cc in cdelt3s:
                    if round(cc, nrdecimals) not in cdelt3s_r:
                        cdelt3s_r.append(round(cc, nrdecimals))
                caracal.log.warn('    {0:}'.format(cdelt3s_r))
                if len(cdelt3s_r) > 1:
                    caracal.log.error('Rounding was insufficient, cannot proceed.')
                    raise caracal.BadDataError('Inconsistent CDELT3 values in input cubes.')
                else:
                    caracal.log.warn('Changing CDELT3 of all input image.fits and pb.fits cubes to {0:}'.format(cdelt3s_r[0]))
                    for ff in image_filenames:
                        fits.setval(ff, 'cdelt3', value=cdelt3s_r[0])
                        fits.setval(ff.replace('image.fits', 'pb.fits'), 'cdelt3', value=cdelt3s_r[0])
            else:
                caracal.log.error('Not all input cubes have the same CDELT3. Values found:')
                caracal.log.error('    {0:}'.format(cdelt3s))
                caracal.log.error('To proceed Please set mosaic:round_cdelt3 to round the CDELT3 values to an adequate number of decimals.')
                caracal.log.error('This will overwrite CDELT3 in the input cubes.')
                raise caracal.BadDataError('Inconsistent CDELT3 values in input cubes.')

    ##########################################
    # Main part of the worker
    ##########################################

    # Prioritise parameters specified in the config file, under the 'mosaic' worker
    # i.e. 'continuum' or 'spectral'
    specified_mosaictype = config['mosaic_type']
    use_mfs_images = config['use_mfs']
    specified_images = config['target_images']
    label = config['label_in']
    line_name = config['line_name']
    pb_type = config['pb_type']

    # Parameters that depend on the mosaictype
    if specified_mosaictype == 'spectral':
        pb_origin = 'generated by the line_worker'
    else:
        pb_origin = 'that are already in place (generated by the selfcal_worker, or during a previous run of the mosaic_worker)'

    # To ease finding the appropriate files, and to keep this worker self-contained
    if use_mfs_images:
        mfsprefix = '-MFS'
    else:
        mfsprefix = ''

    # please forget pipeline.dataid: it is now pipeline.msbasenames
    # pipeline.prefixes = ['{0:s}-{1:s}-{2:s}'.format(pipeline.prefix,did,config['label_in']) for did in pipeline.dataid]
    # In case there are different pipeline prefixes
    # for i in range(len(pipeline.prefixes)): ### I may need to put this loop back in later

    prefix = pipeline.prefix

    # Delete empty strings from list of specified images (as in default list = [''])
    while '' in specified_images:
        del (specified_images[specified_images.index('')])

    # If nothing is passed via the config file, then specified_images[0] adopts this via the schema
    if not len(specified_images):

        caracal.log.info(
            "No image names were specified via the config file, so they are going to be selected automatically.")
        caracal.log.info(
            "It is assumed that they are all in the highest-numbered subdirectory of 'general:output/continuum' and 'general:output/cubes'.")
        caracal.log.info(
            "You should check the selected image names. If unhappy with the selection, please specify the correct ones to use with mosaic:target_images.")

        # Needed for working out the field names for the targets, so that the correct files can be selected
        all_targets, all_msfile, ms_dict = pipeline.get_target_mss(label)
        n_targets = len(all_targets)
        caracal.log.info(
            'The number of targets to be mosaicked is {0:d}'.format(n_targets))

        # Where the targets are in the output directory
        max_num, last_subdirectory = identify_last_subdirectory(specified_mosaictype)

        # Empty list to add filenames to
        pathnames = []

        # Expecting the same prefix and mfsprefix to apply for all fields to be mosaicked together
        for target in all_targets:

            field = utils.filter_name(target)

            # Use the mosaictype to infer the filenames of the images
            if specified_mosaictype == 'continuum':  # Add name of 2D image output by selfcal_worker

                image_name = '{5:s}/{0:s}/{1:s}_{2:s}_{3:s}{4:s}-image.fits'.format(
                    last_subdirectory, prefix, field, str(max_num), mfsprefix, pipeline.continuum)
                specified_images.append(image_name)

            else:  # i.e. mosaictype = 'spectral', so add name of cube output by line_worker

                image_name = '{5:s}/{0:s}/{1:s}_{2:s}_{3:s}{4:s}-image.fits'.format(
                    last_subdirectory, prefix, field, line_name, mfsprefix, pipeline.cubes)
                if mfsprefix == '':
                    # Following the naming in line_worker
                    image_name = image_name.replace('-image', '.image')
                specified_images.append(image_name)

    caracal.log.info('PLEASE CHECK -- Images to be mosaicked are:')
    caracal.log.info(specified_images)

    # Although montage_mosaic checks whether pb.fits files are present, we need to do this earlier in the worker,
    # so that we can create simple Gaussian (or Mauchian) primary beams if need be
    for image_name in specified_images:

        pb_name = image_name.replace('image.fits', 'pb.fits')

        # Need the corresponding pathname for the image being considered
        index_to_use = specified_images.index(image_name)

        if os.path.exists(pb_name):
            caracal.log.info(
                '{0:s} is already in place, and will be used by montage_mosaic.'.format(pb_name))

        else:

            if specified_mosaictype == 'spectral':
                caracal.log.error(
                    '{0:s} does not exist. Please make sure that it is in place before proceeding.'.format(pb_name))
                caracal.log.error(
                    'You may need to re-run the line_worker with pb_cube enabled. EXITING.')
                raise caracal.ConfigurationError("missing primary beam file {}".format(pb_name))

            else:  # i.e. mosaictype == 'continuum'

                caracal.log.info(
                    '{0:s} does not exist, so going to create a pb.fits file instead.'.format(pb_name))

                if pb_type == 'gaussian':

                    # Create rudimentary primary-beam, which is assumed to be a Gaussian with FWMH = 1.02*lambda/D
                    image_hdu = fits.open(image_name)
                    image_header = image_hdu[0].header
                    # i.e. [ RA, Dec ]. Assuming that these are in units of deg.
                    image_centre = [image_header['CRVAL1'], image_header['CRVAL2']]
                    # Again assuming that these are in units of deg.
                    image_cell = image_header['CDELT2']
                    image_imsize = image_header['NAXIS1']

                    recipe.add(build_beam, 'build_gaussian_pb',
                               {
                                   # Units of Hz. The default assumes that MeerKAT data is being processed
                                   'obs_freq': config['ref_frequency'],
                                   'centre': image_centre,
                                   'cell': image_cell,
                                   'imsize': image_imsize,
                                   'out_beam': pb_name,
                               },
                               input=pipeline.input,
                               # Was pipeline=pipeline.output before the restructure of the output directory
                               output=pipeline.output,
                               label='build_gaussian_pb:: Generating {0:s}'.format(pb_name))

                    # Confirming freq and dish_diameter values being used for the primary beam
                    caracal.log.info('Observing frequency = {0:f} Hz, dish diameter = {1:f} m'.format(
                        config['ref_frequency'], config['dish_diameter']))
                    caracal.log.info('If these are not the values that you were expecting to be used for primary-beam creation, then '
                                     'please delete the newly-created beams and re-run the mosaic worker with ref_frequency and dish_diameter '
                                     'set in the config file.')

                else:  # i.e. pb_type == 'mauchian'

                    filename = image_name
                    freq = config['ref_frequency']  # Units of Hz. The default assumes that MeerKAT data is being processed
                    make_mauchian_pb(filename, freq)

                    # Confirming freq value being used for the primary beam
                    caracal.log.info('Observing frequency = {0:f} Hz'.format(freq))
                    if freq == 1383685546.875:  # i.e. if the default value was used
                        caracal.log.info('If you did not want this value (i.e. the default) to be used for primary-beam creation, then '
                                         'please delete the newly-created beams and re-run the mosaic worker with ref_frequency set in the config file.')
                    else:
                        caracal.log.info('as set via ref_frequency in the config file, and used for primary-beam creation.')

                pb_origin = 'generated by the mosaic_worker'

    caracal.log.info('Checking for *pb.fits files now complete.')

    # Will need it later, unless Sphe has a more elegant method
    original_working_directory = os.getcwd()

    caracal.log.info(
        'Now creating symlinks to images and beams, in case they are distributed across multiple subdirectories')
    # To get the symlinks created in the correct directory
    input_directory = pipeline.continuum if specified_mosaictype == 'continuum' else pipeline.cubes
    os.chdir(input_directory)

    # Empty list to add filenames to, as we are not to pass 'image_1', etc, to the recipe
    image_filenames = []

    # Start by assuming that 'image' is of the form 'image_1/image_filename'
    for specified_image in specified_images:
        split_imagename = specified_image.split('/')
        subdirectory = '/'.join(split_imagename[:-1])
        image_filename = split_imagename[-1]
        image_filenames.append(image_filename)

        if not specified_image.split(input_directory)[0]:
            specified_image = specified_image.replace(input_directory, '')
        else:
            specified_image = '{0:s}/{1:s}'.format('/'.join(['..' for ss in input_directory.split('/')]), specified_image)
        if specified_image[0] == '/':
            specified_image = specified_image[1:]

        symlink_for_image_command = 'ln -sf {0:s} {1:s}'.format(specified_image, image_filename)
        os.system(symlink_for_image_command)
        specified_beam = specified_image.replace('image.fits', 'pb.fits')
        beam_filename = image_filename.replace('image.fits', 'pb.fits')

        symlink_for_beam_command = 'ln -sf {0:s} {1:s}'.format(specified_beam, beam_filename)
        os.system(symlink_for_beam_command)

    # To get back to where we were before symlink creation
    os.chdir(original_working_directory)

    # Prefix of the output files should be either the default (pipeline.prefix) or that specified by the user via the config file
    mosaic_prefix = config['name']
    if mosaic_prefix == '':  # i.e. this has been set via the schema
        mosaic_prefix = pipeline.prefix

    # List of images in place, and have ensured that there are corresponding pb.fits files,
    # so now ready to add montage_mosaic to the caracal recipe

    image_filenames = ['{0:s}/{1:s}'.format(input_directory, ff) for ff in image_filenames]
    input_directory = '.'

    if specified_mosaictype == 'spectral':
        recipe.add(consistent_cdelt3, 'cdelt3_check',
                   {
                       "image_filenames": image_filenames,
                       "input_directory": input_directory,
                       "nrdecimals": config['round_cdelt3'],
                   },
                   input=input_directory,
                   output=pipeline.mosaics,
                   label='cdelt3_check')
        recipe.run()
        recipe.jobs = []

    if pipeline.enable_task(config, 'domontage'):
        recipe.add('cab/mosaicsteward', 'mosaic-steward',
                   {
                       "mosaic-type": specified_mosaictype,
                       "domontage": True,
                       "cutoff": config['cutoff'],
                       "name": mosaic_prefix,
                       "target-images": image_filenames,
                   },
                   input=input_directory,
                   output=pipeline.mosaics,
                   label='MosaicSteward:: Re-gridding {0:s} images before mosaicking them. For this mode, the mosaic_worker is using *pb.fits files {1:s}.'.format(specified_mosaictype, pb_origin))

    else:  # Written out for clarity as to what difference the 'domontage' setting makes
        recipe.add('cab/mosaicsteward', 'mosaic-steward',
                   {
                       "mosaic-type": specified_mosaictype,
                       "domontage": False,
                       "cutoff": config['cutoff'],
                       "name": mosaic_prefix,
                       "target-images": image_filenames,
                   },
                   input=input_directory,
                   output=pipeline.mosaics,
                   label='MosaicSteward:: Re-gridding of images and beams is assumed to be already done, so straight to mosaicking {0:s} images. For this mode, the mosaic_worker is using *pb.fits files {1:s}.'.format(specified_mosaictype, pb_origin))

    recipe.run()
    recipe.jobs = []

    # Set mosaic bunit, bmaj, bmin, bpa
    bunits, bmajs, bmins, bpas = [], [], [], []
    for ff in image_filenames:
        bunits.append(fits.getval(ff, 'bunit'))
        bmajs.append(fits.getval(ff, 'bmaj'))
        bmins.append(fits.getval(ff, 'bmin'))
        bpas.append(fits.getval(ff, 'bpa'))
    if np.unique(np.array(bunits)).shape[0] == 1:
        mosbunit = bunits[0]
    else:
        raise caracal.BadDataError('Inconsistent BUNIT values in input cubes. Cannot proceed')
    mosbmaj = np.median(np.array(bmajs))
    mosbmin = np.median(np.array(bmins))
    mosbpa = np.median(np.array(bpas))
    caracal.log.info('Setting BUNIT = {0:}, BMAJ = {1:}, BMIN = {2:}, BPA = {3:} in mosaic FITS headers'.format(mosbunit, mosbmaj, mosbmin, mosbpa))

    # Add missing keys and convert some keys from string to float in the mosaic FITS headers
    for ff in ['.fits', '_noise.fits', '_weights.fits']:
        fitsfile = '{0:s}/{1:s}{2:s}'.format(pipeline.mosaics, mosaic_prefix, ff)
        fits.setval(fitsfile, 'bunit', value=mosbunit)
        fits.setval(fitsfile, 'bmaj', value=mosbmaj)
        fits.setval(fitsfile, 'bmin', value=mosbmin)
        fits.setval(fitsfile, 'bpa', value=mosbpa)
        for hh in 'crval3,crval4,crpix3,crpix4,cdelt3,cdelt4,crota2'.split(','):
            try:
                fits.setval(fitsfile, hh, value=float(fits.getval(fitsfile, hh)))
                caracal.log.info('Header key {0:s} found and converted to float in file {1:s}'.format(hh, fitsfile))
            except BaseException:
                caracal.log.info('Header key {0:s} not found in file {1:s}'.format(hh, fitsfile))

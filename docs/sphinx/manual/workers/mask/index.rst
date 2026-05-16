.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _mask:
 
==========================================
mask
==========================================
 
.. toctree::
   :maxdepth: 1
 
Create .FITS mask from a catalog and (optionally) merge with an existing .FITS mask provided by the user. WARNING - At the moment this worker can only be executed on a single target at a time. Iterating over N targets is not done automatically.



.. _mask_enable:

--------------------------------------------------
**enable**
--------------------------------------------------

  *bool*

  Execute the mask worker.



.. _mask_label_in:

--------------------------------------------------
**label_in**
--------------------------------------------------

  *str*, *optional*, *default = corr*

  Label of the .MS file that contains information about the target.



.. _mask_label_out:

--------------------------------------------------
**label_out**
--------------------------------------------------

  *str*, *optional*, *default = catalog_mask*

  Prefix used for the name of the .FITS mask created by this worker. The full name consists of this prefix followed by the target name extracted by the observation_config worker. To use this output .FITS mask as a clean mask in the self_cal worker users should set relevant entry of cleanmask_method to label_out.



.. _mask_centre_coord:

--------------------------------------------------
**centre_coord**
--------------------------------------------------

  *list* *of str*, *optional*, *default = HH:MM:SS , DD:MM:SS*

  Coordinates of the centre of the field-of-view (read from reference_dir by default).



.. _mask_mask_size:

--------------------------------------------------
**mask_size**
--------------------------------------------------

  *int*, *optional*, *default = 1800*

  Number of pixels in the mask. This must be the same as img_npix in the selfcal worker.



.. _mask_cell_size:

--------------------------------------------------
**cell_size**
--------------------------------------------------

  *float*, *optional*, *default = 2.*

  Size of pixels in the mask, in units of arcsec. This must be the same as img_cell in the selfcal worker.



.. _mask_extended_source_map:

--------------------------------------------------
**extended_source_map**
--------------------------------------------------

  *str*, *optional*, *default = Fornaxa_vla.FITS*

  Name of the input mask for particularly-extended sources in the field.



.. _mask_catalog_query:

--------------------------------------------------
**catalog_query**
--------------------------------------------------

  Query catalog to select field/sources for constructing the mask.

  **enable**

    *bool*, *optional*, *default = true*

    Enable the 'query_catalog' segment.

  **catalog**

    *{"NVSS", "SUMSS"}*, *optional*, *default = SUMSS*

    Name of catalog to query. Options are 'NVSS' and 'SUMSS'.

  **image_width**

    *str*, *optional*, *default = 1.2d*

    Angular size of the region of sky that we want to mask (e.g. '1.2d', where 'd' indicates degrees). This should be kept larger than the dirty image.

  **nvss_thr**

    *float*, *optional*, *default = 10e-3*

    Flux-density threshold for selecting sources in the radio map, corrected for the primary beam. Value given is in units of Jy, or is the minimum signal-to-noise ratio (i.e. number of sigma_rms), used for SoFiA source-finding.



.. _mask_pbcorr:

--------------------------------------------------
**pbcorr**
--------------------------------------------------

  Apply a primary-beam correction to the input image before extracting the mask.

  **enable**

    *bool*, *optional*, *default = true*

    Enable the 'pb_correction' segment.

  **frequency**

    *float*, *optional*, *default = 1.420405752*

    Since the primary-beam size changes with frequency, provide the central frequency of the considered dataset.



.. _mask_make_mask:

--------------------------------------------------
**make_mask**
--------------------------------------------------

  Build mask from an existing image using SoFiA and/or a threshold cutoff.

  **enable**

    *bool*, *optional*, *default = true*

    Enable the 'make_mask' segment.

  **mask_method**

    *{"thresh", "sofia"}*, *optional*, *default = sofia*

    The tool to use for masking. Options are 'thresh' and 'sofia'.

  **input_image**

    *{"pbcorr", "path_to_mask"}*, *optional*, *default = pbcorr*

    Input image where to create mask ???? what is this ???

  **thr_lev**

    *int*, *optional*, *default = 5*

    Flux-density threshold for selecting sources in the SUMSS map, corrected for the primary beam. Value given is in units of Jy, or is the minimum signal-to-noise ratio (i.e. number of sigma_rms), used for SoFiA source-finding.

  **scale_noise_window**

    *int*, *optional*, *default = 101*

    Size of the window over which SoFiA measures the local rms, in units of pixels.



.. _mask_merge_with_extended:

--------------------------------------------------
**merge_with_extended**
--------------------------------------------------

  Merge newly-determined mask components with the existing mask for the extended source.

  **enable**

    *bool*, *optional*, *default = False*

    Execute segment 'merge_with_extended'.

  **extended_source_map**

    *str*, *optional*, *default = extended_mask.fits*

    Name of the mask-image of the extended source to merge with the current mask-image.

  **mask_method**

    *{"thresh", "sofia"}*, *optional*, *default = thresh*

    The tool to use for masking. Options are 'thresh' and 'sofia'.

  **thr_lev**

    *float*, *optional*, *default = 8e-2*

    Flux-density threshold for selecting sources in the SUMSS map, corrected for the primary beam. Value given is in units of Jy, or is the minimum signal-to-noise ratio (i.e. number of sigma_rms), used for SoFiA source-finding.



.. _mask_report:

--------------------------------------------------
**report**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  (Re)generate a full HTML report at the end of this worker.



.. _mask_cabs:

--------------------------------------------------
**cabs**
--------------------------------------------------

  *list* *of map*, *optional*, *default = ' '*

  Specifies non-default image versions and/or tags for Stimela cabs. Running with scissors: use with extreme caution.


.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _mosaic:
 
==========================================
mosaic
==========================================
 
.. toctree::
   :maxdepth: 1
 
Mosaic the 2D-images (or cubes) made with the selfcal/crosscal (or line) worker. If not available on disc, the primary beam is built by the mosaic worker, assuming a Gaussian shape with FWHM = 1.02 lambda/D.



.. _mosaic_enable:

--------------------------------------------------
**enable**
--------------------------------------------------

  *bool*

  Execute the mosaic worker.



.. _mosaic_mosaic_type:

--------------------------------------------------
**mosaic_type**
--------------------------------------------------

  *{"continuum", "spectral"}*

  Type of mosaic to be made, either continuum (2D) or spectral (3D).



.. _mosaic_target_images:

--------------------------------------------------
**target_images**
--------------------------------------------------

  *list* *of str*, *optional*, *default = ' '*

  List of .FITS images/cubes to be mosaicked. Their file names must end with "image.fits" and must include the path relative to the current working directory. These images/cubes MUST be located within (sub-directories of) the current working directory.



.. _mosaic_label_in:

--------------------------------------------------
**label_in**
--------------------------------------------------

  *str*, *optional*, *default = corr*

  For autoselection of images, this needs to match the label/label_cal setting used for the selfcal/crosscal worker (when mosaicking continuum images) or the label setting used for the line worker (when mosaicking cubes).



.. _mosaic_line_name:

--------------------------------------------------
**line_name**
--------------------------------------------------

  *str*, *optional*, *default = HI*

  Spectral mode only -- If autoselection is used to find the final cubes, this needs to match the line_name parameter used for the line worker.



.. _mosaic_use_mfs:

--------------------------------------------------
**use_mfs**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  Continuum mode only -- If the images to be mosaicked were created using MFS, in the selfcal or crosscal worker, then this needs to be indicated via this parameter.



.. _mosaic_name:

--------------------------------------------------
**name**
--------------------------------------------------

  *str*, *optional*, *default = ' '*

  The prefix to be used for output files. Default is the pipeline prefix, as set for the general worker.



.. _mosaic_domontage:

--------------------------------------------------
**domontage**
--------------------------------------------------

  Re-grid the input images, and associated beams.

  **enable**

    *bool*, *optional*, *default = True*

    Enable the 'domontage' (i.e. re-gridding) segment.



.. _mosaic_cutoff:

--------------------------------------------------
**cutoff**
--------------------------------------------------

  *float*, *optional*, *default = 0.1*

  The cutoff in the primary beam. It should be a number between 0 and 1.



.. _mosaic_pb_type:

--------------------------------------------------
**pb_type**
--------------------------------------------------

  *{"gaussian", "mauchian"}*, *optional*, *default = gaussian*

  If no continuum pb.fits are already in place, user needs to choose whether a rudimentary primary beam is created ('gaussian') or one that follows the model of Mauch et al. (2020), relevant for MeerKAT data ('mauchian').



.. _mosaic_dish_diameter:

--------------------------------------------------
**dish_diameter**
--------------------------------------------------

  *float*, *optional*, *default = 13.5*

  If 'pb_type' has been set to 'gaussian', user needs to specify the dish diameter (in units of m).



.. _mosaic_ref_frequency:

--------------------------------------------------
**ref_frequency**
--------------------------------------------------

  *float*, *optional*, *default = 1383685546.875*

  If no continuum pb.fits are already in place, user needs to specify the reference frequency (in units of Hz) so that primary beams can be created.



.. _mosaic_round_cdelt3:

--------------------------------------------------
**round_cdelt3**
--------------------------------------------------

  *int*, *optional*, *default = 0*

  If mosaic_type is "spectral", round CDELT3 in the header of the input cubes to the number of decimal digits given by this parameter (0 means do not round). This is useful when the CDELT3 values of the input cubes are not identical (which would make the mosaicking algorithm crash) but the differences are small and can be ignored. Note that the CDELT3 values of the input cubes are overwritten with the common, rounded value (if the rounding is sufficient to find a common value).



.. _mosaic_report:

--------------------------------------------------
**report**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  (Re)generate a full HTML report at the end of this worker.



.. _mosaic_cabs:

--------------------------------------------------
**cabs**
--------------------------------------------------

  *list* *of map*, *optional*, *default = ' '*

  Specifies non-default image versions and/or tags for Stimela cabs. Running with scissors: use with extreme caution.


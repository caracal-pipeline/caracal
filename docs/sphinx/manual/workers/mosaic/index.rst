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

  *{"continuum", "line"}*

  Type of mosaic to be made, either continuum (2D) or line (3D).



.. _mosaic_line_name:

--------------------------------------------------
**line_name**
--------------------------------------------------

  *str*, *optional*, *default = HI*

  Line mode only -- If autoselection is used to find the final cubes, this needs to match the line_name parameter used for the line worker.



.. _mosaic_use_mfs:

--------------------------------------------------
**use_mfs**
--------------------------------------------------

  *bool*, *optional*, *default = True*

  If the images to be mosaicked were created using MFS, in the selfcal or crosscal worker, then this needs to be indicated via this parameter. Ignored if mosaic_type:line.



.. _mosaic_label_in:

--------------------------------------------------
**label_in**
--------------------------------------------------

  *str*, *optional*, *default = corr*

  For autoselection of images/cubes to be mosaicked. For mosaic_type:continuum it should match selfcal:label_in, for mosaic_type:line it should match line:label_in.



.. _mosaic_target_images:

--------------------------------------------------
**target_images**
--------------------------------------------------

  *list* *of str*, *optional*, *default = ' '*

  For manual selection images/cubes to be mosaicked. They must be located within (sub-directories of) the parent of "general:output". Their file names must end with "image.fits" and must include the path relative to the parent of "general:output".



.. _mosaic_name:

--------------------------------------------------
**name**
--------------------------------------------------

  *str*, *optional*, *default = ' '*

  The prefix to be used for all output files. Default is the global pipeline prefix general:prefix.



.. _mosaic_num_workers:

--------------------------------------------------
**num_workers**
--------------------------------------------------

  *int*, *optional*, *default = 0*

  Number of worker threads to distribute the regridding of the input images / cubes to the output mosaic pixel grid. Default=0 means all available threads. The more the workers, the higher the RAM usage.



.. _mosaic_beam_cutoff:

--------------------------------------------------
**beam_cutoff**
--------------------------------------------------

  *float*, *optional*, *default = 0.1*

  The cutoff in the primary beam. The default of 0.1 means going down to the 10 percent level for each pointing. Set to zero for no primary beam cutoff.



.. _mosaic_mosaic_cutoff:

--------------------------------------------------
**mosaic_cutoff**
--------------------------------------------------

  *float*, *optional*, *default = 0.2*

  Sensitivity cutoff in the final mosaic. Pixels with a noise level > minimum mosaic noise / cutoff are blanked in all final products. E.g. The default of 0.2 means blanking in the mosaic all pixels with a noise level > 5x the minimum mosaic noise level. Set to zero for no cutoff (but some cutoff may still result from mosaic:beam_cutoff setting).



.. _mosaic_associated_mosaics:

--------------------------------------------------
**associated_mosaics**
--------------------------------------------------

  *list* *of str*, *optional*, *default = ' '*

  Also make mosaics of the associated .fits files, selecting between 'mask', 'model' and 'residual'. Multiple choices allowed. Give user choice as a list. The default is to not make any associated mosaics.



.. _mosaic_pb_type:

--------------------------------------------------
**pb_type**
--------------------------------------------------

  *{"gaussian", "mauchian"}*, *optional*, *default = gaussian*

  For mosaic_type:continuum only. If no continuum pb.fits are in place create them either as 2D Gaussians or as in Mauch et al. (2020).



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

  If continuum pb.fits need to be created, specify the reference frequency (in Hz) for appropriate scaling of the PB shape.



.. _mosaic_round_cdelt3:

--------------------------------------------------
**round_cdelt3**
--------------------------------------------------

  *int*, *optional*, *default = 0*

  For mosaic_type:line only. Round CDELT3 in the header of the input cubes to the number of decimal digits given by this parameter (0 means do not round). This is useful when the CDELT3 values of the input cubes are not identical (which would make the mosaicking algorithm crash) but the differences are small and can be removed with no impact on the science. Note that the CDELT3 values of the input cubes are overwritten with the common, rounded value (if the rounding is sufficient to find a common value). Use with caution.



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


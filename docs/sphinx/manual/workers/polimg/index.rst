.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _polimg:
 
==========================================
polimg
==========================================
 
.. toctree::
   :maxdepth: 1
 
Perform polarization imaging.



.. _polimg_enable:

--------------------------------------------------
**enable**
--------------------------------------------------

  *bool*

  Execute the polimg worker.



.. _polimg_label_in:

--------------------------------------------------
**label_in**
--------------------------------------------------

  *str*, *optional*, *default = corr*

  Label of the .MS files to process.



.. _polimg_rewind_flags:

--------------------------------------------------
**rewind_flags**
--------------------------------------------------

  Rewind flags of the input .MS file(s) to specified version. Note that this is not applied to .MS file(s) you might be running "transfer_apply_gains" on.

  **enable**

    *bool*, *optional*, *default = True*

    Enable segment rewind_flags.

  **mode**

    *{"reset_worker", "rewind_to_version"}*, *optional*, *default = reset_worker*

    If mode = 'reset_worker' rewind to the flag version before this worker if it exists, or continue if it does not exist; if mode = 'rewind_to_version' rewind to the flag version given by 'version' and 'transfer_apply_gains_version' below.

  **version**

    *str*, *optional*, *default = auto*

    Flag version to restore. This is applied to the .MS file(s) identified by "label" above. Set to "null" to skip this rewinding step. If 'auto' it will rewind to the version prefix_workername_before, where 'prefix' is set in the 'general' worker, and 'workername' is the name of this worker including the suffix '__X' if it is a repeated instance of this worker in the configuration file. Note that all flag versions saved after this version will be deleted.



.. _polimg_overwrite_flagvers:

--------------------------------------------------
**overwrite_flagvers**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  Allow CARACal to overwrite existing flag versions. Not recommended. Only enable this if you know what you are doing.



.. _polimg_ncpu:

--------------------------------------------------
**ncpu**
--------------------------------------------------

  *int*, *optional*, *default = 0*

  Number of CPUs used by WSClean. If set to 0 all available CPUs are used.



.. _polimg_stokes:

--------------------------------------------------
**stokes**
--------------------------------------------------

  *{"QU", "QUV", "IQU", "IQUV", "I", "Q", "U", "V"}*, *optional*, *default = QUV*

  Stokes image to create. For this worker the options are "QU","QUV","IQU","IQUV","I","Q","U","V". Before this step, we recomand to perform the I imaging with the selfcal worker and to do the polarization imaging on the self-calibrated MS.



.. _polimg_make_images:

--------------------------------------------------
**make_images**
--------------------------------------------------

  make images

  **enable**

    *bool*, *optional*, *default = True*

    List of options

  **minuvw_m**

    *int*, *optional*, *default = 0*

    Exclude baselines shorter than this value (given in metres) from the imaging and self-calibration loop.

  **img_npix**

    *int*, *optional*, *default = 1800*

    Number of pixels in output image.

  **img_padding**

    *float*, *optional*, *default = 1.3*

    Padding in WSClean. Set to -1 if you do not want to disable.

  **img_gain**

    *float*, *optional*, *default = 0.10*

    Fraction of the peak that is cleaned in each minor iteration. Set to -1 if you do not want to disable.

  **img_mgain**

    *float*, *optional*, *default = 0.90*

    Gain for major iterations in WSClean. I.e., maximum fraction of the image peak that is cleaned in each major iteration. A value of 1 means that all cleaning happens in the image plane and no major cycle is performed.

  **img_cell**

    *float*, *optional*, *default = 2.*

    Image pixel size (in units of arcsec).

  **img_weight**

    *{"briggs", "uniform", "natural"}*, *optional*, *default = briggs*

    Type of image weighting, where the options are 'briggs', 'uniform', and 'natural'. If 'briggs', set the 'img_robust' parameter.

  **img_robust**

    *float*, *optional*, *default = 0.*

    Briggs robust value.

  **img_mfs_weighting**

    *bool*, *optional*, *default = false*

    Enables MF weighting. Default is enabled.

  **img_taper**

    *str*, *optional*, *default = 0.*

    Gaussian taper for imaging (in units of arcsec).

  **img_maxuv_l**

    *float*, *optional*, *default = 0.*

    Taper for imaging (in units of lambda).

  **img_transuv_l**

    *float*, *optional*, *default = 10.*

    Transition length of tukey taper (taper-tukey in WSClean, in % of maxuv).

  **img_niter**

    *int*, *optional*, *default = 1000000*

    Number of cleaning iterations.

  **img_nmiter**

    *int*, *optional*, *default = 0*

    Number of major cycles.

  **img_nchans**

    *int*, *optional*, *default = 3*

    Number of channels in output image.

  **img_chan_range**

    *str*, *optional*, *default = ' '*

    Channel range to be imaged. Comma-separated integer values.

  **img_joinchans**

    *bool*, *optional*, *default = True*

    Join channels to create MFS image.

  **img_squared_chansjoin**

    *bool*, *optional*, *default = True*

    Search peaks in the sum of Q^2 and/or U^2 image to clean. If 'join-polarisations' will set to True peaks will be search in the sum of Q^2+U^2.

  **img_join_polarizations**

    *bool*, *optional*, *default = True*

    In combination with 'img_squared_chansjoin' will clean the image searching the peaks in the sum of Q^2+U^2.

  **img_specfit_nrcoeff**

    *int*, *optional*, *default = 2*

    Number of spectral polynomial terms to fit to each clean component. This is equal to the order of the polynomial plus 1. Use 0 to disable spectral fitting. Note that spectral fitting is required if you want to do subsequent continumm subtraction using crystalball.

  **img_multiscale**

    *bool*, *optional*, *default = False*

    Switch on multiscale cleaning.

  **img_multiscale_scales**

    *str*, *optional*, *default = ' '*

    Comma-separated integer scales for multiscale cleaning in pixels. If set to an empty string WSClean selects the scales automatically. These include the 0 scale, a scale calculated based on the beam size, and all scales obtained increasing the scale by a factor of 2 until the image size is reached.

  **img_nrdeconvsubimg**

    *int*, *optional*, *default = 0*

    Speed-up deconvolution by splitting the image into a number of subimages, which are deconvolved in parallel. This parameter sets the number of subimages as follows. If set to 1 no parallel deconvolution is performed. If set to 0 the number of subimages is the same as the number of CPUs used by the selfcal worker (see "ncpu" parameter above). If set to a number > 1 , the number of subimages is greater than or equal to the one requested by the user.

  **img_nwlayers_factor**

    *int*, *optional*, *default = 3*

    Use automatic calculation of the number of w-layers, but multiple that number by the given factor. This can e.g. be useful for increasing w-accuracy. In practice, if there are more cores available than the number of w-layers asked for then the number of w-layers used will equal the number of cores available. Set to -1 if you do not want to disable.

  **col**

    *str*, *optional*, *default = DATA, CORRECTED_DATA*

    Column(s) to image.

  **clean_cutoff**

    *float*, *optional*, *default = 0.5*

    Cleaning threshold to be used by WSClean. This is given as the number of sigma_rms to be cleaned down to, where sigma_rms is the noise level estimated by WSClean from the residual image before the start of every major deconvolution iteration.

  **cleanmask_method**

    *str*, *optional*, *default = wsclean*

    Method used to create the clean mask. The possible values are 1) 'wsclean' to use WSClean's auto-masking (threshold set by clean_mask_threshold below); 2) a prefix string to use an existing .FITS mask located in output/masking and called name.fits.

  **cleanmask_thr**

    *float*, *optional*, *default = 5.0*

    Threshold used to create the clean mask when WSClean. This is given as the number of sigma_rms to be cleaned down to, where sigma_rms is the (local) noise level. Set to -1 if you do not want to disable.

  **cleanmask_localrms**

    *bool*, *optional*, *default = False*

    Use a local-rms measurement when creating a clean mask with WSClean. This local-rms setting is also used for the clean_threshold above. Otherwise it is only used to define the clean mask, and clean_threshold is in terms of the global noise (rather than the local noise).

  **cleanmask_localrms_window**

    *int*, *optional*, *default = 31*

    Width of the window used to measure the local rms when creating the clean mask. The window width is in PSFs for clean_mask_method = 'wsclean'.

  **weighting_rank_filter**

    *float*, *optional*, *default = 3.*

    Filter the weights and set high weights to the local mean. The level parameter specifies the filter level; any value larger than level\*localmean will be set to level\*localmean.

  **absmem**

    *float*, *optional*, *default = 100.0*

    Specifies a fixed amount of memory in gigabytes.



.. _polimg_make_extra_images:

--------------------------------------------------
**make_extra_images**
--------------------------------------------------

  make convolved images and/or PB images.

  **enable**

    *bool*, *optional*, *default = False*

    List of options

  **schema**

    *str*, *optional*, *default = both*

    specify if you want single channel images (single), cubes (cube), or both (both).

  **convl_images**

    *bool*, *optional*, *default = False*

    Convolve output images to a common resolution

  **convl_beam**

    *str*, *optional*, *default = ' '*

    Target beam for images. If empty the target beam is the channel zero beam. Otherwise set as 'bmin, bmaj, bpa' in deg units.

  **make_pb_images**

    *bool*, *optional*, *default = True*

    Make PB images



.. _polimg_flagging_summary:

--------------------------------------------------
**flagging_summary**
--------------------------------------------------

  Output the flagging summary.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flagging_summary' segment.



.. _polimg_report:

--------------------------------------------------
**report**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  (Re)generate a full HTML report at the end of this worker.



.. _polimg_cabs:

--------------------------------------------------
**cabs**
--------------------------------------------------

  *list* *of map*, *optional*, *default = ' '*

  Specifies non-default image versions and/or tags for Stimela cabs. Running with scissors: use with extreme caution.


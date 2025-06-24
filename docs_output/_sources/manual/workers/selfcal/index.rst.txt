.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _selfcal:
 
==========================================
selfcal
==========================================
 
.. toctree::
   :maxdepth: 1
 
Perform self-calibration on the data.



.. _selfcal_enable:

--------------------------------------------------
**enable**
--------------------------------------------------

  *bool*

  Execute the selfcal worker.



.. _selfcal_label_in:

--------------------------------------------------
**label_in**
--------------------------------------------------

  *str*, *optional*, *default = corr*

  Label of the .MS files to process.



.. _selfcal_rewind_flags:

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

  **transfer_apply_gains_version**

    *str*, *optional*, *default = auto*

    Flag version to restore. This is applied to the .MS file(s) identified by "transfer_to_label" in the "transfer_apply_gains" section below. Set to "null" to skip this rewind step. If 'auto' it will rewind to the version prefix_workername_before, where 'prefix' is set in the 'general' worker, and 'workername' is the name of this worker including the suffix '__X' if it is a repeated instance of this worker in the configuration file. Note that all flag versions saved after this version will be deleted.



.. _selfcal_overwrite_flagvers:

--------------------------------------------------
**overwrite_flagvers**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  Allow CARACal to overwrite existing flag versions. Not recommended. Only enable this if you know what you are doing.



.. _selfcal_calibrate_with:

--------------------------------------------------
**calibrate_with**
--------------------------------------------------

  *{"meqtrees", "cubical"}*, *optional*, *default = cubical*

  Tool to use for calibration. Options are meqtrees and cubical.



.. _selfcal_spwid:

--------------------------------------------------
**spwid**
--------------------------------------------------

  *int*, *optional*, *default = 0*

  Provide spectral window ID.



.. _selfcal_ncpu:

--------------------------------------------------
**ncpu**
--------------------------------------------------

  *int*, *optional*, *default = 0*

  Number of CPUs to use for distributed processing. If set to 0 all available CPUs are used. This parameter is passed on to the following software in the selfcal worker, WSClean for imaging, Cubical and MeqTrees for calibration, PyBDSF for source finding.



.. _selfcal_minuvw_m:

--------------------------------------------------
**minuvw_m**
--------------------------------------------------

  *int*, *optional*, *default = 0*

  Exclude baselines shorter than this value (given in metres) from the imaging and self-calibration loop.



.. _selfcal_img_npix:

--------------------------------------------------
**img_npix**
--------------------------------------------------

  *int*, *optional*, *default = 1800*

  Number of pixels in output image.



.. _selfcal_img_padding:

--------------------------------------------------
**img_padding**
--------------------------------------------------

  *float*, *optional*, *default = 1.3*

  Padding in WSClean.



.. _selfcal_img_gain:

--------------------------------------------------
**img_gain**
--------------------------------------------------

  *float*, *optional*, *default = 0.10*

  Fraction of the peak that is cleaned in each minor iteration.



.. _selfcal_img_mgain:

--------------------------------------------------
**img_mgain**
--------------------------------------------------

  *float*, *optional*, *default = 0.90*

  Gain for major iterations in WSClean. I.e., maximum fraction of the image peak that is cleaned in each major iteration. A value of 1 means that all cleaning happens in the image plane and no major cycle is performed.



.. _selfcal_img_cell:

--------------------------------------------------
**img_cell**
--------------------------------------------------

  *float*, *optional*, *default = 2.*

  Image pixel size (in units of arcsec).



.. _selfcal_img_weight:

--------------------------------------------------
**img_weight**
--------------------------------------------------

  *{"briggs", "uniform", "natural"}*, *optional*, *default = briggs*

  Type of image weighting, where the options are 'briggs', 'uniform', and 'natural'. If 'briggs', set the 'img_robust' parameter.



.. _selfcal_img_robust:

--------------------------------------------------
**img_robust**
--------------------------------------------------

  *float*, *optional*, *default = 0.*

  Briggs robust value.



.. _selfcal_img_mfs_weighting:

--------------------------------------------------
**img_mfs_weighting**
--------------------------------------------------

  *bool*, *optional*, *default = false*

  Enables MF weighting. Default is enabled.



.. _selfcal_img_taper:

--------------------------------------------------
**img_taper**
--------------------------------------------------

  *str*, *optional*, *default = 0.*

  Gaussian taper for imaging (in units of arcsec).



.. _selfcal_img_maxuv_l:

--------------------------------------------------
**img_maxuv_l**
--------------------------------------------------

  *float*, *optional*, *default = 0.*

  Taper for imaging (in units of lambda).



.. _selfcal_img_transuv_l:

--------------------------------------------------
**img_transuv_l**
--------------------------------------------------

  *float*, *optional*, *default = 10.*

  Transition length of tukey taper (taper-tukey in WSClean, in % of maxuv).



.. _selfcal_img_niter:

--------------------------------------------------
**img_niter**
--------------------------------------------------

  *int*, *optional*, *default = 1000000*

  Number of cleaning iterations.



.. _selfcal_img_nmiter:

--------------------------------------------------
**img_nmiter**
--------------------------------------------------

  *int*, *optional*, *default = 0*

  Number of major cycles.



.. _selfcal_img_cleanborder:

--------------------------------------------------
**img_cleanborder**
--------------------------------------------------

  *float*, *optional*, *default = 1.3*

  Clean border.



.. _selfcal_img_nchans:

--------------------------------------------------
**img_nchans**
--------------------------------------------------

  *int*, *optional*, *default = 3*

  Number of channels in output image.



.. _selfcal_img_channelrange:

--------------------------------------------------
**img_channelrange**
--------------------------------------------------

  *list* *of int*, *optional*, *default = -1*

  Only image the given channel range. Indices specify channel indices, end index is exclusive. .e.g. 0, 1023. Default '-1' means all channels.



.. _selfcal_img_joinchans:

--------------------------------------------------
**img_joinchans**
--------------------------------------------------

  *bool*, *optional*, *default = True*

  Join channels to create MFS image.



.. _selfcal_img_specfit_nrcoeff:

--------------------------------------------------
**img_specfit_nrcoeff**
--------------------------------------------------

  *int*, *optional*, *default = 2*

  Number of spectral polynomial terms to fit to each clean component. This is equal to the order of the polynomial plus 1. Use 0 to disable spectral fitting. Note that spectral fitting is required if you want to do subsequent continumm subtraction using crystalball.



.. _selfcal_img_stokes:

--------------------------------------------------
**img_stokes**
--------------------------------------------------

  *{"I"}*, *optional*, *default = I*

  Stokes image to create.



.. _selfcal_img_multiscale:

--------------------------------------------------
**img_multiscale**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  Switch on multiscale cleaning.



.. _selfcal_img_multiscale_scales:

--------------------------------------------------
**img_multiscale_scales**
--------------------------------------------------

  *str*, *optional*, *default = ' '*

  Comma-separated integer scales for multiscale cleaning in pixels. If set to an empty string WSClean selects the scales automatically. These include the 0 scale, a scale calculated based on the beam size, and all scales obtained increasing the scale by a factor of 2 until the image size is reached.



.. _selfcal_img_multiscale_bias:

--------------------------------------------------
**img_multiscale_bias**
--------------------------------------------------

  *float*, *optional*, *default = 0.6*

  Comma-separated set of biases for multiscale cleaning. This balances between how sensitive the algorithm is towards large scales compared to smaller scales. Lower values will clean larger scales earlier and deeper. Its default is 0.6, which means something like “if a peak is 0.6 times larger at a 2x larger scale, select the larger scale”



.. _selfcal_img_nonegative:

--------------------------------------------------
**img_nonegative**
--------------------------------------------------

  *bool*, *optional*, *default = ' '*

  Do not allow negative components during cleaning



.. _selfcal_img_nrdeconvsubimg:

--------------------------------------------------
**img_nrdeconvsubimg**
--------------------------------------------------

  *int*, *optional*, *default = 1*

  Speed-up deconvolution by splitting the image into a number of subimages, which are deconvolved in parallel. This parameter sets the number of subimages as follows. If set to 1 no parallel deconvolution is performed. If set to 0 the number of subimages is the same as the number of CPUs used by the selfcal worker (see "ncpu" parameter above). If set to a number > 1 , the number of subimages is greater than or equal to the one requested by the user.



.. _selfcal_img_nwlayers_factor:

--------------------------------------------------
**img_nwlayers_factor**
--------------------------------------------------

  *int*, *optional*, *default = 3*

  Use automatic calculation of the number of w-layers, but multiple that number by the given factor. This can e.g. be useful for increasing w-accuracy. In practice, if there are more cores available than the number of w-layers asked for then the number of w-layers used will equal the number of cores available.



.. _selfcal_img_sofia_settings:

--------------------------------------------------
**img_sofia_settings**
--------------------------------------------------

  SoFiA source finder settings used for the imaging iterations whose entry in 'image/cleanmask_method' below is 'sofia'. The resulting clean mask is located in <output>/masking.

  **kernels**

    *list* *of float*, *optional*, *default = 0., 3., 6., 9.*

    FWHM of spatial Gaussian kernels in pixels.

  **pospix**

    *bool*, *optional*, *default = True*

    Merges only positive pixels of sources in mask.

  **flag**

    *bool*, *optional*, *default = False*

    Set whether flag regions are to be used (True) or not (False).

  **flagregion**

    *list* *of str*, *optional*, *default = ' '*

    Pixel/channel range(s) to be flagged prior to source finding. Format is [[x1, x2, y1, y2, z1, z2], ...].

  **inputmask**

    *str*, *optional*, *default = ' '*

    User-provided input-mask that will be (regridded if needed and) added onto the SoFiA mask.



.. _selfcal_img_breizorro_settings:

--------------------------------------------------
**img_breizorro_settings**
--------------------------------------------------

  Breizorro settings used for the imaging iterations whose entry in 'image/cleanmask_method' below is 'breizorro'. The resulting clean mask is located in <output>/masking.

  **boxsize**

    *int*, *optional*, *default = 50*

    Box size over which to compute stats (default = 50)

  **dilate**

    *int*, *optional*, *default = 0*

    Apply dilation with a radius of R pixels

  **fill_holes**

    *bool*, *optional*, *default = false*

    Fill holes (i.e. entirely closed regions) in mask



.. _selfcal_cal_niter:

--------------------------------------------------
**cal_niter**
--------------------------------------------------

  *int*, *optional*, *default = 2*

  Number of self-calibration iterations to perform.



.. _selfcal_start_iter:

--------------------------------------------------
**start_iter**
--------------------------------------------------

  *int*, *optional*, *default = 1*

  Start selfcal iteration loop at this start value (1-indexed).



.. _selfcal_cal_gain_cliplow:

--------------------------------------------------
**cal_gain_cliplow**
--------------------------------------------------

  *float*, *optional*, *default = 0.5*

  Lower threshold for clipping on gain amplitude.



.. _selfcal_cal_gain_cliphigh:

--------------------------------------------------
**cal_gain_cliphigh**
--------------------------------------------------

  *float*, *optional*, *default = 2.*

  Upper threshold for clipping on gain amplitude.



.. _selfcal_cal_timeslots_chunk:

--------------------------------------------------
**cal_timeslots_chunk**
--------------------------------------------------

  *int*, *optional*, *default = -1*

  Chunk data up by this number of timeslots. This limits the amount of data processed at once. Smaller chunks allow for a smaller RAM footprint and greater parallelism but sets an upper limit on the time solution intervals that may be employed. 0 means 'use the full time-axis' but does not cross scan boundaries. -1 means 'use the largest solution interval'.



.. _selfcal_cal_model_mode:

--------------------------------------------------
**cal_model_mode**
--------------------------------------------------

  *{"vis_only", "pybdsm_only", "pybdsm_vis"}*, *optional*, *default = vis_only*

  Mode for using a calibration model, based on visibilities and/or PyBDSM source-finding. Options are vis_only, pybdsm_only, and pybdsm_vis. 'vis_only' means that only MODEL_DATA will be used to to calibrate. 'pybdsm_only' means that PyBDSM-generated, tigger-format local sky models will be used. 'pybdsm_vis' is the same as the 'pybdsm_only' mode except for the last iteration of selfcal, where the PyBDSM-based model is complemented by MODEL_DATA. This third mode is only to be used with output_data set to 'CORR_RES' (below) and is very tricky. Therefore, user discretion is advised.



.. _selfcal_cal_bjones:

--------------------------------------------------
**cal_bjones**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  Enable calculation of the B-Jones matrix, for bandpass calibration.



.. _selfcal_cal_cubical:

--------------------------------------------------
**cal_cubical**
--------------------------------------------------

  Parameters that only apply when using CubiCal for the calibration.

  **max_prior_error**

    *float*, *optional*, *default = 0.3*

    Flag solution intervals where the prior variance estimate is above this value.

  **max_post_error**

    *float*, *optional*, *default = 0.3*

    Flag solution intervals where the posterior variance estimate is above this value.

  **chan_chunk**

    *int*, *optional*, *default = -1*

    Chunk data up by this number of channels. This limits the amount of data processed at once. Smaller chunks allow for a smaller RAM footprint and greater parallelism but sets an upper limit on the frequency solution intervals that may be employed. 0 means 'use the full frequency-axis' but does not cross SPW boundaries. -1 means 'use the largest solution interval'.

  **weight_col**

    *str*, *optional*, *default = WEIGHT*

    Column with weights for use in CubiCal.

  **shared_mem**

    *str*, *optional*, *default = 100Gb*

    Set the amount of shared memory for CubiCal.

  **flag_madmax**

    *bool*, *optional*, *default = True*

    Flags based on maximum of mad in CubiCal.

  **madmax_flag_thr**

    *list* *of int*, *optional*, *default = 0, 10*

    Threshold for madmax flagging in CubiCal, where the provided list works exactly as described in CubiCal readthedocs for the parameter --madmax-threshold.

  **solterm_niter**

    *list* *of int*, *optional*, *default = 50, 50, 50*

    Number of iterations per Jones term for CubiCal. Always a 3 digit array with iterations for 'G,B,GA' even when B or GA are not used.

  **overwrite**

    *bool*, *optional*, *default = True*

    Allow CubiCal to overwrite the existing gain_tables and other CubiCal output for self-calibration that were produced in a previous run of the selfcal worker with the same prefix.

  **dist_max_chunks**

    *int*, *optional*, *default = 4*

    Maximum number of time/freq data-chunks to load into memory simultaneously. If set to 0, then as many data-chunks as possible will be loaded.

  **out_derotate**

    *bool*, *optional*, *default = False*

    Explicitly enables or disables derotation of output visibilities. Default (None) is to use the –model-pa-rotate and –model-feed-rotate settings.

  **model_pa_rotate**

    *bool*, *optional*, *default = False*

    Apply parallactic angle rotation to model visibilities. Enable this for alt-azmounts, unless your model visibilities are already rotated.

  **model_feed_rotate**

    *str*, *optional*, *default = ' '*

    Apply a feed angle rotation to the model visibilities. Use 'auto' to read angles from FEED subtable, or give an explicit value in degrees.

  **gain_plot**

    Use cubical gain-plotter to plot diagnostic plots for self-calibration.

    **enable**

      *bool*, *optional*, *default = False*

      Enable the plotting of diagnostics, using gain-plotter.

    **diag**

      *{"ri", "ap", "none"}*, *optional*, *default = ap*

      Plot diagonal elements as real/imag or amp/phase.

    **off_diag**

      *{"ri", "ap", "none"}*, *optional*, *default = none*

      Also plot off-diagonal elements as real/imag or amp/phase.

    **nrow**

      *int*, *optional*, *default = 6*

      Number of plot rows.

    **ncol**

      *int*, *optional*, *default = 12*

      Number of plot columns.



.. _selfcal_cal_meqtrees:

--------------------------------------------------
**cal_meqtrees**
--------------------------------------------------

  Parameters that only apply when using MeqTrees for the calibration.

  **two_step**

    *bool*, *optional*, *default = False*

    Trigger a two-step calibration process in MeqTrees where the phase-only calibration is applied before continuing with amplitude + phase-calibration. Aimfast is turned on to determine the solution sizes automatically.



.. _selfcal_aimfast:

--------------------------------------------------
**aimfast**
--------------------------------------------------

  Quality assessment parameter.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'aimfast' segment.

  **tol**

    *float*, *optional*, *default = 0.02*

    Relative change in weighted mean of metrics (specified via convergence_criteria below) from aimfast.

  **convergence_criteria**

    *list* *of str*, *optional*, *default = ' '*

    The residual statistic to check convergence against. Every metric/criterion listed will be combined into a weighted mean. Options are 'DR' (dynamic range), 'MEAN' (mean of the residual flux), 'STDDev' (standard deviation), 'SKEW' (skewness, 3rd-moment), and 'KURT' (kurtosis, 4th-moment). However, note that when cal_model_mode = 'vis_only', 'DR' is no longer an option. Default is '', which means no convergence is checked.

  **area_factor**

    *int*, *optional*, *default = 6*

    A multiplicative factor that sets the total area over which the metrics are calculated, where total_area = psf_size\*area_factor. This area is centred on the position of peak flux-density in the image.

  **radius**

    *float*, *optional*, *default = 0.6*

    Cross-matching radius (in units of arcsec), for comparing source properties in a catalogue before and after an iteration of self-calibration.

  **normality_model**

    *{"normaltest", "shapiro"}*, *optional*, *default = normaltest*

    The type of normality test, to use for testing how well the residual image is modelled by a normal distribution. Options are 'normaltest' (i.e. D'Agostino) and 'shapiro'.

  **plot**

    *bool*, *optional*, *default = False*

    Generate html plots for comparing catalogues and residuals.

  **online_catalog**

    Perform an online catalog comparison

    **enable**

      *bool*, *optional*, *default = False*

      Enable online comparison

    **catalog_type**

      *{"nvss", "sumss"}*, *optional*, *default = nvss*

      Online catalog type to compare local models



.. _selfcal_image:

--------------------------------------------------
**image**
--------------------------------------------------

  Imaging parameters.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'image' segment.

  **col**

    *list* *of str*, *optional*, *default = DATA, CORRECTED_DATA*

    Column(s) to image.

  **clean_cutoff**

    *list* *of float*, *optional*, *default = 0.5, 0.5*

    Cleaning threshold to be used by WSClean. This is given as the number of sigma_rms to be cleaned down to, where sigma_rms is the noise level estimated by WSClean from the residual image before the start of every major deconvolution iteration.

  **cleanmask_method**

    *list* *of str*, *optional*, *default = wsclean, wsclean*

    Method used to create the clean mask. The possible values are 1) 'wsclean' to use WSClean's auto-masking (threshold set by clean_mask_threshold below); 2) 'sofia' to create a clean mask using SoFiA (threshold set by clean_mask_threshold below, and additional settings in sofia_settings, do not use if output_data = CORR_RES ); 3) 'breizorro'  to create a clean mask using Breizorro (threshold set by clean_mask_threshold below, and additional settings in breizorro_settings; 4) a prefix string to use an existing .FITS mask located in output/masking and called prefix_target.fits, where the name of the target is set automatically by the pipeline. The latter .FITS mask could be the one created by the masking worker, in which case the prefix set here should correspond to label_out in the masking worker. Note that this third  maskingm ethod can be used on multiple targets in a single pipeline run as long as they all have a corresponding prefix_target.fits mask in output/masking.

  **cleanmask_thr**

    *list* *of float*, *optional*, *default = 10.0, 6.0*

    Threshold used to create the clean mask when clean_mask_method = 'wsclean', 'sofia' or 'breizorro'. This is given as the number of sigma_rms to be cleaned down to, where sigma_rms is the (local) noise level.

  **cleanmask_localrms**

    *list* *of bool*, *optional*, *default = False, False*

    Use a local-rms measurement when creating a clean mask with clean_mask_method = 'wsclean' or 'sofia'. If clean_mask_method = 'wsclean', this local-rms setting is also used for the clean_threshold above. Otherwise it is only used to define the clean mask, and clean_threshold is in terms of the global noise (rather than the local noise).

  **cleanmask_localrms_window**

    *list* *of int*, *optional*, *default = 31, 31*

    Width of the window used to measure the local rms when creating the clean mask. The window width is in pixels for clean_mask_method = 'sofia', and in PSFs for clean_mask_method = 'wsclean'.

  **ncpu_img**

    *int*, *optional*, *default = 0*

    Number of threads used by wsclean; has a default value of '0'. If specified in the configuration file, will overrule the value set by ncpu, which is the global default for both cubical and wsclean

  **absmem**

    *float*, *optional*, *default = 100.0*

    Specifies a fixed amount of memory in gigabytes.

  **nr_parallel_grid**

    *int*, *optional*, *default = 1*

    Will execute multiple gridders simultaneously when using w-stacking. When parallel gridding is not possible, or when image sizes are reasonably large (say 5k-10k), the w-gridder might be a better choice.

  **use_wgridder**

    *bool*, *optional*, *default = False*

    Use the w-gridding gridder developed by Martin Reinecke. Otherwise, the default will be w-stacking.



.. _selfcal_extract_sources:

--------------------------------------------------
**extract_sources**
--------------------------------------------------

  Source-finding parameters.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'extract_sources' segment.

  **sourcefinder**

    *{"pybdsm", "sofia"}*, *optional*, *default = pybdsm*

    Set the source finder to be used. Options are 'pybdsm' (i.e. pybdsf) and 'sofia'.

  **local_rms**

    *bool*, *optional*, *default = False*

    Use a local-rms estimate when applying the source-finding detection threshold.

  **spi**

    *bool*, *optional*, *default = False*

    Extract the spectral index for the fitted sources.

  **thr_pix**

    *list* *of int*, *optional*, *default = 5*

    Pixel threshold to be used for the source finder. I.e. the minimum number of contiguous pixels for emission to be classed as a 'source'.

  **thr_isl**

    *list* *of int*, *optional*, *default = 3*

    Threshold to be used by the source finder to set the island boundary, given in the number of sigma above the mean. This determines the extent of the island used for fitting.

  **detection_image**

    *bool*, *optional*, *default = False*

    Constrain the PyBDSM source-finding to only find sources included in the clean model.

  **breizorro_image**

    Use breizorro image.

    **enable**

      *bool*, *optional*, *default = False*

      Use a breizorro product image to perform source finding in order to do source comparison.

    **sum_to_peak**

      *float*, *optional*, *default = 500*

      Sum to peak ratio of flux islands to mask in original image. Default = 500, will mask everything with a ratio above 500.



.. _selfcal_calibrate:

--------------------------------------------------
**calibrate**
--------------------------------------------------

  Calibration parameters.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'calibrate' segment.

  **model**

    *list* *of str*, *optional*, *default = 1,2*

    Model number to use, or a combination of models. E.g. '1+2' to use the first and second models for calibration.

  **output_data**

    *list* *of str*, *optional*, *default = CORR_DATA*

    Data to output after calibration. Options are 'PA_DATA', 'CORR_RES', 'CORR_DATA' or 'CORRECTED_DATA', where CORR_DATA and CORRECTED_DATA are synonyms. Note that for 'PA_DATA' only parallactic angle corrections will be applied to the data to produce 'CORRECTED_DATA' column.

  **gain_matrix_type**

    *list* *of str*, *optional*, *default = Fslope, Fslope*

    Gain matrix type. 'GainDiagPhase' = phase-only calibration, 'GainDiagAmp' = amplitude only, 'GainDiag' = Amplitude + Phase, 'Gain2x2' = Amplitude + Phase taking non-diagonal terms into account, 'Fslope' = delay selfcal (for which solution intervals should be set to at least twice the values you would use for GainDiagPhase). Note that Fslope does not work with MeqTrees.

  **gsols_timeslots**

    *list* *of int*, *optional*, *default = 2*

    G-Jones time solution interval. The parameter cal_timeslots_chunk above should be a multiple of Gsols_time. 0 entails using a single solution for the full time of the observations.

  **gsols_chan**

    *list* *of int*, *optional*, *default = 0*

    G-Jones frequency solution interval. The parameter chan_chunk in calibrate section should a multiple of Gsols_channel. 0 entails using a single solution for the full bandwidth.

  **bsols_timeslots**

    *list* *of int*, *optional*, *default = 0*

    B-Jones solutions for individual calibration steps in time.

  **bsols_chan**

    *list* *of int*, *optional*, *default = 2*

    B-Jones solutions for individual calibration steps in frequency.

  **gasols_timeslots**

    *list* *of int*, *optional*, *default = -1*

    Time intervals for amplitude calibration in CubiCal. 0 indicates average all. -1 defaults to Gsols_timeslots. If different from Gsols_timeslots, a second matrix is used and applied.

  **gasols_chan**

    *list* *of int*, *optional*, *default = -1*

    Channel intervals for amplitude calibration in CubiCal. 0 indicates average all. -1 defaults to Gsols_channel. If different from Gsols_channels, a second matrix is used and applied.



.. _selfcal_restore_model:

--------------------------------------------------
**restore_model**
--------------------------------------------------

  Take the modelled source(s) and restore it(/them) to the final, calibrated residual image.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'restore_model' segment.

  **model**

    *str*, *optional*, *default = 1+2*

    Model number to use, or a combination of models. E.g. '1+2' to use the first and second models.

  **clean_model**

    *str*, *optional*, *default = 3*

    Clean model number to use, or combination of clean models. E.g. '1+2' to use the first and second clean models.



.. _selfcal_flagging_summary:

--------------------------------------------------
**flagging_summary**
--------------------------------------------------

  Output the flagging summary.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flagging_summary' segment.



.. _selfcal_transfer_apply_gains:

--------------------------------------------------
**transfer_apply_gains**
--------------------------------------------------

  Interpolate gains over the high frequency-resolution data.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'transfer_apply_gains' segment.

  **transfer_to_label**

    *str*, *optional*, *default = corr*

    Label of cross-calibrated .MS file to which to transfer and apply the selfcal gains.

  **interpolate**

    To interpolate the gains or not to interpolate the gains. That is indeed the question.

    **enable**

      *bool*, *optional*, *default = True*

      Enable gain interpolation.

    **timeslots_int**

      *int*, *optional*, *default = -1*

      Solution interval in time (units of timeslots/integration time) for transferring gains. -1 means use the solution interval from the calibration that is applied.

    **chan_int**

      *int*, *optional*, *default = -1*

      Solution interval in frequency (units of channels) for transferring gains. -1 means use the solution interval from the calibration that is applied.

    **timeslots_chunk**

      *int*, *optional*, *default = -1*

      Time chunk in units of timeslots for transferring gains with CubiCal. -1 means use the solution interval from the calibration that is applied.

    **chan_chunk**

      *int*, *optional*, *default = -1*

      Frequency chunk in units of channels for transferring gains with CubiCal. '0' means the whole spw. -1 means use the solution interval from the calibration that is applied.



.. _selfcal_transfer_model:

--------------------------------------------------
**transfer_model**
--------------------------------------------------

  Transfer the model from the last WSClean imaging run to the MODEL_DATA column of another .MS .

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'transfer_model' segment.

  **transfer_to_label**

    *str*, *optional*, *default = corr*

    Label of the .MS file to which to transfer the model.

  **model**

    *str*, *optional*, *default = auto*

    Name of the sky model file. (Currently the only supported format is that of WSClean component lists.) When 'auto', the pipeline builds the file name from the input parameters of the selfcal loop. The file is assumed to be in the 'output' directory.

  **row_chunks**

    *int*, *optional*, *default = 0*

    Number of rows of the input .MS that are processed in a single chunk.

  **model_chunks**

    *int*, *optional*, *default = 0*

    Number of sky model components that are processed in a single chunk.

  **within**

    *str*, *optional*, *default = ' '*

    Give the JS9 region file. Only sources within those regions will be included.

  **points_only**

    *bool*, *optional*, *default = False*

    Select only 'point' sources.

  **num_sources**

    *int*, *optional*, *default = 0*

    Select only N brightest sources.

  **num_workers**

    *int*, *optional*, *default = 0*

    Explicitly set the number of worker threads. Default is 0, meaning it uses all threads.

  **mem_frac**

    *float*, *optional*, *default = 0.05*

    Fraction of system RAM that can be used. Used when setting automatically the chunk size.



.. _selfcal_report:

--------------------------------------------------
**report**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  (Re)generate a full HTML report at the end of this worker.



.. _selfcal_cabs:

--------------------------------------------------
**cabs**
--------------------------------------------------

  *list* *of map*, *optional*, *default = ' '*

  Specifies non-default image versions and/or tags for Stimela cabs. Running with scissors: use with extreme caution.


.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _ddcal:
 
==========================================
ddcal
==========================================
 
.. toctree::
   :maxdepth: 1
 
Perform direction-dependent calibration on the data (SHARED-RISK DEVELOPMENT MODE).



.. _ddcal_enable:

--------------------------------------------------
**enable**
--------------------------------------------------

  *bool*

  Execute the ddcal worker (i.e. carry out DD-calibration).



.. _ddcal_label_in:

--------------------------------------------------
**label_in**
--------------------------------------------------

  *str*, *optional*, *default = corr*

  Label of the .MS files to process. By default uses the 'corr' label_in for self-calibrated dataset.



.. _ddcal_use_pb:

--------------------------------------------------
**use_pb**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  Enable primary beam usage in making the DD-corrected DDFacet image. Note that this is EXPERIMENTAL and currently only available for MeerKAT data.



.. _ddcal_shared_mem:

--------------------------------------------------
**shared_mem**
--------------------------------------------------

  *int*, *optional*, *default = 400*

  Shared memory for tasks in units of GBs. Does not work with singularity.



.. _ddcal_image_dd:

--------------------------------------------------
**image_dd**
--------------------------------------------------

  Imaging parameters for DD calibration with DDFacet.

  **enable**

    *bool*, *optional*, *default = True*

    Enable the 'image_dd' segment.

  **npix**

    *int*, *optional*, *default = 8000*

    Number of pixels in the image. Note that DDFacet has its own super-special scheme to decide the actual number of pixels, so this is only an approximation.

  **use_mask**

    *bool*, *optional*, *default = True*

    Enable clean mask for DDFacet initial imaging. Note that this doubles the imaging time since it runs DDFacet twice -- once to get a preliminary image to make a mask with (mask is made by the cleanmask tool), and once to get the final image with masking. Previous WSClean masks cannot be used because pixel numbers might be different.

  **mask_sigma**

    *float*, *optional*, *default = 10.0*

    The number of standard deviations (i.e. sigma_rms) to use when clipping the initial image for masking.

  **mask_boxes**

    *int*, *optional*, *default = 9*

    Divide the initial image (for making the mask) into this number of boxes, then perform sigma clipping in each of these boxes.

  **mask_niter**

    *int*, *optional*, *default = 20*

    The number of sigma-clipping iterations to perform on the image, for masking, or set to 0 to clip until convergence is achieved.

  **mask_overlap**

    *float*, *optional*, *default = 0.3*

    Overlap region for the boxes, given as a fraction of the number of boxes.

  **mask_tol**

    *float*, *optional*, *default = 0.75*

    Tolerance for dilating the mask. Dilation will stop when the percentage difference between dilations is smaller than this value.

  **cell**

    *float*, *optional*, *default = 1.3*

    Pixel size in arcsec.

  **facets_nfacets**

    *int*, *optional*, *default = 24*

    Number of facets to use, and is the same as the Facets-NFacets parameter of DDFacet.

  **weight_col**

    *{"WEIGHT_SPECTRUM", "WEIGHT", "IMAGING_WEIGHT"}*, *optional*, *default = WEIGHT*

    Read data weights from the specified column. Options are WEIGHT_SPECTRUM, WEIGHT, and (for rarer occasions) IMAGING_WEIGHT.

  **weight_mode**

    *{"Natural", "Uniform", "Robust", "Briggs"}*, *optional*, *default = Briggs*

    UV weighting mode. Options are 'Natural', 'Uniform', 'Robust', and 'Briggs'.

  **weight_robust**

    *float*, *optional*, *default = -0.4*

    Briggs robustness parameter, from -2 (more uniform) to 2 (more natural).

  **deconv_maxminoriter**

    *int*, *optional*, *default = 100000*

    Number of clean iterations.

  **freq_nband**

    *int*, *optional*, *default = 10*

    Number of frequency bands for gridding.

  **freq_ndegridband**

    *int*, *optional*, *default = 15*

    Number of frequency bands for degridding. 0 means degrid each channel.

  **deconv_rmsfactor**

    *float*, *optional*, *default = 0.0*

    Set the minor-cycle stopping-threshold to X\*{residual RMS}, where X is this parameter value.

  **deconv_peakfactor**

    *float*, *optional*, *default = 0.25*

    Set the minor-cycle stopping-threshold to X\*{peak residual}, where X is this parameter value.

  **deconv_mode**

    *{"HMP", "Hogbom", "SSD", "GAClean"}*, *optional*, *default = Hogbom*

    The deconvolution algorithm to use. Options are 'HMP' (Hybrid Matching Pursuit, aka multiscale/multifrequency), 'Hogbom' (Hogbom's CLEAN algorithm), 'SSD' (SubSpace Deconvolution algorithm), and 'GAClean' (Genetic Algorithm Clean). Please direct queries to DDFacet Developers for further details.

  **deconv_gain**

    *float*, *optional*, *default = 0.1*

    Gain setting for the deconvolution loops.

  **deconv_fluxthr**

    *float*, *optional*, *default = 1.0e-6*

    Absolute flux-density threshold at which deconvolution is stopped, in units of Jy. Relevant for HMP and Hogbom modes.

  **deconv_allownegative**

    *bool*, *optional*, *default = True*

    Allow negative components for cleaning (valid for HMP and Hogbom modes).

  **hogbom_polyfitorder**

    *int*, *optional*, *default = 6*

    Order of the polynomial to be used for frequency fitting.

  **parallel_ncpu**

    *int*, *optional*, *default = 0*

    Number of processes / threads to use in parallel mode. 0 = use all of those available. 1 = disable parallelism.

  **predict_colname**

    *str*, *optional*, *default = MODEL_DATA*

    MS column to write the predicted visibilities corresponding to the model. Setting '' will disable this parameter.

  **log_memory**

    *bool*, *optional*, *default = True*

    Log the memory usage by DDFacet.

  **cache_reset**

    *bool*, *optional*, *default = True*

    Reset all caches (including PSF and dirty image). Change from default at your own risk.

  **log_boring**

    *bool*, *optional*, *default = True*

    Enable progress bars and other pretty console output. Doesn't seem to work. But who knows, try it out.

  **data_colname**

    *str*, *optional*, *default = CORRECTED_DATA*

    Data column to use for initial imaging. Defaults to 'CORRECTED_DATA', the assumption being that self-calibration has already been done on the measurement set.

  **data_colname_postcal**

    *str*, *optional*, *default = SUBDD_DATA*

    Data column to use for imaging after dd-calibration. Defaults to 'SUBDD_DATA', so as to not overwrite the corrected data. If data size increase is a concern, switch to 'CORRECTED_DATA'

  **data_chunkhours**

    *float*, *optional*, *default = 0.05*

    Chunk data into time bins of X hours to conserve memory, where X is this parameter.

  **output_mode**

    *{"Dirty", "Clean", "Predict", "PSF"}*, *optional*, *default = Clean*

    Output mode of DDFacet. Options are 'Dirty', 'Clean', 'Predict', and 'PSF'. This setting defaults to 'Clean', since that is what we want to do in this worker.



.. _ddcal_calibrate_dd:

--------------------------------------------------
**calibrate_dd**
--------------------------------------------------

  Direction-dependent calibration parameters.

  **enable**

    *bool*, *optional*, *default = True*

    Enable the 'calibrate_dd' segment.

  **sigma**

    *float*, *optional*, *default = 4.5*

    Sigma threshold to use in detecting outlier regions in images, via CATDagger (which is enabled by setting 'de_sources_mode', below, to 'auto'). The default value of 4.5 works well, but a lower value may be needed for some images.

  **min_dist_from_phcentre**

    *int*, *optional*, *default = 1300*

    The radius (in number of pixels), from the centre of the image, out to which sources will not be tagged for DD-calibration. (This is because sources close to the phase centre may not have been cleaned deeply enough.) The default is kept at 1300 (which roughly corresponds to 30').

  **dist_ncpu**

    *int*, *optional*, *default = 1*

    The number of cpus for distributed computing.

  **de_sources_mode**

    *str*, *optional*, *default = manual*

    Mode in which sources are tagged for DD calibration. Options are 'auto' (which uses CATDagger), and 'manual' (for which one needs to provide a list of sources). Use 'auto' with caution and at your own risk.

  **de_target_manual**

    *list* *of str*, *optional*, *default = ' '*

    List of target fieldnames for carrying out DD calibration. The remaining fields will not undergo DD calibration.

  **de_sources_manual**

    *list* *of str*, *optional*, *default = ' '*

    List of sources per target to tag for DD calibration, in the same order as the 'de_target_manual' list. Use ';' to separate different sources per target.

  **sol_min_bl**

    *float*, *optional*, *default = 100*

    The minimum baseline length to solve for.

  **madmax_enable**

    *bool*, *optional*, *default = true*

    Enable madmax flagging in CubiCal.

  **madmax_thr**

    *list* *of int*, *optional*, *default = 0, 10*

    Threshold for MAD flagging per baseline (specified in number of standard deviations). Residuals exceeding mad-thr\*MAD/1.428 will be flagged. MAD is computed per baseline. This can be specified as a list e.g. N1,N2,N3,... The first value is used to flag residuals before a solution starts (use 0 to disable), the next value is used when the residuals are first recomputed during the solution several iterations later (see -chi-int), etc. A final pass may be done at the end of the solution. The last value in the list is reused if necessary. Using a list with gradually-decreasing values may be sensible.

  **madmax_global_thr**

    *list* *of int*, *optional*, *default = 0, 12*

    Threshold for global median MAD (MMAD) flagging. MMAD is computed as the median of the per-baseline MADs. Residuals exceeding S\*MMAD/1.428 will be flagged.

  **madmax_estimate**

    *{"corr", "all", "diag", "offdiag"}*, *optional*, *default = corr*

    MAD estimation mode. Use 'corr' for a separate estimate per baseline and correlation. Otherwise, a single estimate per baseline is computed using 'all' correlations, or only the 'diag' or 'offdiag' correlations.

  **dd_data_col**

    *str*, *optional*, *default = CORRECTED_DATA*

    Column to calibrate, with the assumption that you have already run the selfcal worker.

  **dd_out_data_col**

    *str*, *optional*, *default = SUBDD_DATA*

    Output data column. Note that the ddcal worker is currently hardcoded for this being set to 'SUBDD_DATA'.

  **dd_weight_col**

    *str*, *optional*, *default = WEIGHT*

    Column to read weights from, and apply them by default. Specify an empty string to disable this parameter.

  **dd_sol_stall_quorum**

    *float*, *optional*, *default = 0.95*

    Minimum percentage of solutions that must have stalled before terminating the solver.

  **dd_g_type**

    *str*, *optional*, *default = complex-2x2*

    Gain matrix type for the G-Jones matrix. Keep this set to 'complex-2x2', because DD-calibration fails otherwise.

  **dd_g_clip_high**

    *float*, *optional*, *default = 1.5*

    Amplitude clipping -- flag solutions with any amplitudes above this value for G-Jones matrix.

  **dd_g_clip_low**

    *float*, *optional*, *default = 0.5*

    Amplitude clipping -- flag solutions with any amplitudes below this value for G-Jones matrix.

  **dd_g_update_type**

    *str*, *optional*, *default = phase-diag*

    Determines update type. This does not change the Jones solver type, but restricts the update rule to pin the solutions within a certain subspace.

  **dd_g_max_prior_error**

    *float*, *optional*, *default = 0.35*

    Flag solution intervals where the prior error estimate is above this value for G-Jones matrix.

  **dd_g_max_post_error**

    *float*, *optional*, *default = 0.35*

    Flag solution intervals where the posterior variance estimate is above this value for G-Jones matrix.

  **dd_dd_max_prior_error**

    *float*, *optional*, *default = 0.35*

    Flag solution intervals where the prior error estimate is above this value for DE term.

  **dd_dd_max_post_error**

    *float*, *optional*, *default = 0.35*

    Flag solution intervals where the posterior variance estimate is above this value for DE term.

  **dd_g_timeslots_int**

    *int*, *optional*, *default = 10*

    Time solution interval in timeslot units for G-Jones matrix.

  **dd_g_chan_int**

    *int*, *optional*, *default = 0*

    Frequency solution interval in channel units for G-Jones matrix.

  **dd_dd_timeslots_int**

    *int*, *optional*, *default = 100*

    Time solution interval in timeslot units for DE-Jones matrix.

  **dd_dd_chan_int**

    *int*, *optional*, *default = 100*

    Frequency solution interval in channel units for DE-Jones matrix.

  **dist_nworker**

    *int*, *optional*, *default = 0*

    Number of processes.



.. _ddcal_copy_data:

--------------------------------------------------
**copy_data**
--------------------------------------------------

  Copy DD-calibrated data to CORRECTED_DATA column. THIS IS DANGEROUS - only if you want to go ahead with line imaging.

  **enable**

    *bool*, *optional*, *default = True*

    Enable copying of DD-calibrated data to CORRECTED_DATA column.



.. _ddcal_image_wsclean:

--------------------------------------------------
**image_wsclean**
--------------------------------------------------

  WSClean imaging paramaters for ddcal worker.

  **enable**

    *bool*, *optional*, *default = True*

    Enable WSClean imaging of the DD-calibrated data.

  **img_ws_npix**

    *int*, *optional*, *default = 1800*

    Number of pixels in output image.

  **img_ws_padding**

    *float*, *optional*, *default = 1.3*

    Padding in WSClean.

  **img_ws_mgain**

    *float*, *optional*, *default = 0.90*

    Gain for the major cycle during image CLEANing.

  **img_ws_cell**

    *float*, *optional*, *default = 2.*

    Image pixel size (in arcsec).

  **img_ws_weight**

    *{"briggs", "uniform", "natural"}*, *optional*, *default = briggs*

    Image weighting type. Options are 'briggs', 'uniform', and 'natural'. If 'briggs', set the img_ws_robust parameter below.

  **img_ws_robust**

    *float*, *optional*, *default = 0.*

    Briggs robust value.

  **img_ws_uvtaper**

    *str*, *optional*, *default = 0*

    Taper for imaging (in arcsec).

  **img_ws_niter**

    *int*, *optional*, *default = 1000000*

    Number of cleaning iterations.

  **img_ws_nmiter**

    *int*, *optional*, *default = 0*

    Number of major cycles.

  **img_ws_cleanborder**

    *float*, *optional*, *default = 1.3*

    Clean border.

  **img_ws_nchans**

    *int*, *optional*, *default = 3*

    Number of channels in output image.

  **img_ws_joinchans**

    *bool*, *optional*, *default = True*

    Join channels to create MFS image.

  **img_ws_specfit_nrcoeff**

    *int*, *optional*, *default = 2*

    Number of spectral polynomial terms to fit to each clean component. This is equal to the order of the polynomial plus 1.

  **img_ws_stokes**

    *{"I"}*, *optional*, *default = I*

    Stokes image to create. For this first release of CARACal, the only option is 'I'.

  **img_ws_auto_mask**

    *float*, *optional*, *default = 7*

    Auto-masking threshold, given as the number of sigma_rms.

  **img_ws_auto_thr**

    *float*, *optional*, *default = 0.5*

    Auto-clean threshold, given as the number of sigma_rms.

  **img_ws_col**

    *str*, *optional*, *default = CORRECTED_DATA*

    Column to image.

  **img_ws_fits_mask**

    *str*, *optional*, *default = catalog_mask.fits*

    Filename of fits mask (in output/masking folder).

  **img_ws_multi_scale**

    *bool*, *optional*, *default = False*

    Switch on multiscale cleaning.

  **img_ws_multi_scale_scales**

    *list* *of int*, *optional*, *default = 0, 5, 10, 20*

    Scales for multiscale cleaning, in pixels.

  **img_ws_local_rms**

    *bool*, *optional*, *default = False*

    Switch on local-rms measurement for cleaning.



.. _ddcal_transfer_model_dd:

--------------------------------------------------
**transfer_model_dd**
--------------------------------------------------

  Repredict WSClean model to the highest channel resolution.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'transfer_model_dd' segment.

  **dd_model**

    *str*, *optional*, *default = auto*

    Name of the sky-model file. (Currently the only supported format is that of WSClean component lists.) When set to 'auto', the pipeline builds the file name from the input parameters of the selfcal loop. The file is assumed to be in the 'output' directory.

  **dd_row_chunks**

    *int*, *optional*, *default = 0*

    Number of rows of input .MS that are processed in a single chunk.

  **dd_model_chunks**

    *int*, *optional*, *default = 0*

    Number of sky-model components that are processed in a single chunk.

  **dd_within**

    *str*, *optional*, *default = ' '*

    Give JS9 region file. Only sources within those regions will be included.

  **dd_points_only**

    *bool*, *optional*, *default = False*

    Select only 'point' sources.

  **dd_num_sources**

    *int*, *optional*, *default = 0*

    Select only N brightest sources.

  **dd_num_workers**

    *int*, *optional*, *default = 0*

    Explicitly set the number of worker threads. Default is 0, meaning it uses all threads.

  **dd_mem_frac**

    *float*, *optional*, *default = 0.5*

    Fraction of system RAM that can be used. Used when setting automatically the chunk size.



.. _ddcal_report:

--------------------------------------------------
**report**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  (Re)generate a full HTML report at the end of this worker.



.. _ddcal_cabs:

--------------------------------------------------
**cabs**
--------------------------------------------------

  *list* *of map*, *optional*, *default = ' '*

  Specifies non-default image versions and/or tags for Stimela cabs. Running with scissors: use with extreme caution.


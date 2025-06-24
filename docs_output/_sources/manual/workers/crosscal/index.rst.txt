.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _crosscal:
 
==========================================
crosscal
==========================================
 
.. toctree::
   :maxdepth: 1
 
Carry out Cross calibration of the data (delay, bandpass and gain calibration).



.. _crosscal_enable:

--------------------------------------------------
**enable**
--------------------------------------------------

  *bool*

  Execute the crosscal worker.



.. _crosscal_label_in:

--------------------------------------------------
**label_in**
--------------------------------------------------

  *str*

  Label of the .MS file(s) to work on.



.. _crosscal_label_cal:

--------------------------------------------------
**label_cal**
--------------------------------------------------

  *str*, *optional*, *default = 1gc1*

  Label for output files (calibration tables, images).



.. _crosscal_rewind_flags:

--------------------------------------------------
**rewind_flags**
--------------------------------------------------

  Rewind flags to specified version.

  **enable**

    *bool*, *optional*, *default = True*

    Enable the rewind_flags segement.

  **mode**

    *{"reset_worker", "rewind_to_version"}*, *optional*, *default = reset_worker*

    If mode = 'reset_worker' rewind to the flag version before this worker if it exists, or continue if it does not exist; if mode = 'rewind_to_version' rewind to the flag version given by 'version' below.

  **version**

    *str*, *optional*, *default = auto*

    Flag version to rewind to. If 'auto' it will rewind to the version prefix_workername_before, where 'prefix' is set in the 'general' worker, and 'workername' is the name of this worker including the suffix '__X' if it is a repeated instance of this worker in the configuration file. Note that all flag versions that had been saved after this version will be deleted.



.. _crosscal_overwrite_flagvers:

--------------------------------------------------
**overwrite_flagvers**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  Allow Caracal to overwrite existing flag versions. Not recommended. Only enable this if you know what you are doing.



.. _crosscal_uvrange:

--------------------------------------------------
**uvrange**
--------------------------------------------------

  *str*, *optional*, *default = >50*

  Select what UV range should be used throughout this worker following the CASA notation (e.g., ">100"). The default units are metres but other units can be used (e.g., ">0.5klambda").



.. _crosscal_set_model:

--------------------------------------------------
**set_model**
--------------------------------------------------

  Fill in the MODEL column of the .MS file(s) for the field selected by "field" below in preparation for crosscalibration. This can use CASA setjy for point-source models, or MeqTrees for available local sky models.

  **enable**

    *bool*, *optional*, *default = True*

    Execute the set_model segment.

  **meerkat_band**

    *{"L", "UHF"}*, *optional*, *default = L*

    Select the MeerKAT observation band (only 'L 'or 'UHF' accepted for now). Set to 'L' by default. If both "meerkat_skymodel" and "meerkat_band" are enabled, then custom models of calibrator fields are used. For UHF band only models for PKS 1934-638 and PKS 408-65 are currently available. It is adviced to use them to avoid effects of calibrator field substructure.

  **meerkat_skymodel**

    *bool*, *optional*, *default = False*

    Use the MeerKAT local sky model (lsm) of the calibrator field instead of a point source model. At the moment a MeerKAT lsm is only available for the calibrator PKS 1934-638. For the calibrator 0408-6545 a model is available but is not well tested yet and we do not recommend using it.

  **meerkat_crystalball_skymodel**

    *bool*, *optional*, *default = False*

    Use the MeerKAT sky model of the calibrator field made for Crystalball.

  **meerkat_crystalball_memory_fraction**

    *float*, *optional*, *default = 0.5*

    Fraction of system RAM that can be used when using Crystalball to predict calibrator visibilities in the model column. Used when setting automatically the chunk size.

  **meerkat_crystalball_ncpu**

    *int*, *optional*, *default = 0*

    Explicitly set the number of worker threads. Default is 0, meaning it uses all threads.

  **meerkat_crystalball_num_sources**

    *int*, *optional*, *default = 0*

    Select only N brightest sources when using Crystalball to predict calibrator visibilities in the model column.

  **meerkat_crystalball_row_chunks**

    *int*, *optional*, *default = 0*

    Number of rows of input .MS that are processed in a single chunk when using Crystalball to predict calibrator visibilities in the model column. If zero, it will be set automatically.

  **meerkat_crystalball_model_chunks**

    *int*, *optional*, *default = 0*

    Number of model components that are processed in a single chunk when using Crystalball to predict calibrator visibilities in the model column. If zero, it will be set automatically.

  **unity**

    *bool*, *optional*, *default = False*

    Enables setting the calibrator to unity.

  **field**

    *str*, *optional*, *default = fcal*

    Set the field to execute the set_model segment on. Specify either the field number, field name or field specification as per obsconf worker (e.g., "fcal").

  **tile_size**

    *int*, *optional*, *default = 128*

    Size of tile (time bins) to process. Can be used to reduce memory footprint.

  **threads**

    *int*, *optional*, *default = 8*

    Number of threads used by MeqTrees if meerkat_skymodel above is enabled.



.. _crosscal_primary:

--------------------------------------------------
**primary**
--------------------------------------------------

  Calibrating on the bandpass/flux calibrator field.

  **reuse_existing_gains**

    *bool*, *optional*, *default = False*

    Reuse gain tables if they exist. Note that this does not check whether the existing tables were obtained with the same Caracal settings. Use with caution.

  **order**

    *str*, *optional*, *default = KGB*

    Order in which to solve for gains for this field. E.g, if order is set to 'KGB', then we solve for delays, then gains and finally bandpass. The full options are 1) K - delay calibration, 2) G - gain calibration (decide whether to solve for amplitude, phase or  both with 'calmode' below), 3) B - bandpass calibration, 4) A - automatic flagging with CASA tfcrop (existing gains will be applied first).

  **calmode**

    *list* *of str*, *optional*, *default = a, ap, ap*

    For each step in 'order' above, set whether to solve for phase ('p'), amplitude ('a') or both ('ap'). This is actually only relevant when solving for the gains, i.e., for the G steps in 'order' above. However, users should include an entry (even just an empty string) for all steps in 'order'.

  **solint**

    *list* *of str*, *optional*, *default = 120s, 120s, inf*

    For each step in 'order' above, set the solution interval. Set to 'inf' to obtain a single solution (see also 'combine' below). Include time units, e.g., '120s' or '2min'.

  **combine**

    *list* *of str*, *optional*, *default = '', '', scan*

    For each step in 'order' above, set along what axis the data should be combined before solving. Options are '' (i.e., no data combination; solutions break at obs, scan, field, and spw boundarie), 'obs', 'scan', 'spw', 'field'. To combine along multiple axes use comma-separated axis names in a single string, e.g., 'obs,scan'. This setting is only relevant for the steps of type K, G and B included in 'order' above. For A steps this setting is ignored and an empty string may be used.

  **b_solnorm**

    *bool*, *optional*, *default = False*

    Normalise average solution amplitude to 1.0

  **b_fillgaps**

    *int*, *optional*, *default = 70*

    Fill flagged channels in the bandpass solutions by interpolation.

  **b_smoothwindow**

    *int*, *optional*, *default = 1*

    Size of the mean running window for smoothing of the bandpass (in channels). A size of 1 means no smoothing.

  **scanselection**

    *str*, *optional*, *default = ' '*

    String specifying (in CASA format) which scans to select during solving on primary

  **spw_k**

    *str*, *optional*, *default = ' '*

    Only use this subset(s) of the band to compute 'K' gains. Default uses full band

  **spw_g**

    *str*, *optional*, *default = ' '*

    Only use this subset(s) of the band to compute 'GF' gains. Default uses full band

  **plotgains**

    *bool*, *optional*, *default = True*

    Plot gains with ragavi-gains. The .html plots are located in <output>/diagnostic_plots/crosscal/.

  **flag**

    Flagging settings used for all "A" (= auto-flagging) steps included in "order" above. These steps include applying the existing gains and flagging the corrected data.

    **col**

      *{"corrected", "residual"}*, *optional*, *default = corrected*

      Data column to flag on

    **usewindowstats**

      *{"none", "sum", "std", "both"}*, *optional*, *default = std*

      Calculate additional flags using sliding window statistics

    **combinescans**

      *bool*, *optional*, *default = False*

      Accumulate data across scans depending on the value of ntime

    **flagdimension**

      *{"freq", "time", "freqtime", "timefreq"}*, *optional*, *default = freqtime*

      Dimensions along which to calculate fits (freq/time/freqtime/timefreq)

    **timecutoff**

      *float*, *optional*, *default = 4.0*

      Flagging thresholds in units of deviation from the fit

    **freqcutoff**

      *float*, *optional*, *default = 3.0*

      Flagging thresholds in units of deviation from the fit

    **correlation**

      *str*, *optional*, *default = ' '*

      Correlation



.. _crosscal_secondary:

--------------------------------------------------
**secondary**
--------------------------------------------------

  Calibrating on the gain calibrator field.

  **reuse_existing_gains**

    *bool*, *optional*, *default = False*

    Reuse gain tables if they exist. Note that this does not check whether the existing tables were obtained with the same Caracal settings. Use with caution.

  **apply**

    *str*, *optional*, *default = B*

    Calibration terms solved for in the primary segment that should be applied to the secondary calibrator before solving for the terms in 'order' below.

  **order**

    *str*, *optional*, *default = KGAF*

    Order of the calibration/flagging/imaging steps for the secondary calibrator. E.g, if order is set to 'KGAF', we solve for delays, then for gains, after that the existing gains (KG) are applied before flagging the calibrated data, and finally, we solve for gains and transfer the flux scale from the primary step. The full options are 1) K - delay calibration; 2) G - gain calibration (set whether to solve for amplitude, phase or both with 'calmode' below); 3) F - same as G, but imedietly followed by a fluxscale. Note that a G table must exist from the primary step for this work; 4) B - bandpass calibration; 5) A - automatic flagging with CASA tfcrop (existing gains will be applied first); 6) I - imaging with WSClean using the settings in 'image' below, which fills the MODEL column of  the .MS file(s) with a sky model and, therefore, enables self-calibration with a subsequent G step.

  **calmode**

    *list* *of str*, *optional*, *default = a, ap, None, ap*

    For each step in 'order' above, set whether to solve for phase ('p'), amplitude ('a') or both ('ap'). This is actually only relevant when solving for the gains, i.e., for the G steps in 'order' above. However, users should include an entry (even just an empty string) for all steps in 'order'.

  **solint**

    *list* *of str*, *optional*, *default = 120s, inf, None, 120s*

    For each step in 'order' above, set the solution interval. Set to 'inf' to obtain a single solution (see also 'combine' below). Include time units, e.g., '120s' or '2min'.

  **combine**

    *list* *of str*, *optional*, *default = '', '', None, ''*

    For each step in 'order' above, set along what axis the data should be combined before solving. Options are '' (i.e., no data combination; solutions break at obs, scan, field, and spw boundarie), 'obs', 'scan', 'spw', 'field'. To combine along multiple axes use comma-separated axis names in a single string, e.g., 'obs,scan'. This setting is only relevant for the steps of type K, G and B included in 'order' above. For A steps this setting is ignored and an empty string may be used.

  **b_solnorm**

    *bool*, *optional*, *default = False*

    Normalise average solution amplitude to 1.0

  **b_fillgaps**

    *int*, *optional*, *default = 70*

    Fill flagged channels in the bandpass solutions by interpolation.

  **b_smoothwindow**

    *int*, *optional*, *default = 1*

    Size of the mean running window for smoothing of the bandpass (in channels). A size of 1 means no smoothing.

  **scanselection**

    *str*, *optional*, *default = ' '*

    String specifying (in CASA format) which scans to select during solving on secondary

  **spw_k**

    *str*, *optional*, *default = ' '*

    Only use this subset(s) of the band to compute 'K' gains. Default uses full band

  **spw_g**

    *str*, *optional*, *default = ' '*

    Only use this subset(s) of the band to compute 'GF' gains. Default uses full band

  **plotgains**

    *bool*, *optional*, *default = True*

    Plot gains with ragavi-gains. The .html plots are located in <output>/diagnostic_plots/crosscal/.

  **flag**

    Flagging settings used for all "A" (= auto-flagging) steps included in "order" above. These steps include applying the existing gains and flagging the corrected data.

    **col**

      *{"corrected", "residual"}*, *optional*, *default = corrected*

      Data column to flag on

    **usewindowstats**

      *{"none", "sum", "std", "both"}*, *optional*, *default = std*

      Calculate additional flags using sliding window statistics

    **combinescans**

      *bool*, *optional*, *default = False*

      Accumulate data across scans depending on the value of ntime

    **flagdimension**

      *{"freq", "time", "freqtime", "timefreq"}*, *optional*, *default = freqtime*

      Dimensions along which to calculate fits (freq/time/freqtime/timefreq)

    **timecutoff**

      *float*, *optional*, *default = 4.0*

      Flagging thresholds in units of deviation from the fit

    **freqcutoff**

      *float*, *optional*, *default = 3.0*

      Flagging thresholds in units of deviation from the fit

    **correlation**

      *str*, *optional*, *default = ' '*

      Correlation

  **image**

    Image settings for imaging secondary calibrator

    **npix**

      *int*, *optional*, *default = 4096*

      Number of pixels in output image

    **padding**

      *float*, *optional*, *default = 1.5*

      Padding in WSclean

    **mgain**

      *float*, *optional*, *default = 0.85*

      Image CLEANing gain

    **cell**

      *float*, *optional*, *default = 0.5*

      Image pixel size (arcsec)

    **weight**

      *{"briggs", "uniform", "natural"}*, *optional*, *default = briggs -1.0*

      Image weighting type. If Briggs, set the img robust parameter

    **external_fits_masks**

      *list* *of str*, *optional*, *default = ' '*

      List of file names (without .FITS extension) located in input/. The naming must have the field ID in the file name. e.g. specify as "mask-image-4" for a file called "mask-image-4.fits" in input/ where "4" is the field ID. NB - the masks need to be on the same WCS grid as the images being made. Otherwise a WSCleans auto-masking will be used (threshold set by auto_mask below)

    **auto_mask**

      *float*, *optional*, *default = 10*

      Auto masking threshold

    **auto_threshold**

      *float*, *optional*, *default = 1.5*

      Auto clean threshold

    **col**

      *str*, *optional*, *default = CORRECTED_DATA*

      Column to image

    **local_rms**

      *bool*, *optional*, *default = True*

      switch on local rms measurement for cleaning

    **rms_window**

      *int*, *optional*, *default = 150*

      switch on local rms measurement for cleaning

    **niter**

      *int*, *optional*, *default = 120000*

      Number of cleaning iterations

    **nchans**

      *int*, *optional*, *default = 7*

      Number of channesls in output image

    **fit_spectral_pol**

      *int*, *optional*, *default = 2*

      Number of spectral polynomial terms to fit to each clean component. This is equal to the order of the polynomial plus 1.



.. _crosscal_apply_cal:

--------------------------------------------------
**apply_cal**
--------------------------------------------------

  Apply calibration

  **applyto**

    *list* *of str*, *optional*, *default = bpcal, gcal, target*

    Fields to apply calibration to

  **calmode**

    *{"=", "calflag", "calflagstrict", "trial", "flagonly", "flagonlystrict"}*, *optional*, *default = calflag*

    Calibration mode, the default being "calflag" - calibrates and applies flags from solutions. See CASA documentation for info on other modes.



.. _crosscal_summary:

--------------------------------------------------
**summary**
--------------------------------------------------

  Prints out the butcher's bill, i.e. data flagging summary at the end of cross calibration process.

  **enable**

    *bool*, *optional*, *default = True*

    Execute printing flagging summary.



.. _crosscal_report:

--------------------------------------------------
**report**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  (Re)generate a full HTML report at the end of this worker.



.. _crosscal_cabs:

--------------------------------------------------
**cabs**
--------------------------------------------------

  *list* *of map*, *optional*, *default = ' '*

  Specifies non-default image versions and/or tags for Stimela cabs. Running with scissors: use with extreme caution.


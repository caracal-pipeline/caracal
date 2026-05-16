.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _line:
 
==========================================
line
==========================================
 
.. toctree::
   :maxdepth: 1
 
Process visibilities for spectral line work and create line cubes and images.



.. _line_enable:

--------------------------------------------------
**enable**
--------------------------------------------------

  *bool*

  Execute the line worker.



.. _line_label_in:

--------------------------------------------------
**label_in**
--------------------------------------------------

  *str*, *optional*, *default = corr*

  Label defining the name of the .MS files to be processed. The .MS file names are composed using the .MS names set by dataid in the getdata worker, followed by the target ID (one file per target), followed by this label. This is the format used by CARACal whenever it writes an .MS file to disk (e.g., in the transform worker).



.. _line_line_name:

--------------------------------------------------
**line_name**
--------------------------------------------------

  *str*, *optional*, *default = HI*

  Suffix to be used for the name of the output files (data cubes etc).



.. _line_restfreq:

--------------------------------------------------
**restfreq**
--------------------------------------------------

  *str*, *optional*, *default = 1.420405752GHz*

  Spectral line rest frequency.



.. _line_ncpu:

--------------------------------------------------
**ncpu**
--------------------------------------------------

  *int*, *optional*, *default = 0*

  Number of CPUs to use for distributed processing. If set to 0 all available CPUs are used. This parameter is currently only passed on to WSClean for line imaging.



.. _line_rewind_flags:

--------------------------------------------------
**rewind_flags**
--------------------------------------------------

  Rewind flags of the input .MS file(s) to specified version. Note that this is not applied to the .MS file(s) you might be running "transfer_apply_gains" on.

  **enable**

    *bool*, *optional*, *default = True*

    Enable the 'rewind_flags' segment.

  **mode**

    *{"reset_worker", "rewind_to_version"}*, *optional*, *default = reset_worker*

    If set to 'reset_worker', rewind to the flag version before this worker if it exists, or continue if it does not exist; if set to 'rewind_to_version', rewind to the flag version given by 'version' and 'mstransform_version' below.

  **version**

    *str*, *optional*, *default = auto*

    Flag version to restore. This is applied to the .MS file(s) identified by "label" above. Set to "null" to skip this rewinding step. If 'auto' it will rewind to the version prefix_workername_before, where 'prefix' is set in the 'general' worker, and 'workername' is the name of this worker including the suffix '__X' if it is a repeated instance of this worker in the configuration file. Note that all flag versions saved after this version will be deleted.

  **mstransform_version**

    *str*, *optional*, *default = auto*

    Flag version to restore. This is applied to the .MS file(s) identified by "label" above plus the "_mst" suffix. Set to "null" to skip this rewind step. If 'auto' it will rewind to the version prefix_workername_before, where 'prefix' is set in the 'general' worker, and 'workername' is the name of this worker including the suffix '__X' if it is a repeated instance of this worker in the configuration file. Note that all flag versions saved after this version will be deleted.



.. _line_overwrite_flagvers:

--------------------------------------------------
**overwrite_flagvers**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  Allow CARACal to overwrite existing flag versions. Not recommended. Only enable this if you know what you are doing.



.. _line_subtractmodelcol:

--------------------------------------------------
**subtractmodelcol**
--------------------------------------------------

  Replace the CORRECTED_DATA column of the .MS file(s) with the difference CORRECTED_DATA - MODEL_DATA. This is useful for continuum subtraction as it subtracts the continuum clean model written to MODEL_DATA. WARNING! The CORRECTED_DATA column is overwritten. To undo this operation enable the addmodelcol segment in this worker.

  **enable**

    *bool*, *optional*, *default = True*

    Enable the 'subtractmodelcol' segment.

  **force**

    *bool*, *optional*, *default = False*

    Force the model subtraction regardless of the number of previous subtractions.



.. _line_addmodelcol:

--------------------------------------------------
**addmodelcol**
--------------------------------------------------

  Replace the CORRECTED_DATA column of the .MS file(s) with the sum CORRECTED_DATA + MODEL_DATA. This is useful to undo the operation performed by subtractmodelcol in this worker. WARNING! The CORRECTED_DATA column is overwritten.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'addmodelcol' segment.

  **force**

    *bool*, *optional*, *default = False*

    Force the model addition regardless of the number of previous additions.



.. _line_mstransform:

--------------------------------------------------
**mstransform**
--------------------------------------------------

  Perform Doppler-tracking corrections and/or UVLIN continuum subtraction with CASA mstransform. For each input .MS file, this produces an output .MS file whose name is the same as that of the input .MS file plus the suffix "_mst".

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'mstransform' segment.

  **col**

    *str*, *optional*, *default = corrected*

    Which column of the .MS file(s) to process.

  **doppler**

    Include the Doppler-tracking correction in the run of CASA mstransform.

    **enable**

      *bool*, *optional*, *default = True*

      Enable the 'doppler' (i.e. Doppler correction) segment.

    **telescope**

      *{"askap", "atca", "gmrt", "meerkat", "vla", "wsrt"}*

      Name of the telescope used to take the data. This is used to set the telescope's geographical coordinates when calculating the Doppler correction. Default is 'meerkat'. Current options are askap, atca, gmrt, meerkat, vla, wsrt.

    **mode**

      *{"frequency"}*, *optional*, *default = frequency*

      Regridding mode (channel/velocity/frequency/channel_b). IMPORTANT! Currently, only frequency mode is supported. Other modes will throw an error.

    **frame**

      *{"", "topo", "geo", "lsrk", "lsrd", "bary", "galacto", "lgroup", "cmb", "source"}*, *optional*, *default = bary*

      Output reference frame. Current options are '', topo, geo, lsrk, lsrd, bary, galacto, lgroup, cmb, and source.

    **veltype**

      *{"radio", "optical"}*, *optional*, *default = radio*

      Velocity used when regridding if mode = velocity. Current options are radio,and optical.

    **changrid**

      *str*, *optional*, *default = auto*

      Output channel grid for Doppler correction. Default is 'auto', and the pipeline will calculate the appropriate channel grid. If not 'auto' then it must be in the format 'nchan,chan0,chanw' where nchan is an integer, and chan0 and chanw must include units appropriate for the chosen mode (see parameter 'mode' above).

  **uvlin**

    Include UVLIN-like continuum subtraction in the run of CASA mstransform.

    **enable**

      *bool*, *optional*, *default = True*

      Enable the 'UVLIN' segment.

    **fitorder**

      *int*, *optional*, *default = 1*

      Polynomial order of the continuum fit.

    **fitspw**

      *str*, *optional*, *default = ' '*

      Selection of line-free channels using CASA syntax (e.g. '0:0~100;150~300'). If set to null, a fit to all unflagged visibilities will be performed.

    **exclude_known_sources**

      *bool*, *optional*, *default = False*

      Exclude from the UVLIN fit the channels corresponding to known line sources listed in a catalogue. The catalogue file has the name given by the parameter 'known_sources_cat' below and is located in the 'input' directory specified in the 'general' worker. The  resulting channel selection is combined with the one provided by the 'fitspw' parameter above. Some published catalogues are included in the CARACal repository and are ready for use. See 'know_sources_cat' below.

    **known_sources_cat**

      *str*, *optional*, *default = ' '*

      Catalogue of known line sources. The catalogue is in ASCII format, one row per source, with columns (1) source ID, (2) RA (hh:mm:ss.s), (3) Dec (dd:mm:ss.s), (4) Vmin (km/s, optical convention), (5) Vmax (km/s, optical convention), (6) line flux (Jy km/s). The HIPASS catalogue from Meyer et al. (2004), MNRAS, 350, 1195 is included in CARACal with the required format (file name hicat_caracal.txt).

    **known_sources_radius**

      *float*, *optional*, *default = 1.0*

      Only line sources within this radius (in deg) from the pointing centre are excluded from the UVLIN fit.

    **known_sources_flux**

      *float*, *optional*, *default = 0.0*

      Only line sources brighter than this flux (in Jy km/s) are excluded from the UVLIN fit (no primary beam correction included).

    **known_sources_dv**

      *float*, *optional*, *default = 30.*

      Remove (add) this velocity buffer from (to) the Vmin (Vmax) values in the catalogue to avoid errors caused by anoccounted-for Doppler shifts. This parameter is given in km/s.

  **obsinfo**

    *bool*, *optional*, *default = True*

    Create obsinfo.txt and obsinfo.json per .MS file created by CASA mstransform.



.. _line_flag_mst_errors:

--------------------------------------------------
**flag_mst_errors**
--------------------------------------------------

  Run AOFlagger to flag any faulty visibilities produced by CASA mstransform.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flag_mst_errors' segment.

  **strategy**

    *str*, *optional*, *default = postmst.rfis*

    AOFlagger strategy file.

  **readmode**

    *{"indirect", "memory", "auto"}*, *optional*, *default = auto*

    AOflagger read mode. If set to 'indirect', AOflagger temporarily writes a reordered .MS file to disc, which results in fast flagging but requires free disc space. If set to 'memory', AOflagger reads the .MS file into memory, which is even faster than 'indirect' but is impossible for large files. If set to 'auto', AOflagger will decide between the 'memory' mode and the 'direct' mode -- the slowest mode -- in which AOFlagger reads baselines by scanning the entire file for the data relevant for the currently required baseline.



.. _line_flag_u_zeros:

--------------------------------------------------
**flag_u_zeros**
--------------------------------------------------

  flag RFI at u=0

  **enable**

    *bool*, *optional*, *default = False*

    Enable the flag_u_zeros segment

  **use_mstransform**

    *bool*, *optional*, *default = True*

    Run flagging algorithm on the .MS file(s) produced by the mstransform section of this worker instead of the input .MS file(s).

  **transfer_flags**

    *list* *of str*, *optional*, *default = ' '*

    List of datasets to which to transfer the u=0 flags. The list should only include the labels which identify those datasets, following the usual CARACal label convention and the 'use_mstransform' setting of this flag_u_zeros segment. The flags are calculated using the dataset identified by 'label_in' above. Flags can only be transferred to MS files with the same number of channels as the 'label_in' dataset. In case of different number of channels CARACal will crash.

  **method**

    *{"madThreshold", "q99"}*, *optional*, *default = madThreshold*

    Define flagging method. Either q99 or madThreshold (median+threshold\*mad)

  **make_plots**

    *bool*, *optional*, *default = True*

    Make Plots or not

  **cleanup**

    *bool*, *optional*, *default = True*

    Remove intermediate ms files, images and FFTs

  **robust**

    *float*, *optional*, *default = 1.5*

    robust weighting for the images

  **taper**

    *float*, *optional*, *default = 60*

    size of gaussian tapering in arcseconds

  **imsize**

    *int*, *optional*, *default = 400*

    size of the images in pixel,

  **cell**

    *float*, *optional*, *default = 20.*

    size of pixel in arcseconds. In the FFT the pixel size in lambda is given by:duv = 1./(imsize\*cell\*pi/(3600.\*180.)), uv cell is in lambda

  **chans**

    *list* *of int*, *optional*, *default = 0,100*

    lowest and highest channel of the spw to consider for imaging

  **thresholds**

    *list* *of float*, *optional*, *default = 300*

    threshold for cutoff of amplitudes in the FFT, default=300

  **dilateU**

    *int*, *optional*, *default = 0*

    extend flag selection to N nearby cells along the U axis in both directions

  **dilateV**

    *int*, *optional*, *default = 0*

    extend flag selection to N nearby cells along the V axis in both directions



.. _line_sunblocker:

--------------------------------------------------
**sunblocker**
--------------------------------------------------

  Use sunblocker to grid the visibilities and flag UV cells affected by solar RFI. See description of sunblocker on github repository gigjozsa/sunblocker in method phazer of module sunblocker.py.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'sunblocker' segment.

  **use_mstransform**

    *bool*, *optional*, *default = True*

    Run sunblocker on the .MS file(s) produced by the mstransform section of this worker instead of the input .MS file(s).

  **imsize**

    *int*, *optional*, *default = 900*

    Image size (pixels). Use the same as in the make_cube section. This is used to set up the gridding of the visibilities.

  **cell**

    *float*, *optional*, *default = 2.*

    Pixel size (arcsec). Use the same as in the make_cube section. This is used to set up the gridding of the visibilities.

  **thr**

    *float*, *optional*, *default = 4.*

    Flag UV cells whose visibility deviates by more than this threshold from the average visibility on the UV plane. The threshold is in units of the rms dispersion of all visibilities.

  **vampirisms**

    *bool*, *optional*, *default = False*

    Use only daytime data when calculating which UV cells to flag (and flag only daytime data).

  **flagonlyday**

    *bool*, *optional*, *default = False*

    Apply the flags to data taken during day time only. Note that all data are used when calculating which UV cells to flag if vampirisms is set to false.

  **uvmin**

    *float*, *optional*, *default = 0.*

    Minimum uvdistance to be analysed (in wavelengths, lambda).

  **uvmax**

    *float*, *optional*, *default = 2000*

    Maximum uvdistance to be analysed (in wavelengths, lambda).



.. _line_predict_noise:

--------------------------------------------------
**predict_noise**
--------------------------------------------------

  Print to log-caracal.txt the expected natural noise level of the line cube (Stokes I, single channel) based on Tsys/eff and dish diameter below.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'predict_noise' segment.

  **tsyseff**

    *float*, *optional*, *default = 20.5*

    Value of Tsys/eff in K.

  **diam**

    *float*, *optional*, *default = 13.5*

    Dish diameter in m.



.. _line_make_cube:

--------------------------------------------------
**make_cube**
--------------------------------------------------

  Make a line cube using either WSClean + SoFiA (optional for clean masks) or CASA Clean.

  **enable**

    *bool*, *optional*, *default = false*

    Enable the 'make_cube' segment.

  **image_with**

    *{"wsclean", "casa"}*, *optional*, *default = wsclean*

    Choose whether to image with WSClean + SoFiA ('wsclean') or with CASA Clean ('casa').

  **use_mstransform**

    *bool*, *optional*, *default = True*

    Image the .MS file(s) produced by the mstransform section of this worker instead of the input .MS file(s).

  **stokes**

    *str*, *optional*, *default = I*

    Polarizations in output cube (I,Q,U,V,XX,YY,XY,YX,RR,LL,RL,LR and combinations).

  **spwid**

    *int*, *optional*, *default = 0*

    Spectral window to use.

  **nchans**

    *int*, *optional*, *default = 0*

    Number of channels of the line cube, where 0 means all channels.

  **firstchan**

    *int*, *optional*, *default = 0*

    First channel of the line cube.

  **binchans**

    *int*, *optional*, *default = 1*

    Integer binning of channels.

  **npix**

    *seq*, *optional*, *default = 900 , 900*

    Image size in pixels. List of integers (width and height) or a single integer for square images.

  **cell**

    *float*, *optional*, *default = 2*

    Pixel size (arcsec).

  **padding**

    *float*, *optional*, *default = 1.2*

    Images have initial size padding\*npix, and are later trimmed to the image size set via the 'npix' parameter.

  **weight**

    *{"natural", "uniform", "briggs"}*, *optional*, *default = briggs*

    Options for the type of weighting to be used are natural, uniform, or briggs. When using Briggs weighting, the additional robust parameter has to be specified.

  **robust**

    *float*, *optional*, *default = 0*

    Robust parameter in case of Briggs weighting.

  **taper**

    *float*, *optional*, *default = 0*

    Gaussian taper FWHM in arcsec. Zero means no tapering.

  **niter**

    *int*, *optional*, *default = 1000000*

    Maximum number of clean iterations to perform.

  **gain**

    *float*, *optional*, *default = 0.1*

    Fraction of the peak that is cleaned in each minor iteration.

  **wscl_onlypsf**

    *bool*, *optional*, *default = False*

    If set to true, WSClean will only make the dirty PSF cube, adding the best-fitting Gaussian parameter of each channel to the header. No other cube is made, and the parameter niter is ignored.

  **wscl_mgain**

    *float*, *optional*, *default = 1.0*

    Gain value for major iterations in WSClean. I.e., the maximum fraction of the image peak that is cleaned in each major iteration. A value of 1 means that all cleaning happens in the image plane and no major cycle is performed.

  **wscl_sofia_niter**

    *int*, *optional*, *default = 2*

    Maximum number of WSClean + SoFiA iterations. The initial cleaning is done with WSClean automasking or with a user-provided clean mask. Subsequent iterations use a SoFiA clean mask. A value of 1 means that WSClean is only executed once and SoFiA is not used. The value of this parameter must be >= 1. Values < 1 will be ignored, and a value of 1 will be used instead.

  **wscl_sofia_converge**

    *float*, *optional*, *default = 1.1*

    Stop the WSClean + SoFiA iterations if the cube RMS has dropped by a factor < wscl_sofia_converge when comparing the last two iterations (considering only channels that were cleaned). If set to 0 then the maximum number of iterations is performed regardless of the change in RMS.

  **wscl_removeintermediate**

    *bool*, *optional*, *default = False*

    If set to true, WSClean + SoFiA intermediate-cubes are deleted from the output directory. If set to false, WSClean + SoFiA intermediate-cubes are retained in the output directory.

  **wscl_user_clean_mask**

    *str*, *optional*, *default = ' '*

    User-provided WSClean clean-mask for the first WSClean + SoFiA iteration (i.e. give the filename of the clean-mask, which is to be located in the output/masking folder).

  **wscl_auto_mask**

    *float*, *optional*, *default = 10*

    Cleaning threshold used only during the first iteration of WSClean. This is given as the number of sigma_rms to be cleaned down to, where sigma_rms is the noise level estimated by WSClean from the residual image before the start of every major deconvolution iteration. WSClean will clean blindly down to this threshold (wscl_auto_mask), before switching to the auto-threshold set via wscl_auto_threshold.

  **wscl_auto_thr**

    *float*, *optional*, *default = 0.5*

    Cleaning threshold used for subsequent iterations of WSClean. This is given as the number of sigma_rms to be cleaned down to, where sigma_rms is the noise level estimated by WSClean from the residual image before the start of every major deconvolution iteration.

  **wscl_make_cube**

    *bool*, *optional*, *default = True*

    If set to true, the output of WSClean is a data cube. If set to false, the output is one .FITS image per spectral channel.

  **wscl_noupdatemod**

    *bool*, *optional*, *default = True*

    If set to true, WSClean will not store the line clean model in MODEL_DATA.

  **wscl_multiscale**

    *bool*, *optional*, *default = False*

    Switch on WSClean multiscale cleaning.

  **wscl_multiscale_scales**

    *str*, *optional*, *default = ' '*

    Comma-separated integer scales for multiscale cleaning in pixels. If set to an empty string WSClean selects the scales automatically. These include the 0 scale, a scale calculated based on the beam size, and all scales obtained increasing the scale by a factor of 2 until the image size is reached.

  **wscl_multiscale_bias**

    *float*, *optional*, *default = 0.6*

    Parameter to set the bias during multiscale cleaning, where a lower bias will give preference to larger angular scales.

  **wscl_nrdeconvsubimg**

    *int*, *optional*, *default = 1*

    Speed-up deconvolution by splitting each channel into a number of subimages, which are deconvolved in parallel. This parameter sets the number of subimages as follows. If set to 1 no parallel deconvolution is performed. If set to 0 the number of subimages is the same as the number of CPUs used by the line worker (see "ncpu" parameter above). If set to a number > 1 , the number of subimages is greater than or equal to the one requested by the user.

  **wscl_beam**

    *seq*, *optional*, *default = 0, 0, 0*

    Set Bmaj,Bmin,PA of the beam to be used for restoring the clean components. The units are arcsec for Bmaj and Bmin, degrees for PA. Bmaj and Bmin are FWHM. The default values of [0, 0, 0] mean that WSClean chooses the restoring beam based on a 2d Gaussian fit to the dirty beam.

  **casa_thr**

    *str*, *optional*, *default = 10mJy*

    Flux-density level to stop CASA cleaning. It must include units, e.g. '1.0mJy'.

  **casa_port2fits**

    *bool*, *optional*, *default = False*

    Port CASA output to fits files.



.. _line_remove_stokes_axis:

--------------------------------------------------
**remove_stokes_axis**
--------------------------------------------------

  Remove the Stokes axis from the line cube.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'remove_stokes_axis' segment.



.. _line_pb_cube:

--------------------------------------------------
**pb_cube**
--------------------------------------------------

  Make a primary-beam cube.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'pb_cube' segment.

  **apply_pb**

    *bool*, *optional*, *default = False*

    Whether or not to apply the primary-beam correction to the image cube.

  **pb_type**

    *{"gauss", "mauch"}*, *optional*, *default = gauss*

    Choose between a Gaussian (gauss) primary beam model or the MeerKAT Mauch et al. (2020) model (mauch).

  **dish_size**

    *float*, *optional*, *default = 13.5*

    Dish diameter in meters. Only used in the Gaussian primary beam model

  **cutoff**

    *float*, *optional*, *default = 0.1*

    Primary beam pixels below this value are set to NaN.



.. _line_freq_to_vel:

--------------------------------------------------
**freq_to_vel**
--------------------------------------------------

  Convert the spectral axis' header keys of the line cube from frequency to velocity in the radio definition, v=c(1-obsfreq/restfreq). No change of spectra reference frame is performed.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'freq_to_vel' segment.

  **reverse**

    *bool*, *optional*, *default = False*

    Perform the inverse transformation and change the cube's 3rd axis from radio velocity to frequency.



.. _line_imcontsub:

--------------------------------------------------
**imcontsub**
--------------------------------------------------

  Use the final output image cube (lastiter true) or all (lastiter false) when using wsclean or a specified set of cubes. Fit a function or filter along the third axis, subtract it from the original, and return the result. Possible is a polynomial fit (fitmode = poly) or a Savitzky-Golay filter. In case of the Savitzky-Golay filter the window length is given by the parameter with the name length. The polynomial order of either polynomial or the filter is specified with the parameter polyorder. A Savitzky-Golay filter with polynomial order 0 is a median filter. Optionally a mask data cube with the same dimensions of the input data cube can be provided.  Voxels for which the mask data cube is not equal to zero are ignored. For the polynomial fit the voxels are simply ignored. In case of the Savitzky Golay filter, an iterative process is started.  All masked voxels are set to zero and a median filter is run along the frequency axis. After that the Savitzky-Golay filter is run sgiters times. If the parameter sgiters is set to 0, only one Savitzky-Golay filter is applied (no initial median filtering, does not work for ). With the parameter fitted the user can optionally supply the name of the output fitted data cube.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'imcontsub' segment.

  **incubus**

    *list* *of str*, *optional*, *default = ' '*

    List of input cubes; will select either image or dirty cubes if empty or not specified

  **lastiter**

    *bool*, *optional*, *default = True*

    If incubus is empty, select only the last iteration for continuum subtraction (true) or all (false)

  **fitmode**

    *{"poly", "savgol"}*, *optional*, *default = poly*

    Type of fit ('poly' or 'savgol')

  **length**

    *int*, *optional*, *default = 25*

    Length of the sliding window in channels (only used for fitmode = savgol must be odd, default is 25)

  **polyorder**

    *int*, *optional*, *default = 0*

    Order of the polynomial or of the Savitzky-Golay filter (default is 0)

  **mask**

    *str*, *optional*, *default = ' '*

    Mask cubes to use. '' means do not use mask cubes or those specified with parameter masculin. 'clean' means use clean masks if available. 'sofia' means use sofia masks if available.

  **masculin**

    *list* *of str*, *optional*, *default = ' '*

    List of input mask cubes. Only used if mask = ''. Must be empty or the same number of cubes as the input cubes.

  **sgiters**

    *int*, *optional*, *default = 0*

    Number of Savitzky-Golay filter iterations (default is 0)

  **kertyp**

    *{"gauss", "tophat"}*, *optional*, *default = tophat*

    Kernel type to convolve the polynomial fit with ('gauss', 'tophat')

  **kersiz**

    *int*, *optional*, *default = 0*

    Kernel size to convolve the polynomial fit with (pixel, 0 means no convolution)

  **outfit**

    *bool*, *optional*, *default = False*

    Produce fitted data cubes (True means yes, default is False)

  **outfitcon**

    *bool*, *optional*, *default = False*

    Produce fitted and convolved data cubes (True means yes, default is False)



.. _line_sofia:

--------------------------------------------------
**sofia**
--------------------------------------------------

  Run SoFiA source-finder on the final HI cubes to produce a detection mask, moment images and catalogues. Note that these settings are not used to make clean masks.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'sofia' segment.

  **imcontsub**

    *bool*, *optional*, *default = False*

    Use results of imcontsub instead of image cubes if available

  **flag**

    *bool*, *optional*, *default = False*

    Use flag regions?

  **flagregion**

    *list* *of int*, *optional*, *default = 0, 0, 0, 0, 0, 0*

    Pixel/channel range(s) to be flagged prior to source finding. Format is [[x1, x2, y1, y2, z1, z2], ...].

  **rmsMode**

    *str*, *optional*, *default = mad*

    Method to determine rms ('mad' for using median absolute deviation, 'std' for using standard deviation, 'negative' for using Gaussian fit to negative voxels).

  **thr**

    *float*, *optional*, *default = 4.0*

    SoFiA source-finding threshold, in terms of the number of sigma_rms to go down to (i.e. the minimum signal-to-noise ratio).

  **merge**

    *bool*, *optional*, *default = False*

    Merge pixels detected by any of the SoFiA source-finding algorithms into objects. If enabled, pixels with a separation of less than mergeX pixels in the X direction, mergeY pixels in the Y direction, and mergeZ channels in the Z direction will be merged and identified as a single object in the mask. Objects whose extent is smaller than minSizeX, minSizeY or minSizeZ will be removed from the mask.

  **mergeX**

    *int*, *optional*, *default = 2*

    Merging radius (in pixels) in the X direction (RA axis).

  **mergeY**

    *int*, *optional*, *default = 2*

    Merging radius (in pixels) in the Y direction (Dec axis).

  **mergeZ**

    *int*, *optional*, *default = 3*

    Merging radius (in channels) in Z direction (spectral axis).

  **minSizeX**

    *int*, *optional*, *default = 3*

    Minimum size (in pixels) in the X direction (RA axis).

  **minSizeY**

    *int*, *optional*, *default = 3*

    Minimum size (in pixels) in the Y direction (Dec axis).

  **minSizeZ**

    *int*, *optional*, *default = 3*

    Minimum size (in channels) in the Z direction (spectral axis).

  **cubelets**

    *bool*, *optional*, *default = True*

    Create a cubelet for each detected emission-line object.

  **mom0**

    *bool*, *optional*, *default = True*

    Create a moment-0 image of the field.

  **mom1**

    *bool*, *optional*, *default = True*

    Create a moment-1 image of the field.



.. _line_sharpener:

--------------------------------------------------
**sharpener**
--------------------------------------------------

  Run sharpener to extract and plot the spectra of all continuum sources brighter than a given threshold.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'sharpener' segment.

  **catalog**

    *{"NVSS", "PYBDSF"}*, *optional*, *default = PYBDSF*

    Type of catalogue to use. Options are PYBDSF and NVSS.

  **chans_per_plot**

    *int*, *optional*, *default = 50*

    Number of channels to plot per detailed plot.

  **thr**

    *float*, *optional*, *default = 20*

    Threshold to select sources in online catalogue (in units of mJy).

  **width**

    *str*, *optional*, *default = 1.0d*

    Field-of-view of output catalogue (in units of degrees).

  **label**

    *str*, *optional*, *default = ' '*

    Prefix label of plot names and titles.



.. _line_report:

--------------------------------------------------
**report**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  (Re)generate a full HTML report at the end of this worker.



.. _line_cabs:

--------------------------------------------------
**cabs**
--------------------------------------------------

  *list* *of map*, *optional*, *default = ' '*

  Specifies non-default image versions and/or tags for Stimela cabs.  Running with scissors: use with extreme caution. Inline format is: Format is [{name: cabname, tag: stimela_cab_version}].


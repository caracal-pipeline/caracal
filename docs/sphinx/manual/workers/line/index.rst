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

  Make a line cube using either WSClean + SoFiA-2 (optional for clean masks) or CASA Clean.

  **enable**

    *bool*, *optional*, *default = false*

    Enable the 'make_cube' segment.

  **image_with**

    *{"wsclean", "casa"}*, *optional*, *default = wsclean*

    Choose whether to image with WSClean + SoFiA-2 ('wsclean') or with CASA Clean ('casa').

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

    Maximum number of WSClean + SoFiA-2 iterations. The initial cleaning is done with WSClean automasking or with a user-provided clean mask. Subsequent iterations use a SoFiA-2 clean mask. A value of 1 means that WSClean is only executed once and SoFiA-2 is not used. The value of this parameter must be >= 1. Values < 1 will be ignored, and a value of 1 will be used instead.

  **wscl_sofia_converge**

    *float*, *optional*, *default = 1.1*

    Stop the WSClean + SoFiA-2 iterations if the cube RMS has dropped by a factor < wscl_sofia_converge when comparing the last two iterations (considering only channels that were cleaned). If set to 0 then the maximum number of iterations is performed regardless of the change in RMS.

  **wscl_removeintermediate**

    *bool*, *optional*, *default = False*

    If set to true, WSClean + SoFiA-2 intermediate-cubes are deleted from the output directory. If set to false, WSClean + SoFiA-2 intermediate-cubes are retained in the output directory.

  **wscl_user_clean_mask**

    *str*, *optional*, *default = ' '*

    User-provided WSClean clean-mask for the first WSClean + SoFiA-2 iteration (i.e. give the filename of the clean-mask, which is to be located in the output/masking folder).

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

  Perform continuum subtraction in the image plane.

  **enable**

    *bool*, *optional*, *default = false*

    Enable the 'imcontusb' segment.

  **label_out**

    *str*, *optional*, *default = ' '*

    Name of ouput image

  **input_cube**

    *str*, *optional*, *default = ' '*

    (EXPERIMENTAL - Succesfully tested only for single-target MS files. Use at own risk in all other cases.) Name of input datacube located in /output/ (preferentially in outpupt/cubes/cube_xx/ . Where xx is the highest number. To use when running imcontsub independently.

  **mask_image**

    *str*, *optional*, *default = ' '*

    1) 'sofia' to use the mask output of SoFiA  2) a prefix string to use an existing .FITS mask located in output_label/masking and called prefix_target.fits (name of the target is set automatically by the pipeline and the mask should be named accordingly). The .FITS mask could be the one created by the masking worker, in which case the prefix set here should correspond to label_out in the masking worker. Note that this second masking method can be used on multiple targets in a single pipeline run as long as they all have a corresponding prefix_target.fits mask in output_label/masking.

  **sigma_clip**

    *seq*, *optional*, *default = 5*

    Sigma clip for each iteration. Only required if mask-image is not given.

  **order**

    *seq*, *optional*, *default = 3*

    Order of spline. If given as a list of size N, then N iterations will be perfomed.

  **segments**

    *seq*, *optional*, *default = 1000.*

    Width of spline segments in km/s. Ideally, this is larger than the largest single-sightline line-width in the data cube. Default is 1000 km/s. Must be given as list of the same size as --order.

  **cont_fit_tol**

    *float*, *optional*, *default = 0*

    Minimum perentage of valid spectrum data points required to do a fit. Spectra below this tolerance will be set to NaN. Leaving this unset may result in poor or NaN spectra in the output cubes"

  **overwrite**

    *bool*, *optional*, *default = False*

    Overwrite output image if it already exists

  **rest_freq**

    *float*, *optional*, *default = 1420.4057*

    Cube rest frequency in MHz. Will ignore the one in the FITS header if it exists.

  **ra_chunks**

    *int*, *optional*, *default = 4*

    Chunking along RA-axis. If set to 1, no Chunking is perfomed.

  **ncpus**

    *int*, *optional*, *default = 4*

    Number of workers (one per CPU)



.. _line_do_sourcefinding:

--------------------------------------------------
**do_sourcefinding**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  Use sofia2 to do source finding. Settings taken from sofia2_settings. The output products are placed in the sofia directory in the same directory as the input image.



.. _line_sofia2_settings:

--------------------------------------------------
**sofia2_settings**
--------------------------------------------------

  Settings to be used in the making of a clean_mask and / or source finding for moment maps and HI characterisation.

  **imcontsub**

    *bool*, *optional*, *default = False*

    Use results of imcontsub instead of image cubes if available

  **input_mask**

    *str*, *optional*, *default = ' '*

    Name of input mask cube. Needs to be located in the same directory as the input cube i.e. output/cubes/cube_<n>.

  **input_noise**

    *str*, *optional*, *default = ' '*

    Name of input noise cube. Needs to be located in the same directory as the input cube i.e. output/cubes/cube_<n>.

  **input_weights**

    *str*, *optional*, *default = ' '*

    Name of input weights cube. Needs to be located in the same directory as the input cube i.e. output/cubes/cube_<n>.

  **flag_auto**

    *{"true", "false", "channels", "pixels"}*, *optional*, *default = false*

    SoFiA-2 will attempt to automatically flag spectral channels and spatial pixels affected by interference or artefacts based on their RMS noise level. If set to channels, only spectral channels will be flagged. If set to pixels, only spatial pixels will be flagged.

  **flag_catalog**

    *str*, *optional*, *default = ' '*

    Name of catalogue file containing two columns that specify the longitude and latitude coordinates of sky positions to be flagged in the native coordinate system and units of the input data cube. The two columns can be separated by spaces, tabulators or commas.  Needs to be located in <>.

  **flag_radius**

    *int*, *optional*, *default = 5*

    Radius around the sky positions listed in the catalogue provided by flag_catalog that should be flagged. If 0, then only the nearest pixel to the position will be flagged. Otherwise, pixels within the specified radius around the nearest pixel will be flagged.

  **flag_region**

    *list* *of int*, *optional*, *default = 0, 0, 0, 0, 0, 0*

    Region(s) to be flagged in the input data cube prior to processing. The flagging region must contain a multiple of six comma-separated integer values of the following format; x_min, x_max, y_min, y_max, z_min, z_max, ... (all in units of pixels and 0-based). Pixels within those regions will be set to blank in the input cube.

  **flag_threshold**

    *float*, *optional*, *default = 5.0*

    Relative threshold in multiples of the standard deviation to be applied by the automatic flagging algorithm. Only relevant if flag_auto is enabled.

  **scaleNoise**

    the noise scaling modules is to measure the noise level in the input cube and then divide the input cube by the noise. This can be used to correct for spatial or spectral noise variations across the input cube prior to running the source finder.

    **enable**

      *bool*, *optional*, *default = True*

      Enable the noise scaling section.

    **mode**

      *{"spectral", "local"}*, *optional*, *default = spectral*

      If set to spectral, the noise level will be determined for each spectral channel by measuring the noise within each image plane. This is useful for data cubes where the noise varies with frequency. If set to local, the noise level will be measured locally in window running across the entire cube in all three dimensions. This is useful for data cubes with more complex noise variations, such as interferometric images with primary-beam correction applied.

    **statistic**

      *{"std", "mad", "gauss"}*, *optional*, *default = mad*

      Standard deviation, median absolute deviation and Gaussian fitting to the flux histogram, respectively. Standard deviation is by far the fastest algorithm, but it is also the least robust one with respect to emission and artefacts in the data. Median absolute deviation and Gaussian fitting are far more robust in the presence of strong, extended emission or artefacts, but will usually take longer.

    **fluxRange**

      *{"positive", "negative", "full"}*, *optional*, *default = negative*

      Flux range to be used in the noise measurement. If set to negative or positive, only pixels with negative or positive flux will be used, respectively. This can be useful to prevent real emission or artefacts from affecting the noise measurement. If set to full, all pixels will be used in the noise measurement irrespective of their flux.

    **windowXY**

      *int*, *optional*, *default = 25*

      Standard deviation, median absolute deviation and Gaussian fitting to the flux histogram, respectively. Standard deviation is by far the fastest algorithm, but it is also the least robust one with respect to emission and artefacts in the data. Median absolute deviation and Gaussian fitting are far more robust in the presence of strong, extended emission or artefacts, but will usually take longer.

    **windowZ**

      *int*, *optional*, *default = 15*

      Standard deviation, median absolute deviation and Gaussian fitting to the flux histogram, respectively. Standard deviation is by far the fastest algorithm, but it is also the least robust one with respect to emission and artefacts in the data. Median absolute deviation and Gaussian fitting are far more robust in the presence of strong, extended emission or artefacts, but will usually take longer.

    **interpolate**

      *bool*, *optional*, *default = False*

      If set to true, linear interpolation will be used to interpolate the measured local noise values in between grid points. If set to false, the entire grid cell will instead be filled with the measured noise value.

    **scfind**

      *bool*, *optional*, *default = False*

      If true and global or local noise scaling is enabled, then noise scaling will additionally be applied after each smoothing operation in the S+C finder. This might be useful in certain situations where large-scale artefacts are present in interferometric data. However, this feature should be used with great caution, as it has the potential to do more harm than good.

  **scfind**

    The S+C finder operates by iteratively smoothing the data cube with a user-defined set of smoothing kernels, measuring the noise level on each smoothing scale, and adding all pixels with an absolute flux above a user-defined relative threshold to the source detection mask.

    **enable**

      *bool*, *optional*, *default = True*

      Enable the Smooth + Clip (S+C) finder section.

    **kernelsXY**

      *list* *of int*, *optional*, *default = [0, 3, 6]*

      Comma-separated list of spatial Gaussian kernel sizes to apply. The individual kernel sizes must be floating-point values and denote the full width at half maximum (FWHM) of the Gaussian used to smooth the data in the spatial domain. A value of 0 means that no spatial smoothing will be applied.

    **kernelsZ**

      *list* *of int*, *optional*, *default = [0, 3, 7, 15]*

      Comma-separated list of spectral Boxcar kernel sizes to apply. The individual kernel sizes must be odd integer values of 3 or greater and denote the full width of the Boxcar filter used to smooth the data in the spectral domain. A value of 0 means that no spectral smoothing will be applied.

    **threshold**

      *float*, *optional*, *default = 4.0*

      Flux threshold to be used by the S+C finder relative to the measured noise level in each smoothing iteration. In practice, values in the range of about 3 to 5 have proven to be useful in most situations, with lower values in that range requiring use of the reliability filter to reduce the number of false detections.

    **statistic**

      *{"std", "mad", "gauss"}*, *optional*, *default = mad*

      Standard deviation, median absolute deviation and Gaussian fitting to the flux histogram, respectively. Standard deviation is by far the fastest algorithm, but it is also the least robust one with respect to emission and artefacts in the data. Median absolute deviation and Gaussian fitting are far more robust in the presence of strong, extended emission or artefacts, but will usually take longer.

    **fluxRange**

      *{"positive", "negative", "full"}*, *optional*, *default = negative*

      Flux range to be used in the noise measurement. If set to negative or positive, only pixels with negative or positive flux will be used, respectively. This can be useful to prevent real emission or artefacts from affecting the noise measurement. If set to full, all pixels will be used in the noise measurement irrespective of their flux.

  **linker**

    The linker will be run to merge the pixels detected by the source finder into coherent detections that can then be parameterised and catalogued. If false, the pipeline will be terminated after source finding, and no catalogue or source products will be created. Disabling the linker can be useful if only the raw mask from the source finder is needed.

    **enable**

      *bool*, *optional*, *default = True*

      Enable the linker section.

    **radiusXY**

      *int*, *optional*, *default = 3*

      Maximum merging length in the spatial dimension. Pixels with a separation of up to this value will be merged into the same source.

    **radiusZ**

      *int*, *optional*, *default = 3*

      Maximum merging length in the spectral dimension. Pixels with a separation of up to this value will be merged into the same source.

    **minSizeXY**

      *int*, *optional*, *default = 2*

      Minimum size of sources in the spatial dimension in pixels. Sources that fall below this limit will be discarded by the linker.

    **minSizeZ**

      *int*, *optional*, *default = 2*

      Minimum size of sources in the spectral dimension in pixels. Sources that fall below this limit will be discarded by the linker.

    **maxSizeXY**

      *int*, *optional*, *default = 0*

      Maximum size of sources in the spatial dimension in pixels. Sources that exceed this limit will be discarded by the linker. If the value is set to 0, maximum size filtering will be disabled.

    **maxSizeZ**

      *int*, *optional*, *default = 0*

      Maximum size of sources in the spectral dimension in pixels. Sources that exceed this limit will be discarded by the linker. If the value is set to 0, maximum size filtering will be disabled.

  **reliability**

    Determine the reliability of each detection with positive total flux by comparing the density of positive and negative detections in a three-dimensional parameter space. Sources below the specified reliability threshold will then be discarded. Note that this will require a sufficient number of negative detections, which can usually be achieved by setting the source finding threshold to somewhere around 3 to 4 times the noise level.

    **enable**

      *bool*, *optional*, *default = False*

      Enable the reliability section.

    **threshold**

      *float*, *optional*, *default = 0.9*

      Reliability threshold in the range of 0 to 1. Sources with a reliability below this threshold will be discarded.

    **scaleKernel**

      *float*, *optional*, *default = 0.4*

      When estimating the density of positive and negative detections in parameter space, the size of the Gaussian kernel used in this process is determined from the covariance of the distribution of negative detections in parameter space. This parameter setting can be used to scale that kernel by a constant factor.

    **autoKernel**

      *bool*, *optional*, *default = False*

      If set to true, SoFiA-2 will try to automatically determine the optimal reliability kernel scale factor. If the algorithm fails to converge, then the default value of scaleKernel will be used instead.

    **minPixels**

      *int*, *optional*, *default = 0*

      Minimum total number of spatial and spectral pixels within the source mask for detections to be considered reliable. The reliability of any detection with fewer pixels will be set to zero by default.

    **minSNR**

      *float*, *optional*, *default = 3.0*

      Lower signal-to-noise limit for reliable sources. Detections that fall below this threshold will be deemed unreliable and assigned a reliability of 0.

    **plot**

      *bool*, *optional*, *default = True*

      Diagnostic plots (in EPS format) will be created to allow the quality of the reliability estimation to be assessed. It is advisable to generate and inspect these plots to ensure that the outcome of the reliability filtering procedure is satisfactory.

  **dilation**

    Source mask dilation whereby the mask of each source will be grown outwards until the resulting increase in integrated flux drops below a given threshold or the maximum number of iterations is reached.

    **enable**

      *bool*, *optional*, *default = False*

      Enable the dilation section.

    **iterationsXY**

      *int*, *optional*, *default = 10*

      Sets the maximum number of spatial iterations for the mask dilation algorithm. Once this number of iterations has been reached, mask dilation in the spatial plane will stop even if the flux increase still exceeds the threshold set by threshold.

    **iterationsZ**

      *int*, *optional*, *default = 5*

      Sets the maximum number of spectral iterations for the mask dilation algorithm. Once this number of iterations has been reached, mask dilation in the spatial plane will stop even if the flux increase still exceeds the threshold set by threshold.

    **threshold**

      *float*, *optional*, *default = 0.001*

      If a positive value is provided, mask dilation will end when the increment in the integrated flux during a single iteration drops below this value times the total integrated flux (from the previous iteration), or when the maximum number of iterations has been reached. Specifying a negative threshold will disable flux checking altogether and always carry out the maximum number of iterations.

  **parameter**

    The parametrisation module will be enabled to measure the basic phyiscal parameters of each detected source.

    **enable**

      *bool*, *optional*, *default = False*

      Enable the parametrisation section.

  **output_writeCubelets**

    *bool*, *optional*, *default = False*

    Create individual source products for each detected source, including sub-cubes, masks, moment maps and integrated spectra. The source products will be written to a sub-directory with the suffix _cubelets. Each source product will be labelled with the source ID number for identification.

  **output_thresholdMom12**

    *float*, *optional*, *default = 0.0*

    Create the moment 1 and 2 maps for each individual detection will be created using only those spectral channels where the flux density exceeds this value times the local RMS noise level. E.g., setting output.thresholdMom12 to a value of 3.0 would set a 3-sigma flux density threshold for moments 1 and 2. Note that this setting has no effect on moment 0 maps or global moment 1 and 2 maps.

  **output_writeCatASCII**

    *bool*, *optional*, *default = False*

    Create a source catalogue will be produced in human-readable ASCII format. The catalogue file will have the suffix _cat.txt.

  **output_writeCatXML**

    *bool*, *optional*, *default = False*

    Create a source catalogue will be produced in VO-compatible XML format. The catalogue file will have the suffix _cat.xml.

  **output_writeRawMask**

    *bool*, *optional*, *default = False*

    Create a data cube containing the raw, binary source mask produced by the source finder prior to linking will be written in FITS format. The raw mask cube will have the suffix _mask-raw.fits.

  **output_writeMask2d**

    *bool*, *optional*, *default = False*

    Create an image containing a two-dimensional projection of the 3D mask cube will be written in FITS format. The 2D mask image will have the suffix _mask-2d.fits. Note that some sources may be hidden behind others in this 2D projection.

  **output_writeMoments**

    *bool*, *optional*, *default = False*

    Create images of the spectral moments 0, 1 and 2 and the number of channels in each pixel of the moment 0 map will be written in FITS format. The maps will have the suffix _mom0.fits, _mom1.fits, _mom2.fits and _chan.fits. Note that moments 1 and 2 and the number of channels will not be produced if the input data cube is only two-dimensional.



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


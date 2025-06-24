.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _flag:
 
==========================================
flag
==========================================
 
.. toctree::
   :maxdepth: 1
 
Flagging of the data. The selected flagging steps are executed in the same order in which they are given below.



.. _flag_enable:

--------------------------------------------------
**enable**
--------------------------------------------------

  *bool*

  Execute the flag worker.



.. _flag_field:

--------------------------------------------------
**field**
--------------------------------------------------

  *{"target", "calibrators"}*, *optional*, *default = calibrators*

  Fields that should be flagged. It can be set to either 'target' or 'calibrators' (i.e., all calibrators) as defined in the obsconf worker. Note that this selection is ignored -- i.e., all fields in the selected .MS file(s) are flagged -- in the flagging steps 'flag_mask' and 'flag_Urange' (see below). If a user wants to only flag a subset of the calibrators the selection can be further refined using 'calfields' below. The value of 'field' is also used to compose the name of the .MS file(s) that should be flagged, as explained in 'label_in' below.



.. _flag_label_in:

--------------------------------------------------
**label_in**
--------------------------------------------------

  *str*, *optional*, *default = ' '*

  This label is added to the input .MS file(s) name, given in the getdata worker, to define the name of the .MS file(s) that should be flagged. These are <input>_<label>.ms if 'field' (see above) is set to 'calibrators', or <input>-<target>_<label>.ms if 'field' is set to 'target' (with one .MS file for each target in the input .MS). If empty, the original .MS is flagged with the field selection explained in 'field' above.



.. _flag_calfields:

--------------------------------------------------
**calfields**
--------------------------------------------------

  *str*, *optional*, *default = auto*

  If 'field' above is set to 'calibrators', users can specify here what subset of calibrators to process. This should be a comma-separated list of 'xcal' ,'bpcal', 'gcal' and/or 'fcal', which were all set by the obsconf worker. Alternatively, 'auto' selects all calibrators.



.. _flag_rewind_flags:

--------------------------------------------------
**rewind_flags**
--------------------------------------------------

  Rewind flags to specified version.

  **enable**

    *bool*, *optional*, *default = True*

    Enable the 'rewind_flags' segment.

  **mode**

    *{"reset_worker", "rewind_to_version"}*, *optional*, *default = reset_worker*

    If mode = 'reset_worker' rewind to the flag version before this worker if it exists, or continue if it does not exist; if mode = 'rewind_to_version' rewind to the flag version given by 'version' below.

  **version**

    *str*, *optional*, *default = auto*

    Flag version to rewind to. If set to 'auto' it will rewind to the version prefix_workername_before, where 'prefix' is set in the 'general' worker, and 'workername' is the name of this worker including the suffix '__X' if it is a repeated instance of this worker in the configuration file. Note that all flag versions that had been saved after this version will be deleted.



.. _flag_overwrite_flagvers:

--------------------------------------------------
**overwrite_flagvers**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  Allow CARACal to overwrite existing flag versions. Not recommended. Only enable this if you know what you are doing.



.. _flag_unflag:

--------------------------------------------------
**unflag**
--------------------------------------------------

  Unflag all visibilities for the selected field(s).

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'unflag' segment.



.. _flag_flag_autopowerspec:

--------------------------------------------------
**flag_autopowerspec**
--------------------------------------------------

  Flags antennas based on drifts in the scan average of the auto-correlation spectra per field. This doesn't strictly require any calibration. It is also not field-structure dependent, since it is just based on the DC of the field. Compares scan to median power of scans per field per channel. Also compares antenna to median of the array per scan per field per channel. This should catch any antenna with severe temperature problems.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flag_autopowerspec' segment.

  **scan_thr**

    *int*, *optional*, *default = 3*

    Threshold for flagging (in sigma) above the rest of the scans per field per channel.

  **ant_group_thr**

    *int*, *optional*, *default = 5*

    Threshold for flagging (in sigma) above array median-power spectra per scan per field per channel.

  **col**

    *str*, *optional*, *default = DATA*

    Data column to flag.

  **threads**

    *int*, *optional*, *default = 8*

    Number of threads to use.



.. _flag_flag_autocorr:

--------------------------------------------------
**flag_autocorr**
--------------------------------------------------

  Flag auto-correlations, through the FLAGDATA task in CASA.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flag_autocorr' segment.



.. _flag_flag_quack:

--------------------------------------------------
**flag_quack**
--------------------------------------------------

  Do quack flagging, i.e. flag the beginning and/or end chunks of each scan. Again, this is done through FLAGDATA.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flag_quack' segment.

  **interval**

    *float*, *optional*, *default = 8.*

    Time interval (in seconds) to flag.

  **mode**

    *{"beg", "endb", "tail", "end"}*, *optional*, *default = beg*

    Quack flagging mode. Options are 'beg' (which flags the beginning of the scan), 'endb' (which flags the end of the scan), 'tail' (which flags everything but the first specified seconds of the scan), and 'end' (which flags all but the last specified seconds of the scan).



.. _flag_flag_elevation:

--------------------------------------------------
**flag_elevation**
--------------------------------------------------

  Use CASA FLAGDATA to flag antennas with pointing elevation outside the selected range.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flag_elevation' segment.

  **low**

    *float*, *optional*, *default = 0*

    Lower elevation limit. Antennas pointing at an elevation below this value are flagged.

  **high**

    *float*, *optional*, *default = 90*

    Upper elevation limit. Antennas pointing at an elevation above this value are flagged.



.. _flag_flag_shadow:

--------------------------------------------------
**flag_shadow**
--------------------------------------------------

  Use CASA FLAGDATA to flag shadowed antennas.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flag_shadow' segment.

  **tol**

    *float*, *optional*, *default = 0.*

    Amount of shadow allowed (in metres). A positive number allows antennas to overlap in projection. A negative number forces antennas apart in projection.

  **full_mk64**

    *bool*, *optional*, *default = False*

    Consider all MeerKAT-64 antennas in the shadowing calculation, even if only a subarray is used.



.. _flag_flag_spw:

--------------------------------------------------
**flag_spw**
--------------------------------------------------

  Use CASA FLAGDATA to flag spectral windows/channels.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flag_spw' segment.

  **chans**

    *str*, *optional*, *default = *:856~880MHz , *:1658~1800MHz, *:1419.8~1421.3MHz*

    Channels to flag. Given as "spectral window index:start channel ~ end channel" e.g. "\*:856~880MHz". End channels are not inclusive.

  **ensure_valid**

    *bool*, *optional*, *default = True*

    Check whether the channel selection returns any data. If it does not, FLAGDATA is not executed (preventing the pipeline from crashing). This check only works with the following spw formats (multiple, comma-separated selections allowed), "\*:firstchan~lastchan"; "firstspw~lastspw:firstchan~lastchan"; "spw:firstchan~lastchan"; "firstchan~lastchan". Channels are assumed to be in frequency (Hz, kHz, MHz, GHz allowed; if no units are given it assumes Hz).



.. _flag_flag_time:

--------------------------------------------------
**flag_time**
--------------------------------------------------

  Use CASA FLAGDATA to flag a specified timerange in the data.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flag_time' segment.

  **timerange**

    *str*, *optional*, *default = ' '*

    Timerange to flag. Required in the format 'YYYY/MM/DD/HH:MM:SS~YYYY/MM/DD/HH:MM:SS'.

  **ensure_valid**

    *bool*, *optional*, *default = False*

    Check whether the timerange is in the MS being considered. This stops the pipeline from crashing when multiple datasets are being processed.



.. _flag_flag_scan:

--------------------------------------------------
**flag_scan**
--------------------------------------------------

  Use CASA FLAGDATA to flag bad scans.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flag_scan' segment.

  **scans**

    *str*, *optional*, *default = 0*

    Use CASA FLAGDATA syntax for selecting scans to flag.



.. _flag_flag_antennas:

--------------------------------------------------
**flag_antennas**
--------------------------------------------------

  Flag bad antennas. Or just the ones you have sworn a vendetta against.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flag_antennas' segment.

  **antennas**

    *str*, *optional*, *default = 0*

    Use CASA FLAGDATA syntax for selecting antennas to flag.

  **timerange**

    *str*, *optional*, *default = ' '*

    Timerange to flag. Required in the format 'YYYY/MM/DD/HH:MM:SS~YYYY/MM/DD/HH:MM:SS'.

  **ensure_valid**

    *bool*, *optional*, *default = False*

    Check whether the timerange is in the MS being considered. This stops the pipeline from crashing when multiple datasets are being processed.



.. _flag_flag_mask:

--------------------------------------------------
**flag_mask**
--------------------------------------------------

  Apply a static mask to flag known RFI in all fields of the selected MS file(s).

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flag_mask' segment.

  **mask**

    *str*

    The mask to apply. This can be provided by the user, but CARACal also provides an existing static mask for MeerKAT, specify 'meerkat.rfimask.npy' to use it.

  **uvrange**

    *str*, *optional*, *default = ' '*

    Select range in UV-distance (CASA-style range, e.g. 'lower~upper') for flagging. This is in units of metres. Leaving this parameter blank will select the entire range in UV-distance.



.. _flag_flag_manual:

--------------------------------------------------
**flag_manual**
--------------------------------------------------

  Manually flag subsets of data, using a syntax based on the CASA flagdata task

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flag_manual' segment.

  **rules**

    *list* *of str*, *optional*, *default = ' '*

    Sequence of flagging rules, of the form "ms_pattern key:value key:value"



.. _flag_flag_rfi:

--------------------------------------------------
**flag_rfi**
--------------------------------------------------

  Flag RFI using AOFlagger, Tricolour, or CASA FLAGDATA with tfcrop.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'flag_rfi' segment.

  **flagger**

    *{"aoflagger", "tricolour", "tfcrop"}*, *optional*, *default = aoflagger*

    Choose flagger for automatic flagging. Options are 'aoflagger', 'tricolour' and 'tfcrop'.

  **col**

    *str*, *optional*, *default = DATA*

    Specify which column to flag.

  **aoflagger**

    **strategy**

      *str*, *optional*, *default = firstpass_Q.rfis*

      The AOFlagger strategy file to use.

    **ensure_valid**

      *bool*, *optional*, *default = True*

      Ensure that the selected AOFlagger strategy is compatible with the type of correlations present in the input .MS file(s). E.g., attempts to flag on Stokes V for an .MS with XX and YY only will result in an error and CARACal exiting. The rules are 1. XY,YX must be present in order to flag on Stokes V,U (or on XY,YX), and 2. XX,YY must be present in order to flag on Stokes I,Q (or on XX,YY). Disable this parameter only if you know what you are doing.

    **readmode**

      *{"indirect", "memory", "auto"}*, *optional*, *default = auto*

      AOflagger read mode. If set to 'indirect', AOflagger temporarily writes a reordered .MS file to disc, which results in fast flagging but requires free disc space. If set to 'memory', AOflagger reads the .MS file into memory, which is even faster than 'indirect' but is impossible for large files. If set to 'auto', AOflagger will decide between the 'memory' mode and the 'direct' mode -- the slowest mode -- in which AOFlagger reads baselines by scanning the entire file for the data relevant for the currently required baseline.

  **tricolour**

    **backend**

      *{"numpy", "zarr-disk"}*, *optional*, *default = numpy*

      Visibility and flag data is re-ordered from an MS-row ordering into time-frequency windows ordered by baseline. Options are 'numpy' (if only a few scans worth of visibility data need to be re-ordered) and 'zarr-disk' (for larger data sizes, where re-ordering on disk, rather than in memory, is necessary).

    **mode**

      *{"auto", "manual"}*, *optional*, *default = auto*

      If mode is set to 'manual', Tricolour uses the flagging strategy set via 'strategy' below. If mode is set to 'auto', it uses the strategy in 'strategy_narrowband' in case of small bandwidth of the .MS file(s).

    **strategy**

      *str*, *optional*, *default = mk_rfi_flagging_calibrator_fields_firstpass.yaml*

      Name of the Tricolour strategy file.

    **strat_narrow**

      *str*, *optional*, *default = calibrator_mild_flagging.yaml*

      Name of the Tricolour strategy file to be used for an MS with narrow bandwidth, if mode = 'auto' (see above).

  **tfcrop**

    **usewindowstats**

      *{"none", "sum", "std", "both"}*, *optional*, *default = std*

      Calculate additional flags using sliding-window statistics. Options are 'none', 'sum', 'std', and 'both'. See usewindowstats, within documentation for CASA FLAGDATA, for further details.

    **combinescans**

      *bool*, *optional*, *default = False*

      Accumulate data across scans, depending on the value set for the 'ntime' parameter.

    **flagdimension**

      *{"freq", "time", "freqtime", "timefreq"}*, *optional*, *default = freqtime*

      Dimension(s) along which to calculate a fit(/fits) to the data. Options are 'freq', 'time', 'freqtime', and 'timefreq'. Note that the differences between 'freqtime' and 'timefreq' are only apparent if RFI in one dimension is signifcantly stronger than in the other.

    **timecutoff**

      *float*, *optional*, *default = 4.0*

      Flagging threshold, in units of standard deviation (i.e. sigma) from the fit along the time axis.

    **freqcutoff**

      *float*, *optional*, *default = 3.0*

      Flagging threshold, in units of standard deviation (i.e. sigma) from the fit along the frequency axis.

    **correlation**

      *str*, *optional*, *default = ' '*

      Specify the correlation(s) to be considered for flagging with 'tfcrop'. E.g. 'XX,YY', or leave as '' to select all correlations.



.. _flag_inspect:

--------------------------------------------------
**inspect**
--------------------------------------------------

  Use the diagnostic products of RFInder to inspect the presence of RFI.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'inspect' segment.

  **telescope**

    *{"meerkat", "apertif", "wsrt"}*, *optional*, *default = meerkat*

    Name of the telescope. Options are 'meerkat', 'apertif', and 'wsrt'.

  **field**

    *str*, *optional*, *default = target*

    Field over which to determine flag statistics. Options are 'gcal', 'bpcal', and 'target'.

  **polarization**

    *{"xx", "XX", "yy", "YY", "xy", "XY", "yx", "YX", "q", "Q"}*, *optional*, *default = q*

    Select the polarization, e.g. 'xx', 'yy', 'xy', 'yx', 'q' (and also each of these in upper case).

  **spw_enable**

    *bool*, *optional*, *default = True*

    Enable averaging/rebinning in frequency.

  **spw_width**

    *int*, *optional*, *default = 10*

    Frequency width of rebinned output table (in units of MHz).

  **time_enable**

    *bool*, *optional*, *default = True*

    Enable averaging/rebinning in time.

  **time_step**

    *int*, *optional*, *default = 5*

    Time steps (in units of minutes).



.. _flag_summary:

--------------------------------------------------
**summary**
--------------------------------------------------

  Use CASA FLAGDATA in 'summary' mode to write flagging summary at the end of the pre-calibration flagging.

  **enable**

    *bool*, *optional*, *default = True*

    Enable the 'summary' segment.



.. _flag_report:

--------------------------------------------------
**report**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  (Re)generate a full HTML report at the end of this worker.



.. _flag_cabs:

--------------------------------------------------
**cabs**
--------------------------------------------------

  *list* *of map*, *optional*, *default = ' '*

  Specifies non-default image versions and/or tags for Stimela cabs. Running with scissors: use with extreme caution.


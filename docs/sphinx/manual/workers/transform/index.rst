.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _transform:
 
==========================================
transform
==========================================
 
.. toctree::
   :maxdepth: 1
 
Split, average and/or calibrate the data.



.. _transform_enable:

--------------------------------------------------
**enable**
--------------------------------------------------

  *bool*

  Execute the transform worker.



.. _transform_field:

--------------------------------------------------
**field**
--------------------------------------------------

  *str*, *optional*, *default = target*

  Fields to be split off from the input MS (see split_field below) or whose phase centre should be changed (see changecentre below). Options are (separately) 'target', 'calibrators', 'bpcal', 'gcal' and 'fcal'. Also valid is any combination of 'bpcal', 'gcal' and 'fcal' in a comma-separated string (e.g. 'bpcal, fcal').



.. _transform_label_in:

--------------------------------------------------
**label_in**
--------------------------------------------------

  *str*, *optional*, *default = ' '*

  Label of the input dataset. If split_field/changecentre below are enabled, label_in must be a single value. Else, if concat below is enabled, label_in must include at least two comma-separated values.



.. _transform_label_out:

--------------------------------------------------
**label_out**
--------------------------------------------------

  *str*, *optional*, *default = corr*

  Label of the output dataset.



.. _transform_rewind_flags:

--------------------------------------------------
**rewind_flags**
--------------------------------------------------

  Rewind flags to specified version. Only works with a single-valued label_in (i.e., it does not loop over multiple comma-separated values in label_in).

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'rewind_flags' segment.

  **version**

    *str*, *optional*, *default = ' '*

    Flag version to restore. Note that all flag versions saved after this version will be deleted.



.. _transform_split_field:

--------------------------------------------------
**split_field**
--------------------------------------------------

  Make new MS of targets or calibrators.

  **enable**

    *bool*, *optional*, *default = True*

    Enable the 'split_field' segment.

  **time_avg**

    *str*, *optional*, *default = ' '*

    Time averaging to apply to the data, in units of seconds. If this parameter is instead set to '' or '0s' then no time averaging is applied.

  **chan_avg**

    *int*, *optional*, *default = 1*

    Frequency averaging to apply to the data, given as the number of channels per frequency bin. If this parameter is set to '', '0', or '1', then no frequency averaging is applied.

  **col**

    *str*, *optional*, *default = corrected*

    Column to be split, where the default is 'corrected'.

  **correlation**

    *str*, *optional*, *default = ' '*

    Select the correlations, e.g. 'XX', 'YY'. Setting this to '' means that all correlations are selected.

  **scan**

    *str*, *optional*, *default = ' '*

    Select the scan(s), e.g. '0,2,5'. Setting this to '' means that all scans are selected.

  **create_specweights**

    *bool*, *optional*, *default = True*

    Create a WEIGHT_SPECTRUM column in the output MS.

  **spw**

    *str*, *optional*, *default = ' '*

    Select spectral windows and channels, following the same syntax as for the 'mstransform' task in CASA. Setting this to '' means that all spectral windows and channels are selected.

  **antennas**

    *str*, *optional*, *default = ' '*

    Select antennas, following the same syntax as for the 'mstransform' task in CASA. Setting this to '' means that all antennas are selected.

  **nthreads**

    *int*, *optional*, *default = 1*

    Number of OMP threads to use (currently maximum limited by number of polarizations)

  **otfcal**

    Apply on-the-fly (OTF) calibration.

    **enable**

      *bool*, *optional*, *default = False*

      Enable the 'otfcal' segment.

    **callib**

      *str*, *optional*, *default = ' '*

      Name of the callib file to be used, if user has their own.

    **label_cal**

      *str*, *optional*, *default = ' '*

      Label of the calibration tables to be used.

    **pol_callib**

      *str*, *optional*, *default = ' '*

      Name of the polarization callib file to be used. A corresponding .json file must exist.

    **label_pcal**

      *str*, *optional*, *default = ' '*

      Label of the polarization calibration tables to be used.

    **output_pcal_ms**

      *{"final", "intermediate", "both"}*, *optional*, *default = final*

      Controls which MSs are produced with polcal in effect. 'final' will have polcal-corrected data in DATA. 'intermediate' will have KGB-corrected data in DATA and polcal-corrected data in CORRECTED_DATA. 'both' will produce both (second one, the 'intermediate', will be labelled 'tmp_'+label_out).

    **derotate_pa**

      *bool*, *optional*, *default = True*

      Apply parallactic angle derotation, to put QU into the sky polarization frame.

    **interpolation**

      Overrides interpolations settings for OTF calibration

      **delay_cal**

        *{"default", "nearest", "linear", "nearestPD", "linearPD"}*, *optional*, *default = nearest*

        Interpolation type (in time) for K-Jones (delay) solutions. 'nearest' is recommended for CASA, as 'linear' can produce unexpected results.

      **gain_cal**

        *{"default", "nearest", "linear", "nearestPD", "linearPD"}*, *optional*, *default = linear*

        Interpolation type (in time) for G-Jones (gain) solutions.

      **bp_cal**

        *{"default", "nearest", "linear", "nearestPD", "linearPD"}*, *optional*, *default = linear*

        Interpolation type (in time) for B-Jones (bandpass) solutions.



.. _transform_changecentre:

--------------------------------------------------
**changecentre**
--------------------------------------------------

  Change the phase centre.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'changecentre' segment.

  **ra**

    *str*, *optional*, *default = 0h0m0.0s*

    J2000 RA of the new phase centre, in the format XXhXXmXX.XXs .

  **dec**

    *str*, *optional*, *default = 0d0m0.0s*

    J2000 Dec of the new phase centre, in the format XXdXXmXX.XXs .



.. _transform_concat:

--------------------------------------------------
**concat**
--------------------------------------------------

  For each input .MS and target, concatenate together all .MS files with the different labels included in label_in (comma-separated values). The MS files are concatenated along the frequency axis. This only works if the frequency ranges covered by the MS files are contiguous and if the number of channels is the same for all MS files.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'concat' segment.

  **col**

    *str*, *optional*, *default = data*

    Column(s) to be contatenated, where the default is 'data'; use 'all' for all columns.



.. _transform_obsinfo:

--------------------------------------------------
**obsinfo**
--------------------------------------------------

  Get observation information.

  **enable**

    *bool*, *optional*, *default = True*

    Enable the 'obsinfo' segment.

  **listobs**

    *bool*, *optional*, *default = True*

    Run the CASA 'listobs' task to get observation information.

  **summary_json**

    *bool*, *optional*, *default = True*

    Run the MSUtils summary function to get observation information written as a JSON file.



.. _transform_report:

--------------------------------------------------
**report**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  (Re)generate a full HTML report at the end of this worker.



.. _transform_cabs:

--------------------------------------------------
**cabs**
--------------------------------------------------

  *list* *of map*, *optional*, *default = ' '*

  Specifies non-default image versions and/or tags for Stimela cabs. Running with scissors: use with extreme caution.


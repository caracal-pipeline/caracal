.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _prep:
 
==========================================
prep
==========================================
 
.. toctree::
   :maxdepth: 1
 
Prepare the data for calibration and imaging.



.. _prep_enable:

--------------------------------------------------
**enable**
--------------------------------------------------

  *bool*

  Execute the prep worker (i.e. the  data-preparation step).



.. _prep_label_in:

--------------------------------------------------
**label_in**
--------------------------------------------------

  *str*

  If this label is an empty string this worker operates on the input .MS file(s) given in the getdata worker. If the label is not an empty string then it is added to the input .MS file(s) name (specified for the getdata worker) to define the name of the .MS file(s) to work on. These are named <input>_<label>.ms if 'field' (see below) is set to 'calibrators', or <input>-<target>_<label>.ms if 'field' is set to 'target' (with one .MS file for each target in the input .MS).



.. _prep_field:

--------------------------------------------------
**field**
--------------------------------------------------

  *{"target", "calibrators"}*, *optional*, *default = calibrators*

  In combination with a non-empty 'label_in' (see above), 'field' defines which .MS file(s) to work on. This parameter is ignored if 'label_in' is empty. Options are 'target' and 'calibrators'.



.. _prep_tol:

--------------------------------------------------
**tol**
--------------------------------------------------

  *float*, *optional*, *default = 360.0*

  Tolerance (in arcseconds) for matching calibrator to database names.



.. _prep_tol_diff:

--------------------------------------------------
**tol_diff**
--------------------------------------------------

  *float*, *optional*, *default = 1.0*

  Tolerance (in arcseconds) for checking if coordinates differ from those in the database so much that rephasing is required. Please keep the value less than 'tol' .



.. _prep_fixuvw:

--------------------------------------------------
**fixuvw**
--------------------------------------------------

  Fix the UVW coordinates through the CASA task 'fixvis'.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'fixvis' segment.



.. _prep_fixcalcoords:

--------------------------------------------------
**fixcalcoords**
--------------------------------------------------

  Fix the coordinates of the bandpass calibrators.

  **enable**

    *bool*, *optional*, *default = False*

    Enable fixing calibrator coodinate fixing segment.



.. _prep_clearcal:

--------------------------------------------------
**clearcal**
--------------------------------------------------

  Clear out calibrated data and reset the previous predicted model.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'clearcal' segment

  **addmodel**

    *bool*, *optional*, *default = True*

    Enable 'addmodel' which will add the scratch columns if they don't exist. Switch this on when using crystalball models for primary.



.. _prep_manage_flags:

--------------------------------------------------
**manage_flags**
--------------------------------------------------

  Manage flags.

  **enable**

    *bool*, *optional*, *default = True*

    Enable the 'manage_flags' segment.

  **mode**

    *{"legacy", "restore"}*, *optional*, *default = legacy*

    Mode for managing flags. If set to 'legacy', save the current FLAG column as a 'caracal_legacy' flag version if a flag version with that name does not exisy yet; else restore the 'caracal_legacy' flag version and delete all flag versions created after it. If set to 'restore', restore flags from the flag version specified by 'version' below, and delete all flag versions created after that version.

  **version**

    *str*, *optional*, *default = auto*

    Name of the flag version to restore. If set to 'auto', rewind to the version prefix_workername_before, where 'prefix' is set in the 'general' worker, and 'workername' is the name of this worker including the suffix '__X' if it is a repeated instance of this worker in the configuration file. Note that all flag versions saved after this version will be deleted.



.. _prep_specweights:

--------------------------------------------------
**specweights**
--------------------------------------------------

  How to initialize spectral weights.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'spectral_weights' segment.

  **mode**

    *{"uniform", "calculate", "delete"}*, *optional*, *default = uniform*

    Mode for spectral weights. Options are 'uniform' (set all weights to unity), 'estimate' (estimate spectral weights from frequency-dependent SEFD/Tsys/Noise values, and see 'estimate' segment of this section), and 'delete' (delete WEIGHT_SPECTRUM column if it exists).

  **calculate**

    Calculate spectral weights from frequency-dependent SEFD/Tsys/Noise values.

    **statsfile**

      *str*, *optional*, *default = use_package_meerkat_spec*

      File with SEFD/Tsys/Noise data. If data is from the MeerKAT telescope, you can specify 'use_package_meerkat_spec' to use package data.

    **weightcols**

      *list* *of str*, *optional*, *default = WEIGHT, WEIGHT_SPECTRUM*

      Column names for spectral weights.

    **noisecols**

      *list* *of str*, *optional*, *default = SIGMA, SIGMA_SPECTRUM*

      Column names for noise values.

    **apply**

      *bool*, *optional*, *default = True*

      Write columns to file.



.. _prep_report:

--------------------------------------------------
**report**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  (Re)generate a full HTML report at the end of this worker.



.. _prep_cabs:

--------------------------------------------------
**cabs**
--------------------------------------------------

  *list* *of map*, *optional*, *default = ' '*

  Specifies non-default image versions and/or tags for Stimela cabs. Running with scissors: use with extreme caution.


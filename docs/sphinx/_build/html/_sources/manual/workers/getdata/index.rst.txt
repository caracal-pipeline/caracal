.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _getdata:
 
==========================================
getdata
==========================================
 
.. toctree::
   :maxdepth: 1
 
Download and/or convert/unarchive data so that it is in the measurement set (MS) format for further processing.



.. _getdata_dataid:

--------------------------------------------------
**dataid**
--------------------------------------------------

  *list* *of str*

  Basename of MS. For MeerKAT data to be downloaded by CARACal, this should be the data ID of the observation.



.. _getdata_extension:

--------------------------------------------------
**extension**
--------------------------------------------------

  *str*, *optional*, *default = ms*

  Extension of raw (input) visibility data



.. _getdata_untar:

--------------------------------------------------
**untar**
--------------------------------------------------

  Unarchive MS from an archive file.

  **enable**

    *bool*, *optional*, *default = False*

    Enable the 'untar' segment.

  **tar_options**

    *str*, *optional*, *default = -xvf*

    The tar options to pass to the 'tar' command.



.. _getdata_report:

--------------------------------------------------
**report**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  (Re)generate a full HTML report at the end of this worker.



.. _getdata_ignore_missing:

--------------------------------------------------
**ignore_missing**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  Ignore missing matches/files in the dataid list and proceed with the files that were found. If none can be found, an exception will be raised refardless.



.. _getdata_cabs:

--------------------------------------------------
**cabs**
--------------------------------------------------

  *list* *of map*, *optional*, *default = ' '*

  Specifies non-default image versions and/or tags for Stimela cabs. Running with scissors: use with extreme caution.


.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _general:
 
==========================================
general
==========================================
 
.. toctree::
   :maxdepth: 1
 
General pipeline information, including data IDs, and prefixes for output files.



.. _general_title:

--------------------------------------------------
**title**
--------------------------------------------------

  *str*, *optional*, *default = ' '*

  An optional project title.



.. _general_msdir:

--------------------------------------------------
**msdir**
--------------------------------------------------

  *str*, *optional*, *default = msdir*

  Location where CARACal will write and expect to find .MS files. The only exception is that of read-only input .MS files, which can be located in rawdatadir (see below).



.. _general_rawdatadir:

--------------------------------------------------
**rawdatadir**
--------------------------------------------------

  *str*, *optional*, *default = ' '*

  If set to an empty string this parameter is ignored. If not set to an empty string, this is the directory where CARACal expects to find the input .MS files. This directory and the input .MS files within it can be reado-only, which makes it possible to work on large data without moving them within a shared machine. Any .MS file further created by CARACal is still written to msdir (see above).



.. _general_input:

--------------------------------------------------
**input**
--------------------------------------------------

  *str*, *optional*, *default = input*

  Location where CARACal expects to find various input files (e.g., RFI flagging strategy files).



.. _general_output:

--------------------------------------------------
**output**
--------------------------------------------------

  *str*, *optional*, *default = output*

  Location where CARACal writes output products.



.. _general_prefix:

--------------------------------------------------
**prefix**
--------------------------------------------------

  *str*, *optional*, *default = caracal*

  Prefix for CARACal output products.



.. _general_prep_workspace:

--------------------------------------------------
**prep_workspace**
--------------------------------------------------

  *bool*, *optional*, *default = True*

  Initialise the pipeline by copying input files (i.e. those that are MeerKAT specific, flagging strategies, beam model, etc.).



.. _general_init_notebooks:

--------------------------------------------------
**init_notebooks**
--------------------------------------------------

  *list* *of str*, *optional*, *default = std-progress-report, project-logs*

  Install standard radiopadre notebooks, given by list of basenames.



.. _general_report_notebooks:

--------------------------------------------------
**report_notebooks**
--------------------------------------------------

  *list* *of str*, *optional*, *default = detailed-final-report*

  Like init_notebooks, but will also be automatically rendered to HTML when report=True in a worker



.. _general_final_report:

--------------------------------------------------
**final_report**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  Render report_notebooks to HTML at the end of each pipeline run



.. _general_backend:

--------------------------------------------------
**backend**
--------------------------------------------------

  *{"docker", "udocker", "singularity", "podman"}*, *optional*, *default = docker*

  Which container backend to use (docker, udocker, singularity, podman)



.. _general_cabs:

--------------------------------------------------
**cabs**
--------------------------------------------------

  *list* *of map*, *optional*, *default = ' '*

  Specifies non-default image versions and/or tags for Stimela cabs. Running with scissors: use with extreme caution.


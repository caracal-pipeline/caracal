.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
==========================
Flagging and flag versions
==========================
 
.. toctree::
   :maxdepth: 1
 
**[relevant workers:** :ref:`flag`, :ref:`crosscal`, :ref:`selfcal`, :ref:`line`\ **]**

Several CARACal workers can flag visibilities. The most obvious one is the :ref:`flag`
worker. However, the :ref:`crosscal` and :ref:`selfcal` workers can also flag based on
both  gains and visibilities; and the :ref:`line` worker can flag solar RFI and potential
continuum subtraction errors.

If necessary, users can navigate through the different flagging steps thanks to the flag
versions saved to disk by CARACal.
Most workers can indeed rewind the flags to a specified version, which
allows users to correct errors or repeat certain processing steps without too much effort.

Here we explain the CARACal's management of flag versions, and describe the :ref:`flag`
worker. The flagging done by the other workers mentioned above is described
elsewhere in the :ref:`reduction` section of this CARACal :ref:`manual`.

---------------------------
Management of flag versions
---------------------------

Every run of a CARACal worker that can result in new flags saves two flag version to disc:
one before and one after the worker run. These flag versions are called:

* *<prefix>_<worker_name>_before*
* *<prefix>_<worker_name>_after*

where <prefix> is set by :ref:`general: prefix <general_prefix>`, and the worker name is taken from the 
:ref:`configfile`.

Furthermore, the :ref:`prep` worker (which does not flag) saves a flag version called
*caracal_legacy* for the input .MS (unless that flag version exists already); and the
:ref:`transform` worker (which does not flag either) saves a flag version called
*caracal_legacy* for the .MS file that it creates. A typical :ref:`workflow` starts with one of
these two workers. Thus, .MS files processed by CARACal should always have *caracal_legacy*
as first item of the time-ordered list of flag versions.

Flag versions are stored following the order in which they were created. This makes it
possible to rewind flags to a specified state. All CARACal workers where this operation
is useful have indeed a *rewind_flags* section. When rewinding flags to a certain version,
all versions saved after that are deleted. The exact usage of flags rewinding is
explained in the :ref:`workers` pages.

---------------
The flag worker
---------------

The :ref:`flag` worker can run on the input .MS files given in :ref:`getdata: dataid <getdata_dataid>`
or on .MS files created by
CARACal at various stages of the pipeline (e.g., by the :ref:`transform` worker).
The name of the .MS files to be flagged (if other than the input files) is based on the name of
the input .MS files and a label set by the :ref:`flag: label_in <flag_label_in>` parameter
in this worker. As an example, if the .MS files were created by the :ref:`transform` worker
then :ref:`flag: label_in <flag_label_in>` should be  the same as :ref:`transform: label_out <transform_label_out>`.

The :ref:`flag` worker cannot flag both calibrators and target in one go. To flag both,
two separate :ref:`flag` blocks are required in the :ref:`configfile`, as show in
:ref:`workflow`. In each block the user can set what to flag through the :ref:`flag: field <flag_field>`
parameter.

The :ref:`flag` worker allows users to flag the data in a variety of ways. Unless
otherwise stated below, flagging is done with the CASA task FLAGDATA. Follow the links
below for a detailed documentation of the individual flagging modes.

* Unflag all data.
* Flag on autocorrelations to catch antennas with obvious problems using the custom
  program POLITSIYAKAT (:ref:`flag: flag_autopowerspec <flag_flag_autopowerspec>`).
  Individual scans are compared to the median of all scans per field and channel; and
  individual antennas are compared to the median of all antennas per scan, field and channel.
  Both methods have their own flagging threshold, which users can tune. Users can also set
  which column and which fields to flag.
* Flag all autocorrelations (:ref:`flag: flag_autocorr <flag_flag_autocorr>`).
* Flag specific portions of the beginning and/or end of each scan
  (:ref:`flag: flag_quack <flag_flag_quack>`).
  As in the CASA task FLAGDATA, users can set the time interval that should be flagged
  and the quackmode.
* Flag shadowed antennas (:ref:`flag: flag_shadow <flag_flag_shadow>`).
  Users can tune the amount of shadowing allowed before flagging an antenna.
  For observations obtained with a MeerKAT subarray it is possible to include offline
  antennas in the shadowing calculation.
* Flag selected channel ranges (:ref:`flag: flag_spw <flag_flag_spw>`).
* Flag selected time ranges (:ref:`flag: flag_time <flag_flag_time>`).
* Flag selected antennas (:ref:`flag: flag_antennas <flag_flag_antennas>`).
  Within this task, users can limit the flagging of selected antennas to a
  selected timerange.
* Flag selected scans (:ref:`flag: flag_scan <flag_flag_scan>`).
* Flag according to a static mask of bad frequency ranges using the custom program
  RFIMASKER (:ref:`flag: flag_mask <flag_flag_mask>`). The mask file should
  be located in the *input* directory set by :ref:`general: input <general_input>`.
  Users can decide to limit the flagging to a selected UV range. This could be useful
  to flag short baselines only.
* Flag RFI choosing between the available algorithms and strategies (:ref:`flag: flag_rfi <flag_flag_rfi>`).
  Possible choices include AOFlagger, Tricolour, CASA tfcrop. The requested
  AOFlagger or tricolour strategy file should be located in the *input* directory set by
  :ref:`general: input <general_input>`.
  CARACal comes with a number of strategy files, which are located in
  https://github.com/caracal-pipeline/caracal/tree/master/caracal/data/meerkat_files
  and are copied to the *input* directory by the
  :ref:`general` worker. However, users can copy their own strategy file to the same
  *input* directory and use it within CARACal.

Finally, a summary of the flags can be obtained with :ref:`flag: summary <flag_summary>`.
The summary is available at the relevant log file (see :ref:`products`).

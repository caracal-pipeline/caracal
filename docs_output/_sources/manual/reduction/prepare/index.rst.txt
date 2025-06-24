.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
======
Set up
======
 
.. toctree::
   :maxdepth: 1

**[relevant workers:** :ref:`general`, :ref:`getdata`, :ref:`obsconf`\ **]**

--------------------------------
Directories and input file names
--------------------------------
 
A run of CARACal must always start by setting up a number of directory and file
names. This is done through the :ref:`general` and :ref:`getdata` workers.

In the :ref:`general` worker users give:

* the *msdir* directory where to find/write .MS files (:ref:`general: msdir <general_msdir>`) with the
  only exception of read-only input .MS files, which can be located at *rawdatadir* (see below);
* if necessary, the *rawdatadir* directory where to find the read-only input .MS files, which
  facilitates the processing of large files on shared machines without having to move them
  (:ref:`general: rawdatadir <general_rawdatadir>`);
* the *output* directory where to write all output data products (:ref:`general: output <general_output>`);
* the *input* directory where to find various input files, such as AOflagger strategies etc.
  (:ref:`general: input <general_input>`);
* the prefix for the output data products (:ref:`general: prefix <general_prefix>`).

If they do not exist yet, the directories *msdir*, *input* and *output* can be created by setting
:ref:`general: prep_workspace <general_prep_workspace>` to *true*. This also copies
files from the caracal/data/meerkat_files directory to the *input* directory set above.

In the :ref:`getdata` worker users give the name of the .MS files to be processed
(:ref:`getdata: dataid <getdata_dataid>`). Furthermore, the following optional steps are available:
Optionally, the input files can be untar-ed (:ref:`getdata: untar <getdata_untar>`).

--------
Metadata
--------

Finally, before starting the actual data processing users need to provide some
info about the content of the input .MS files through the :ref:`obsconf` worker.
Target and calibrators name can be set by editing the relevant parameters of
this worker:

* :ref:`obsconf: target <obsconf_target>`
* :ref:`obsconf: bpcal <obsconf_bpcal>`
* :ref:`obsconf: fcal <obsconf_fcal>`
* :ref:`obsconf: gcal <obsconf_gcal>`

In fact, CARACal can automatically extract most of these info from the .MS files
themselves. To do so users should enable the :ref:`obsconf: obsinfo <obsconf_obsinfo>`
parameter. This writes a .JSON and a .TXT file to disc. Once the .JSON file is
on disc, the  above parameters can be read automatically from that file.
Users can, however, set any of the above manually.

Special attention should be paid to the choice of a reference antenna.
Users should carefully select a good reference antenna for calibration
with the :ref:`obsconf: refant <obsconf_refant>`
parameter.

---------------
Additional info
---------------

The :ref:`obsconf` worker can also produce a plot showing the elevation track of all
targets and calibrators, and return the time range during which the data were
taken with the Sun below the horizon.

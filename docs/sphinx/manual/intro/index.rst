.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _intro:

============
Introduction
============
 
.. toctree::
   :maxdepth: 1
 
What is CARACal?
------------------

CARACal is a pipeline to reduce radio interferometry continuum and spectral line data in
total intensity. It works on data from any radio interferometer as long as they are in
“measurement set” format.

CARACal is essentially a collection of Python/Stimela scripts.
`Stimela <https://github.com/ratt-ru/Stimela>`_ is a platform-independent radio
interferometry scripting framework based on Python and container technology
(e.g., Docker, Singularity). Stimela allows users to
execute tasks from many different data reduction packages in Python without having to
install those packages individually (e.g., CASA, MeqTrees, AOflagger, SoFiA, etc.).
Using Stimela, the different software packages are available through a unified scheme.
CARACal consists of a sequence of Stimela scripts, which it links and runs sequentially.

Within CARACal --- and throughout this documentation --- the individual Stimela scripts are called
"workers". Each CARACal worker corresponds to a specific section of the data reduction
process (e.g., flagging, cross-calibration). Each worker
executes several tasks from the interferometry packages included in Stimela (e.g., the
cross-calibration worker can calibrate delays, bandpass, gains and flux scale).

In practice, users tell CARACal what to do --- and how to do it --- via a YAML configuration file.
The configuration file has one section for each run of a worker (some workers, e.g., the flagging
one, might need to be run multiple times). By editing the configuration
file users control the workers' options, deciding which tasks to run and with what settings.
An explanation of the configuration file syntax is given in the :ref:`configfile`
section of this manual.

Users will not have to touch anything but the configuration file. They can check
what has happened through a variety of data products, including images, diagnostic plots and log files.
A list of all CARACal data products is available at the :ref:`products` section of this manual.

In the rest of this Introduction we give the complete list and a brief description of each worker.
A more comprehensive
description is available in the :ref:`reduction` section of this manual, which follows
the flow of a typical data reduction process. The full list of parameters available for
the individual workers through the configuration file can be found at the :ref:`workers`
section of this manual or following the links below.

.. _workerlist:

List and Brief description of CARACal workers
---------------------------------------------

The following workers are available in CARACal. Typically, they are executed in the
same order in which they are given below. Only the first three workers (general, getdata
and  obsconf) should always be executed. All other workers are optional.

:ref:`general`
^^^^^^^^^^^^^^

This worker sets the name of various input/output directories
and the prefix used for the output data products (e.g., diagnostic plots, images, etc.).

:ref:`getdata`
^^^^^^^^^^^^^^

This worker sets the name of the files to be processed and whether any conversion to
.MS format is necessary.

:ref:`obsconf`
^^^^^^^^^^^^^^

This worker collects basic information on the content of the .MS files to be
processed (e.g., target and calibrators' name, channelisation, etc.). The worker can also
extract this information automatically from the .MS metadata.

:ref:`transform`
^^^^^^^^^^^^^^^^

This worker splits the calibrators (in preparation for
cross-calibration) or the targets (in preparation for imaging) to new .MS files.
Time and frequency averaging is available, as well as phase rotation to
a new phase centre. Crosscalibration can be applied on the fly while splitting.

:ref:`prep`
^^^^^^^^^^^

This worker prepares the data for calibration and imaging. For example, it can
recalculate UVW coordinates, add spectral weights based on Tsys measurements, and
flag a "legacy" flag version.

:ref:`flag`
^^^^^^^^^^^

This worker flags the data and returns statistics on the flags. As all other
workers, it can be run multiple times within a single CARACal run as explained at
:ref:`configfile` (though this feature is not necessarily useful for all workers).
It can flag data based on, e.g., channel-, antenna- and time selection, or using automated
algorithms that run on autocorrelations (to catch antennas with clear problems) or
crosscorrelations. It can also unflag all data.

:ref:`crosscal`
^^^^^^^^^^^^^^^

This worker cross-calibrates the data. Users can design their own calibration strategy including
delay, bandpass, gains and flux scale calibration, self-calibration of the secondary, and flagging.
The calibration is applied to the calibrators' visibilities for later inspection.
Numerous settings are available for users to decide how to calibrate. Gain plots are produced.

:ref:`inspect`
^^^^^^^^^^^^^^

This worker plot the visibilities for diagnostic purpose. Several different kinds of plots can be made.

:ref:`mask`
^^^^^^^^^^^

This worker creates an a-priori clean mask based on NVSS or SUMSS, 
to be used during the continuum imaging/self-calibration loop. It can also merge the
resulting mask with a mask based on an existing image.

:ref:`selfcal`
^^^^^^^^^^^^^^

This worker performs continuum imaging and standard (i.e., direction-independent)
self-calibration. Automated convergence of the calibration procedure is optionally
available. This worker can also interpolate and transfer sky model and calibration tables
to another .MS (e.g., from a coarse- to a fine-channel .MS file).

:ref:`line`
^^^^^^^^^^^

This worker creates spectral-line cubes and images. It can subtract the continuum via both
model and UVLIN-like subtraction, Doppler correct, flag solar RFI, perform
automated iterative cleaning with 3D clean masks, and, finally, run a spectral-line source
finder.

:ref:`mosaic`
^^^^^^^^^^^^^

This worker mosaics continuum images or line cubes using a Gaussian primary beam with FWHM
= 1.02 lambda / antenna_diameter out to a cutoff level.

.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _workflow:
 
================
CARACal workflow
================
 
.. toctree::
   :maxdepth: 1

A CARACal run involves running a sequence of CARACal workers following the order in which they
are listed in the user configuration file. For reference see :ref:`workerlist`.

Users can design their own CARACal workflow with some level of flexibility. The exact workflow
(i.e., the exact sequence of CARACal workers) will depend on science goals but also on I/O
constraints. Below we provide two examples.

.. _recommended:

Recommended workflow
--------------------

The currently recommended CARACal workflow minimises the data volume increase
during the course of a full CARACal run, and allows to treat the input .MS file(s)
as read-only.

This workflow is implemented in all sample configuration files located at 
https://github.com/caracal-pipeline/caracal/tree/master/caracal/sample_configurations .

| :ref:`general`
| Compulsory worker to set up data/input/output directories.
| :ref:`getdata`
| Compulsory worker to specify the input .MS files.
| :ref:`obsconf`
| Compulsory worker to set up target/calibrators names.

| :ref:`transform`
| Split calibrators-only .MS files, one per input .MS file.
| :ref:`prep`
| Prepare the calibrators-only .MS files for processing.
| :ref:`flag`
| Flag the calibrators-only .MS files.
| :ref:`crosscal`
| Derive the cross-calibration tables and apply them to the calibrators.
| :ref:`inspect`
| Inspect the calibrated calibrator's visibilities to check the quality of the
| cross-calibration.

| :ref:`transform`
| Split target-only .MS files, one per input .MS file and target, applying the
| cross-calibration on the fly.
| :ref:`prep`
| Prepare the target-only .MS files for processing.
| :ref:`flag`
| Flag the target-only .MS files.

| :ref:`transform`
| Average the target-only .MS files in frequency for continuum imaging.
| :ref:`flag`
| Flag line channels in the averaged target-only .MS files.
| :ref:`selfcal`
| Make a continuum image of each target, self-calibrate, and transfer both gains
| and continuum model to the full-frequency-resolution target-only .MS files.

| :ref:`line`
| Subtract the continuum, Doppler correct and make the line cube and moment
| images from the target-only .MS files.

| :ref:`mosaic`
| Mosaic the continuum images of the targets.
| :ref:`mosaic`
| Mosaic the line cubes.

Note that this workflow includes multiple runs of several workers. These require a
"__<suffix>" in the configuration file, as described at the page :ref:`configfile`.

The data volume budget of this workflow is:

* input .MS:

  * DATA

* target-only .MS:

  * DATA
  * CORRECTED_DATA
  * MODEL

* calibrators-only .MS (negligible data volume):

  * DATA
  * CORRECTED_DATA
  * MODEL

* target-only frequency-averaged .MS (negligible data volume):

  * DATA
  * CORRECTED_DATA
  * MODEL

Simple workflow
---------------

Simpler workflows than the recommended one are possible. For example, the workflow
below employs less worker runs. However, it modifies the input .MS file(s), results into
a larger data volume increase, and runs the self-calibration loop on larger .MS files.

| :ref:`general`
| Compulsory worker to set up data/input/output directories.
| :ref:`getdata`
| Compulsory worker to specify the input .MS files.
| :ref:`obsconf`
| Compulsory worker to set up target/calibrators names.

| :ref:`prep`
| Prepare the input .MS files for processing.
| :ref:`flag`
| Flag the calibrators in the input .MS files.
| :ref:`crosscal`
| Derive the cross-calibration tables and apply them to target and calibrators.
| :ref:`inspect`
| Inspect the calibrated calibrator's visibilities to check the quality of the
| cross-calibration.

| :ref:`transform`
| Split cross-calibrated target-only .MS files, one per input .MS file and target.
| :ref:`flag`
| Flag the target-only .MS files.
| :ref:`selfcal`
| Make a continuum image of each target, self-calibrate, and transfer both gains
| and continuum model to the full-frequency-resolution target-only .MS files.

| :ref:`line`
| Subtract the continuum, Doppler correct and make the line cube and moment
| images from the target-only .MS files.

| :ref:`mosaic`
| Mosaic the continuum images of the targets.
| :ref:`mosaic`
| Mosaic the line cubes.


The data volume budget of this workflow is:

* input .MS

  * DATA
  * CORRECTED_DATA
  * MODEL

* target-only .MS

  * DATA
  * CORRECTED_DATA
  * MODEL

Typically, this is 50% larger than for the :ref:`recommended` above.

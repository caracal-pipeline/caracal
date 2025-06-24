.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
======================================
Continuum imaging and self-calibration
======================================
 
.. toctree::
   :maxdepth: 1
 
**[relevant workers:** :ref:`transform`, :ref:`flag`, :ref:`selfcal`, :ref:`mask`\ **]**

-------------------------------------------
Split, average and flag target visibilities
-------------------------------------------

Following cross-calibration, CARACal creates a new .MS file which contains the
cross-calibrated target visibilities only. This is done by the :ref:`transform`
worker. In case the cross-calibration tables have not been applied to the target
by the :ref:`crosscal` worker, :ref:`transform` can do so on the fly while
splitting using the CASA task MSTRANSFORM.

Optionally, the :ref:`transform` worker can average in time and/or frequency
while splitting. Depending on the science goals, it might be useful to run this
worker more than once. E.g., the first time to create a frequency-averaged dataset
for continuum imaging and self-calibration, and the second time to create a
narrow-band dataset for spectral-line work. The possibility of running this
worker multiple times within a single CARACal run allows users to design the
best :ref:`workflow` for their project.

Before self-calibrating it might also be good to flag the target's visibilities.
(Typically the target is not flagged before applying the cross-calibration.) This can
be done with the :ref:`flag` worker (which was probably already run on the
calibrators' visibilities before cross-calibration) setting :ref:`flag: field <flag_field>`
to target.

--------------------------------------
Image the continuum and self-calibrate
--------------------------------------

Having cross-calibrated, split, optionally averaged and flagged the target, it is now possible
to iteratively image the radio continuum emission and self-calibrate the visibilities.
The resulting gain tables and continuum model can also be transferred to another
.MS file (particularly useful for spectral line work). All this can be done with the
:ref:`selfcal` worker.

Several parameters allow users to set up both the imaging and self-calibration
according to their needs. Imaging is done with WSclean, and the parameters of this
imaging software are available in the :ref:`selfcal` worker. Calibration is done
with either Cubical or MeqTrees, and also in this case the :ref:`selfcal` worker
includes the parameters available in those packages.

Additional parameters allow users to decide how many calibration iterations to
perform through the parameter :ref:`selfcal: cal_niter  <selfcal_cal_niter>`.
For a value N, the code will create N+1 images following the sequence image1,
selfcal1, image2, selfcal2, ... imageN, selfcalN, imageN+1.

Optionally, users can enable :ref:`selfcal: aimfast  <selfcal_aimfast>`, which
at each new iteration compares the new continuum image with the previous one and
decides whether the image has improved significantly. In case it has not, no further
iterations are performed. In this case therefore :ref:`selfcal: cal_niter  <selfcal_cal_niter>`
is the maximum number of iterations.

While imaging it is usually convenient to identify where to clean. Within CARACal this can be
done in several different ways through the parameter :ref:`selfcal: image: clean_mask_method <selfcal_image>`:

* with WSclean automated masking method, which cleans blindly down to a masking threshold,
  defines the clean mask as the ensamble  of all cleaned pixels, and then re-cleans them
  down to a deeper clean cutoff;
* with SoFiA, which makes a clean mask for the Nth imaging run from the (N-1)th image;
* with a clean mask made by the :ref:`mask` worker or supplied by the user.

Several parameters allow users to control the calibration step in the :ref:`selfcal` worker. 
Users can set the time and frequency solution intervals. Gain phase and amplitude can both be solved
for, each with its own time and frequency solution interval (more standard phase-only
self-calibration is also possible). We refer to the :ref:`selfcal` page for a full description
of all available modes and parameters.

-----------------------
Gain and model transfer
-----------------------

If the self-cal loop was executed on a frequency-averaged .MS file, it might
be necessary to transfer the resulting gains and continuum model back to the
original, full-frequency-resolution .MS file. This is done with 
:ref:`selfcal: transfer_apply_gains  <selfcal_transfer_apply_gains>` (using Cubical)
and :ref:`selfcal: transfer_model  <selfcal_transfer_model>` (using Crystalball),
respectively. The latter allows users to limit the model transfer to the N brightest
sources, to sources in a region, or to point sources only. Be aware that the
model transfer step can be very time consuming for large .MS files.

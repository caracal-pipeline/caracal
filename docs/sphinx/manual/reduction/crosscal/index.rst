.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
=========================
Cross-calibration
=========================
 
.. toctree::
   :maxdepth: 1
 
**[relevant workers:** :ref:`crosscal`, :ref:`inspect`\ **]**

Cross-calibration runs largely on CASA tasks. Using these tasks, CARACal allows users to
solve for delays, bandpass, gains and flux scale in several different ways. The :ref:`crosscal`
worker operates within the framework that .MS files  include
a primary (bandpass and flux) calibrator and, optionally, a secondary (gains) calibrator.

Just as a classic, simple example, it is possible to solve for:

1. time-independent antenna delays and normalised bandpass based on the observation of the
   primary calibrator;
2. time-dependent antenna flux scale based on the observation of the primary calibrator;
3. time-dependent antenna gains based on the observation of the secondary calibrator;
4. time-dependent antenna flux scale at fine time resolution obtained by scaling the gains
   from step 3 above to the gains from step 2 above.

However, CARACal allows users to take less traditional cross-calibtation steps, too,
such as self-calibration on the secondary calibrator, or delay
calibration on the secondary, and to flag the calibrated visibilities in between
calibration steps.

-------------------------------------
Flexible cross-calibration strategies
-------------------------------------

CARACal allows for powerful and sophisticated cross-calibration strategies thanks
to the flexibility provided by the parameters :ref:`crosscal: primary: order <crosscal_primary>`
and :ref:`crosscal: secondary: order <crosscal_secondary>`. These allow users to build
their favourite sequence of calibration/imaging/flagging steps choosing among:

* K = delay calibration with CASA GAINCAL
* B = bandpass calibration with CASA BANDPASS
* G = gain amplitude and/or phase calibration with CASA GAINCAL
* F = gain amplitude and/or phase calibration with CASA GAINCAL, followed by bootstrapping
  of the flux scale from the primary calibrator with CASA FLUXSCALE (secondary calibrator only)
* I = imaging with WSCLEAN (secondary calibrator only)
* A = flagging with CASA FLAGDATA using the tfcrop algorithm

Each of these steps may have its own settings with respect to gain type (e.g., each G could
be amplitude-only, phase-only, or both amplitude and phase), solution interval, normalisation,
data combination at boundaries (e.g., scan, SPW), imaging and flagging settings. 

For example, :ref:`crosscal: primary: order <crosscal_primary>`: KGBAKGB results in:

* delay calibration (K);
* gain calibration (G) applying the intial K on the fly;
* bandpass calibration (B) applying the initial K and G on the fly;
* flagging of the visibilities with the initial K, G and B applied;
* final K calibration applying the initial G and B on the fly;
* final G calibration applying the final K and initial B on the fly;
* final B calibration applying the final K and G on the fly.

In this example, it would be possible to set different solution intervals for the
initial and final G through the :ref:`crosscal: primary: solint <crosscal_primary>`
parameter, which is a sequence containing one entry per element in
:ref:`crosscal: primary: order <crosscal_primary>`. In case the solution interval
is not relevant (A and I steps) users can  give an empty string ''. The same applies to
the calibration parameters :ref:`crosscal: primary: calmode <crosscal_primary>` and
:ref:`crosscal: primary: combine <crosscal_primary>`.

An example for the secondary is :ref:`crosscal: secondary: order <crosscal_primary>`: FIG,
which results in:

* gain calibration and bootstrapping of the flux scale;
* imaging;
* gain calibration.

Note that in this example no bootstrapping of the flux scale is necessary
after the second gain calibration G because the gains are now self-calibrated on
a I sky model which, following the initial F, is already on the correct flux scale.

We refer to the :ref:`crosscal` page for a complete description of all cross-calibration parameters.

------------------------------------------------
Apply the cross-calibration and diagnostic plots
------------------------------------------------

CARACal can apply the cross calibration tables to all calibrators (useful for diagnostics).
It can also apply it to the target, although this can also be done by the :ref:`transform` worker
on the fly while splitting the  target from the input .MS file.
When applyin the calibration, the :ref:`crosscal` worker adopts the following interpolation rules:

* Delay calibration: applied to primary, secondary, target with nearest, linear, linear interpolation, respectively.
* Bandpass calibration: applied to primary, secondary, target with nearest, linear, linear interpolation, respectively.
* Gain calibration before bootstrapping the flux scale: applied to primary, secondary, target
  with linear, linear, linear interpolation, respectively.
* Gain calibration after bootstrapping the flux scale: applied to primary, secondary, target
  with linear, nearest, linear interpolation, repsectively.

The :ref:`crosscal` worker produces .HTML plots of the various calibration terms for later, interactive
inspection. Furthermore, the :ref:`inspect` worker produces .PNG plots of the caibrators'
calibrated visibilities to check the quality of the calibration. A variety of standard plots are produced,
such as phase-vs-uvdistance and real-vs-imaginary. Furthermore, users can define their own plots as
described in the :ref:`inspect` page.

*We strongly recommend that users inspect the .HTML and .PNG plots produced by the*
:ref:`crosscal` *and* :ref:`inspect` *workers to ensure that the quality of the cross-calibration
is adequate to their science goals.*

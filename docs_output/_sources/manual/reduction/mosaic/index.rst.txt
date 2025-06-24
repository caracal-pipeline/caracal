.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
==========
Mosaicking
==========
 
.. toctree::
   :maxdepth: 1

**[relevant workers:** :ref:`mosaic`\ **]**

A single CARACal run can image multiple target fields distributed
in an arbitrary manner over multiple input .MS files. If adjacent, these
fields could be mosaicked together both in continuum and spectral line
using the :ref:`mosaic` worker. This worker can take existing primary beam
images or make its own. At the moment these are simple Gaussian primary
beams. More realistic primary beam are likely to be implemented in the
future.

This section of the CARACal :ref:`manual` is under development and
currently lacks detailed information on mosaicking. For complete
information on the modes and parameters please see the :ref:`mosaic` worker
page.

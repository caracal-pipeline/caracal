.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _products:
 
=============
Data products
=============
 
.. toctree::
   :maxdepth: 1

The following data products are written by CARACal.
 
Diagnostic Plots
^^^^^^^^^^^^^^^^
Diagnostic plots are located in the output/diagnostic_plots directory, where the cross and self calibration plots are separated into their respective subdirectory.

Calibration Tables
^^^^^^^^^^^^^^^^^^
Calibration tables produced by the pipeline in the cross calibration process can be found in the output/caltables directory, while the self calibration products can be found in the output/continuum/selfcal_products directory.

Continuum Images
^^^^^^^^^^^^^^^^
Continuum images are located in the output/continuum directory, labeled as image_N where N is the number in the self calibration process.
 
Spectral-line Cubes
^^^^^^^^^^^^^^^^^^^
Spectral-line cubes are located in the output/cubes directory, labeled as cube_N where N is the Nth cube produced from the clean + mask process. 

Spectral-line Moment Images
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Spectral-line moment images are located (when source finding is enabled) in the respective cube_N directory. For example, the moment maps of cube_3 are located in output/cubes/cube_3.

Mask Files
^^^^^^^^^^
Mask files produced by the pipeline are located in the output/masking directory. Self-provided masks to be used by the pipeline also need to be located in this directory.

Log Files
^^^^^^^^^
A copy of the pipeline log is located in the output directory. All logs are located in the output/logs directory, with the timestamp YYYYMM-HHMM. A copy of the config file is located in the output/cfgFiles directory, with the same timestamp. 

Reports
^^^^^^^
HTML Reports on the observation(s) and sources are located in the output/reports directory.

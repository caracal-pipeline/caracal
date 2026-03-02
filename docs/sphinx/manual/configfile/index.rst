.. meerkathi documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _configfile:

==================
Configuration file
==================
 
.. toctree::
   :maxdepth: 1
 
Users settings are passed to CARACal through a configuration file consisting of a
sequence of blocks --- each corresponding to the run of a CARACal worker. The workers
are run following the order in which they appear in the configuration file.
For reference see :ref:`workerlist`.

The following workers must always run be and, therefore, must always appear in the
configuration file: :ref:`general`, :ref:`getdata` and :ref:`obsconf`. All
other workers are optional.

Within each worker's block of the configuration file, the worker's parameters are arranged
in a nested structure following the YAML syntax rules (see https://yaml.readthedocs.io).
As an example, a block of the config file may look like::

  worker_name:
    enable: true
    parameter_1: value_1
    parameter_2:
      parameter_2_1: value_2_1
      parameter_2_2: value_2_2
    parameter_3: value_3
    ...

The complete list of all workers' parameters is available at :ref:`workers`,
where the parameters' nesting is also illustrated.

Workers can be executed more than once in a single run of CARACal. This could be useful,
for example, if a user wants to flag the data both before and after cross-calibration.
To indicate a new run of a worker the worker name must be followed by "__<suffix>" in the
configuration file (note the double underscore). The first run of a worker can also have
a "__<suffix>" but does not have to. In the example above, the flag
worker must thus appear twice in the configuration file::

   flag__beforecrosscal:
     enable: true
     parameter_1: value_1A
     ...

   [other workers]
   
   flag__aftercrosscal:
     enable: true
     parameter_1: value_1B
     ...

Most parameters are optional and do not need to be included in the configuration file.
Their default values are set to work in as many cases as possible. A few parameters are
compulsory. The pages at :ref:`workers` indicate whether a parameter is optional, its data
type, allowed values (if applicable) and default value.

CARACal comes with a set of sample configuration files. These are available at
https://github.com/caracal-pipeline/caracal/tree/master/caracal/sample_configurations
and include, for example:

* minimalConfig.yml, which includes as few parameters as possible and performs a basic
  data reduction including both continuum and spectral line imaging;
* meerkat-continuum-defaults.yml, which is optimised for the reduction of data taken with
  the MeerKAT telescope for the purpose of total-intensity continuum imaging.

Users could take these sample configuration files as a starting point for their work.

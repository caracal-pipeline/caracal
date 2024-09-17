=======
CARACal
=======

|Build Version|
|Doc Status|
|Pypi Version|
|Python Versions|
|Project License|

CARACal stands for Containerized Automated Radio Astronomy Calibration and is a pipeline for radio interferometry data reduction

Main website: `caracal.rtfd.io <https://caracal.readthedocs.io/>`_

It includes the Install & Run instructions described below, and much more.

==================
Installation & Run
==================

Usage and publication policy
-------------------------------

When using CARACal please be aware of and adhere to the `CARACal publication policy <https://docs.google.com/document/d/e/2PACX-1vTqZoKhdewnWvxvEI4C9DxI-IHP1FTCoC5Iqz_MqlX63q8UnnpcqoZvVf-sSfqACu8sA_nufkXICUH6/pub>`_.

Requirements
------------
* `Python <https://www.python.org>`_ 3.8 or higher.
* Container technology of your choice. It can be one of the following:
    * `Docker <https://www.docker.com>`_
    * `Podman <https://podman.io>`_
    * `Singularity <https://github.com/sylabs/singularity>`_ 3.5 - 3.9 (nearly all functionality available for > 2.6.0-dist, see `here <https://github.com/caracal-pipeline/caracal/issues/1154>`_ for further information) 
        * `Apptainer <https://apptainer.org>`_ does not support all CARACal functionalities (at the moment).

============
Installation
============

We strongly recommend and describe an installation using a `Python3` virtual environment. Only try outside a virtual environment if you know what you are doing. Any name as ``${name}`` occurring in the description below can be chosen arbitrarily. If it symbolises directories or files, those directories or files should exist and the user should have **write** access.

There are three (3) available methods to install the `caracal` pipeline: 

1. Manual
---------

Choose the name of the virtual environment `${caracal-venv}`. Then:

..  code-block:: bash

    $ python3 -m venv ${caracal-venv}

OR, if the command above does not work

..  code-block:: bash

    virtualenv -p python3 ${caracal-venv}
    source ${caracal-venv}/bin/activate
    pip install -U pip setuptools wheel

For a CARACal stable release run:

..  code-block:: bash

    pip install -U caracal

CARACal has a few optional dependencies (*scipy, astropy, regions, astroquery*) which are not installed by default. But to get full functionality, you can install them by running:

..  code-block:: bash

    pip install -U caracal[all]

And CARACal developer version which is not recommended for users:

..  code-block:: bash

    pip install -U 'caracal[all] @ git+https://github.com/caracal-pipeline/caracal.git@master'



*Ignore any error messages concerning `pyregion`.*

2. `caratekit.sh` script
------------------------

Download the installation script `caratekit.sh <https://github.com/caracal-pipeline/caracal/blob/master/caratekit.sh>`_ . Choose the parent directory ``${workspace}`` and the name of the CARACal directory ``${caracal_dir}``. Any name as ``${name}`` occurring in the description below can be chosen arbitrarily. If it symbolises directories or files, those directories or files should exist and the user should have write acccess.

If using `Docker <https://www.docker.com>`_:

..  code-block:: bash

    caratekit.sh -ws ${workspace} -cr -di -ct ${caracal_dir} -rp install -f -kh


If using `Singularity <https://github.com/sylabs/singularity>`_:

..  code-block:: bash

    caratekit.sh -ws ${workspace} -cr -si -ct ${caracal_testdir} -rp install -f -kh


3. Poetry (For developers)
--------------------------

Installation from source using `poetry`. First, install poetry:

..  code-block:: bash

    pip install poetry


In the working directory where source is checked out run `poetry install` or to include all optional dependencies:

..  code-block:: bash

    poetry install --extras all

=========================================
Installation on (ILIFU) slurm environment
=========================================

The installation of CARACal on ilifu has been tried and tested on the Ubuntu 20.0 operating system, although, it should also work on other OS versions. On the login node, follow these instructions:

..  code-block:: bash

    cd /path/to/working/area
    module add python/3.9.4
    python3 -m venv <venv-name>
    source <venv-name>/bin/activate
    pip install -U pip setuptools wheel
    git clone https://github.com/caracal-pipeline/caracal.git
    pip install -U -e caracal
    deactivate

where ``/path/to/working/area`` is the actual path to the directory where you wish to install CARACal.
In principle, this can also be done in the Slurm environment of ILIFU by submitting an *sbatch* script.
Please amend your `pip install` command with `[all]` option when needed in accordance with the instructions above.
Install the latest release with:

..  code-block:: bash

    pip install -U caracal


NB: The stimela singularity images needed for CARACal are stored in this location: ``/software/astro/caracal/``
where you can access the latest version of the images, for example, ``/software/astro/caracal/STIMELA_IMAGES_1.7.0``. 

=======
Running
=======

If you installed CARACal manually, activate the virtual environment with:

..  code-block:: bash

    source ${caracal-venv}/bin/activate

If you installed CARACal with the ``caratekit.sh`` script, activate the virtual environment with:

..  code-block:: bash

    source ${workspace}/${caracal_dir}/caracal_venv/bin/activate

If using `Docker <https://www.docker.com>`_ run CARACal with:

..  code-block:: bash

    caracal -c ${your-configuration-file}

If using `Singularity <https://github.com/sylabs/singularity>`_ run CARACal with:

..  code-block:: bash

    caracal -ct singularity -c ${your-configuration-file}

For more detailed installation instructions, troubleshooting tips and a full user manual please see `caracal.readthedocs.io <https://caracal.readthedocs.io>`_.

==========
New issues
==========

We encourage users who experience problems installing or running CARACal to check for known issues or open a new issue at
our `GitHub issues page <https://github.com/caracal-pipeline/caracal/issues>`_.

When opening a new issue, please include your:
  #. installation type (e.g., Docker, Singularity)
  #. software version (both CARACal and Stimela)
  #. CARACal configuration file
  #. CARACal log files.

In case you are *not* carrying out a fresh installation, remove earlier Stimela images with:

..  code-block:: bash

    stimela clean -ac


Then, if using `Docker <https://www.docker.com>`_:

..  code-block:: bash

    stimela pull

If using `Singularity <https://github.com/sylabs/singularity>`_, choose a pull folder ``${singularity_pull_folder}``, where the `Singularity <https://github.com/sylabs/singularity>`_ images are stored and define an environment variable by adding this in the rc file of your shell (e.g. .bashrc):

..  code-block:: bash

    export SINGULARITY_PULLFOLDER=${WORKSPACE_ROOT}/singularity_images

and run:

..  code-block:: bash

    stimela pull -s

If using `Podman <https://podman.io>`_:

..  code-block:: bash

  stimela pull -p 

=======
License
=======

This project is licensed under the GNU General Public License v2.0 - see license_ for details.

==========
Contribute
==========

Contributions are always welcome! Please ensure that you adhere to our coding
standards pep8_.

.. |Doc Status| image:: https://readthedocs.org/projects/caracal/badge/?version=latest
                :target: http://caracal.readthedocs.io/en/latest
                :alt:

.. |Pypi Version| image:: https://img.shields.io/pypi/v/caracal.svg
                  :target: https://pypi.python.org/pypi/caracal
                  :alt:
.. |Build Version| image:: https://github.com/caracal-pipeline/caracal/actions/workflows/continuous_integration.yml/badge.svg
                  :target: https://github.com/caracal-pipeline/caracal/actions/workflows/continuous_integration.yml/
                  :alt:

.. |Python Versions| image:: https://img.shields.io/badge/python-3.8+-blue.svg
                     :target: https://pypi.python.org/pypi/caracal/
                     :alt:

.. |Project License| image:: https://img.shields.io/badge/license-GPL-blue.svg
                     :target: https://github.com/caracal-pipeline/caracal/blob/master/LICENSE
                     :alt:


.. _license: https://github.com/caracal-pipeline/caracal/blob/master/LICENSE
.. _pep8: https://www.python.org/dev/peps/pep-0008

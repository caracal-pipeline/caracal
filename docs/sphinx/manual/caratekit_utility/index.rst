.. meerkathi documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
=================
Caratekit utility
=================
 
.. toctree::
   :maxdepth: 1

``caratekit.sh`` is a shell script originally intended to test CARACal. It has no dependencies other than an existing bash shell installed on your computer system (most UNIX systems use bash as a default). To learn about the full range of options to use caratekit.sh type ``caratekit.sh -h`` or ``caratekit -v``
once it is installed. Here, we restrict the description to two standard use-cases: the installation of CARACal using ``caratekit.sh`` and three methods to consecutively reduce data with ``caratekit.sh``. The advantage of a data reduction using ``caratekit.sh`` is that ``caratekit.sh`` automatically creates some system information that can be used to analyse potential issues and that can be linked in the `CARACal issue tracker <https://github.com/ska-sa/caracal/issues>`__.

Installing ``caratekit.sh``
---------------------------

Choose: 

  - A workspace directory ``${workspace}`` to install ``caratekit.sh`` 

  - A target directory ``${carate_target}`` to install ``caratekit.sh`` in. By default, this is ``/usr/local/bin``. Some of those locations require a sudo password. If you don’t have one, choose a directory to which you have write access. 

Then type:

::

   $ cd ${workspace}
   $ git clone https://github.com/ska-sa/caracal.git
   $ caracal/caratekit.sh -i

Follow the instructions.

Finally, type:

::

   $ rm -rf ./caracal

To get a full display of the potential switches and usage of ``caratekit.sh`` type:

::

   $ caratekit.sh -h

or

::

   $ caratekit.sh -v

Updating ``caratekit.sh``
-------------------------

We assume that you have chosen to make ``caratekit.sh`` visible in the ``${PATH}``. Type:

::

   $ caratekit.sh -i

and follow the instructions.

Installing and upgrading CARACal using ``caratekit.sh`` for further use with ``caratekit.sh``
---------------------------------------------------------------------------------------------

The syntax is the same for upgrading or installing CARACal. The user chooses:
 
  - The location ``${workspace}`` of a parent directory to a ``caratekit`` test directory.
  - A name ``${caracal_testdir}`` of the caracal test directory Installation/upgrade with `Docker <https://www.docker.com/>`__ as containerisation technology:

::

   $ caratekit.sh -ws ${workspace} -cr -di -ct ${caracal_testdir} -rp install -f

Installation/upgrade with `Singularity <https://github.com/sylabs/singularity>`__ as containerisation technology:

::

   $ caratekit.sh -ws ${workspace} -cr -si -ct ${caracal_testdir} -rp install -f

**Do not use -cr until the release**. This installs the current release version of CARACal. Alternatively, the switch ``-cr`` can be omitted, to install the current master development branch (newest features but no guarantees). This creates the following directory structure:

::

   ${workspace}
   └──${caracal_testdir}
      ├── caracal (local caracal copy)
      ├── caracal_venv (virtualenv)
      ├── home (local home directory)
      ├── report (report directory)
      │   └── install
      │       ├── install.sh.txt (template shell script)
      │       └── install-sysinfo.txt (system information file)
      └── (stimela-singularity)

caracal is a local copy of CARACal (release branch **currently master branch**), which is installed using the ``virtualenv`` ``caracal_venv``. ``home`` is a replacement of the ``${HOME}`` directory, in which currently only one hidden directory, ``.stimela`` is stored. This can be ignored in nearly all cases, but it is essential to have. The report directory contains automatically generated reports about ``caratekit.sh`` runs. The example run is called ``install`` (see switch ``-rp``). This is reflected in a sub-directory named ``install`` in the report directory. This ``caratekit.sh`` run creates two report files, ``install.sh.txt``, a bash-script re-tracing the installation steps (not documenting the creation of the reports), and ``install-sysinfo.txt``, a file containing information about the system and the installed software of the machine that is being used. Generally, a ``caratekit.sh`` run can generate multiple report sub-directories, each of which can contain up to four files and one directory (see next section `Data reduction using caratekit.sh <#data_reduction_using_caratekit_sh>`__).

Data reduction using caratekit.sh
---------------------------------

Multiple variants are possible, here we present three.

The user uses the same - Workspace directory ``${workspace}`` as has been used to install ``caratekit.sh`` - The same target directory ``${carate_target}`` that ``caratekit.sh`` has been installed in.

The user chooses:
  - The name ``${project}`` of the data reduction project
  - The location ``${configfile}.yml`` of a CARACal configuration file. Templates can be found in the directory ``${workspace}/${caracal_testdir}/caracal/caracal/sample_configurations``. A choice to start with is the file ``minimalConfig.yml``.
  - The name ``${rawdata}`` of a directory containing the measurement sets (which have to have the suffix ``.ms``) that are supposed to be processed in the data reduction.

Specifying the data to be reduced
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``caratekit.sh`` assumes by default, that any measurement sets in the ``${rawdata}`` file are meant to be processed by the pipeline. A copy of the ``${configfile}.yml`` configuration file will be modified accordingly before the data reduction process is started. This can be changed by adding the switch ``--copy-config-data`` or  ``-cc`` to the ``caratekit.sh`` calls below. In that case, only measurement sets with the data ids specified in the configuration file will be processed. Alternatively, the user can specify explicityly the ids (the names of the measurement sets removing the suffix ``.ms``) using the switch ``--copy-data-id ARG`` ``-ci ARG``, where ``ARG`` is a comma- separated list of the ids to use.

Start and single CARACal run
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the user assumes to run CARACal only once but also at the beginning of any other data reduction process the user edits the file ``${configfile}.yml`` following the CARACal description. Notice that using ``caratekit.sh`` the default is that the contents of the parameter ``dataid`` will be replaced to reflect the measurement sets found in the ``${rawdata}`` directory. This can be overridden by using the ``caratekit.sh`` ``-kc`` switch. A (partial) data reduction is then conducted following the command `Docker <https://www.docker.com/>`__:

::

   $ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project} -cs ${configfile}.yml -td ${rawdata}

`Singularity <https://github.com/sylabs/singularity>`__:

::

   $ caratekit.sh -ws ${workspace} -cd -si -ct ${caracal_testdir} -rp ${project} -cs ${configfile}.yml -td ${rawdata}

After that, if everything has gone well, the directory tree of the ``${project}`` has the following structure:

::

   ${workspace}
   └──${caracal_testdir}
      ├── caracal (local caracal copy)
      ├── caracal_venv (virtualenv)
      ├── home (local home directory)
      ├── ${project} (CARACal project directory)
      │   ├── input
      │   ├── ${configfile}.yml
      │   ├── msdir
      │   ├── output
      │   └── stimela_parameter_files
      ├── report (report directory)
      │   └── install
      │   │     ├── install.sh.txt (template shell script)
      │   │     └── install-sysinfo.txt (system information file)
      │   └── ${project}
      │       ├── ${project}-${configfile}-log-caracal.txt
      │       ├── ${project}-${configfile}.yml.txt
      │       ├── ${project}.sh.txt
      │       └── ${project}-sysinfo.txt
      └── (stimela-singularity)

With above settings, ``caratekit.sh`` copies the measurement sets found in ``${rawdata}`` into the newly created directory ``${project}/msdir`` (see switch ``-md`` for moving test data instead), and the CARACal configuration file ``${configfile}`` into the directory ``${project}``, to then start CARACal using the ``${configfile}`` (``caracal -c ${configfile}``). It also creates a new sub-directory ``${project}`` to report. Apart from the shell script ``${project}.sh.txt`` and the system info file ``${project}-sysinfo.txt``, this sub-directory contains a copy ``${project}-${configfile}-log-caracal.txt`` of the CARACal logfile and a copy ``${project}-${configfile}.yml.txt`` of the CARACal configuration file. Should the data reduction process be interrupted by an error, a further sub-directory ``${project}-badlogs`` to ``${caracal_testdir}/report/${project}`` is created containing all logfiles indicating an error (the logfiles are literally parsed for the expression “ERROR” and added if it is found).

*Any bug report to the* \ `CARACal issue tracker <https://github.com/ska-sa/caracal/issues>`__\ * can be substantially improved by submitting the files in the specific ``report`` directory along with the issue.*

Follow-up steps to conduct a data reduction by method 1: renaming the project
*****************************************************************************

If the data reduction is supposed to be conducted in several steps (e.g. giving the user the chance to inspect intermediate data products), there are two methods that can be used:

Using method 1, the user defines a set of consecutive configuration files, for simplicity ``${configfile}_00``, ``${configfile}_01``, … , and a set of consecutive project names ``${project}_00``, ``${project}_01``, … . The individual names are the user’s choice, but some logical structure is recommended.

By using the ``-cf ${project}_jj -cf ${project}_ii`` switches, the directory ``${project}_ii`` is re-named into ``${project}_jj`` and a symbolic link ``${project}_ii`` is created pointing to directory ``${project}_jj``. Then, the data reduction is re-started using the CARACal configuration file provided with switch ``-cf``. The purpose of this is to maintain data reduction reports corresponding to the single data reduction steps, which would otherwise be overwritten.

E.g, invoking (`Docker <https://www.docker.com/>`__)

::

   $ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project}_00 -cs ${configfile}_00.yml -td ${rawdata}
   $ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project}_01 -cf ${project}_00 -cs ${configfile}_01.yml -td ${rawdata}
   $ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project}_02 -cf ${project}_01 -cs ${configfile}_02.yml -td ${rawdata}
   ...

or (`Singularity <https://github.com/sylabs/singularity>`__)

::

   $ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project}_00 -cs ${configfile}_00.yml -td ${rawdata}
   $ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project}_01 -cf ${project}_00 -cs ${configfile}_01.yml -td ${rawdata}
   $ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project}_02 -cf ${project}_01 -cs ${configfile}_02.yml -td ${rawdata}
   ...

creates the directory structure:

::

   ${workspace}
   └──${caracal_testdir}
      ⋮
      ├── ${project}_00 (link to ${project}_01)
      ├── ${project}_01 (link to ${project}_02)
      ├── ${project}_02
      │   ⋮
      ├── report (report directory)
      │   ├── install
      │   │   ⋮ 
      │   ├── ${project}_00
      │   │   ⋮ 
      |   ├── ${project}_01
      │   │   ⋮ 
      │   └── ${project}_02
      │       ⋮ 
      ⋮

such that issues and reports can be tracked by the names.

Follow-up steps to a data reduction by method 2: using a middle name
********************************************************************
Using method 2, the user defines a set of consecutive configuration files, for simplicity ``${configfile}_a``, ``${configfile}_b``, … , and a set of consecutive medifixes (middle names), e.g. ``00``, ``01``, … for report files. The medifixes will then be inserted in the report file names. If the user does not choose the same medifixes (as in the example), a set of report files with different names are created for each ``caratekit.sh`` call. The individual names are again the user’s choice…

E.g, invoking

(`Docker <https://www.docker.com/>`__)

::

   $ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project} -rm "00" -cs ${configfile}_a.yml -td ${rawdata}
   $ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project} -rm "01" -cs ${configfile}_b.yml -td ${rawdata}
   $ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project} -rm "02" -cs ${configfile}_c.yml -td ${rawdata}
   ...

or (`Singularity <https://github.com/sylabs/singularity>`__)

::

   $ caratekit.sh -ws ${workspace} -cd -si -ct ${caracal_testdir} -rp ${project} -rm "00" -cs ${configfile}_a.yml -td ${rawdata}
   $ caratekit.sh -ws ${workspace} -cd -si -ct ${caracal_testdir} -rp ${project} -rm "01" -cs ${configfile}_b.yml -td ${rawdata}
   $ caratekit.sh -ws ${workspace} -cd -si -ct ${caracal_testdir} -rp ${project} -rm "02" -cs ${configfile}_c.yml -td ${rawdata}
   ...

creates the directory structure:

::

   ${workspace}
   └──${caracal_testdir}
      ⋮
      ├── ${project}
      │   ├── ${configfile}_a.yml
      │   ├── ${configfile}_b.yml
      |   ├── ${configfile}_c.yml
      │   ⋮
      ├── report (report directory)
      │   ├── install
      │   ├── ${project}
      │   │   ├── ${project}-00.sh.txt
      │   │   ├── ${project}-00-sysinfo.txt
      │   │   ├── ${project}-00-${configfile}_a.yml.txt
      |   │   ├── ${project}-00-${configfile}_a-log-caracal.txt
      │   │   ├── ${project}-01.sh.txt
      │   │   ├── ${project}-01-sysinfo.txt
      │   │   ├── ${project}-01-${configfile}_b.yml.txt
      |   │   ├── ${project}-01-${configfile}_b-log-caracal.txt 
      │   │   ⋮
      │   ⋮ 
      ⋮

such that issues and reports can be tracked, again, by the names. This method has the advantage that the ${project} directory keeps its name, but the disadvantage that the report directory might become large. We leave the choice to the user.
Quickly installing CARACal using ``caratekit.sh``
-------------------------------------------------
This method is described for the user who does not want to use ``caratekit.sh`` to reduce their data but want to use it only to install CARACal.

Download `caratekit.sh <https://github.com/ska-sa/caracal/blob/master/caratekit.sh>`__ . Choose the parent directory ``${workspace}`` and the name of the CARACal directory ``${caracal_dir}``.

If using `Docker <https://www.docker.com>`__:

::

  $ caratekit.sh -ws ${workspace} -cr -di -ct ${caracal_dir} -rp install -f -kh

If using `Singularity <https://github.com/sylabs/singularity>`__:

::

  caratekit.sh -ws ${workspace} -cr -si -ct ${caracal_testdir} -rp install -f -kh

To run CARACal manually, activate the virtual environment with:
::

  source ${workspace}/${caracal_dir}/caracal_venv/bin/activate

then type:
::

  caracal - c ${your-configuration-file}

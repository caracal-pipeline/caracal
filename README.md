The **Containerized Automated Radio Astronomy Calibration (CARACal)** pipeline is a Python-based script to reduce radiointerferometric data in the cm- to dm-wavelength range in an automatized fashion, reducing user-intervention to a minimum (ideally requiring only a configuration), and at the same time relying on state-of-the-arts data reduction software and  data reduciton techniques. It makes use of [Stimela](https://github.com/ratt-ru/Stimela), a platform-independent radio astronomy scripting environment, providing pre-fabricated containerized software packages. [Stimela](https://github.com/ratt-ru/Stimela) itself relies on [Kern](https://kernsuite.info/), a suite of [Ubuntu](https://ubuntu.com/) radio astronomy packages.
- [Full documentation](#--full-documentation)
- [Download & Install](#download---install)
  * [Usage and publication policy](#usage-and-publication-policy)
  * [Software requirements and supported platforms](#software-requirements-and-supported-platforms)
  * [Installing and running CARACal](#installing-and-running-caracal)
    + [Installing and running manually](#installing-and-running-manually)
      - [Manual installation](#manual-installation)
        * [Current development branch](#current-development-branch)
      - [Running CARACal manually](#running-caracal-manually)
      - [Upgrading a manual CARACal installation](#upgrading-a-manual-caracal-installation)
        * [Current development branch](#current-development-branch-1)
    + [Installation and data reduction using caratekit.sh](#installation-and-data-reduction-using-caratekitsh)
      - [Installing ``caratekit.sh``](#installing---caratekitsh--)
      - [Updating ``caratekit.sh``](#updating---caratekitsh--)
      - [Installing and upgrading CARACal using ``caratekit.sh``](#installing-and-upgrading-caracal-using---caratekitsh--)
      - [Data reduction using caratekit.sh](#data-reduction-using-caratekitsh)
        * [Start and single CARACal run](#start-and-single-caracal-run)
        * [Follow-up steps to a data reduction by method 1: renaming the project](#follow-up-steps-to-a-data-reduction-by-method-1--renaming-the-project)
        * [Follow-up steps to a data reduction by method 2: using a middle name](#follow-up-steps-to-a-data-reduction-by-method-2--using-a-middle-name)
  * [CARACal switches](#caracal-switches)
    + [Getting help](#getting-help)
    + [Dumping a copy of the minimal default configuration file to the current directory](#dumping-a-copy-of-the-minimal-default-configuration-file-to-the-current-directory)
    + [Running the pipeline](#running-the-pipeline)
  * [Tips and Troubleshooting](#tips-and-troubleshooting)
    + [Installing a Python 3 virtualenv](#installing-a-python-3-virtualenv)
    + [Stimela cache file](#stimela-cache-file)
    + [Singularity-specific issues](#singularity-specific-issues)

<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>

# Full documentation
https://caracal.readthedocs.io

# Download & Install

## Usage and publication policy

When using CARACal please be aware of and adhere to the [CARACal publication policy](https://docs.google.com/document/d/12LjHM_e1G4kWRfCLcz0GgM8rlXOny23vVdcriiA8ayU).

## Software requirements and supported platforms
Most dependencies are taken care of by using [pip](https://pypi.org/project/pip/) and [PiPy](https://pypi.org) to control Python dependencies and [Stimela](https://github.com/ratt-ru/Stimela/tree/master/stimela) as a platform-independent scripting framework. This leaves a small number of known dependencies (currently only one) that need to be installed prior to using CARACal:
- [Python](https://www.python.org/) 3.6 or higher
- [Singularity](https://github.com/sylabs/singularity) > 2.6.0-dist is required only if [Singularity](https://github.com/sylabs/singularity) is chosen as containerization technology to run [Stimela](https://github.com/ratt-ru/Stimela/tree/master/stimela) with (no known [Docker](https://www.docker.com/) dependencies).

CARACal fully supports [Docker](https://www.docker.com/) and [Singularity](https://github.com/sylabs/singularity). It is also possible to use it in combination with [Podman](https://podman.io/).

## Installing and running CARACal

Shell-style is used to indicate names and paths of directories and files which can be chosen by the user: ``${name}``.

### Installing and running manually
We recommend and describe an installation using a virtual environment created with [Virtualenv](https://virtualenv.pypa.io/en/latest/). This is not a requirement, but strongly recommended.
The user chooses:
- the name (including path) ${caracal-venv} of the virtualenv
- the location ``${singularity_pull_folder}`` of a [Singularity](https://github.com/sylabs/singularity) pull-folder, where [Singularity](https://github.com/sylabs/singularity) images are stored (not required when using only [Docker](https://www.docker.com/) or [Podman](https://podman.io/), the latter currently not being fully supported).

#### Manual installation
The latest CARACal release can be obtained by typing **(Not yet implemented)**:
```
$ python3 -m venv ${caracal-venv}  
$ source ${caracal-venv}/bin/activate
$ pip install -U pip setuptools wheel
$ pip install -U caracal
```
Using [Docker](https://www.docker.com/):
```
$ stimela build
```
Using [Singularity](https://github.com/sylabs/singularity) (choose a [Singularity](https://github.com/sylabs/singularity) pull-folder ``${singularity_pull_folder}``):  

```  
$ stimela pull --singularity --pull-folder ${singularity_pull_folder}
```

Using [Podman](https://podman.io/) (currently not fully supported):
```
$ stimela pull -p
```
Please see [Installing a Python 3 virtualenv](#Installing-a-Python-3-virtualenv) for more information on virtualenv.

##### Current development branch
*Warning: the current development branch obviously contains the most recent developments but it might contain bugs.*
It is also possible not to install the release version but instead the current development version of CARACal. 

*Warning: the current development branch obviously contains the most recent developments but it might contain bugs. We take no responsibility for development versions of CARACal.*

Do do so, replace above pip installation of CARACal 
```
pip install -U caracal
```
with:
```
$ pip install -U git+https://github.com/ska-sa/caracal.git#egg=caracal
```
#### Running CARACal manually
The user needs to know the name ``${caracal-venv}`` of the virtualenv used during the installation and the name ``${singularity_pull_folder}`` of the [Singularity](https://github.com/sylabs/singularity) pull folder used when installing CARACal. For the most basic usage, the user generally chooses:
- the name ${my_caracal_run_dir} (and hence the location) of a folder in which the data reduction takes place
- a template configuration file ``${template_config}`` to use for the data reduction, as can be downloaded (and re-named to your choice) from the [sample_configurations](https://github.com/ska-sa/caracal/tree/master/caracal/sample_configurations) folder in the CARACal repository. To start, we recommend to use [``minimal_config.yml``](https://github.com/ska-sa/caracal/blob/master/caracal/sample_configurations/minimalConfig.yml).
- a name for the final configuration file ``${my_config_file}`` to use.

An example run then looks like:
```
$ mkdir ${my_caracal_run_dir}
$ cd ${my_caracal_run_dir}
```
Copy ``${template_config}`` into ``${my_caracal_run_dir}``, then re-name (this is strictly not necessary):
```
$ mv ${template_config} ${my_config_file}
```
Create ``msdir`` inside ``${my_caracal_run_dir}`` 
```
$ mkdir ${my_caracal_run_dir}/msdir
```
and move/copy your raw measurement set files into that directory. Edit your ``${my_config_file}`` to link to those files. If the names of your measurement sets are ``a.ms b.ms c.ms``, then edit the line
```
dataid=[] -> dataid=['a','b','c']
```
in ${my_config_file}. Do analogously for different names of your measurement sets.

Finally, run CARACal, after activating your virtualenv:
```
$ source ${caracal-venv}/bin/activate
```
Using [Docker](https://www.docker.com/):
```
$ caracal -c ${my_config_file}
```
Using [Singularity](https://github.com/sylabs/singularity) (using the [Singularity](https://github.com/sylabs/singularity) pull-folder ``${singularity_pull_folder}`` created during the installation):  

```  
$ caracal -c ${my_config_file} --container-tech singularity -sid ${singularity_pull_folder}
```
Using podman (currently not fully supported):
```
$ caracal -c ${my_config_file} --container-tech podman
```
#### Upgrading a manual CARACal installation
The following steps should lead to an upgraded CARACal:
Activate your virtualenv:
```
$ source ${caracal-venv}/bin/activate
```
Then run the following commands, like outlined in [Manual installation](#Manual-installation):
```
$ pip install -U caracal
```
Using [Docker](https://www.docker.com/):
```
$ stimela build
```
Using [Singularity](https://github.com/sylabs/singularity) (choose a [Singularity](https://github.com/sylabs/singularity) pull-folder ``${singularity_pull_folder}``):  

```  
$ stimela pull --singularity --pull-folder ${singularity_pull_folder}
```

Using [Podman](https://podman.io/) (currently not fully supported):
```
$ stimela pull -p
```
Please see [Stimela cache file](#stimela-cache-file) for troubleshooting for a more forceful upgrade.

##### Current development branch
*Warning: the current development branch obviously contains the most recent developments but it might contain bugs.*
It is also possible not to install the release version but instead the current development version of CARACal. 

*Warning: the current development branch obviously contains the most recent developments but it might contain bugs. We take no responsibility for development versions of CARACal.*

Do do so, replace above pip installation of CARACal 
```
pip install -U --force-reinstall caracal
```
with:
```
$ pip install -U --force-reinstall git+https://github.com/ska-sa/caracal.git#egg=caracal
```
### Installation and data reduction using caratekit.sh

#### Installing ``caratekit.sh``
The user chooses:
- A workspace directory ``${workspace}`` to install ``caratekit.sh``
- A target directory ``${carate_target}`` to install ``caratekit.sh`` in. By default, this is ``/usr/local/bin``. Some of those locations require a sudo password. If you don't have one, choose a directory to which you have write access.
Then type:
```
$ cd ${workspace}
$ git clone https://github.com/ska-sa/caracal.git
$ caracal/caratekit.sh -i
```
Follow the instructions.
Finally, type:
```
$ rm -rf ./caracal
```
To get a full display of the potential switches and usage of ``caratekit.sh`` type:
```
$ caratekit.sh -h
```
or 
```
$ caratekit.sh -v
```
#### Updating ``caratekit.sh``
We assume that you have chosen to make ``caratekit.sh`` visible in the ``${PATH}``.
Type:
```
$ caratekit.sh -i
```
and follow the instructions.

#### Installing and upgrading CARACal using ``caratekit.sh``
The syntax is the same for upgrading or installing CARACal.
The user chooses:
- The location ``${workspace}`` of a parent directory to a ``caratekit`` test directory.
- A name ``${caracal_testdir}`` of the caracal test directory
Installation/upgrade with [Docker](https://www.docker.com/) as containerisation technology:
```
$ caratekit.sh -ws ${workspace} -cr -di -ct ${caracal_testdir} -rp install -f
```
Installation/upgrade with [Singularity](https://github.com/sylabs/singularity) as containerisation technology:
```
$ caratekit.sh -ws ${workspace} -cr -si -ct ${caracal_testdir} -rp install -f
```
**Do not use -cr until the release**
This installs the current release version of CARACal. Alternatively, the switch ``-cr`` can be omitted, to install the current master development branch (newest features but no guarantees).
This creates the following directory structure:
```
${workspace}
└──${caracal_testdir}
   ├── caracal (local caracal copy)
   ├── caracal_venv (virtualenv)
   ├── home (local home directory)
   ├── report (report directory)
   │   └── install
   │       ├── install.sh.txt (template shell script)
   │       └── install_sysinfo.txt (system information file)
   └── (stimela_singularity)
```
caracal is a local copy of caracal (release branch **currently master branch**), which is installed using the ``virtualenv`` ``caracal_venv``. ``home`` is a replacement of the ``${HOME}`` directory, in which currently only one hidden directory, ``.stimela`` is stored. This can be ignored in nearly all cases, but it is essential to have. The report directory contains automatically generated reports about ``caratekit.sh`` runs. The example run is called ``install`` (see switch ``-rp``). This is reflected in a sub-directory named ``install`` in the report directory. This ``caratekit.sh`` run creates two report files, ``install.sh.txt``, a bash-script re-tracing the installation steps (not documenting the creation of the reports), and ``install_sysinfo.txt``, a file containing information about the system and the installed software of the machine that is being used. Generally, a ``caratekit.sh`` run can generate multiple report sub-directories, each of which can contain up to four files and one directory (see next section [Data reduction using ``caratekit.sh``](#data_reduction_using_caratekit_sh)). 

#### Data reduction using caratekit.sh
Multiple variants are possible, here we present three.

The user uses the same
- Workspace directory ``${workspace}`` as has been used to install ``caratekit.sh``
- The same target directory ``${carate_target}`` that ``caratekit.sh`` has been installed in.

The user chooses:
- The name ``${project}`` of the data reduction project
- The location ``${configfile}.yml`` of a CARACal configuration file. Templates can be found in the directory ``${workspace}/${caracal_testdir}/caracal/caracal/sample_configurations``. A choice to start with is the file ``minimalConfig.yml``.
 - The name ``${rawdata}`` of a  directory containing the measurement sets (which have to have the suffix ``.ms``) that are supposed to be processed in the data reduction.
 
##### Start and single CARACal run
If the user assumes to run CARACal only once but also at the beginning of any other data reduction process the user edits the file ``${configfile}.yml`` following the CARACal description. Notice that using ``caratekit.sh`` the default is that the contents of the parameter ``dataid`` will be replaced to reflect the measurement sets found in the ``${rawdata}`` directory. This can be overridden by using the ``caratekit.sh`` ``-kc`` switch. A (partial) data reduction is then conducted following the command
[Docker](https://www.docker.com/):
```
$ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project} -cs ${configfile}.yml -td ${rawdata}
```
[Singularity](https://github.com/sylabs/singularity):
```
$ caratekit.sh -ws ${workspace} -cd -si -ct ${caracal_testdir} -rp ${project} -cs ${configfile}.yml -td ${rawdata}
```
After that, if everything has gone well, the directory tree of the ``${project}`` has the following structure:
```
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
   │   │     └── install_sysinfo.txt (system information file)
   │   └── ${project}
   │       ├── ${project}_${configfile}_log-caracal.txt
   │       ├── ${project}_${configfile}.yml.txt
   │       ├── ${project}.sh.txt
   │       └── ${project}_sysinfo.txt
   └── (stimela_singularity)
```
With above settings, ``caratekit.sh`` copies the measurement sets found in ``${rawdata}`` into the newly created directory ``${project}/msdir``, and the CARACal configuration file ``${configfile}`` into the directory ``${project}``, to then start CARACal using the ``${configfile}`` (``caracal -c ${configfile}``). It also creates a new sub-directory ``${project}`` to  report. Apart from the shell script ``${project}.sh.txt`` and the system info file ``${project}_sysinfo.txt``, this sub-directory contains a copy ``${project}_${configfile}_log-caracal.txt`` of the CARACal logfile and a copy ``${project}_${configfile}.yml.txt`` of the CARACal configuration file. Should the data reduction process be interrupted by an error, a further sub-directory ``${project}_badlogs`` to ``${caracal_testdir}/report/${project}`` is created containing all logfiles indicating an error (the logfiles are literally parsed for the expression "ERROR" and added if it is found).

*Any bug report to the [CARACal issue tracker](https://github.com/ska-sa/caracal/issues) can be substantially improved by submitting the files in the specific ``report`` directory along with the issue.*
##### Follow-up steps to a data reduction by method 1: renaming the project
If the data reduction is supposed to be conducted in several steps (e.g. giving the user the chance to inspect intermediate data products), there are two methods that can be used:

Using method 1, the user defines a set of consecutive configuration files, for simplicity ``${configfile}_00``, ``${configfile}_01``, ... , and a set of consecutive project names ``${project}_00``, ``${project}_01``, ... . The individual names are the user's choice, but some logical structure is recommended. 

By using the ``-cf ${project}_jj -cf ${project}_ii`` switches, the directory  ``${project}_ii`` is re-named into ``${project}_jj`` and a symbolic link ``${project}_ii`` is created pointing to directory ``${project}_jj``. Then, the data reduction is re-started using the  CARACal configuration file provided with switch ``-cf``. The purpose of this is to maintain data reduction reports corresponding to the single data reduction steps, which would otherwise be overwritten. 

E.g, invoking
([Docker](https://www.docker.com/))
```
$ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project}_00 -cs ${configfile}_00.yml -td ${rawdata}
$ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project}_01 -cf ${project}_00 -cs ${configfile}_01.yml -td ${rawdata}
$ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project}_02 -cf ${project}_01 -cs ${configfile}_02.yml -td ${rawdata}
...
```
or ([Singularity](https://github.com/sylabs/singularity))
```
$ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project}_00 -cs ${configfile}_00.yml -td ${rawdata}
$ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project}_01 -cf ${project}_00 -cs ${configfile}_01.yml -td ${rawdata}
$ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project}_02 -cf ${project}_01 -cs ${configfile}_02.yml -td ${rawdata}
...
```
creates the directory structure:
```
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
```
such that issues and reports can be tracked by the names.

 ##### Follow-up steps to a data reduction by method 2: using a middle name
Using method 2, the user defines a set of consecutive configuration files, for simplicity ``${configfile}_a``, ``${configfile}_b``, ... , and a set of consecutive middle names ("midfixes"), e.g. ``00``, ``01``, ... . The individual names are again the user's choice... 

By using the ``-cf ${project}_jj -cf ${project}_ii`` switches, the directory  ``${project}_ii`` is re-named into ``${project}_jj`` and a symbolic link ``${project}_ii`` is created pointing to directory ``${project}_jj``. Then, the data reduction is re-started using the  CARACal configuration file provided with switch ``-cf``. The purpose of this is to maintain data reduction reports corresponding to the single data reduction steps, which would otherwise be overwritten. 

E.g, invoking
([Docker](https://www.docker.com/))
```
$ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project} -rm "00" -cs ${configfile}_a.yml -td ${rawdata}
$ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project} -rm "01" -cs ${configfile}_b.yml -td ${rawdata}
$ caratekit.sh -ws ${workspace} -cd -di -ct ${caracal_testdir} -rp ${project} -rm "02" -cs ${configfile}_c.yml -td ${rawdata}
...
```
or ([Singularity](https://github.com/sylabs/singularity))
```
$ caratekit.sh -ws ${workspace} -cd -si -ct ${caracal_testdir} -rp ${project} -rm "00" -cs ${configfile}_a.yml -td ${rawdata}
$ caratekit.sh -ws ${workspace} -cd -si -ct ${caracal_testdir} -rp ${project} -rm "01" -cs ${configfile}_b.yml -td ${rawdata}
$ caratekit.sh -ws ${workspace} -cd -si -ct ${caracal_testdir} -rp ${project} -rm "02" -cs ${configfile}_c.yml -td ${rawdata}
...
```
creates the directory structure:
```
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
   │   │   ├── ${project}_00.sh.txt
   │   │   ├── ${project}_00_sysinfo.txt
   │   │   ├── ${project}_00_${configfile}_a.yml.txt
   |   │   ├── ${project}_00_${configfile}_a_log-caracal.txt
   │   │   ├── ${project}_01.sh.txt
   │   │   ├── ${project}_01_sysinfo.txt
   │   │   ├── ${project}_01_${configfile}_b.yml.txt
   |   │   ├── ${project}_01_${configfile}_b_log-caracal.txt 
   │   │   ⋮
   │   ⋮ 
   ⋮
```
such that issues and reports can be tracked, again, by the names. This method has the advantage that the ${project} directory keeps its name, but the disadvantage that the report directory might become large. We leave the choice to the user.

## CARACal switches
After activating the virtualenv
```
$ source ${caracal-venv}/bin/activate
```
CARACal has several switches. In its entirety, they can be accessed by invoking help.
### Getting help
```
$ caracal --help
```
### Dumping a copy of the minimal default configuration file to the current directory
```
$ caracal -gd ${configuration_file}
```
where ${configuration_file} is the target name of the configuration file.
### Running the pipeline
```
$ caracal -c ${configuration_file}
```
## Tips and Troubleshooting
### Installing a Python 3 virtualenv
To use CARACal it is recommended to install a virtualenv with Python >= 3.5.
On Ubuntu, do:
```
$ sudo apt-get update
$ sudo apt-get install python3-pip
```
For Mac OSX, find installation instructions e.g. [here](https://vgkits.org/blog/pip3-macos-howto/).

Create a virtual environment called ``${caracal-venv}``:
```  
$ python3 -m venv ${caracal-venv}  
```
E.g.:
```  
$ python3 -m venv /home/myagi/vs/caracal_venv  
```
If this does not work on your machine, try:
```
$ virtualenv -p python3 ${cvenv}
```
Activate and upgrade the ``${cvenv}``:
```
$ source ${cvenv}/bin/activate
```
if you are using bash or sh and
```
> source ${cvenv}/bin/activate.csh
```
if you are using csh or tcsh . 
Check if a Python > 3.5 is installed inside the virtualenv:
```
(caracal-venv)$ python --version
```
From now on all python installations using pip will be contained inside ${caracal-venv} and your global [Python](https://www.python.org/) is not affected. Install and/or upgrade the installation tools inside your virtualenv:
```
$ pip install -U pip setuptools wheel
```

### Stimela cache file
When re-building/pullng/updating stimela (any stimela call above), sometimes problems will arise with the cache file of stimela, whose standard location is
```
~/.stimela
```
If you run into unexplicable errors when installing a stimela version, including a failed update (possibly resulting in a repeating error when running CARACal), do:
```
$ rm ~/.stimela/*
$ stimela ...
```

before re-building. If that does not work, re-building the dependencies might help.
```
> pip install --upgrade --force-reinstall caracal
> rm ~/.stimela/*
> stimela ...
```
### Singularity-specific issues
If you get a "Too many open files" error when running WSClean increase the system-wide max number of open files with "ulimit -n <max_number>". You can also add this command to the venv/bin/activate script so you don't have to do    this manually every time.


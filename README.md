[![Jenkins build Status](https://jenkins.meqtrees.net/job/meerkathi-cron/badge/icon)](https://jenkins.meqtrees.net/job/meerkathi-cron/)

## Install

## Download & Install

### On Linux

0. Clone this repository
Use https and your github credentials, then go to the pipeline folder 'meerkathi'.
```
git clone https://github.com/ska-sa/meerkathi.git
cd meerkathi
```
1. Start and activate virtual environment outside the meerkathi directory
```
$ cd ..
$ virtualenv meerkathi-venv
$ source meerkathi-venv/bin/activate
$ pip install pip wheel setuptools -U
```
2. If working from master branch it may be necessary to install bleeding edge fixes from upstream dependencies. Please install the requirements.txt requirements:
```
$ pip install -U -r <absolute path to meerkathi folder>/requirements.txt
```
3. Install meerKATHI
```
$ pip install <absolute path to meerkathi folder>[extra_diagnostics]
$ export PYTHONPATH='' # Ensure that you use venv Python
```
If the requirements cannot be installed on your system you may omit [extra_diagnostics]. This will disable report rendering.
4. Build Stimela
```
$ stimela build
```
5. Updating from origin
```
$ source meerkathi-venv/bin/activate
$ cd <absolute path to meerkathi folder>
$ git fetch origin
$ git merge origin/master
$ pip install --upgrade pip
$ pip install -U -r <absolute path to meerkathi folder>/requirements.txt
$ pip install <absolute path to meerkathi folder>
$ rm ~/.stimela/*
$ stimela pull
$ stimela build
```

### On Mac

0. create a python environment

`conda create env --name meer_venv`

1. activate environment

`source activate meer_venv`

2. clone `meerkathi`
```
git clone https://github.com/ska-sa/meerkathi.git
cd meerkathi
git submodule update --init --recursive 
```
3. Start and activate virtual environment
```
$ virtualenv meerkathi-venv
$ source meerkathi-venv/bin/activate
$ pip install pip wheel setuptools -U
```
4. If working from master branch it may be necessary to install bleeding edge fixes from upstream dependencies. Please install the requirements.txt requirements:
```
$ pip install -U -r <absolute path to meerkathi folder>/requirements.txt
```
5. Install meerKATHI
```
$ pip install <absolute path to meerkathi folder>
$ export PYTHONPATH='' # Ensure that you use venv Python
```

6. Build stimela

`stimela build`

7. run meerkathi

`meerkathi -c path_to_configuration_file`

WARNING: if `/etc/shadow` is not found:
```
pip install --upgrade git+https://github.com/SpheMakh/Stimela.git@issue-241
stimela clean -ac
stimela clean -aC
stimela build --no-cache
```

## Running the pipeline
1. Activate the MeerKATHI virtual environment
2. Make sure that the data all the required data is present
3. Check if your installation is set up properly: 
```
$ meerkathi --help
```
4. GUI config editor can be started with (you need an X-session and Mozilla Firefox >= 56.0):
```
$ meerkathi -ce
```
5. An existing configuration file can be edited as from the gui (ie. don't specify -c):
```
$ meerkathi
```
6. (Alternatively) dump a copy of the default configuration file to the current directory:
```
$ meerkathi -gd <configuration file>
```
7. Run the pipeline
```
$ meerkathi -c <configuration file>
```
8. View generated reports (produced even in event of partial pipeline failure)
```
$ meerkathi -c <configuration file> -rv
```
## Help and descriptions
For Stimela see https://github.com/SpheMakh/Stimela and wiki therein.
For this pipeline and MeerKAT specific notes see [wiki](https://github.com/ska-sa/meerkathi/wiki) of this repository.

## Singularity specific issues
If you get a "Too many open files" error when running WSClean increase the system-wide max number of open files with "ulimit -n <max_number>". You can also add this command to the venv/bin/activate script so you don't have to do this manually every time.

## Install
0. Clone this repository
Use https and your github credentials, then go to the pipeline folder 'meerkathi'.
```
git clone https://github.com/ska-sa/meerkathi.git
cd meerkathi
```
1. Check out submodules
```
git submodule update --init --recursive 
```
2. Start and activate virtual environment
```
$ virtualenv meerkathi-venv
$ source meerkathi-venv/bin/activate
$ pip install pip wheel setuptools -U
$ pip install <absolute path to meerkathi folder>
$ export PYTHONPATH='' # Ensure that you use venv Python
```

3. Build Stimela
```
$ stimela build
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
5. An existing configuration file can be edited as follows:
```
$ meerkathi -c <configuration file> -ce
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

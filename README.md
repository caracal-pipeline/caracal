## Set up (do this once)
0. Clone this repository
Use https and your github credentials, then go to the pipeline folder 'meerkathi'.
```
github clone https://github.com/SpheMakh/meerkathi.git
cd meerkathi
```
1. Start and activate virtual environment
```
$ virtualenv meerkathi-venv
$ source meerkathi-venv/bin/activate
$ export PYTHONPATH='' # Ensure that you use venv Python
```

2. Install stimela in venv
```
pip install git+https://github.com/SpheMakh/Stimela
```

3. Build Stimela (don't do this on com4, I've alredy built it)
```
stimela build
```
## Running the pipeline (Assuming you've done steps above)

1. Navigate to MeerKATHI virtual environment, and activate it
```
$ cd <MeerKATHI virtual environment>
$ source bin/activate
```
or
```
$ source <MeerKATHI virtual environment>/bin/activate
```
2. Move into pipeline folder and execute pipeline
Assume that we are in the meerkathi local clone of the repository.
```
$ cd pipeline
$ stimela run meerkathi-pipeline.py
```

## Open xpra shell on one of the coms
nnn is a random port number, don't use 112

```
ssh -X kat@com4.kat.ac.za
xpra start :nnn --start-child=roxterm
```

on client side then:

```
xpra attach ssh:kat@192.168.1.54:nnn
```

## Download data from archive
In a browser use the archive browser:

```
kat-archive.kat.ac.za:8080/archive_search
```

Specify your source (IC 5264) as target, select newest to oldest as sort order and search. Copy-paste the corresponding h5 file address <pathtofile>.

On one of the coms, one of the scratch's (create subdirectory) type something like:

```
ln -s <pathtofile>
```

which will result in a <filename> in your current directory.

```
h5toms.py <h5name> --no-auto --flags '' --channel-range 20873 21639 -o "IC5264.ms" --dumptime=30
```

will then result in your IC5264.ms

## Calculating channel ranges

Assume that the shell command calc calculates an expression and you are working in tcsh.

```
set v = 1940000
set nu_cent = `calc "1.420405752E6/(1+${v}/c)"`
set chwid = `calc 856000/32768`
set ch_cent = `calc "($nu_cent-856000)/$chwid"`
set tenmhz = `calc 10000/$chwid`
set startchan = `calc $ch_cent-$tenmhz`
set endchan = `calc $ch_cent+$tenmhz`
echo $startchan $endchan
```

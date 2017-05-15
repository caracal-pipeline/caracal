## Set up

1. Start and activate virtual environment
```
$ virtualenv meerkathi-venv
$ source meerkathi-venv/activate
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

4. Finally move into pipeline folder and get started
```
$ cd pipeline
$ stimela run meerkath-pipeline.py
```

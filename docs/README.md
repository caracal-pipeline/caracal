# How to update the Caracal documentation

Make sure that you've got the latest `caracal`.

Build the documentation with Python 3.12. From the repository root, install the
documentation dependencies:

```bash
uv sync --python 3.12 --group docs
source .venv/bin/activate
```

Then go into the `docs` directory and run:
```
python make_caracal_docs.py
cd sphinx
make html
cd ../
```

Then commit and push your changes to this repository.

And you're done! The readthedocs page https://caracal.readthedocs.io will be updated automatically.

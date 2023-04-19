import importlib
from caracal import ExtraDependencyError


def checkimport(package):
    """
    Check if a package is installed.
    """
    exists = importlib.util.find_spec(package)
    if exists:
        return True
    else:
        return False


def extras(packages):
    if isinstance(packages, str):
        packages = [packages]

    def mydecorator(func):
        def inner_func(*args, **kw):
            for package in packages:
                if not checkimport(package):
                    raise ExtraDependencyError(extra=package)
            return func(*args, **kw)
        return inner_func
    return mydecorator

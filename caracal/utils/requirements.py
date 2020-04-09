# -*- coding: utf-8 -*-

import logging

from decorator import decorate


class OptionalImportError(ImportError):
    pass


def noop_decorator():
    """ Noop Decorator returning the original function """
    def _function_decorator(fn):
        return fn

    return _function_decorator


def throwing_decorator(ex):
    """ Return a decorator throwing ``ex`` """
    def _function_decorator(fn):
        def _wrapper(*args, **kwargs):
            raise ex

        return decorate(fn, _wrapper)

    return _function_decorator


def logging_decorator(msg):
    """ Return a decorator logging ``msg`` """
    def _function_decorator(fn):
        def _wrapper(*args, **kwargs):
            # Delay caracal log import
            from caracal import log
            log.warning("Skipping %s.\n%s", fn.__name__, msg)
            return msg

        return decorate(fn, _wrapper)

    return _function_decorator


def requires(*args, skip=False):
    """
    Decorator returning either the original function, or a
    dummy function returning a :class:`OptionalImportError` when called,
    depending on whether ``ImportError` objects are supplied
    as decorator arguments.

    Used in the following way:

    .. code-block:: python

        try:
            from scipy import interpolate
        except ImportError as e:
            # https://stackoverflow.com/a/29268974/1611416, pep 3110 and 344
            opt_import_err = e
        else:
            opt_import_err = None

        @requires('pip install scipy', opt_import_err)
        def function(*args, **kwargs):
            return interpolate(...)

    Parameters
    ----------
    *args : tuple of ``None``, str or ImportError
        ``None`` values are ignored.
        ``str`` values are treated as messages that will be
        raised in the :class:`OptionalImportError`.
        ``ImportError`` values will be mentioned in the
        :class:`OptionalImportError`
    skip : {False, True}
        If False, the dummy function raises an
        :class:`OptionalImportError`
        If True, the dummy function instead logs
        a warning and returns a message describing the problem.

    Returns
    -------
    decorated_function : callable
        A function
    """
    messages = []
    import_errors = []

    for a in args:
        if a is None:
            continue
        elif isinstance(a, str):
            messages.append(a)
        elif isinstance(a, ImportError):
            import_errors.append(a)
        else:
            raise TypeError("args must be "
                            "None, strings or ImportErrors. "
                            "Received %s" % str(a))

    # All imports succeeded
    if len(import_errors) == 0:
        return noop_decorator()

    lines = []

    # Output messages added by user (if any)
    if len(messages) > 0:
        lines.extend(messages)
    else:
        lines.append("Optional imports were missing")

    # Now output the ImportErrors
    lines.extend(["", "ImportErrors"])
    lines.extend(str(e) for e in import_errors)

    # Generate an exception that will be passed
    # to the decorator to be thrown by the wrapped function
    msg = '\n'.join(lines)

    # Merely log the message if we're asked to skip
    if skip:
        return logging_decorator(msg)

    return throwing_decorator(OptionalImportError(msg))

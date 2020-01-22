# -*- coding: utf-8 -*-

import logging

from decorator import decorate

log = logging.getLogger(__name__)


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
            log.warning("Skipping %s.\n%s", fn.__name__, msg)

        return decorate(fn, _wrapper)

    return _function_decorator


def requires(*args, skip=False):
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
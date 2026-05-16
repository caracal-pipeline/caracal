class CaracalException(RuntimeError):
    """Base class for pipeline logic errors"""

    pass


class PlayingWithFire(RuntimeError):
    """Silly settings chosen."""

    pass


class UserInputError(CaracalException):
    """Something wrong with user input"""

    pass


class ConfigurationError(CaracalException):
    """Something wrong with the configuration"""

    pass


class BadDataError(CaracalException):
    """Something wrong with the data"""

    pass


class ExtraDependencyError(Exception):
    """Optional dependencies are missing"""

    def __init__(self, message=None, extra=None):
        default_message = "Pipeline run requires optional dependencies, please re-install caracal as:"
        " \n 'pip install caracal[all]'"
        if extra:
            extra = f"or, install the missing package as: \n 'pip install caracal[{extra}]'"
        else:
            extra = ""

        if message:
            self.message = message
        else:
            self.message = default_message + extra

        super().__init__(self.message)

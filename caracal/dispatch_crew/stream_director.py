import sys
import logging
from io import StringIO


class stream_director(object):
    def __init__(self, logger, log_level=logging.INFO):
        class stream_logger(StringIO):
            def __init__(self, logger, log_level, fileno=0, *args, **kwargs):
                self.logger = logger
                self.log_level = log_level
                self.__fileno = fileno
                StringIO.__init__(self, *args, **kwargs)

            @property
            def fileno(self):
                return lambda: self.__fileno

            def is_not_log(self, line):
                # avoid recursive writeout by checking for a tag
                return line.find("caracal -") < 0 and line.find("INFO -") < 0 and \
                    line.find("ERROR -") < 0 and line.find("WARNING -") < 0 and \
                    line.find("DEBUG -") < 0 and line.find("CRITICAL -") < 0 and \
                    line.find("INFO:caracal") < 0 and line.find("ERROR:caracal") < 0 and \
                    line.find("WARNING:caracal") < 0 and line.find("DEBUG:caracal") < 0 and \
                    line.find("CRITICAL:caracal") < 0

            def writelines(self, lines):
                StringIO.writelines(self, lines)
                for line in lines:
                    if self.is_not_log(line):
                        self.logger.log(self.log_level, line.rstrip())

            def write(self, buf):
                StringIO.write(self, buf)

                for line in buf.rstrip().splitlines():
                    if self.is_not_log(line):
                        self.logger.log(self.log_level, line.rstrip())

        self.__stdout_logger = stream_logger(
            logger, logging.INFO, fileno=sys.stdout.fileno())
        self.__stderr_logger = stream_logger(
            logger, logging.CRITICAL, fileno=sys.stderr.fileno())

    def __enter__(self):
        self.old_stdout, self.old_stderr = sys.stdout, sys.stderr
        self.old_stdout.flush()
        self.old_stderr.flush()

        sys.stdout = self.__stdout_logger
        sys.stderr = self.__stderr_logger

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr

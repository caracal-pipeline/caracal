from multiprocessing import Process
import os
import signal


class interruptable_process(Process):
    def __init__(self, target):
        """
        Interruptable process

        Args:
        @target: method to execute in separate process 
        """
        self.__pid = 0

        def __run(target_proc):
            self.__pid = os.getpid()
            target_proc()
        Process.__init__(self, target=lambda: __run(target))

    def interrupt(self):
        try:
            os.kill(self.__pid, signal.SIGINT)
        except KeyboardInterrupt:
            pass  # do not pass onto parent process

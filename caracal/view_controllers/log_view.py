import npyscreen
from logging import StreamHandler
import caracal

KEEP_NLINES = 1000


class log_view(npyscreen.Pager):
    def __init__(self, *args, **kwargs):
        class logging_handler(StreamHandler):
            def __init__(self, parent_view, *args, **kwargs):
                self.__parent_view = parent_view
                self.setFormatter(caracal.log_console_formatter)
                StreamHandler.__init__(self, *args, **kwargs)

            @property
            def parent_view(self):
                return self.__parent_view

            def emit(self, record):
                msg = str(self.format(record))
                self.parent_view.values.insert(0, msg)
                self.parent_view.values = self.parent_view.values[:KEEP_NLINES]
                self.parent_view.parent.display()

        self.__log_handler = logging_handler(self)
        caracal.add_log_handler(self.__log_handler)
        npyscreen.Pager.__init__(self, *args, **kwargs)

    def destroy(self):
        caracal.remove_log_handler(self.__log_handler)
        npyscreen.MultiLineEdit.destroy(self)

    @property
    def logging_handler(self):
        return self.__log_handler

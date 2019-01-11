import npyscreen
from logging import StreamHandler
from meerkathi import add_log_handler, remove_log_handler, log_formatter

class log_view(npyscreen.Pager):
    def __init__(self, *args, **kwargs):
        class logging_handler(StreamHandler):
            def __init__(self, parent_view, *args, **kwargs):
                self.__parent_view = parent_view
                StreamHandler.__init__(self, *args, **kwargs)
                self.setFormatter(log_formatter)
            @property
            def parent_view(self):
                return self.__parent_view

            def emit(self, record): 
                msg = str(self.format(record))
                simple_msg = msg 
                self.parent_view.values.insert(0, simple_msg)
                self.parent_view.parent.display()

        self.__log_handler = logging_handler(self)
        add_log_handler(self.__log_handler)
        npyscreen.Pager.__init__(self, *args, **kwargs)

    def destroy(self):
        remove_log_handler(self.__log_handler)
        npyscreen.MultiLineEdit.destroy(self)

    @property
    def logging_handler(self):
        return self.__log_handler
    

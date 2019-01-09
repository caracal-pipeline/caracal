import npyscreen

class input_box(npyscreen.ActionPopup):
    def __init__(self, event_loop, labeltype, labeltext, editvalue, on_ok=None, on_cancel=None, lines=5, *args, **kwargs):
        self.__labeltype = labeltype
        self.__labeltext = labeltext
        self.__editvalue = editvalue
        self.__event_loop = event_loop
        self.__event_loop.setNextFormPrevious()
        self.__on_ok = on_ok
        self.__on_cancel = on_cancel
        npyscreen.ActionPopup.__init__(self, *args, name="User input", lines=lines, **kwargs)
        

    def create(self):
        self.edt_val = self.add(self.__labeltype, hidden=False, name=self.__labeltext, value=self.__editvalue)
        
    def afterEditing(self):
        pass
    
    def on_ok(self):
        if self.__on_ok is not None:
            self.__on_ok(self.edt_val.value)
    
    def on_cancel(self):
        if self.__on_cancel is not None:
            self.__on_cancel()
        self.__event_loop.switchFormPrevious()

class message_box(npyscreen.Popup):
    def __init__(self, event_loop, editvalue, lines=5, labeltext="(i)\t", title="INFO", *args, **kwargs):
        self.__labeltype = npyscreen.TitleText
        self.__labeltext = labeltext
        self.__title = title
        self.__editvalue = editvalue
        self.__event_loop = event_loop
        self.__event_loop.setNextFormPrevious()
        npyscreen.Popup.__init__(self, *args, name=self.__title, lines=lines, **kwargs)

    def create(self):
        self.edt_val = self.add(self.__labeltype, hidden=False, name=self.__labeltext, value=self.__editvalue)
        self.edt_val.editable=False
        
    def afterEditing(self):
        self.__event_loop.switchFormPrevious()

class warning_box(message_box):
    def __init__(self,  event_loop, editvalue, lines=5, labeltext="(!!!)\t", title="WARNING", *args, **kwargs):
        message_box.__init__(self, event_loop, editvalue, *args, lines=lines, labeltext=labeltext, title=title,  **kwargs)

class error_box(message_box):
    def __init__(self,  event_loop, editvalue, lines=5, labeltext="(!E!)\t", title="ERROR",  *args, **kwargs):
        message_box.__init__(self, event_loop, editvalue, *args, lines=lines, labeltext=labeltext, title=title,  **kwargs)
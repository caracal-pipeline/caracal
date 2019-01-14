import npyscreen
import re

class input_box(npyscreen.ActionPopup):
    def __init__(self, event_loop, labeltype, labeltext, editvalue, on_ok=None, on_cancel=None, lines=5, title="User input", *args, **kwargs):
        self.__labeltype = labeltype
        self.__labeltext = re.sub("[-_]", "", labeltext)
        self.__editvalue = editvalue
        self.__event_loop = event_loop
        self.__on_ok = on_ok
        self.__on_cancel = on_cancel

        min_width = max(len(self.__labeltext) + len(self.__editvalue), len(title)) + 20
        kwargs['columns'] = kwargs['minimum_columns'] = min_width
        npyscreen.ActionPopup.__init__(self, *args, name=title, lines=lines, **kwargs)
        

    def create(self):
        self.edt_val = self.add(self.__labeltype, hidden=False, name=self.__labeltext, value=self.__editvalue)

    def afterEditing(self):
        self.__event_loop.switchFormPrevious()
    
    def on_ok(self):
        if self.__on_ok is not None:
            self.__on_ok(self.edt_val.value)

    def on_cancel(self):
        if self.__on_cancel is not None:
            self.__on_cancel()

class optioned_input_box(input_box):
    def __init__(self, event_loop, labeltext, editvalues, defaultvalue, on_ok=None, on_cancel=None, *args, **kwargs):
        self.__labeltext = labeltext
        self.__editvalue = editvalues
        self.__defaultvalue = defaultvalue
        min_width = max(len(self.__labeltext) + len(self.__defaultvalue), len(kwargs.get('title', ""))) + 20
        kwargs['columns'] = kwargs['minimum_columns'] = min_width
        input_box.__init__(self, event_loop, npyscreen.TitleSelectOne, labeltext, editvalues, on_ok, on_cancel, lines=len(self.__editvalue)+4, *args, **kwargs)

    def create(self):
        self.edt_val = self.add(npyscreen.TitleSelectOne, hidden=False, name=self.__labeltext, values=self.__editvalue, 
                                value=[self.__editvalue.index(self.__defaultvalue)], 
                                scroll_exit=True, max_height=len(self.__editvalue))

class action_popup_okonly(npyscreen.ActionPopup):
    def create_control_buttons(self):
        npyscreen.ActionPopup.create_control_buttons(self)
        self._widgets__[2].hidden = True

class message_box(action_popup_okonly):
    def __init__(self, event_loop, editvalue, lines=5, labeltext="(i)\t", title="INFO", on_ok=None, *args, **kwargs):
        self.__labeltype = npyscreen.TitleText
        self.__labeltext = labeltext
        self.__title = title
        self.__editvalue = editvalue
        self.__event_loop = event_loop
        self.__on_ok = on_ok
        kwargs['color'] = 'MBOXDEFAULT'
        kwargs['labelcolor'] = 'MBOXDEFAULT'
        min_width = max(len(self.__labeltext) + len(self.__editvalue), len(title)) + 20
        kwargs['columns'] = kwargs['minimum_columns'] = min_width
        action_popup_okonly.__init__(self, *args, name=self.__title, lines=lines, **kwargs)

    def create(self):
        self.edt_val = self.add(self.__labeltype, hidden=False, name=self.__labeltext, value=self.__editvalue,
                                color='MBOXDEFAULT', labelcolor='MBOXDEFAULT')
        self.edt_val.editable=False
        
    def afterEditing(self):
        self.__event_loop.switchFormPrevious()
    
    def on_ok(self):
        if self.__on_ok is not None:
            self.__on_ok()

class warning_box(message_box):
    def __init__(self,  event_loop, editvalue, lines=5, labeltext="(!!!)\t", title="WARNING", *args, **kwargs):
        message_box.__init__(self, event_loop, editvalue, *args, lines=lines, labeltext=labeltext, title=title,  **kwargs)

class error_box(message_box):
    def __init__(self,  event_loop, editvalue, lines=5, labeltext="(!E!)\t", title="ERROR",  *args, **kwargs):
        message_box.__init__(self, event_loop, editvalue, *args, lines=lines, labeltext=labeltext, title=title,  **kwargs)

import npyscreen
import os

import meerkathi
from meerkathi.view_controllers.meerkathi_theme import meerkathi_theme
from meerkathi.dispatch_crew.config_parser import config_parser as cp
from meerkathi.view_controllers.message_boxes import input_box, message_box, warning_box, error_box
from meerkathi.view_controllers.option_editor import option_editor

class web_serve_form(npyscreen.FormBaseNew):
    def __init__(self, event_loop):
        npyscreen.setTheme(meerkathi_theme)
        self.__event_loop = event_loop
        self.__wt_proc = None
        npyscreen.FormBaseNew.__init__(self, name="HTTP server")

    @property
    def event_loop(self):
        return self.__event_loop

    def on_stop_pressed(self):
        if self.__wt_proc is not None:
            self.__wt_proc.interrupt()
            self.__wt_proc.join()
            self.enable_ctrls()

    def on_cfg_editor_pressed(self):
        instance = option_editor(self.__event_loop)
        self.__event_loop.registerForm("OPTIONEDITOR", instance)
        self.__event_loop.switchForm("OPTIONEDITOR")

    def on_edit_port(self, widget):
        try:
            val = widget.value
            newval = int(val)
            cp().update_args_key(["interactive_port"], newval)
            
        except ValueError:
            instance = error_box(self.event_loop, "Cannot convert '{}' to port number (int)".format(val), 
                                 on_ok=lambda: setattr(widget, "value",str(cp().args.interactive_port)))
            self.event_loop.registerForm("MESSAGEBOX", instance)
            self.event_loop.switchForm("MESSAGEBOX")
            

    def enable_ctrls(self):
        self.btn_stop.hidden = True
        self.btn_start.hidden = False
        self.red_webbrowser.editable = True
        self.edt_port.editable = True
        self.DISPLAY()

    def disable_ctrls(self):
        self.btn_stop.hidden = False
        self.btn_start.hidden = True
        self.red_webbrowser.editable = False
        self.edt_port.editable = False
        self.DISPLAY()
    
    def display(self, clear=False):
        self.edt_output.value = os.path.abspath(os.path.join(cp().args.general_output, "report"))
        npyscreen.FormBaseNew.display(self, clear)

    def create(self):
        self.add(npyscreen.TitleText, editable=False, name="Web server control", rely=2)
        self.btn_start = self.add(npyscreen.ButtonPress, name = "Start serving", relx=-21, rely=2,
                                  when_pressed_function=self.run_operations)
        self.btn_stop = self.add(npyscreen.ButtonPress, name = "Stop serving", relx=-21, rely=3,
                                when_pressed_function=self.on_stop_pressed, hidden=True)
        self.btn_back = self.add(npyscreen.ButtonPress, name = "Back", relx=-21, rely=4,
                                 when_pressed_function=lambda: self.on_stop_pressed() or self.__event_loop.switchFormPrevious(), hidden=False)
        self.add(npyscreen.BoxBasic, editable=False, name="Additional options", rely=6, max_height=12)
        self.red_webbrowser = self.add(npyscreen.TitleMultiSelect, max_height=3, rely=8, relx=5, max_width=30, 
                                       name="Open default external webbrowser (may require X11)", value=[0,],
                                       values=["On"], scroll_exit=True)
        self.edt_port = self.add(npyscreen.TitleText, editable=True, name="Host on port", rely=11, relx=5, max_width=30, value=str(cp().args.interactive_port))
        setattr(self.edt_port, "value_changed_callback", self.on_edit_port)

        self.edt_output = self.add(npyscreen.TitleText, editable=False, name="Report dir", 
                                   rely=13, relx=5, max_width=120, value=os.path.abspath(os.path.join(cp().args.general_output, "report")))
        self.edt_output_hint = self.add(npyscreen.TitleText, editable=False, name="\t", 
                                        rely=14, relx=5, max_width=80,  value="(change general->output directory from option_editor to edit)")
        self.btn_config_editor = self.add(npyscreen.ButtonPress, name = "Config editor", rely=12, relx=-25,
                                          when_pressed_function=self.on_cfg_editor_pressed)

    def run_operations(self):
        self.disable_ctrls()
        try:
            do_open_browser = self.red_webbrowser.value == [0]
            self.__wt_proc = meerkathi.start_viewer(cp().args, timeout=0, open_webbrowser=do_open_browser)
            if do_open_browser:
                instance = message_box(self.event_loop, "Web browser (if available) is deamonized. It should open momentarily.")
                self.event_loop.registerForm("MESSAGEBOX", instance)
                self.event_loop.switchForm("MESSAGEBOX")
        except RuntimeError as e:
            instance = error_box(self.__event_loop, str(e))
            self.__event_loop.registerForm("MESSAGEBOX", instance)
            self.__event_loop.switchForm("MESSAGEBOX")
            self.enable_ctrls()
        
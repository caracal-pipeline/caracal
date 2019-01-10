import npyscreen
import exceptions

import meerkathi
from meerkathi.view_controllers.meerkathi_theme import meerkathi_theme
from meerkathi.view_controllers.message_boxes import input_box, message_box, warning_box, error_box
from meerkathi.dispatch_crew.config_parser import config_parser as cp

class option_editor(npyscreen.FormBaseNew):
    def __init__(self, event_loop):
        npyscreen.setTheme(meerkathi_theme)
        self.__event_loop = event_loop
        self.__event_loop.setNextFormPrevious()

        npyscreen.FormBaseNew.__init__(self, name="Pipeline configuration editor")
        

    def on_back_pressed(self):
        self.__event_loop.switchFormPrevious()

        
    def on_load(self, labeltype=npyscreen.TitleFilenameCombo, labeltext="Filename", editvalue=cp().args.config):
        def on_confirm_load(filename):
            cp().args.config = filename
            cp().update_config(cp().args)

        instance = input_box(self.__event_loop, labeltype, labeltext, editvalue, columns=100, on_ok=on_confirm_load)
        self.__event_loop.registerForm("INPUTBOX", instance)
        self.__event_loop.switchForm("INPUTBOX")

    def on_save(self, labeltype=npyscreen.TitleFilename, labeltext="Filename", editvalue="./CustomParset.yaml"):
        def on_confirm_default_parset(filename):
            cp().save_options(filename)
            instance = message_box(self.__event_loop, "Successfully written out default parset settings to {}".format(filename),
                                   minimum_columns=150, columns=120)
            self.__event_loop.registerForm("MESSAGEBOX", instance)
            self.__event_loop.switchForm("MESSAGEBOX")
        instance = input_box(self.__event_loop, labeltype, labeltext, editvalue, on_ok=on_confirm_default_parset)
        self.__event_loop.registerForm("INPUTBOX", instance)
        self.__event_loop.switchForm("INPUTBOX")
  

    def create(self):
        self.btn_load = self.add(npyscreen.ButtonPress, name = "Back",
                                 when_pressed_function=self.on_back_pressed)
        self.btn_edit = self.add(npyscreen.ButtonPress, name = "Load",
                                 when_pressed_function=lambda: self.on_load())
        self.btn_edit = self.add(npyscreen.ButtonPress, name = "Store",
                                 when_pressed_function=lambda: self.on_save())

        
        # t  = F.add(npyscreen.TitleText, name = "Text:",)
        # fn = F.add(npyscreen.TitleFilename, name = "Filename:")
        # fn2 = F.add(npyscreen.TitleFilenameCombo, name="Filename2:")
        # dt = F.add(npyscreen.TitleDateCombo, name = "Date:")
        # s  = F.add(npyscreen.TitleSlider, out_of=12, name = "Slider")
        # ml = F.add(npyscreen.MultiLineEdit,
        #        value = """try typing here!\nMutiline text, press ^R to reformat.\n""",
        #        max_height=5, rely=9)
        # ms = F.add(npyscreen.TitleSelectOne, max_height=4, value = [1,], name="Pick One",
        #         values = ["Option1","Option2","Option3"], scroll_exit=True)
        # ms2= F.add(npyscreen.TitleMultiSelect, max_height =-2, value = [1,], name="Pick Several",
        #         values = ["Option1","Option2","Option3"], scroll_exit=True)


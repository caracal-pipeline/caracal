# -*- coding: utf-8 -*-

import npyscreen
import exceptions

import meerkathi
from meerkathi.view_controllers.meerkathi_theme import meerkathi_theme
from meerkathi.view_controllers.message_boxes import input_box, message_box, warning_box, error_box
from meerkathi.view_controllers.option_editor import option_editor
from meerkathi.dispatch_crew.config_parser import config_parser as cp

class entry_form(npyscreen.FormBaseNew):
    def __init__(self, event_loop):
        npyscreen.setTheme(meerkathi_theme)
        self.__event_loop = event_loop
        npyscreen.FormBaseNew.__init__(self, name="MeerKATHI -- Prototype science pipeline for MeerKAT")
        

    def on_quit_pressed(self):
        self.__event_loop.switchForm(None)
        raise exceptions.SystemExit(0)

    def on_edit_pressed(self):
        instance = option_editor(self.__event_loop)
        self.__event_loop.registerForm("OPTIONEDITOR", instance)
        self.__event_loop.switchForm("OPTIONEDITOR")

    def on_run_pressed(self):
        self.__event_loop.switchForm(None)    
        
    def on_input_default_parset(self, labeltype=npyscreen.TitleFilename, labeltext="Filename", editvalue="./DefaultParset.yaml"):
        def on_confirm_default_parset(filename):
            meerkathi.get_default(filename)
            instance = message_box(self.__event_loop, "Successfully written out default parset settings to {}".format(filename),
                                   minimum_columns=150, columns=120)
            self.__event_loop.registerForm("MESSAGEBOX", instance)
            self.__event_loop.switchForm("MESSAGEBOX")
        instance = input_box(self.__event_loop, labeltype, labeltext, editvalue, on_ok=on_confirm_default_parset)
        self.__event_loop.registerForm("INPUTBOX", instance)
        self.__event_loop.switchForm("INPUTBOX")

    def on_report_view_pressed(self):
        self.__event_loop.switchForm(None)
        meerkathi.start_viewer(cp().args)
        self.__event_loop.switchForm("MAIN")
        
    def create(self):
        self.add(npyscreen.TitleText, editable=False, name="\t\t\t", value="███╗   ███╗███████╗███████╗██████╗ ██╗  ██╗ █████╗ ████████╗██╗  ██╗██╗")
        self.add(npyscreen.TitleText, editable=False, name="\t\t\t", value="████╗ ████║██╔════╝██╔════╝██╔══██╗██║ ██╔╝██╔══██╗╚══██╔══╝██║  ██║██║")
        self.add(npyscreen.TitleText, editable=False, name="\t\t\t", value="██╔████╔██║█████╗  █████╗  ██████╔╝█████╔╝ ███████║   ██║   ███████║██║")
        self.add(npyscreen.TitleText, editable=False, name="\t\t\t", value="██║╚██╔╝██║██╔══╝  ██╔══╝  ██╔══██╗██╔═██╗ ██╔══██║   ██║   ██╔══██║██║")
        self.add(npyscreen.TitleText, editable=False, name="\t\t\t", value="██║ ╚═╝ ██║███████╗███████╗██║  ██║██║  ██╗██║  ██║   ██║   ██║  ██║██║")
        self.add(npyscreen.TitleText, editable=False, name="\t\t\t", value="╚═╝     ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═╝")
        self.add(npyscreen.TitleText, editable=False, name="\t\t\t", value="Module installed at: {0:s} (version {1:s})".format(meerkathi.pckgdir, str(meerkathi.__version__)))
        self.add(npyscreen.TitleText, editable=False, name="\t\t\t", value="A logfile will be dumped here: {0:s}".format(meerkathi.MEERKATHI_LOG))
        self.add(npyscreen.TitleText, editable=False, name="\t")
        self.add(npyscreen.TitleText, editable=False, name="\t")
        self.add(npyscreen.TitleText, editable=False, name="\t")
        self.btn_run = self.add(npyscreen.ButtonPress, name = "Run pipeline",
                                when_pressed_function=self.on_run_pressed)
        self.btn_edit = self.add(npyscreen.ButtonPress, name = "Edit pipeline configuration",
                                 when_pressed_function=self.on_edit_pressed)
        self.btn_report = self.add(npyscreen.ButtonPress, name = "View reports from previous runs",
                                   when_pressed_function=self.on_report_view_pressed)
        self.add(npyscreen.TitleText, editable=False, name="\t")

        self.btn_default = self.add(npyscreen.ButtonPress, name = "Dump default configuration",
                                    when_pressed_function=lambda: self.on_input_default_parset())
        self.add(npyscreen.TitleText, editable=False, name="\t")

        self.btn_quit = self.add(npyscreen.ButtonPress, name = "Quit to MS-DOS",
                                 when_pressed_function=self.on_quit_pressed)




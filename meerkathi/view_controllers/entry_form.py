# -*- coding: utf-8 -*-

import npyscreen
import exceptions

import meerkathi
from meerkathi.view_controllers.meerkathi_theme import meerkathi_theme
from meerkathi.view_controllers.message_boxes import input_box, message_box, warning_box, error_box
from meerkathi.view_controllers.option_editor import option_editor
from meerkathi.view_controllers.web_serve_form import web_serve_form
from meerkathi.dispatch_crew.config_parser import config_parser as cp
from meerkathi.view_controllers.misc_opts_form import misc_opts_form
from meerkathi.view_controllers.execution_form import execution_form

class entry_form(npyscreen.FormBaseNew):
    def __init__(self, *args, **kwargs):
        npyscreen.setTheme(meerkathi_theme) 
        npyscreen.FormBaseNew.__init__(self, *args, **kwargs)
        
    @property
    def event_loop(self):
        return self.parentApp

    def on_quit_pressed(self):
        self.event_loop.switchForm(None)
        raise exceptions.SystemExit(0)

    def on_edit_pressed(self):
        self.event_loop.switchForm("OPTIONEDITOR")

    def on_run_pressed(self):
        self.event_loop["EXECUTIONVIEW"].start_pipeline_next_draw = True
        self.event_loop.switchForm("EXECUTIONVIEW")

    def on_input_default_parset(self, labeltype=npyscreen.TitleFilename, labeltext="Filename", editvalue="./DefaultParset.yaml"):
        def on_confirm_default_parset(filename):
            meerkathi.get_default(filename)
            instance = message_box(self.event_loop, "Successfully written out default parset settings to {}".format(filename))
            self.event_loop.registerForm("MESSAGEBOX", instance)
            self.event_loop.switchForm("MESSAGEBOX")
        instance = input_box(self.event_loop, labeltype, labeltext, editvalue, on_ok=on_confirm_default_parset)
        self.event_loop.registerForm("INPUTBOX", instance)
        self.event_loop.switchForm("INPUTBOX")

    def on_report_view_pressed(self):
        self.event_loop.switchForm("WEBSERVEFORM")

    def on_misc_opts_edit_pressed(self):
        self.event_loop.switchForm("ADVOPTIONEDITOR")
        
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
        self.btn_misc_opts = self.add(npyscreen.ButtonPress, name = "Edit advanced configuration options",
                                      when_pressed_function=lambda: self.on_misc_opts_edit_pressed())
        self.add(npyscreen.TitleText, editable=False, name="\t")

        self.btn_quit = self.add(npyscreen.ButtonPress, name = "Quit to MS-DOS",
                                 when_pressed_function=self.on_quit_pressed)




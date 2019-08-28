# -*- coding: utf-8 -*-

import npyscreen
from threading import Thread

import meerkathi
from meerkathi.view_controllers.meerkathi_theme import meerkathi_theme
from meerkathi.view_controllers.message_boxes import input_box, message_box, warning_box, error_box
from meerkathi.dispatch_crew.config_parser import config_parser as cp
from meerkathi.view_controllers.log_view import log_view

class execution_form(npyscreen.FormBaseNew):
    def __init__(self, *args, **kwargs):
        npyscreen.setTheme(meerkathi_theme)
        self.__pl_proc = None
        self.__initial_display = False
        npyscreen.FormBaseNew.__init__(self, *args, **kwargs)

    @property
    def event_loop(self):
        return self.parentApp
    
    def on_pipeline_complete(self, proc):
        self.lbl_successfailure.value = "Execution {}".format("finished SUCCESSFULLY" if not proc.exitcode else 
                                                              "FAILED! See log for details")
        self.lbl_successfailure.color = "STANDOUT" if proc.exitcode != 0 else "SAFE"
        self.btn_back.hidden = False
        self.btn_back.display()
        self.lbl_successfailure.display()
    
    def on_back_pressed(self):
        self.event_loop.switchFormPrevious()

    def edit(self):
        npyscreen.FormBaseNew.edit(self)
    
    @property
    def start_pipeline_next_draw(self):
        return self.__initial_display

    @start_pipeline_next_draw.setter
    def start_pipeline_next_draw(self, val):
        if not self.start_pipeline_next_draw:
            self.btn_back.hidden = True
            self.lbl_successfailure.value = "Executing"
            self.lbl_successfailure.color = "DEFAULT"
        self.__initial_display = val

    def display(self, clear=False):
        if self.start_pipeline_next_draw:
            self.start_pipeline_next_draw = False
            self.start_pipeline()

        npyscreen.FormBaseNew.display(self, clear)

    def create(self):
        self.box_logger = self.add(npyscreen.BoxBasic, name="Log tail", max_width=-5, max_height=-3, editable=False)
        self.lvw_logger = self.add(log_view, editable=False, value="", max_width=-10, max_height=-5, relx=5, rely=4)
        self.lvw_logger.color = "GOOD"
        self.lvw_logger.widgets_inherit_color = True
        self.lbl_successfailure = self.add(npyscreen.Textfield, editable=False, 
                                           value="Executing", rely=-4, max_width=80)
        

        self.btn_back = self.add(npyscreen.ButtonPress, name = "Back to main screen", hidden=True, editable=True, relx=-31, rely=-4,
                                 when_pressed_function=self.on_back_pressed)
        
        self.add(npyscreen.ButtonPress, name="", rely=-3, width=0, height=0) ##BUG in npyscreen last thing must be enabled and visible
        
        
    def start_pipeline(self):
        """ Run pipeline interactively """
        def __block_and_callback(p, callback_func):
            p.join()
            callback_func(p)

        self.__pl_proc = meerkathi.execute_pipeline(cp().args, cp().arg_groups, block=False)
        Thread(target=__block_and_callback, args=(self.__pl_proc, self.on_pipeline_complete)).start()


    

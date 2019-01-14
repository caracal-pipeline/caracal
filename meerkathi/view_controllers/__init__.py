import npyscreen

from meerkathi.view_controllers.entry_form import entry_form
from meerkathi.view_controllers.execution_form import execution_form
from meerkathi.view_controllers.misc_opts_form import misc_opts_form
from meerkathi.view_controllers.option_editor import option_editor
from meerkathi.view_controllers.web_serve_form import web_serve_form

class event_loop(npyscreen.NPSAppManaged):
    
    def __getitem__(self, key):
        return self._Forms[key]

    def onStart(self):
        self.addForm("MAIN", entry_form, name="Welcome")
        self.addForm("EXECUTIONVIEW", execution_form, name="Pipeline execution")
        self.addForm("ADVOPTIONEDITOR", misc_opts_form, name="Advanced options")
        self.addForm("OPTIONEDITOR", option_editor, name="Pipeline configuration editor")
        self.addForm("WEBSERVEFORM", web_serve_form, name="HTTP server")


    
    
    
    


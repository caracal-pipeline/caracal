import npyscreen

from meerkathi.view_controllers.entry_form import entry_form as mef
class event_loop(npyscreen.NPSAppManaged):
    def onStart(self):
        self.registerForm("MAIN", mef(self))
    
    
    
    


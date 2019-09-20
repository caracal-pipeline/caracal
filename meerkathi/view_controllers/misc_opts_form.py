import npyscreen
import os

import meerkathi
from meerkathi.view_controllers.meerkathi_theme import meerkathi_theme
from meerkathi.dispatch_crew.config_parser import config_parser as cp
from meerkathi.view_controllers.message_boxes import input_box, message_box, warning_box, error_box


class misc_opts_form(npyscreen.Form):
    def __init__(self, *args, **kwargs):
        npyscreen.setTheme(meerkathi_theme)

        npyscreen.Form.__init__(self, *args, **kwargs)

    @property
    def event_loop(self):
        return self.parentApp

    def afterEditing(self):
        self.event_loop.setNextFormPrevious()

    def on_edt_changed(self, widget, key, ufunc_convert):
        try:
            val = widget.value
            __cast = str if ufunc_convert == 'str' else int if ufunc_convert == 'int' else float if ufunc_convert == float else None

            newval = None if __cast == str and (
                val == 'None' or val == '- Unset -') else __cast(val)
            cp().update_args_key([key], newval)

        except ValueError:
            instance = error_box(self.event_loop, "Cannot convert '{}' to {}".format(val, type),
                                 on_ok=lambda: setattr(widget, "value", str(getattr(cp().args, key))))
            self.event_loop.registerForm("MESSAGEBOX", instance)
            self.event_loop.switchForm("MESSAGEBOX")

    def on_red_pipeline_build_order_changed(self, widget):
        newval = widget.value[0] == 1
        cp().update_args_key(["add_all_first"], newval)

    def create(self):
        self.edt_singularity_image_dir = self.add(npyscreen.TitleFilenameCombo, editable=True, select_dir=True,
                                                  name="Directory where stimela singularity images are stored",
                                                  max_width=90, value=str(cp().args.singularity_image_dir))
        setattr(self.edt_singularity_image_dir,
                "value_changed_callback",
                lambda widget: self.on_edt_changed(widget, "singularity_image_dir", "str"))
        self.edt_stimela_build = self.add(npyscreen.TitleText, editable=True,
                                          name="Label of stimela build to use",
                                          max_width=90, value=str(cp().args.stimela_build))
        setattr(self.edt_stimela_build,
                "value_changed_callback",
                lambda widget: self.on_edt_changed(widget, "stimela_build", "str"))
        self.edt_schema = self.add(npyscreen.TitleFilenameCombo, editable=True, select_dir=True,
                                   name="Path to custom schema for worker(s)",
                                   max_width=90, value=str(cp().args.schema))
        setattr(self.edt_schema,
                "value_changed_callback",
                lambda widget: self.on_edt_changed(widget, "schema", "str"))
        self.edt_workers_directory = self.add(npyscreen.TitleFilenameCombo, editable=True, select_dir=True,
                                              name="Directory where pipeline workers can be found",
                                              max_width=90, value=str(cp().args.workers_directory))
        setattr(self.edt_workers_directory,
                "value_changed_callback",
                lambda widget: self.on_edt_changed(widget, "workers_directory", "str"))
        self.red_pipeline_build_order = self.add(npyscreen.TitleSelectOne, max_height=4, max_width=50,
                                                 name="Pipeline build order",
                                                 values=["Execute workers as encountered",
                                                         "Add all workers first, then execute"],
                                                 value=1 if cp().args.add_all_first else 0,
                                                 scroll_exit=True)
        setattr(self.red_pipeline_build_order,
                "value_changed_callback",
                self.on_red_pipeline_build_order_changed)
        pass

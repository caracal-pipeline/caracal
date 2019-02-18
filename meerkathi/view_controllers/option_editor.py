# -*- coding: utf-8 -*-

import npyscreen
import exceptions
import weakref
import textwrap
from collections import OrderedDict

import meerkathi
from meerkathi.view_controllers.meerkathi_theme import meerkathi_theme
from meerkathi.view_controllers.message_boxes import input_box, message_box, warning_box, error_box, optioned_input_box
from meerkathi.dispatch_crew.config_parser import config_parser as cp

class opt_treeview(npyscreen.MLTree):
    def update_help(self, updown):
        seli = min(max(0, self.cursor_line + updown), len(self.values) - 1)
        sel = self.values[seli]
        def __walk_up(node):
            keys = []
            if node._parent is not None:
                keys = keys + __walk_up(node._parent)
            return keys + ([node.key_val[1:]] if node.key_val is not None and node.key_val[0] == "@" else
                           [node.key_val] if node.key_val is not None else 
                           [])
        helpstr = cp().get_schema_help(__walk_up(sel))
        helpstr = "NO HELP FOR THIS PARAMATER -- PLEASE REPORT" if helpstr == None else helpstr
        helpstr = "\n".join(textwrap.wrap(helpstr, 70))
        
        self.parent.lbl_help.value = helpstr
        self.parent.lbl_help.display()
    
    def h_cursor_line_down(self, x):
        """ when hover over """
        self.update_help(updown=+1)
        return npyscreen.MLTree.h_cursor_line_down(self, x)

    def h_cursor_line_up(self, x):
        """ when hover over """
        self.update_help(updown=-1)
        return npyscreen.MLTree.h_cursor_line_up(self, x)

    def h_select(self, msg):
        seli = self._last_cursor_line
        sel = self.values[seli]
        def __walk_up(node):
            keys = []
            if node._parent is not None:
                keys = keys + __walk_up(node._parent)
            keys = [k for k in keys if k != "enable"]
            return keys + ([node.key_val[1:], "enable"] if node.key_val is not None and node.key_val[0] == "@" else
                           [node.key_val] if node.key_val is not None else
                           [])

        def __uinput_factory(chain, db):
            val_type = cp().get_schema_type(chain)
            valid_options = cp().get_schema_enum(chain)
            is_required = cp().get_schema_required(chain)
            seq = cp().get_schema_seq(chain)

            def __numeric_input(val_type, chain, db):
                labeltype = npyscreen.TitleText
                labeltext = "New value"
                editvalue = str(db.get_key(chain))
                def __validate(x):
                    try:
                        __cast = float if val_type == "float" else int
                        result = __cast(x)
                        db.update_key(chain, result)
                        self.parent.rebuild_opt_tree()
                        self.display()
                    except ValueError:
                        instance = error_box(self.parent.event_loop, "Cannot convert {} to {}".format(x, val_type))
                        self.parent.event_loop.registerForm("MESSAGEBOX", instance)
                        self.parent.event_loop.switchForm("MESSAGEBOX")
                
                instance = input_box(self.parent.event_loop, 
                                     labeltype, 
                                     labeltext, 
                                     editvalue, 
                                     on_ok=__validate,
                                     title="Modify {} ({})".format(chain[-1], val_type))
                self.parent.event_loop.registerForm("INPUTBOX", instance)
                self.parent.event_loop.switchForm("INPUTBOX")

            def __string_input(val_type, chain, db, valid_options, is_required):
                labeltext = "New value"
                editvalue = db.get_key(chain)
                def __validate(x):
                    db.update_key(chain, x if not isinstance(valid_options, list) else list(set(valid_options))[x[0]])
                    self.parent.rebuild_opt_tree()
                    self.display()
                
                if isinstance(valid_options, list):
                    instance = optioned_input_box(self.parent.event_loop,  
                                                  labeltext, 
                                                  list(set(valid_options)), 
                                                  on_ok=__validate,
                                                  defaultvalue=editvalue,
                                                  title="Modify {} ({})".format(chain[-1], val_type))
                else:
                    instance = input_box(self.parent.event_loop, 
                                         npyscreen.TitleText, 
                                         labeltext, 
                                         editvalue, 
                                         on_ok=__validate,
                                         title="Modify {} ({})".format(chain[-1], val_type))
                self.parent.event_loop.registerForm("INPUTBOX", instance)
                self.parent.event_loop.switchForm("INPUTBOX")

            def __list_input(val_type, chain, db, valid_options, is_required):
                labeltext = "New value"
                editvalue = ", ".join([str(x) for x in db.get_key(chain)])

                def __validate(x):
                    try:
                        list_vals = [xi.strip() for xi in x.split(",")]
                        results = []
                        for xi in list_vals:    
                            if isinstance(valid_options, list):
                                if xi not in list(set(valid_options)):
                                    raise ValueError("'{}' is not an acceptable value. Requires one of {}.".format(xi, "[{}]".format(",".join(list(set(valid_options))))))
                            __cast = float if val_type == "float" else int if val_type == "int" else str
                            try:
                                results.append(__cast(xi))
                            except ValueError:
                                raise ValueError("Cannot convert '{}' to {}".format(xi, val_type))
                        db.update_key(chain, results)
                        self.parent.rebuild_opt_tree()
                        self.display()
                    except ValueError as e:
                        instance = error_box(self.parent.event_loop, str(e), minimum_columns=90, columns=90)
                        self.parent.event_loop.registerForm("MESSAGEBOX", instance)
                        self.parent.event_loop.switchForm("MESSAGEBOX")
                
                instance = input_box(self.parent.event_loop, 
                                     npyscreen.TitleText, 
                                     labeltext, 
                                     editvalue, 
                                     on_ok=__validate,
                                     title="Modify {} (list: {})".format(chain[-1], val_type))
                self.parent.event_loop.registerForm("INPUTBOX", instance)
                self.parent.event_loop.switchForm("INPUTBOX")

            if val_type == "bool":
                try:
                    db.update_key(chain, 
                                  not db.get_key(chain))
                except ValueError as e:
                    instance = error_box(self.parent.event_loop, str(e))
                    self.parent.event_loop.registerForm("MESSAGEBOX", instance)
                    self.parent.event_loop.switchForm("MESSAGEBOX")
 
                self.parent.rebuild_opt_tree()
                self.display()
            elif val_type == "float" or val_type == "int": 
                __numeric_input(val_type, chain, db)
            elif seq:
                __list_input(seq, chain, db, valid_options, is_required)
            elif val_type == "text" or val_type == "str": # string or string equivalent
                __string_input(val_type, chain, db, valid_options, is_required)
            else:
                raise RuntimeError(chain)
        chain = __walk_up(sel)
        __uinput_factory(chain, cp())
        
class annotated_tree_data(npyscreen.NPSTreeData):
    def __init__(self, key_val=None, *args, **kwargs):
                 self.__key_val = key_val
                 npyscreen.NPSTreeData.__init__(self, *args, **kwargs)
    @property
    def key_val(self):
        return self.__key_val



class option_editor(npyscreen.FormBaseNew):
    def __init__(self, *args, **kwargs):
        npyscreen.setTheme(meerkathi_theme)
        npyscreen.FormBaseNew.__init__(self, *args, **kwargs)
        
    @property
    def event_loop(self):
        return self.parentApp

    def on_back_pressed(self):
        self.event_loop.switchFormPrevious()

        
    def on_load(self, labeltype=npyscreen.TitleFilenameCombo, labeltext="Filename", editvalue=cp().args.config):
        def on_confirm_load(filename):
            cp().update_args_key(chain = ["config"], new_value = filename)
            cp().update_config(update_mode="defaults and args")
            self.rebuild_opt_tree()

        instance = input_box(self.event_loop, labeltype, labeltext, editvalue, columns=100, on_ok=on_confirm_load)
        self.event_loop.registerForm("INPUTBOX", instance)
        self.event_loop.switchForm("INPUTBOX")

    def on_save(self, labeltype=npyscreen.TitleFilename, labeltext="Filename", editvalue="./CustomParset.yaml"):
        def on_confirm_default_parset(filename):
            cp().save_options(filename)
            instance = message_box(self.event_loop, "Successfully written out user modified parset settings to {}".format(filename),
                                   minimum_columns=150, columns=120)
            self.event_loop.registerForm("MESSAGEBOX", instance)
            self.event_loop.switchForm("MESSAGEBOX")
        instance = input_box(self.event_loop, labeltype, labeltext, editvalue, on_ok=on_confirm_default_parset)
        self.event_loop.registerForm("INPUTBOX", instance)
        self.event_loop.switchForm("INPUTBOX")
  
    def rebuild_opt_tree(self):                
        def __populate_tree(groups, tree=None, level=1, level_label=1):
            """ Depth first populate tree with dictionary of options """
            __isdict = lambda x: isinstance(x, dict) or isinstance(x, OrderedDict)

            if tree is None:
                if not __isdict(groups):
                    raise TypeError("Expected groups argument to be of type dictionary")
                tree = annotated_tree_data(key_val=None, content='Root', selectable=False,ignoreRoot=True)
            
            ki = 1
            for k, v in groups.iteritems():
                if __isdict(groups[k]) and groups[k].get('enable', True):
                    label = "(X) {}. {}".format(ki, k) if level == 1 else "(X) {}.{} {}".format(level_label, ki, k)
                    subtree = tree.newChild("@{}".format(k), content=label, selectable=True)
                    __populate_tree(groups[k], 
                                    subtree, 
                                    level=level+1, 
                                    level_label='.'.join([str(x) for x in ([level_label, ki] if level > 1 else [ki])]))
                    ki += 1
                elif __isdict(groups[k]) and not groups[k].get('enable', True):
                    label = "( ) {}. {}".format(ki, k) if level == 1 else "( ) {}.{} {}".format(level_label, ki, k)
                    subtree = tree.newChild("@{}".format(k), content=label, selectable=True)
                    ki += 1
                else:
                    if k not in ["order", "enable"]:
                        label = "{}. {}".format(ki, k) if level == 1 else "{}.{} {}".format(level_label, ki, k)
                        subtree = tree.newChild(k, content="{} = {}".format(label, v), selectable=True)
                        ki += 1
            return tree

        self.trv_options.values = __populate_tree(cp().arg_groups)

    def create(self):
        self.btn_edit = self.add(npyscreen.ButtonPress, name = "Write to disk",
                                 relx=-51, rely=2,
                                 when_pressed_function=lambda: self.on_save())
        self.btn_load = self.add(npyscreen.ButtonPress, name = "Load from disk",
                                 relx=-31, rely=2,
                                 when_pressed_function=lambda: self.on_load())
        self.btn_back = self.add(npyscreen.ButtonPress, name = "Back",
                                 relx=-11, rely=2,
                                 when_pressed_function=self.on_back_pressed)
        
        self.box_help = self.add(npyscreen.BoxBasic, name="Help", max_width=85, rely=4, relx=-89, max_height=20, editable=False)
        msg = "\n".join(textwrap.wrap("Scroll down the tree and hit return to edit values. Sections marked '■' are enabled, "
                                      "while those marked 'Ø' are disabled. Hit enter to toggle them on or off. Certain sections, "
                                      "like the 'general' section, cannot be switched off.", width=70))
        self.lbl_help = self.add(npyscreen.MultiLineEdit, value=msg, max_width=80, rely=6, relx=-83, max_height=16, editable=False)
        self.lbl_help.color = "SAFE"                        
        self.trv_options = self.add(opt_treeview, rely=4, width=80, exit_right=True, scroll_exit=True)
        self.rebuild_opt_tree()

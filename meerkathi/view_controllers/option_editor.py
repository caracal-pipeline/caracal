# -*- coding: utf-8 -*-

import npyscreen
import exceptions
import weakref
from collections import OrderedDict

import meerkathi
from meerkathi.view_controllers.meerkathi_theme import meerkathi_theme
from meerkathi.view_controllers.message_boxes import input_box, message_box, warning_box, error_box
from meerkathi.dispatch_crew.config_parser import config_parser as cp

class opt_treeview(npyscreen.MLTree):
    def when_value_edited(self):
        """ when hover over """
        pass
        
    def h_select(self, msg):
        seli = self._last_cursor_line
        sel = self.values[seli]
        def __walk_up(node):
            keys = []
            if node._parent is not None:
                keys = keys + __walk_up(node._parent)
            return keys + ([node.key_val] if node.key_val is not None else [])
        chain = __walk_up(sel)
        
        def __uinput_factory(chain, db):
            if len(chain) >= 2 and chain[-1] == 'enable':
                db.update_key(chain, 
                                not db.get_key(chain))
            
        __uinput_factory(chain, cp())
        self.parent.rebuild_opt_tree()

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
            cp().update_args_key(chain = ["config"], new_value = filename)
            cp().update_config(update_mode="defaults and args")
            self.rebuild_opt_tree()

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
  
    def rebuild_opt_tree(self):                
        def __populate_tree(groups, tree=None, level=1, level_label=1):
            """ Depth first populate tree with dictionary of options """
            __isdict = lambda x: isinstance(x, dict) or isinstance(x, OrderedDict)
            class annotated_tree_data(npyscreen.NPSTreeData):
                def __init__(self, key_val=None, *args, **kwargs):
                    self.__key_val = key_val
                    npyscreen.NPSTreeData.__init__(self, *args, **kwargs)
                @property
                def key_val(self):
                    return self.__key_val

            if tree is None:
                if not __isdict(groups):
                    raise TypeError("Expected groups argument to be of type dictionary")
                tree = annotated_tree_data(key_val=None, content='Root', selectable=False,ignoreRoot=True)
            
            ki = 1
            for k, v in groups.iteritems():
                if __isdict(groups[k]) and groups[k].get('enable', True):
                    label = "■ {}. {}".format(ki, k) if level == 1 else "■ {}.{} {}".format(level_label, ki, k)
                    subtree = tree.newChild(k, content=label, selectable=True)
                    __populate_tree(groups[k], subtree, level=level+1, level_label=ki)
                    ki += 1
                elif __isdict(groups[k]) and not groups[k].get('enable', True):
                    label = "Ø {}. {}".format(ki, k) if level == 1 else "Ø {}.{} {}".format(level_label, ki, k)
                    subtree = tree.newChild(k, content=label, selectable=True)
                    if 'enable' in groups[k]:
                        subtree = subtree.newChild('enable', content='enable = False', selectable=True)
                    ki += 1
                else:
                    if k != "order":                        
                        subtree = tree.newChild(k, content="{} = {}".format(k, v), selectable=True)        
            return tree

        self.trv_options.values = __populate_tree(cp().arg_groups)

    def create(self):
        self.btn_edit = self.add(npyscreen.ButtonPress, name = "Store",
                                 relx=-31, rely=2,
                                 when_pressed_function=lambda: self.on_save())
        self.btn_load = self.add(npyscreen.ButtonPress, name = "Load",
                                 relx=-21, rely=2,
                                 when_pressed_function=lambda: self.on_load())
        self.btn_back = self.add(npyscreen.ButtonPress, name = "Back",
                                 relx=-11, rely=2,
                                 when_pressed_function=self.on_back_pressed)
        
        self.box_help = self.add(npyscreen.BoxBasic, name="Help", max_width=85, rely=4, relx=-89, max_height=20, editable=False)
        msg = "::::TODO::::The quick brown fox jumps over the lazy dog::::TODO::::\n" * 6
        self.lbl_help = self.add(npyscreen.MultiLineEdit, value=msg, max_width=80, rely=6, relx=-83, max_height=16, editable=False)                        
        self.trv_options = self.add(opt_treeview, rely=4, width=80, exit_right=True, scroll_exit=True)
        self.rebuild_opt_tree()


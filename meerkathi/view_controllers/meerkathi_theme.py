import npyscreen
import curses
class meerkathi_theme(npyscreen.ThemeManager):
    default_colors = {
    'DEFAULT'     : 'WHITE_BLUE',
    'FORMDEFAULT' : 'WHITE_BLUE',
    'NO_EDIT'     : 'BLACK_BLUE',
    'STANDOUT'    : 'RED_BLUE',
    'CURSOR'      : 'YELLOW_WHITE',
    'CURSOR_INVERSE': 'WHITE_YELLOW',
    'LABEL'       : 'WHITE_BLUE',
    'LABELBOLD'   : 'WHITE_BLUE',
    'CONTROL'     : 'YELLOW_BLUE',
    'IMPORTANT'   : 'GREEN_BLUE',
    'SAFE'        : 'GREEN_BLUE',
    'WARNING'     : 'YELLOW_BLUE',
    'DANGER'      : 'RED_BLUE',
    'CRITICAL'    : 'RED_YELLOW',
    'GOOD'        : 'GREEN_BLUE',
    'GOODHL'      : 'GREEN_BLUE',
    'VERYGOOD'    : 'BLUE_GREEN',
    'CAUTION'     : 'YELLOW_BLUE',
    'CAUTIONHL'   : 'BLUE_YELLOW',
    }

    _colors_to_define = ( 
     # DO NOT DEFINE THE WHITE_BLACK COLOR - THINGS BREAK
     #('WHITE_BLACK',      DO_NOT_DO_THIS,      DO_NOT_DO_THIS),
     ('BLACK_BLUE',      curses.COLOR_BLACK,      curses.COLOR_BLUE),
     #('BLACK_ON_DEFAULT', curses.COLOR_BLACK,      -1),
     #('WHITE_ON_DEFAULT', curses.COLOR_WHITE,      -1),
     ('BLUE_BLACK',       curses.COLOR_BLUE,       curses.COLOR_BLACK),
     ('WHITE_BLUE',      curses.COLOR_WHITE,      curses.COLOR_BLUE),
     ('CYAN_BLUE',       curses.COLOR_CYAN,       curses.COLOR_BLUE),
     ('GREEN_BLUE',      curses.COLOR_GREEN,      curses.COLOR_BLUE),
     ('MAGENTA_BLUE',    curses.COLOR_MAGENTA,    curses.COLOR_BLUE),
     ('RED_BLUE',        curses.COLOR_RED,        curses.COLOR_BLUE),
     ('YELLOW_BLUE',     curses.COLOR_YELLOW,     curses.COLOR_BLUE),
     ('BLUE_RED',        curses.COLOR_BLUE,      curses.COLOR_RED),
     ('BLUE_GREEN',      curses.COLOR_BLUE,      curses.COLOR_GREEN),
     ('BLUE_YELLOW',     curses.COLOR_BLUE,      curses.COLOR_YELLOW),
     ('BLUE_CYAN',       curses.COLOR_BLUE,       curses.COLOR_CYAN),
     ('BLUE_WHITE',       curses.COLOR_BLUE,       curses.COLOR_WHITE),
     ('CYAN_WHITE',       curses.COLOR_CYAN,       curses.COLOR_WHITE),
     ('GREEN_WHITE',      curses.COLOR_GREEN,      curses.COLOR_WHITE),
     ('MAGENTA_WHITE',    curses.COLOR_MAGENTA,    curses.COLOR_WHITE),
     ('RED_WHITE',        curses.COLOR_RED,        curses.COLOR_WHITE),
     ('YELLOW_WHITE',     curses.COLOR_YELLOW,     curses.COLOR_WHITE),
     ('WHITE_YELLOW',     curses.COLOR_WHITE,     curses.COLOR_YELLOW),
    )


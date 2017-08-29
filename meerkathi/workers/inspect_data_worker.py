import sys
import os

NAME = 'Inspect data'

def worker(pipeline, recipe, config):

    def get_field(field):
        if field in ['bpcal', 'gcal', 'target', 'fcal']:
            name = getattr(pipeline, field)[i]
        else:
            name = field
        return str(name)

    uvrange = config.get('uvrange', '')
    for i in range(pipeline.nobs):
        msname = pipeline.msnames[i]
        prefix = pipeline.prefixes[i]
        label = config.get('label', '')

        if pipeline.enable_task(config, 'real_imag'):
            fields = config['real_imag'].get('fields', 'gcal,bpcal').split(',')
            for field_ in fields:
                for col in ['baseline', 'scan']:
                    field = get_field(field_)
                    step = 'plot_real_imag_{0:d}'.format(i)
                    recipe.add('cab/casa_plotms', step,
                       {
                        "vis"           : msname,
                        "field"         : field,
                        "correlation"   : 'XX,YY',
                        "timerange"     : '',
                        "antenna"       : '',
                        "xaxis"         : 'imag',
                        "xdatacolumn"   : config['real_imag'].get('datacolumn', 'corrected'),
                        "yaxis"         : 'real',
                        "ydatacolumn"   : config['real_imag'].get('datacolumn', 'corrected'),
                        "avgtime"       : config['real_imag'].get('avgtime', ''),
                        "avgchannel"    : config['real_imag'].get('avgchannel', ''),
                        "coloraxis"     : col,
                        "iteraxis"      : 'corr',
                        "plotfile"      : '{0:s}-{1:s}-{2:s}-{3:s}-reim.png'.format(prefix, label, field_, col),
                        "expformat"     : 'png',
                        "exprange"      : 'all',
                        "overwrite"     : True,
                        "showgui"       : False,
                        "uvrange"       : uvrange,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Plot imag vs real for field {1:s} ms={2:s} col={3:s}'.format(step, field, msname, col))

        if pipeline.enable_task(config, 'amp_phase'):
            fields = config['amp_phase'].get('fields', 'gcal,bpcal').split(',')
            for field_ in fields:
                for col in ['baseline', 'scan']:
                    field = get_field(field_)
                    step = 'plot_amp_phase_{0:d}'.format(i)
                    recipe.add('cab/casa_plotms', step,
                       {
                        "vis"           : msname,
                        "field"         : field,
                        "correlation"   : 'XX,YY',
                        "timerange"     : '',
                        "antenna"       : '',
                        "xaxis"         : 'phase',
                        "xdatacolumn"   : config['amp_phase'].get('datacolumn', 'corrected'),
                        "yaxis"         : 'amp',
                        "ydatacolumn"   : config['amp_phase'].get('datacolumn', 'corrected'),
                        "avgtime"       : config['amp_phase'].get('avgtime', ''),
                        "avgchannel"    : config['amp_phase'].get('avgchannel', ''),
                        "coloraxis"     : col,
                        "iteraxis"      : 'corr',
                        "plotfile"      : '{0:s}-{1:s}-{2:s}-{3:s}-ap.png'.format(prefix, label, field_, col),
                        "expformat"     : 'png',
                        "exprange"      : 'all',
                        "overwrite"     : True,
                        "showgui"       : False,
                        "uvrange"       : uvrange,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='{0:s}:: Plot amp vs phase for field {1:s} ms={2:s} col={3:s}'.format(step, field, msname, col))

        if pipeline.enable_task(config, 'amp_uvwave'):
            fields = config['amp_uvwave'].get('fields', 'gcal,bpcal').split(',')
            for field_ in fields:
                field = get_field(field_)
                step = 'plot_uvwave_{0:d}'.format(i)
                recipe.add('cab/casa_plotms', step,
                   {
                    "vis"           : msname,
                    "field"         : field,
                    "correlation"   : 'XX,YY',
                    "timerange"     : '',
                    "antenna"       : '',
                    "xaxis"         : 'uvwave',
                    "xdatacolumn"   : config['amp_uvwave'].get('datacolumn', 'corrected'),
                    "yaxis"         : 'amp',
                    "ydatacolumn"   : config['amp_uvwave'].get('datacolumn', 'corrected'),
                    "avgtime"       : config['amp_uvwave'].get('avgtime', ''),
                    "avgchannel"    : config['amp_uvwave'].get('avgchannel', ''),
                    "coloraxis"     : 'baseline',
                    "iteraxis"      : 'corr',
                    "expformat"     : 'png',
                    "exprange"      : 'all',
                    "plotfile"      : '{0:s}-{1:s}-{2:s}-ampuvwave.png'.format(prefix, label, field_),
                    "overwrite"     : True,
                    "showgui"       : False,
                    "uvrange"       : uvrange,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Plot uv-wave for field {1:s} ms={2:s}'.format(step, field, msname))

        if pipeline.enable_task(config, 'amp_ant'):
            fields = config['amp_ant'].get('fields', 'gcal,bpcal').split(',')
            for field_ in fields:
                field = get_field(field_)
                step = 'plot_uvwave_{0:d}'.format(i)
                recipe.add('cab/casa_plotms', step,
                   {
                    "vis"           : msname,
                    "field"         : field,
                    "correlation"   : 'XX,YY',
                    "timerange"     : '',
                    "antenna"       : '',
                    "xaxis"         : 'antenna1',
                    "xdatacolumn"   : config['amp_ant'].get('datacolumn', 'corrected'),
                    "yaxis"         : 'amp',
                    "ydatacolumn"   : config['amp_ant'].get('datacolumn', 'corrected'),
                    "avgtime"       : config['amp_ant'].get('avgtime', ''),
                    "avgchannel"    : config['amp_ant'].get('avgchannel', ''),
                    "coloraxis"     : 'corr',
                    "expformat"     : 'png',
                    "exprange"      : 'all',
                    "plotfile"      : '{0:s}-{1:s}-{2:s}-ampant.png'.format(prefix, label, field_),
                    "overwrite"     : True,
                    "showgui"       : False,
                    "uvrange"       : uvrange,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Plot ampant for field {1:s} ms={2:s}'.format(step, field, msname))


        if pipeline.enable_task(config, 'phase_uvwave'):
            fields = config['phase_uvwave'].get('fields', 'gcal,bpcal').split(',')
            for field_ in fields:
                field = get_field(field_)
                step = 'phase_uvwave_{0:d}'.format(i)
                recipe.add('cab/casa_plotms', step,
                   {
                    "vis"           : msname,
                    "field"         : field,
                    "correlation"   : 'XX,YY',
                    "timerange"     : '',
                    "antenna"       : '',
                    "xaxis"         : 'uvwave',
                    "xdatacolumn"   : config['phase_uvwave'].get('datacolumn', 'corrected'),
                    "yaxis"         : 'phase',
                    "ydatacolumn"   : config['phase_uvwave'].get('datacolumn', 'corrected'),
                    "avgtime"       : config['phase_uvwave'].get('avgtime', ''),
                    "avgchannel"    : config['phase_uvwave'].get('avgchannel', ''),
                    "coloraxis"     : 'baseline',
                    "iteraxis"      : 'corr',
                    "expformat"     : 'png',
                    "exprange"      : 'all',
                    "plotfile"      : '{0:s}-{1:s}-{2:s}-phaseuvwave.png'.format(prefix, label, field_),
                    "overwrite"     : True,
                    "showgui"       : False,
                    "uvrange"       : uvrange,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Plot phase uv-wave for field {1:s} ms={2:s}'.format(step, field, msname))


        if pipeline.enable_task(config, 'amp_scan'):
            fields = config['amp_scan'].get('fields', 'gcal,target,bpcal').split(',')
            for field_ in fields:
                field = get_field(field_)
                step = 'plot_ampscan_{0:d}'.format(i)
                recipe.add('cab/casa_plotms', step,
                   {
                    "vis"           : msname,
                    "field"         : field,
                    "correlation"   : 'XX,YY',
                    "timerange"     : '',
                    "antenna"       : '',
                    "xaxis"         : 'scan',
                    "xdatacolumn"   : config['amp_scan'].get('datacolumn', 'corrected'),
                    "yaxis"         : 'amp',
                    "ydatacolumn"   : config['amp_scan'].get('datacolumn', 'corrected'),
                    "avgtime"       : config['amp_scan'].get('avgtime', ''),
                    "avgchannel"    : config['amp_scan'].get('avgchannel', ''),
                    "coloraxis"     : 'baseline',
                    "iteraxis"      : 'corr',
                    "expformat"     : 'png',
                    "exprange"      : 'all',
                    "plotfile"      : '{0:s}-{1:s}-{2:s}-ampscan.png'.format(prefix, label, field_),
                    "overwrite"     : True,
                    "showgui"       : False,
                    "uvrange"       : uvrange,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Plot amp_v_scan for field {1:s} ms={2:s}'.format(step, field, msname))

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

    for i in range(pipeline.nobs):
        msname = pipeline.msnames[i]
        prefix = pipeline.prefixes[i]
        label = config.get('label', '')

        if pipeline.enable_task(config, 'real_imag'):
            fields = config['real_imag'].get('field', 'fcal,bpcal').split(',')
            for field in fields:
                field = get_field(field)
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
                    "coloraxis"     : 'corr',
                    "plotfile"      : '{0:s}-{1:s}-reim.png'.format(prefix, field),
                    "overwrite"     : True,
                    "showgui"       : False,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Plot imag vs real for field {1:s} ms={2:s}'.format(step, field, msname))

        if pipeline.enable_task(config, 'amp_phase'):
            fields = config['amp_phase'].get('field', 'fcal,bpcal').split(',')
            for field in fields:
                field = get_field(field)
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
                    "coloraxis"     : 'corr',
                    "plotfile"      : '{0:s}-{1:s}-ap.png'.format(prefix, field),
                    "overwrite"     : True,
                    "showgui"       : False,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Plot amp vs phase for field {1:s} ms={2:s}'.format(step, field, msname))

        if pipeline.enable_task(config, 'amp_uvwave'):
            fields = config['amp_uvwave'].get('field', 'fcal,bpcal').split(',')
            for field in fields:
                field = get_field(field)
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
                    "coloraxis"     : 'baseline',
                    "expformat"     : 'png',
                    "exprange"      : 'all',
                    "plotfile"      : '{0:s}-{1:s}-uvwave.png'.format(prefix, field),
                    "overwrite"     : True,
                    "showgui"       : False,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Plot uv-wave for field {1:s} ms={2:s}'.format(step, field, msname))

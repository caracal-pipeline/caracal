import os
import sys
import yaml

NAME = 'Inspect data'

# E.g. to split out continuum/<dir> from output/continuum/dir
get_dir_path = lambda string,pipeline : string.split(pipeline.output)[1][1:]

def worker(pipeline, recipe, config):

    def get_field(field):
        if field in ['bpcal', 'gcal', 'target', 'fcal']:
            name = getattr(pipeline, field)[i]
        else:
            name = field
        return str(name)

    uvrange = config.get('uvrange')
    if pipeline.virtconcat:
        msnames = [pipeline.vmsname]
        prefixes = [pipeline.prefix]
        nobs = 1
    else:
        msnames = pipeline.msnames
        prefixes = pipeline.prefixes
        nobs = pipeline.nobs

    for i in range(nobs):
        msname = msnames[i]
        prefix = prefixes[i]
        label = config.get('label')
        
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, prefix)
        corr =  config.get('correlation')
        if corr=='auto':
            with open(msinfo, 'r') as stdr:
                corrs = yaml.load(stdr)['CORR']['CORR_TYPE']
            corrs =  ','.join(corrs)
            corr = corrs

        if pipeline.enable_task(config, 'real_imag') or pipeline.enable_task(config, 'amp_phase') or pipeline.enable_task(config, 'amp_uvwave') or pipeline.enable_task(config, 'amp_ant') or pipeline.enable_task(config, 'phase_uvwave') or pipeline.enable_task(config, 'amp_scan'):
                plot_path = "{0:s}/{1:s}".format(pipeline.diagnostic_plots, 'crosscal')
                if not os.path.exists(plot_path):
                    os.mkdir(plot_path)

        if pipeline.enable_task(config, 'real_imag'):
            fields = config['real_imag'].get('fields')
            for field_ in fields:

                for col in ['baseline', 'scan']:
                    field = get_field(field_)
                    step = 'plot_real_imag_{0:d}'.format(i)
                    recipe.add('cab/casa_plotms', step,
                       {
                        "vis"           : msname,
                        "field"         : field,
                        "correlation"   : corr,
                        "timerange"     : '',
                        "antenna"       : '',
                        "xaxis"         : 'imag',
                        "xdatacolumn"   : config['real_imag'].get('column'),
                        "yaxis"         : 'real',
                        "ydatacolumn"   : config['real_imag'].get('column'),
                        "avgtime"       : config['real_imag'].get('avgtime'),
                        "avgchannel"    : config['real_imag'].get('avgchannel'),
                        "coloraxis"     : col,
                        "iteraxis"      : 'corr',
                        "plotfile"      : '{0:s}/{1:s}-{2:s}-{3:s}-{4:s}-reim.png'.format(get_dir_path(plot_path, pipeline), prefix, label, field_, col),
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
            fields = config['amp_phase'].get('fields')
            for field_ in fields:
                for col in ['baseline', 'scan']:
                    field = get_field(field_)
                    step = 'plot_amp_phase_{0:d}'.format(i)
                    recipe.add('cab/casa_plotms', step,
                       {
                        "vis"           : msname,
                        "field"         : field,
                        "correlation"   : corr,
                        "timerange"     : '',
                        "antenna"       : '',
                        "xaxis"         : 'phase',
                        "xdatacolumn"   : config['amp_phase'].get('column'),
                        "yaxis"         : 'amp',
                        "ydatacolumn"   : config['amp_phase'].get('column'),
                        "avgtime"       : config['amp_phase'].get('avgtime'),
                        "avgchannel"    : config['amp_phase'].get('avgchannel'),
                        "coloraxis"     : col,
                        "iteraxis"      : 'corr',
                        "plotfile"      : '{0:s}/{1:s}-{2:s}-{3:s}-{4:s}-ap.png'.format(get_dir_path(plot_path, pipeline), prefix, label, field_, col),
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
            fields = config['amp_uvwave'].get('fields')
            for field_ in fields:
                field = get_field(field_)
                step = 'plot_uvwave_{0:d}'.format(i)
                recipe.add('cab/casa_plotms', step,
                   {
                    "vis"           : msname,
                    "field"         : field,
                    "correlation"   : corr,
                    "timerange"     : '',
                    "antenna"       : '',
                    "xaxis"         : 'uvwave',
                    "xdatacolumn"   : config['amp_uvwave'].get('column'),
                    "yaxis"         : 'amp',
                    "ydatacolumn"   : config['amp_uvwave'].get('column'),
                    "avgtime"       : config['amp_uvwave'].get('avgtime'),
                    "avgchannel"    : config['amp_uvwave'].get('avgchannel'),
                    "coloraxis"     : 'baseline',
                    "iteraxis"      : 'corr',
                    "expformat"     : 'png',
                    "exprange"      : 'all',
                    "plotfile"      : '{0:s}/{1:s}-{2:s}-{3:s}-ampuvwave.png'.format(get_dir_path(plot_path, pipeline), prefix, label, field_),
                    "overwrite"     : True,
                    "showgui"       : False,
                    "uvrange"       : uvrange,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Plot uv-wave for field {1:s} ms={2:s}'.format(step, field, msname))

        if pipeline.enable_task(config, 'amp_ant'):
            fields = config['amp_ant'].get('fields')
            for field_ in fields:
                field = get_field(field_)
                step = 'plot_uvwave_{0:d}'.format(i)
                recipe.add('cab/casa_plotms', step,
                   {
                    "vis"           : msname,
                    "field"         : field,
                    "correlation"   : corr,
                    "timerange"     : '',
                    "antenna"       : '',
                    "xaxis"         : 'antenna1',
                    "xdatacolumn"   : config['amp_ant'].get('column'),
                    "yaxis"         : 'amp',
                    "ydatacolumn"   : config['amp_ant'].get('column'),
                    "avgtime"       : config['amp_ant'].get('avgtime'),
                    "avgchannel"    : config['amp_ant'].get('avgchannel'),
                    "coloraxis"     : 'corr',
                    "expformat"     : 'png',
                    "exprange"      : 'all',
                    "plotfile"      : '{0:s}/{1:s}-{2:s}-{3:s}-ampant.png'.format(get_dir_path(plot_path, pipeline), prefix, label, field_),
                    "overwrite"     : True,
                    "showgui"       : False,
                    "uvrange"       : uvrange,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Plot ampant for field {1:s} ms={2:s}'.format(step, field, msname))


        if pipeline.enable_task(config, 'phase_uvwave'):
            fields = config['phase_uvwave'].get('fields')
            for field_ in fields:
                field = get_field(field_)
                step = 'phase_uvwave_{0:d}'.format(i)
                recipe.add('cab/casa_plotms', step,
                   {
                    "vis"           : msname,
                    "field"         : field,
                    "correlation"   : corr,
                    "timerange"     : '',
                    "antenna"       : '',
                    "xaxis"         : 'uvwave',
                    "xdatacolumn"   : config['phase_uvwave'].get('column'),
                    "yaxis"         : 'phase',
                    "ydatacolumn"   : config['phase_uvwave'].get('column'),
                    "avgtime"       : config['phase_uvwave'].get('avgtime'),
                    "avgchannel"    : config['phase_uvwave'].get('avgchannel'),
                    "coloraxis"     : 'baseline',
                    "iteraxis"      : 'corr',
                    "expformat"     : 'png',
                    "exprange"      : 'all',
                    "plotfile"      : '{0:s}/{1:s}-{2:s}-{3:s}-phaseuvwave.png'.format(get_dir_path(plot_path, pipeline), prefix, label, field_),
                    "overwrite"     : True,
                    "showgui"       : False,
                    "uvrange"       : uvrange,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Plot phase uv-wave for field {1:s} ms={2:s}'.format(step, field, msname))


        if pipeline.enable_task(config, 'amp_scan'):
            fields = config['amp_scan'].get('fields')
            for field_ in fields:
                field = get_field(field_)
                step = 'plot_ampscan_{0:d}'.format(i)
                recipe.add('cab/casa_plotms', step,
                   {
                    "vis"           : msname,
                    "field"         : field,
                    "correlation"   : corr,
                    "timerange"     : '',
                    "antenna"       : '',
                    "xaxis"         : 'scan',
                    "xdatacolumn"   : config['amp_scan'].get('column'),
                    "yaxis"         : 'amp',
                    "ydatacolumn"   : config['amp_scan'].get('column'),
                    "avgtime"       : config['amp_scan'].get('avgtime'),
                    "avgchannel"    : config['amp_scan'].get('avgchannel'),
                    "coloraxis"     : 'baseline',
                    "iteraxis"      : 'corr',
                    "expformat"     : 'png',
                    "exprange"      : 'all',
                    "plotfile"      : '{0:s}/{1:s}-{2:s}-{3:s}-ampscan.png'.format(get_dir_path(plot_path, pipeline), prefix, label, field_),
                    "overwrite"     : True,
                    "showgui"       : False,
                    "uvrange"       : uvrange,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='{0:s}:: Plot amp_v_scan for field {1:s} ms={2:s}'.format(step, field, msname))

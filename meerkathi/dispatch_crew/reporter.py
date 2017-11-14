import meerkathi
import os
import json
import copy
import re
import nbconvert
import nbformat
from nbconvert import HTMLExporter
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert.preprocessors import CellExecutionError

REPORT_TEMPLATE=os.path.join(meerkathi.scripts.__path__[0], "obs_report", "Observation Report.ipynb")

class reporter:
    def __init__(self, pipeline):
        """ Process and dump a static html report for each ms in the pipeline """
        self.__ms = copy.deepcopy(pipeline.msnames)
        self.__outputdir = pipeline.output
        with open(REPORT_TEMPLATE) as f:
            self.__rep_template = f.read()
        self.__report_dir = os.path.join(self.__outputdir, "reports")
        if not os.path.exists(self.__report_dir):
            os.mkdir(self.__report_dir)

    def generate_reports(self):
        """ generate an html report for every ms in the pipeline """
        report_names = [os.path.join(self.__report_dir, "%s.report.html" % ms) for ms in self.__ms]
        for ms, rep in zip(self.__ms, report_names):
            meerkathi.log.info("Creating a report for dataset id '%s'. "
                               "The report will be dumped here: '%s'." % (ms, rep))
            # grab a fresh template
            ms_rep = nbformat.reads(self.__rep_template, as_version=4)

            def __customize(s):
                s = re.sub(r'msname\s*=\s*\S*',
                           'msname = \'%s\'' % os.path.splitext(os.path.basename(ms))[0],
                           s)
                s = re.sub(r'outputdir\s*=\s*\S*',
                           'outputdir = \'%s\'' % os.path.abspath(self.__outputdir),
                           s)
                return s

            # modify template to add the output directory and ms name
            ms_rep.cells[0]['source'] = '\n'.join(map(__customize, ms_rep.cells[0]['source'].split('\n')))

            # roll
            ep = ExecutePreprocessor(timeout=None, kernel_name='python2')
            try:
                ep.preprocess(ms_rep, {'metadata': {'path': os.path.abspath(self.__outputdir)}})
            except CellExecutionError: # reporting error is non-fatal
                out = None
                msg = 'Error executing the notebook for "%s".\n\n' % ms
                msg += 'See notebook "%s" for the traceback.' % rep
                meerkathi.log.error(msg)
            finally:
                #export to static HTML
                html_exporter = HTMLExporter()
                #html_exporter.template_file = 'basic'
                (body, resources) = html_exporter.from_notebook_node(ms_rep)
                with open(rep, 'w+') as f:
                    f.write(body)
                    #nbformat.write(ms_rep, f)

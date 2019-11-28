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


LEAKAGE_TEMPLATE = os.path.join(os.path.dirname(
    __file__), "obs_report", "Polarization.ipynb")
SOLUTIONS_TEMPLATE = os.path.join(os.path.dirname(
    __file__), "obs_report", "Polcal solutions.ipynb")
REPORT_TEMPLATE = os.path.join(
    __path__[0], "obs_report", "Observation Report.ipynb")
SELFCAL_TEMPLATE = os.path.join(os.path.dirname(
    __file__), "obs_report", "SelfCal Diagnostics.ipynb")


class reporter:
    def __init__(self, pipeline):
        """ Process and dump a static html report for each ms in the pipeline """
        self.__ms = copy.deepcopy(
            [x for x in pipeline.msnames if isinstance(x, str)])
        self.__outputdir = os.path.abspath(pipeline.output)
        with open(REPORT_TEMPLATE) as f:
            self.__rep_template = f.read()
        self.__report_dir = os.path.join(self.__outputdir, "reports")
#        if not os.path.exists(self.__report_dir):
#            os.mkdir(self.__report_dir)

    def generate_calsolutions_report(self, output="output"):
        for ms in self.__ms:
            msbase = os.path.splitext(os.path.basename(ms))[0]
            rep = os.path.join(output, "reports", msbase +
                               "_calsolutions" + ".ipynb.html")
            # read template
            with open(SOLUTIONS_TEMPLATE) as f:
                rep_template = f.read()

            meerkathi.log.info("Creating a report of polarization solutions. "
                               "The report will be dumped here: '%s'." % (rep))

            # grab a fresh template
            sols_rep = nbformat.reads(rep_template, as_version=4)

            def __customize(s):
                s = re.sub(r'OUTPUT\s*=\s*\S*',
                           'OUTPUT = \'%s\'' % output,
                           s)
                s = re.sub(r'K0\s*=\s*\S*',
                           'K0 = \'%s\'' % os.path.join(
                               self.__outputdir, "meerkathi-%s-1gc1.K0" % msbase),
                           s)
                s = re.sub(r'G0\s*=\s*\S*',
                           'G0 = \'%s\'' % os.path.join(
                               self.__outputdir, "meerkathi-%s-1gc1.G0" % msbase),
                           s)
                s = re.sub(r'G1\s*=\s*\S*',
                           'G1 = \'%s\'' % os.path.join(
                               self.__outputdir, "meerkathi-%s-1gc1.G0" % msbase),
                           s)
                s = re.sub(r'B0\s*=\s*\S*',
                           'B0 = \'%s\'' % os.path.join(
                               self.__outputdir, "meerkathi-%s-1gc1.B0" % msbase),
                           s)
                s = re.sub(r'KX\s*=\s*\S*',
                           'KX = \'%s\'' % os.path.join(
                               self.__outputdir, "meerkathi-%s-crosshand_cal.KX" % msbase),
                           s)
                s = re.sub(r'Xref\s*=\s*\S*',
                           'Xref = \'%s\'' % os.path.join(
                               self.__outputdir, "meerkathi-%s-crosshand_cal.Xref" % msbase),
                           s)
                s = re.sub(r'Xfreq\s*=\s*\S*',
                           'Xfreq = \'%s\'' % os.path.join(
                               self.__outputdir, "meerkathi-%s-crosshand_cal.Xf" % msbase),
                           s)
                s = re.sub(r'Dref\s*=\s*\S*',
                           'Dref = \'%s\'' % os.path.join(
                               self.__outputdir, "meerkathi-%s-crosshand_cal.Dref" % msbase),
                           s)
                s = re.sub(r'Dfreq\s*=\s*\S*',
                           'Dfreq = \'%s\'' % os.path.join(
                               self.__outputdir, "meerkathi-%s-crosshand_cal.Df" % msbase),
                           s)
                return s

            # modify template to add the output directory
            sols_rep.cells[0]['source'] = '\n'.join(
                map(__customize, sols_rep.cells[0]['source'].split('\n')))

            # roll
            ep = ExecutePreprocessor(timeout=None, kernel_name='python3')
            try:
                ep.preprocess(
                    sols_rep, {'metadata': {'path': os.path.abspath(os.path.dirname(__file__))}})
            except CellExecutionError:  # reporting error is non-fatal
                out = None
                msg = 'Error executing the solution notebook.\n\n'
                msg += 'See notebook "%s" for the traceback.' % rep
                meerkathi.log.error(msg)
            finally:
                # export to static HTML
                html_exporter = HTMLExporter()
                #html_exporter.template_file = 'basic'
                (body, resources) = html_exporter.from_notebook_node(sols_rep)
                with open(str(rep), 'w+') as f:
                    f.write(body)

    def generate_leakage_report(self, ms, rep, field="PKS1934-638"):
        rep = os.path.join(self.__report_dir, rep)
        # read template
        with open(LEAKAGE_TEMPLATE) as f:
            rep_template = f.read()

        meerkathi.log.info("Creating a report for dataset id '%s'. "
                           "The report will be dumped here: '%s'." % (ms, rep))

        # grab a fresh template
        ms_rep = nbformat.reads(rep_template, as_version=4)

        def __customize(s):
            s = re.sub(r'MSNAME\s*=\s*\S*',
                       'MSNAME = \'%s\'' % ms,
                       s)
            s = re.sub(r'UNPOL_SOURCE\s*=\s*\S*',
                       'UNPOL_SOURCE = \'%s\'' % field,
                       s)
            return s

        # modify template to add the ms name
        ms_rep.cells[0]['source'] = '\n'.join(
            map(__customize, ms_rep.cells[0]['source'].split('\n')))

        # roll
        ep = ExecutePreprocessor(timeout=None, kernel_name='python3')
        try:
            ep.preprocess(
                ms_rep, {'metadata': {'path': os.path.abspath(os.path.dirname(__file__))}})
        except CellExecutionError:  # reporting error is non-fatal
            out = None
            msg = 'Error executing the notebook for "%s".\n\n' % ms
            msg += 'See notebook "%s" for the traceback.' % rep
            meerkathi.log.error(msg)
        finally:
            # export to static HTML
            html_exporter = HTMLExporter()
            #html_exporter.template_file = 'basic'
            (body, resources) = html_exporter.from_notebook_node(ms_rep)
            with open(rep, 'w+') as f:
                f.write(body)

    def generate_selfcal_dqa_report(self):
        # read template
        with open(SELFCAL_TEMPLATE) as f:
            rep_template = f.read()

        report_names = [os.path.join(self.__report_dir,
                                     "%s.selfcal-diagnostics.html" % os.path.basename(ms)) for ms in self.__ms]
        for msi, (ms, rep) in enumerate(zip(self.__ms, report_names)):
            meerkathi.log.info("Creating a report for dataset id '{}'. "
                               "The report will be dumped here: '{}'.".format(ms, rep))
            # grab a fresh template
            ms_rep = nbformat.reads(rep_template, as_version=4)

            def __customize(s):
                s = re.sub(r'msname\s*=\s*\S*',
                           'msname = \'{}\''.format(
                               os.path.splitext(os.path.basename(ms))[0]),
                           s)
                s = re.sub(r'outputdir\s*=\s*\S*',
                           'outputdir = \'{}\''.format(
                               os.path.abspath(self.__outputdir)),
                           s)
                s = re.sub(r'msindex\s*=\s*\S*',
                           'msindex = {}'.format(msi),
                           s)
                return s

            # modify template to add the output directory and ms name
            ms_rep.cells[0]['source'] = '\n'.join(
                map(__customize, ms_rep.cells[0]['source'].split('\n')))

            # roll
            ep = ExecutePreprocessor(timeout=None, kernel_name='python3')

            try:
                ep.preprocess(
                    ms_rep, {'metadata': {'path': os.path.abspath(os.path.dirname(__file__))}})
            except CellExecutionError:  # reporting error is non-fatal
                out = None
                msg = 'Error executing the notebook for "%s".\n\n' % ms
                msg += 'See notebook "%s" for the traceback.' % rep
                meerkathi.log.error(msg)
            finally:
                # export to static HTML
                html_exporter = HTMLExporter()
                #html_exporter.template_file = 'basic'
                (body, resources) = html_exporter.from_notebook_node(ms_rep)
                with open(rep, 'w+') as f:
                    f.write(body)

    def generate_reports(self):
        self.pipeline_overview()
        self.generate_selfcal_dqa_report()
        self.generate_calsolutions_report(
            output=os.path.abspath(self.__outputdir))

    def pipeline_overview(self):
        """ generate an html report for every ms in the pipeline """
        report_names = [os.path.join(self.__report_dir,
                                     "%s.report.html" % os.path.basename(ms)) for ms in self.__ms]
        for msi, (ms, rep) in enumerate(zip(self.__ms, report_names)):
            meerkathi.log.info("Creating a report for dataset id '{}'. "
                               "The report will be dumped here: '{}'.".format(ms, rep))
            # grab a fresh template
            ms_rep = nbformat.reads(self.__rep_template, as_version=4)

            def __customize(s):
                s = re.sub(r'msname\s*=\s*\S*',
                           'msname = \'{}\''.format(
                               os.path.splitext(os.path.basename(ms))[0]),
                           s)
                s = re.sub(r'outputdir\s*=\s*\S*',
                           'outputdir = \'{}\''.format(
                               os.path.abspath(self.__outputdir)),
                           s)
                s = re.sub(r'msindex\s*=\s*\S*',
                           'msindex = {}'.format(msi),
                           s)
                return s

            # modify template to add the output directory and ms name
            ms_rep.cells[0]['source'] = '\n'.join(
                map(__customize, ms_rep.cells[0]['source'].split('\n')))

            # roll
            ep = ExecutePreprocessor(timeout=None, kernel_name='python3')
            try:
                ep.preprocess(
                    ms_rep, {'metadata': {'path': os.path.abspath(self.__outputdir)}})
            except CellExecutionError:  # reporting error is non-fatal
                out = None
                msg = 'Error executing the notebook for "{}".\n\n'.format(ms)
                msg += 'See notebook "{}" for the traceback.'.format(rep)
                meerkathi.log.error(msg)
            finally:
                # export to static HTML
                html_exporter = HTMLExporter()
                #html_exporter.template_file = 'basic'
                (body, resources) = html_exporter.from_notebook_node(ms_rep)
                with open(rep, 'w+') as f:
                    f.write(body)
                    #nbformat.write(ms_rep, f)

# import stimela
import pickle
import sys
import os
import caracal.dispatch_crew.utils as utils
import caracal
import yaml
import stimela.dismissable as sdm
from caracal.workers.utils import manage_flagsets as manflags
from caracal.workers.utils import manage_fields as manfields
from caracal.workers.utils import manage_caltabs as manGtabs
import copy
import re
import json
import glob

import shutil
import numpy

NAME = "Polarization calibration"
LABEL = 'polcal'


def get_dir_path(string, pipeline):
    return string.split(pipeline.output)[1][1:]


# def worker
##################################################################################################
def worker(pipeline, recipe, config):
    wname = pipeline.CURRENT_WORKER
    flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
    flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
    label = config["label_cal"]

    # loop over all MSs for this label
    for i, msbase in enumerate(pipeline.msbasenames):
        msname = pipeline.form_msname(msbase, label_in)
        msinfo = pipeline.get_msinfo(msname)
        prefix = f"{pipeline.prefix_msbases[i]}-{label}"

        ######## set global param
        refant = pipeline.refant[i] or '0'
        scandur = scan_length(msinfo, leakage_cal)

        ######## set local param
        uvcut = config["uvrange"]
        pol_calib = config["pol_calib"]
        leakage_calib = config["leakage_calib"]
        avgstring = ',' + config["avg_bw"]  # solint input param
        plot = config["make_checking_plots"]
        if plot == True:
            ant = config["refant_for_plots"]
            plot_dir = pipeline.diagnostic_plots + "/polcal"
            if not os.path.exists(pipeline.diagnostic_plots + "/polcal"):
                os.mkdir(pipeline.diagnostic_plots + "/polcal")

        ######## check linear feed OK
        def lin_feed(msinfo):
            with open(msinfo, 'r') as f:
                info = yaml.safe_load(f)
            raise SystemExit(nfo['CORR']['CORR_TYPE'])
            if info['CORR']['CORR_TYPE'] == '["XX", "XY", "YX", "YY"]':
                return True
            else:
                return False

        if lin_feed(msinfo) is not True:
            raise RuntimeError("Cannot calibrate polarization!"
                               "Allowed strategies are for linear feed data but corr is " + info['CORR']['CORR_TYPE'])

        ######## -90 deg receptor angle rotation

        ######## define pol and unpol calibrators OK
        polarized_calibrators = {"3C138": {"standard": "manual",
                                           "fluxdensity": [8.4012],
                                           "spix": [-0.54890527955337987, -0.069418066176041668,
                                                    -0.0018858519926001926],
                                           "reffreq": "1.45GHz",
                                           "polindex": [0.075],
                                           "polangle": [-0.19199]},
                                 "3C286": {"standard": "manual",
                                           "fluxdensity": [14.918703],
                                           "spix": [-0.50593909976893958, -0.070580431627712076, 0.0067337240268301466],
                                           "reffreq": "1.45GHz",
                                           "polindex": [0.095],
                                           "polangle": [0.575959]},
                                 }
        polarized_calibrators["J1331+3030"] = polarized_calibrators["3C286"]
        polarized_calibrators["J0521+1638"] = polarized_calibrators["3C138"]

        unpolarized_calibrators = ["PKS1934-63", "J1939-6342", "J1938-6341", "PKS 1934-638", "PKS 1934-63",
                                   "PKS1934-638"]

        ######## prepare data (APPLY KGB AND SPLIT a NEW MSDIR) OK
        # but I WOULD LIKE TO COPY DATA COLUMN INTO RAW, CORRECTED INTO DATA
        # USE DATA TO LLOK FOR SOLUTION AND AFTER APPLYING POLCAL TABLES RESTORE RAW INTO DATA
        # THIS CAN BE DONE WITH PYRAP BUT IT IS NECESSARY TO DEFINE FUNCTIONS OUTSIDE CARACAL
        # recipe.add("cab/casa_applycal",
        #            "apply 0",
        #            {
        #                "vis": msname,
        #                "field": "",
        #                "callib": os.path.join(pipeline.output,'caltables/callibs/{}'.format(config['label_crosscal_in'])),
        #                "parang": False,
        #            },
        #            input=pipeline.input, output=pipeline.output,
        #            label="Apply crosscal")
        #
        # recipe.add("cab/casa_split",
        #            "split_data",
        #            {
        #                "vis": msname,
        #                "outputvis": newmsname,
        #                "datacolumn": "corrected",
        #                "field": "",
        #                "uvrange": uvcut,
        #                "correlation": "",
        #            },
        #            input=pipeline.input, output=pipeline.output, label="split_data")
        #
        # recipe.run()
        # recipe.jobs = []
        def ben_calib(msname, msinfo, recipe, config, pipeline, prefix, label):
            caltable = "%s_%s_%s" % (prefix, config['leakage_calib'], 'ben_cal')
            print("TBD")

        return

        ######## choose the strategy according to config parameters OK
        if leakage_calib in set(unpolarized_calibrators):
            if pol_calib in set(polarized_calibrators):
                ben_calib(msname, msinfo, recipe, config, pipeline, prefix, label)
            else:
                raise RuntimeError("Unknown pol_calib!"
                                   "Currently only these are known on caracal:"
                                   "{1:s}".format(get_field("pol_calib"),
                                                  ", ".join(list(polarized_calibrators.keys()))) + \
                                   "You can use one of these source to calibrate polarization"
                                   "or if none of them is available you can calibrate both leakage (leakage_calib) and polarization (pol_calib)"
                                   "with a source observed at several paralactic angles")

        elif leakage_cal == pol_calib:
            #            if utils.field_observation_length(msinfo, leakage_cal) >= 3:
            print("Hello!")
        #                floi_calib(msname,msinfo,recipe,config,pipeline,prefix,label) #it would be useful to check at the beginning of the task whether the parallactic angle is well covered (i.e. range of 60 deg?)
        # if plot == True:
        #     make_plots(msdir,leakage_cal,ant)
        # if pol_calib in set(polarized_calibrators):
        #     compare_with_model()
        # else:
        #   raise RuntimeError("Cannot calibrate polarization! Unsufficient number of scans for the leakage/pol calibrators.")
        else:
            raise RuntimeError("Cannot calibrate polarization! Allowed strategies are:"
                               "1. Calibrate leakage with a unpolarized source (i.e. {1:s}".format(
                get_field("pol_calib"), ", ".join(list(unpolarized_calibrators.keys()))) + \
                               "   and polarized angle with a know polarized source (i.e. {1:s}".format(
                                   get_field("pol_calib"), ", ".join(list(polarized_calibrators.keys()))) + \
                               "2. Calibrate both leakage and polarized angle with a (known or unknown) polarized source observed at different parallactic angles.")

            ######## apply cal TBD


########################################################################################################################################################################################
def scan_length(msinfo, field):
    with open(msinfo, 'r') as f:
        info = yaml.safe_load(f)

    names = info['FIELD']['NAME']
    ids = info['FIELD']['SOURCE_ID']

    def index(field):
        if isinstance(field, str):
            idx = names.index(field)
        elif isinstance(field, int):
            idx = ids.index(field)
        else:
            raise ValueError("Field cannot be a {0:s}".format(type(field)))
        return idx

    field = str(ids[index(field)])

    return float(info['TIME'][field].values()) / float(utils.field_observation_length(msinfo, field))

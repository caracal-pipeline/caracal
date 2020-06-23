#import stimela
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
########################################################################################################################################################################################
def worker(pipeline, recipe, config):
    wname = pipeline.CURRENT_WORKER
    flags_before_worker = '{0:s}_{1:s}_before'.format(pipeline.prefix, wname)
    flags_after_worker = '{0:s}_{1:s}_after'.format(pipeline.prefix, wname)
    label = config["label_cal"]
    
    if pipeline.virtconcat:
        msnames = [pipeline.vmsname]
        nobs = 1
        prefixes = [pipeline.prefix]
    else:
        msnames = pipeline.msnames
        prefixes = pipeline.prefixes
        nobs = pipeline.nobs

    for i in range(nobs):

        ######## define msname
        if config["label_in"]:
            msname = '{0:s}_{1:s}.ms'.format(msnames[i][:-3],config["label_in"])
        else: msname = msnames[i]

        ######## set global param
        refant = pipeline.refant[i] or '0'
        prefix = prefixes[i]
        msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.obsinfo, msname[:-3])
        prefix = '{0:s}-{1:s}'.format(prefix, label)
        scandur = scan_length(msinfo, leakage_cal)

        ######## set local param
        uvcut = config["uvrange"]
        pol_calib = config["pol_calib"]
        leakage_calib = config["leakage_calib"]
        avgstring = ','+config["avg_bw"] #solint input param
        plot = config["make_checking_plots"]
        if plot = True:
            ant = config["refant_for_plots"]
            plot_dir = pipeline.diagnostic_plots+"/polcal"
            if not os.path.exists(pipeline.diagnostic_plots+"/polcal"):
                    os.mkdir(pipeline.diagnostic_plots+"/polcal")
            
        ######## check linear feed OK
        def lin_feed(msinfo):
            with open(msinfo, 'r') as f:
                info = yaml.safe_load(f)
        
            if info['CORR']['CORR_TYPE'] == '["XX", "XY", "YX", "YY"]':
                return True
            else:
                return False
            
        if lin_feed(msinfo) is not True:
            raise RuntimeError("Cannot calibrate polarization! Allowed strategies are for linear feed data but corr is "+info['CORR']['CORR_TYPE'])
        
        ######## -90 deg receptor angle rotation
        
        
        ######## define pol and unpol calibrators OK
        polarized_calibrators = {"3C138": {"standard": "manual",
                                           "fluxdensity": [8.4012],
                                           "spix": [-0.54890527955337987, -0.069418066176041668, -0.0018858519926001926],
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
        
        unpolarized_calibrators = ["PKS1934-63", "J1939-6342", "J1938-6341", "PKS 1934-638", "PKS 1934-63", "PKS1934-638"]


        ######## prepare data (APPLY KGB AND SPLIT a NEW MSDIR) OK
        # but I WOULD LIKE TO COPY DATA COLUMN INTO RAW, CORRECTED INTO DATA
        # USE DATA TO LLOK FOR SOLUTION AND AFTER APPLYING POLCAL TABLES RESTORE RAW INTO DATA
        # THIS CAN BE DONE WITH PYRAP BUT IT IS NECESSARY TO DEFINE FUNCTIONS OUTSIDE CARACAL
        recipe.add("cab/casa_applycal",
                   "apply 0",
                   {
                       "vis": msname,
                       "field": "",
                       "callib": os.path.join(pipeline.output,'caltables/callibs/{}'.format(config['label_crosscal_in']))
                       "parang": False,
                   },
                   input=pipeline.input, output=pipeline.output,
                   label="Apply crosscal")
        
        recipe.add("cab/casa_split",
                   "split_data",
                   {
                       "vis": msname,
                       "outputvis": newmsname,
                       "datacolumn": "corrected",
                       "field": "",
                       "uvrange": uvcut,
                       "correlation": "",
                   },
                   input=pipeline.input, output=pipeline.output, label="split_data")

        recipe.run()
        recipe.jobs = []
        
        ######## choose the strategy according to config parameters OK
        if leakage_calib in set(unpolarized_calibrators):
            if pol_calib in set(polarized_calibrators):
                caltable = "%s_%s" % (prefix, leakage_calib)
                ben_calib(newmsname)
            else:
                raise RuntimeError("Unknown pol_calib!" 
                                   "Currently only these are known on caracal:"
                                   "{1:s}".format(get_field("pol_calib"), ", ".join(list(polarized_calibrators.keys())))
                                   "You can use one of these source to calibrate polarization"
                                   "or if none of them is available you can calibrate both leakage (leakage_calib) and polarization (pol_calib)"
                                   "with a source observed at several paralactic angles")
    
        elif leakage_cal == pol_calib:
            if utils.field_observation_length(msinfo, leakage_cal) >= 3:
                caltable = "%s_%s" % (prefix, leakage_calib)
                floi_calib(newmsname,leakage_cal,caltable) #it would be useful to check at the beginning of the task whether the parallactic angle is well covered (i.e. range of 60 deg?)
                if plot = True:
                    make_plots(msdir,leakage_cal,ant)
                if pol_calib in set(polarized_calibrators):
                    compare_with_model()
            else:
                raise RuntimeError("Cannot calibrate polarization! Unsufficient number of scans for the leakage/pol calibrators.")
        else:
            raise RuntimeError("Cannot calibrate polarization! Allowed strategies are:"
                               "1. Calibrate leakage with a unpolarized source (i.e. {1:s}".format(get_field("pol_calib"), ", ".join(list(unpolarized_calibrators.keys())))""
                               "   and polarized angle with a know polarized source (i.e. {1:s}".format(get_field("pol_calib"), ", ".join(list(polarized_calibrators.keys())))""
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

    return float(info['TIME'][field].values())/float(utils.field_observation_length(msinfo, field))



########################################################################################################################################################################################
def ben_cal(msname):
    print("TBD")
    return

########################################################################################################################################################################################
def compare_with_model:
    print("TBD")
    return

########################################################################################################################################################################################
def floi_calib(msname, field, caltable):
    #G1
    recipe.add("cab/casa_gaincal",
	       "first gaincal",
	       {
                   "vis": msname,
                   "field": field,
		   "caltable": caltable+'.Gpol1:output',
		   "smodel": ['1','0','0','0'],
        	   "refantmode": 'strict',
		   "refant": ref,
		   "gaintype": 'G',
		   "calmode": 'ap',
		   "parang": False,
		   "solint": 'int',
	       },
               input=pipeline.input, output=pipeline.output,
               label="Gain xcal 1")
    
    #QU
    recipe.add("cab/casa_polfromgain",
               "QU from gain",
               {
                   "vis": msname,
		   "tablein": caltable+'.Gpol1:output',
                   "caltable": caltable+'.Gpol1a:output',
                   "save_result": caltable+'S1_from_QUfit:output',
               },
               input=pipeline.input, output=pipeline.output,
               label="QU from gain")

    ##################################################
    # We search for the scan where the polarization signal is minimum in XX and YY
    # (i.e., maximum in XY and YX):

    tb.open(caltable+'.Gpol1:output')
    scans = tb.getcol('SCAN_NUMBER')
    gains = np.squeeze(tb.getcol('CPARAM'))
    tb.close()
    scanlist = np.array(list(set(scans)))
    ratios = np.zeros(len(scanlist))
    for si, s in enumerate(scanlist):
      filt = scans == s
      ratio = np.sqrt(np.average(np.power(np.abs(gains[0,filt])/np.abs(gains[1,filt])-1.0,2.)))
      ratios[si] = ratio


    bestscidx = np.argmin(ratios)
    bestscan = scanlist[bestscidx]
    #print('Scan with highest expected X-Y signal: '+str(bestscan))
    #####################################################
    recipe.run()
    recipe.jobs = []
    
    #Kcross
    recipe.add("cab/casa_gaincal",
               "Kcross delay",
               {
                   "vis": msname,
                   "caltable": caltable+'.Kcrs:output',
                   "selectdata": True,
		   "field": field,
                   "scan": str(bestscan),
                   "gaintype": 'KCROSS',
                   "solint": 'inf'+avgstring,
		   "refantmode": 'strict',
                   "refant": ref,
		   "smodel": ['1','0','1','0'],
                   "gaintable": [caltable+'.Gpol1:output'],
        	   "interp": ['linear'],
               },
               input=pipeline.input, output=pipeline.output,
               label="Kcross delay")
    
    
    recipe.run()
    recipe.jobs = []
    
    if os.path.isfile(caltable+'S1_from_QUfit:output'):
        with open(caltable+'S1_from_QUfit:output', 'rb') as stdr:
            S1 = pickle.load(stdr, encoding='latin1')
            
        S1=S1[xcal]['SpwAve']
        print("First [I,Q,U,V] fitted model (with I=1 and Q, U fractional): %s"%S1)
    
    else:
        raise RuntimeError("Cannot find "caltable+'S1_from_QUfit:output')
        
    #QU abs delay
    recipe.add("cab/casa_polcal",
	       "Abs phase and QU fit",
               {
		   "vis": msname,
                   "caltable": caltable+'.Xfparang:output',
          	   "field": field,
          	   "spw": '',
          	   "poltype": 'Xfparang+QU',
          	   "solint": 'inf'+avgstring,
          	   "combine": 'scan,obs',
          	   "preavg": scandur,
	 	   "smodel": S1,
          	   "gaintable": [caltable+'.Gpol1:output',caltable+'.Kcrs:output'],
          	   "interp": ['linear','nearest'],
                   "save_result": caltable+'.S2_from_polcal:output',
               },
               input=pipeline.input, output=pipeline.output,
               label="Abs phase and QU fit")
    
    recipe.run()
    recipe.jobs = []
    
    if os.path.isfile(caltable+'.S2_from_polcal:output'):
        with open(caltable+'.S2_from_polcal:output', 'rb') as stdr:
            S2 = pickle.load(stdr, encoding='latin1')
            
        S2=S2[xcal]['SpwAve'].tolist()
        print("Second [I,Q,U,V] fitted model (with I=1 and Q, U fractional): %s"%S2)
        
    else:
        raise RuntimeError("Cannot find "caltable+'.S2_from_polcal:output')
        
    recipe.add("cab/casa_gaincal",
               "second gaincal",
               {
                   "vis": msname,
                   "field": field,
                   "caltable": caltable+'.Gpol2:output',
                   "smodel": S2,
                   "refantmode": 'strict',
                   "refant": ref,
                   "gaintype": 'G',
                   "calmode": 'ap',
                   "parang": True,
                   "solint": 'int',
               },
               input=pipeline.input, output=pipeline.output,
               label="Gain polcal 2")
    
    #LEAKAGE
    recipe.add("cab/casa_polcal",
               "leakage terms",
               {
                   "vis": msname,
                   "caltable": caltable+'.Df0gen:output',
                   "field": field,
                   "spw": '',
                   "solint": 'inf'+avgstring,
       		   "combine": 'obs,scan',
	           "preavg": scandur,
	           "poltype": 'Dflls',
       		   "refant": '', #solve absolute D-term
       		   "smodel": S2,
       		   "gaintable": [caltable+'.Gpol2:output', caltable+'.Kcrs:output', caltable+'.Xfparang:output'],
       		   "gainfield": ['', '', ''],
       		   "interp": ['linear','nearest','nearest'],
               },
               input=pipeline.input, output=pipeline.output,
               label="Leakage terms")
    
    
    # solve for global normalized gain amp (to get X/Y ratios) on pol calibrator (TO APPLY ON TARGET)
    # amp-only and normalized, so only X/Y amp ratios matter
    recipe.add("cab/casa_gaincal",
               "normalize gain ampl for target",
               {
                   "vis": msname,
        	   "caltable": caltable+'.Gxyamp:output',
        	   "field": field,
		   "solint": 'inf',
        	   "combine": 'scan,obs',
        	   "refant": ref,
		   "refantmode": 'strict',
        	   "gaintype": 'G',
        	   "smodel": S2,
        	   "calmode": 'a',
        	   "gaintable": [caltable+'.Kcrs:output', caltable+'.Xfparang:output', caltable+'.Df0gen:output'],
        	   "gainfield": ['', '', ''],
        	   "interp": ['nearest','nearest','nearest'],
        	   "solnorm": True,
        	   "parang": True,
               },
               input=pipeline.input, output=pipeline.output,
               label="Target norm")

def make_plots(msdir,field,ant):
    recipe.add("cab/casa_plotms",
                "plot_firstGpol",
                {
                    "vis": caltable+'.Gpol1:output',
                    "field": '',
                    "xaxis": 'scan',
                    "yaxis": 'amp',
                    "correlation": '/',
                    "coloraxis": 'antenna1',
                    "plotfile": plotdir+'Gpol1.png:output',
                    "overwrite": True,
                },
                input=pipeline.input, output=pipeline.output,
                label="plot Gain xcal 1")

    recipe.add("cab/casa_plotms",
                "plot_beforeKcrs",
                {
                    "vis": msname,
                    "field": field,
                    "xaxis": 'freq',
                    "yaxis": 'phase',
                    "ydatacolumn":'corrected',
                    "avgtime": '1e3',
                    "spw": '',
                    "antenna": ant,
                    "correlation": 'XY,YX',
                    "coloraxis": 'corr',
                    "iteraxis": 'baseline',
                    "overwrite": True,
                    "plotrange": [0,0,-180,180],
                    "plotfile": plot_dir+'beforeKcross.png:output',
                },
                input=pipeline.input, output=pipeline.output,
                label="plot before Kcrs")

    recipe.add("cab/casa_applycal",
                "apply 1",
                {
                    "vis": msname,
                    "field": field,
                    "calwt": True,
                    "gaintable": [caltable+'.Gpol1:output',caltable+'.Kcrs:output'],
                    "interp": ['linear','nearest'],
                    "parang": False,
                },
                input=pipeline.input, output=pipeline.output,
                label="Apply Gpol1 Kcrs")

    recipe.add("cab/casa_plotms",
                "plot_afterKcrs",
                {
                    "vis": msname,
                    "field": field,
                    "xaxis": 'freq',
                    "yaxis": 'phase',
                    "ydatacolumn":'corrected',
                    "avgtime": '1e3',
                    "spw": '',
                    "antenna": ant,
                    "correlation": 'XY,YX',
                    "coloraxis": 'corr',
                    "iteraxis": 'baseline',
                    "overwrite": True,
                    "plotrange": [0,0,-180,180],
                    "plotfile": plot_dir+'afterKcross.png:output',
                },
                input=pipeline.input, output=pipeline.output,
                label="plot after Kcrs")

    # Cross-hand phase: PHASE vs ch and determine channel averaged pol
    recipe.add("cab/casa_plotms",
                "plot_beforeX",
                {
                    "vis": msname,
                    "field": field,
                    "xdatacolumn":'corrected',
                    "ydatacolumn":'corrected',
                    "xaxis": 'real',
                    "yaxis": 'imag',
                    "avgtime": '1e3',
                    "correlation": 'XY,YX',
                    "spw": '',
                    "coloraxis": 'corr',
                    "avgchannel": '10',
                    "avgbaseline": True,
                    "plotfile": plot_dir+'imag_vs_real_beforeXfparang.png:output',
                    "plotrange": [-0.06,0.06,-0.06,0.06],
                },
                input=pipeline.input, output=pipeline.output,
                label="plot before Xf")
    recipe.add("cab/casa_plotms",
                "plot_Xf",
                {
                    "vis": caltable+'.Xfparang:output',
                    "field": '',
                    "xaxis": 'freq',
                    "yaxis": 'phase',
                    "antenna": '0',
                    "coloraxis": 'corr',
                    "gridrows": 2,
                    "rowindex": 0,
                    "clearplots": True,
                    "plotfile": plot_dir+'cal.Xfparang.png:output',
                    "overwrite": True,
                },
                input=pipeline.input, output=pipeline.output,
                label="plot Xf")

    recipe.add("cab/casa_applycal",
                "apply 2",
                {
                    "vis": msname,
                    "field": field,
                    "calwt": True,
                    "gaintable": [caltable+'.Gpol1:output',caltable+'.Kcrs:output',caltable+'.Xfparang:output'],
                    "interp": ['linear','nearest','nearest'],
                    "parang": False,
                },
                input=pipeline.input, output=pipeline.output,
                label="Apply Gpol1 Kcrs Xf")

    recipe.add("cab/casa_plotms",
                "plot_afterX",
                {
                    "vis": msname,
                    "field": field,
                    "xdatacolumn":'corrected',
                    "ydatacolumn":'corrected',
                    "xaxis": 'real',
                    "yaxis": 'imag',
                    "avgtime": '1e3',
                    "correlation": 'XY,YX',
                    "spw": '',
                    "coloraxis": 'corr',
                    "avgchannel": '10',
                    "avgbaseline": True,
                    "plotfile": plot_dir+'imag_vs_real_afterXfparang.png:output',
                    "plotrange": [-0.06,0.06,-0.06,0.06],
                },
                input=pipeline.input, output=pipeline.output,
                label="plot after Xf")

    recipe.add("cab/casa_plotms",
                "plot_Gpol2",
                {
                    "vis": caltable+'.Gpol2:output',
                    "field": '',
                    "xaxis": 'scan',
                    "yaxis": 'amp',
                    "correlation": '/',
                    "coloraxis": 'antenna1',
                    "plotfile": plot_dir+'Gpol2.png:output',
                    "overwrite": True,
                },
                input=pipeline.input, output=pipeline.output,
                label="plot Gain xcal 2")
    return True

recipe.run()
recipe.jobs = []





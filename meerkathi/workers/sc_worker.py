import os
import sys
import yaml
import meerkathi
import stimela.dismissable as sdm
from meerkathi.dispatch_crew import utils
from astropy.io import ascii

NAME = 'Self calibration loop'

CUBICAL_MT = {
    "Gain2x2"      : 'complex-2x2', 
    "GainDiag"      : 'complex-2x2',  #TODO:: Change this. Ask cubical to support this mode
    "GainDiagPhase": 'phase-diag',
}


def worker(pipeline, recipe, config):
    npix = config['img_npix']
    trim = config['img_trim']
    spwid = config.get('spwid', 0)
    cleanborder = 0
    cell = config['img_cell']
    mgain = config['img_mgain']
    niter = config['img_niter']
    auto_thresh = config['img_auto_threshold']
    weight_img = config.get('img_weight', 'briggs')
    robust_img = config.get('img_robust', '0.5')
    nchans = config['img_nchans']
    pol = config.get('img_pol', 'I')
    joinchannels = config['img_joinchannels']
    fit_spectral_pol = config['img_fit_spectral_pol']
    gsols = config.get('cal_Gsols', [1,0])
    bsols = config.get('cal_Bsols', [0,1]) 
    taper = config.get('img_uvtaper', None)
    label = config['label']
    bjones = config.get('cal_Bjones', False)
    time_chunk = config.get('cal_time_chunk', 128)
    ncpu = config.get('ncpu', 9)
    mfsprefix = ["",'-MFS'][int(nchans>1)]

    pipeline.set_cal_msnames(label)
    mslist = pipeline.cal_msnames
    prefix = pipeline.prefix

    # Define image() extract_sources() calibrate()
    # functions for convience

    def image(num):

        key = 'image_{}'.format(num)
        step = 'image_{}'.format(num)
        minuvw = config[key].get('minuvw_m', 0)
        weight = config[key].get('weight', weight_img)
        robust = config[key].get('robust', robust_img)    
        weights = weight +' '+ str(robust)

        image_opts = {                   
                  "msname"    : mslist,
                  "column"    : config[key].get('column', 'DATA'),
                  "weight"    : weights,
                  "npix"      : config[key].get('npix', npix),
                  "trim"      : config[key].get('trim', trim),
                  "scale"     : config[key].get('cell', cell),
                  "prefix"    : '{0:s}_{1:d}'.format(prefix, num),
                  "niter"     : config[key].get('niter', niter),
                  "mgain"     : config[key].get('mgain', mgain),
                  "pol"       : config[key].get('pol', pol),
                  "taper-gaussian" : sdm.dismissable(config[key].get('uvtaper', taper)),
                  "channelsout"     : nchans,
                  "joinchannels"    : config[key].get('joinchannels', joinchannels),
                  "fit-spectral-pol": config[key].get('fit_spectral_pol', fit_spectral_pol),
                  "auto-threshold": config[key].get('auto_threshold', auto_thresh),
                  "local-rms" : config[key].get('local_rms', False),
                  "dft-prediction": config[key].get('dd_modpred',False),
                  "no-update-model-required": config[key].get('no_update_model', False),
                  "fit-spectral-pol": config[key].get('fit_spectral_pol', None),
                  "minuvw-m": minuvw,
              }
        if config[key].get('mask_from_sky', False):
            fitmask = config[key].get('fits_mask', None)
            fitmask_address = 'masking/'+str(fitmask)
            image_opts.update( {"fitsmask" : fitmask_address+':output'})
        
        multiscale = config[key].get('multi_scale', False)
        if multiscale:
            image_opts.update({"multiscale" : multiscale})
            image_opts.update({"multiscale-scales" : sdm.dismissable(config[key].get('multi_scale_scales', None))})

        recipe.add('cab/wsclean', step,
        image_opts,
        input=pipeline.input,
        output=pipeline.output,
        label='{:s}:: Make image after first round of calibration'.format(step))



    def calibrate_meqtrees(num):
        
        key = 'image_{}'.format(num)
        minuvw = config[key].get('minuvw_m', 0)
        key = 'calibrate_{0:d}'.format(num)
        vismodel = config[key].get('add_vis_model', False)
        calmodel = '{0:s}_{1:d}-nullmodel.txt'
        

        with open(os.path.join(pipeline.input, calmodel), 'w') as stdw:
            stdw.write('#format: ra_d dec_d i\n')
            stdw.write('0.0 -30.0 1e-99')

        for i, msname in enumerate(mslist):
          print msname
          gsols_ = config[key].get('Gsols', gsols)
          bsols_ = config[key].get('Bsols', bsols)

          step = 'calibrate_{0:d}'.format(num)
          cal_opts = {
               "skymodel"             : calmodel,
               "add-vis-model"        : vismodel,
               "msname"               : msname,
               "threads"              : ncpu,
               "column"               : "DATA",
               "model-column"         : 'MODEL_DATA',
               "output-data"          : config[key].get('output_data', 'CORR_RES'),
               "output-column"        : "CORRECTED_DATA",
               "prefix"               : '{0:d}_meqtrees'.format(num),
               "label"                : 'cal{0:d}'.format(num),
               "read-flags-from-ms"   : True,
               "read-flagsets"        : "-stefcal",
               "write-flagset"        : "stefcal",
               "write-flagset-policy" : "replace",
               "Gjones"               : True,
               "Gjones-solution-intervals" : gsols_,
               "Gjones-matrix-type"   : config[key].get('gain_matrix_type', 'GainDiag'),
               "Gjones-ampl-clipping"      : True,
               "Gjones-ampl-clipping-low"  : config.get('cal_gain_amplitude_clip_low', 0.5),
               "Gjones-ampl-clipping-high" : config.get('cal_gain_amplitude_clip_high', 1.5),
               "Bjones"                    : config[key].get('Bjones', False),
               "Bjones-ampl-clipping"      : True,
               "Bjones-solution-intervals" : bsols_,
               "Bjones-ampl-clipping"      : config[key].get('Bjones', bjones),
               "Bjones-ampl-clipping-low"  : config.get('cal_gain_amplitude_clip_low', 0.5),
               "Bjones-ampl-clipping-high" : config.get('cal_gain_amplitude_clip_high', 1.5),
               "make-plots"           : True,
               "tile-size"            : time_chunk,
             }

          if int(minuvw) != 0:
            line = '[tmp_conf_meqtrees.conf.tdl]\ncalibrate-ifrs=>{0:d}'.format(minuvw)
            meq_conf_file = pipeline.output+'/masking/meq_conf.tdl'
            f = open(meq_conf_file,'w')
            f.write(line)
            f.close()
            meq_conf_file = 'masking/'+'meq_conf.tdl'
            cal_opts.update({"tdlconf" : meq_conf_file+':output'})

          recipe.add('cab/calibrator', step,
          cal_opts,
          input=pipeline.input,
          output=pipeline.output,
          label="{0:s}:: Calibrate step {1:d} ms={2:s}".format(step, num, msname))

    # selfcal loop
    if pipeline.enable_task(config, 'image_1'):
        image(1)

    # SOFIA makes a mask

    if pipeline.enable_task(config, 'image_11'):
        image(11)

    if pipeline.enable_task(config, 'calibrate_1'):
        calibrate_meqtrees(1)

    if pipeline.enable_task(config, 'image_2'):
        image(2)

    if pipeline.enable_task(config, 'calibrate_2'):
        calibrate_meqtrees(2)

    if pipeline.enable_task(config, 'image_3'):
        image(3)

    if pipeline.enable_task(config, 'calibrate_3'):
        calibrate_meqtrees(3)

    if pipeline.enable_task(config, 'image_4'):
        image(4)

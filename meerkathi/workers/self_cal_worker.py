import os
import sys
import yaml
import json
import meerkathi
import stimela.dismissable as sdm
from meerkathi.dispatch_crew import utils

NAME = 'Self calibration loop'

CUBICAL_OUT = {
    "CORR_DATA"    : 'sc',
    "CORR_RES"     : 'sr',
}

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
    auto_mask = config['img_auto_mask']
    robust = config['img_robust']
    nchans = config['img_nchans']
    pol = config.get('img_pol', 'I')
    thresh_pix = config['sf_thresh_pix']
    thresh_isl = config['sf_thresh_isl']
    column = config['img_column']
    joinchannels = config['img_joinchannels']
    fit_spectral_pol = config['img_fit_spectral_pol']
    gsols = config.get('cal_Gsols', [])
    bsols = config.get('cal_Bsols', [])
    taper = config.get('img_uvtaper', None)
    label = config['label']
    bjones = config.get('cal_Bjones', False)
    time_chunk = config.get('cal_time_chunk', 128)
    ncpu = config.get('ncpu', 9)
    mfsprefix = ["", '-MFS'][int(nchans>1)]
    cal_niter = config.get('cal_niter', 1)
    label_hires = config.get('hires_label', 'hires')
    pipeline.set_cal_msnames(label)
    pipeline.set_hires_msnames(label_hires)
    mslist = pipeline.cal_msnames
    hires_mslist = pipeline.hires_msnames
    prefix = pipeline.prefix

    # Define image() extract_sources() calibrate()
    # functions for convience

    def image(num):
        key = 'image'
        mask = False
        if config[key].get('peak_based_mask_on_dirty', False):
            mask = True
            step = 'image_{}_dirty'.format(num)
            recipe.add('cab/wsclean', step,
                  {
                      "msname"    : mslist,
                      "column"    : config[key].get('column', column)[num-1 if len(config[key].get('column')) >= num else -1],
                      "weight"    : 'briggs {}'.format(config.get('robust', robust)),
                      "npix"      : config[key].get('npix', npix),
                      "trim"      : config[key].get('trim', trim),
                      "scale"     : config[key].get('cell', cell),
                      "pol"       : config[key].get('pol', pol),
                      "channelsout"   : nchans,
                      "taper-gaussian" : sdm.dismissable(config[key].get('uvtaper', taper)),
                      "prefix"    : '{0:s}_{1:d}'.format(prefix, num),
                  },
            input=pipeline.input,
            output=pipeline.output,
            label='{:s}:: Make dirty image to create clean mask'.format(step))

            step = 'mask_dirty_{}'.format(num)
            recipe.add('cab/cleanmask', step,
               {
                 "image"           :  '{0:s}_{1:d}{2:s}-image.fits:output'.format(prefix, num, mfsprefix),
                 "output"          :  '{0:s}_{1:d}-mask.fits'.format(prefix, num),
                 "dilate"          :  False,
                 "peak-fraction"   :  0.5,
                 "no-negative"     :  True,
                 "boxes"           :  1,
                 "log-level"       :  'DEBUG',
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Make mask based on peak of dirty image'.format(step))

        elif config[key].get('mask', False):
            mask = True
            sigma = config[key].get('mask_sigma', None)
            pf = config[key].get('mask_peak_fraction', None)
            step = 'mask_{}'.format(num)
            recipe.add('cab/cleanmask', step,
               {
                 "image"           :  '{0:s}_{1:d}{2:s}-image.fits:output'.format(prefix, num-1, mfsprefix),
                 "output"          :  '{0:s}_{1:d}-mask.fits'.format(prefix, num),
                 "dilate"          :  False,
                 "peak-fraction"   :  sdm.dismissable(pf),
                 "sigma"           :  sdm.dismissable(sigma),
                 "no-negative"     :  True,
                 "boxes"           :  1,
                 "log-level"       :  'DEBUG',
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Make mask based on peak of dirty image'.format(step))

        step = 'image_{}'.format(num)
        image_opts = {
                  "msname"    : mslist,
                  "column"    : config[key].get('column', column)[num-1 if len(config[key].get('column')) >= num else -1],
                  "weight"    : 'briggs {}'.format(config[key].get('robust', robust)),
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
                  "auto-threshold": config[key].get('auto_threshold',[auto_thresh])[num-1 if len(config[key].get('auto_threshold', [auto_thresh])) >= num else -1],
                  "multiscale" : config[key].get('multi_scale', False),
                  "multiscale-scales" : sdm.dismissable(config[key].get('multi_scale_scales', None)),
              }
        if config[key].get('mask_from_sky', False):
            fitmask = config[key].get('fits_mask', None)
            fitmask_address = 'masking/'+str(fitmask)
            image_opts.update( {"fitsmask" : fitmask_address+':output'})
        elif mask:
            image_opts.update( {"fitsmask" : '{0:s}_{1:d}-mask.fits:output'.format(prefix, num)} )
        else:
            image_opts.update({"auto-mask" : config[key].get('auto_mask',[auto_mask])[num-1 if len(config[key].get('auto_mask', [auto_mask])) >= num else -1]})

        recipe.add('cab/wsclean', step,
        image_opts,
        input=pipeline.input,
        output=pipeline.output,
        label='{:s}:: Make image after first round of calibration'.format(step))

    def make_cube(num, imtype='model'):
        im = '{0:s}_{1}-cube.fits:output'.format(prefix, num)
        step = 'makecube_{}'.format(num)
        images = ['{0:s}_{1}-{2:04d}-{3:s}.fits:output'.format(prefix, num, i, imtype) for i in range(nchans)]
        recipe.add('cab/fitstool', step,
            {
                "image"     : images,
                "output"    : im,
                "stack"     : True,
                "fits-axis" : 'FREQ',
            },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Make convolved model'.format(step))

        return im

    def extract_sources(num):
        key = 'extract_sources'
        if config[key].get('detection_image', False):
            step = 'detection_image_{0:d}'.format(num)
            detection_image = prefix + '-detection_image_{0:d}.fits:output'.format(num)
            recipe.add('cab/fitstool', step,
                {
                    "image"    : [prefix+'_{0:d}{2:s}-{1:s}.fits:output'.format(num, im, mfsprefix) for im in ('image','residual')],
                    "output"   : detection_image,
                    "diff"     : True,
                    "force"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Make convolved model'.format(step))
        else:
            detection_image = None

        spi_do = config[key].get('spi', False)
        if spi_do:
            im = make_cube(num, 'image')
        else:
            im = '{0:s}_{1:d}{2:s}-image.fits:output'.format(prefix, num, mfsprefix)

        step = 'extract_{0:d}'.format(num)
        calmodel = '{0:s}_{1:d}-pybdsm'.format(prefix, num)
        if detection_image:
            blank_limit = 1e-9
        else:
            blank_limit = None
        if len(config[key].get('thresh_pix', thresh_pix)) >= num:
            recipe.add('cab/pybdsm', step,
                {
                    "image"         : im,
                    "thresh_pix"    : config[key].get('thresh_pix', thresh_pix)[num-1 if len(config[key].get('thresh_pix')) >= num else -1],
                    "thresh_isl"    : config[key].get('thresh_isl', thresh_isl)[num-1 if len(config[key].get('thresh_isl')) >= num else -1],
                    "outfile"       : '{:s}.fits:output'.format(calmodel),
                    "blank_limit"   : sdm.dismissable(blank_limit),
                    "adaptive_rms_box" : True,
                    "port2tigger"   : True,
                    "multi_chan_beam": spi_do,
                    "spectralindex_do": spi_do,
                    "detection_image": sdm.dismissable(detection_image),
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Extract sources'.format(step))

    def predict_from_fits(num, model, index):
        if isinstance(model, str) and len(model.split('+'))==2:
            combine = True
            mm = model.split('+')
            # Combine FITS models if more than one is given
            step = 'combine_models_' + '_'.join(map(str, mm))
            calmodel = '{0:s}_{1:d}-FITS-combined.fits:output'.format(prefix, num)
            cubes = [ make_cube(n, 'model') for n in mm]
            recipe.add('cab/fitstool', step,
                {
                    "image"    : cubes,
                    "output"   : calmodel,
                    "sum"      : True,
                    "force"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Add clean components'.format(step))
        else:
            calmodel = make_cube(num)

        step = 'predict_fromfits_{}'.format(num)
        recipe.add('cab/lwimager', 'predict', {
                "msname"        : mslist[index],
                "simulate_fits" : calmodel,
                "column"        : 'MODEL_DATA',
                "img_nchan"     : nchans,
                "img_chanstep"  : 1,
                "nchan"         : pipeline.nchans[index][spwid],
                "cellsize"      : cell,
                "chanstep"      : 1,
        },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Predict from FITS ms={1:s}'.format(step, mslist[index]))


    def combine_models(models, num, enable=True):
        model_names = ['{0:s}_{1:s}-pybdsm.lsm.html:output'.format(
                       prefix, m) for m in models]
        model_names_fits = ['{0:s}/{1:s}_{2:s}-pybdsm.fits'.format(
                            pipeline.output, prefix, m) for m in models]
        calmodel = '{0:s}_{1:d}-pybdsm-combined.lsm.html:output'.format(prefix, num)

        if enable:
            step = 'combine_models_' + '_'.join(map(str, models))
            recipe.add('cab/tigger_convert', step,
                {
                    "input-skymodel"    : model_names[0],
                    "append"    : model_names[1],
                    "output-skymodel"   : calmodel,
                    "rename"  : True,
                    "force"   : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Combined models'.format(step))

        return calmodel, model_names_fits


    def calibrate_meqtrees(num):
        key = 'calibrate'
        
        if num == cal_niter:
            vismodel = config[key].get('add_vis_model', False)  
        else:
            vismodel = False
        #force to calibrate with model data column if specified by user

        if config[key].get('model_mode', None) == 'pybdsm_vis':
            vismodel = True
            calmodel = '{0:s}_{1:d}-nullmodel.txt'.format(prefix, num)
            model = config[key].get('model', num)[num-1]
            with open(os.path.join(pipeline.input, calmodel), 'w') as stdw:
                stdw.write('#format: ra_d dec_d i\n')
                stdw.write('0.0 -30.0 1e-99')
            for i, msname in enumerate(mslist):
                predict_from_fits(num, model, i)

            modelcolumn = None
        
        elif config[key].get('model_mode', None) == 'pybdsm_only':
            model = config[key].get('model', num)[num-1]
            if isinstance(model, str) and len(model.split('+')) > 1:
                mm = model.split('+')
                calmodel, fits_model = combine_models(mm, num,
                                           enable=False if pipeline.enable_task(
                                           config, 'aimfast') else True)
            else:
                model = int(model)
                calmodel = '{0:s}_{1:d}-pybdsm.lsm.html:output'.format(prefix, model)
                fits_model = '{0:s}/{1:s}_{2:d}-pybdsm.fits'.format(pipeline.output, prefix, model)

            modelcolumn = None
        
        elif  config[key].get('model_mode', None) == 'vis_only':
            vismodel = True
            modelcolumn = 'MODEL_DATA'
            calmodel = '{0:s}_{1:d}-nullmodel.txt'.format(prefix, num)
            with open(os.path.join(pipeline.input, calmodel), 'w') as stdw:
                stdw.write('#format: ra_d dec_d i\n')
                stdw.write('0.0 -30.0 1e-99')

        if config[key].get('Gsols', gsols) == [] or \
                       config[key].get('Bsols', gsols) == []:
            config[key]['Bjones'] = True

        for i,msname in enumerate(mslist):
            print(config[key].get('Gsols_time'))
            if not config[key].get('Gsols_time') or \
                       not config[key].get('Gsols_channel'):
                gsols_ = gsols
            else:
                gsols_ = [config[key].get('Gsols_time', gsols[0])[num-1] if num <= len(config[key].get('Gsols_time',gsols[0])) else gsols[0],
                          config[key].get('Gsols_channel', gsols[1])[num-1] if num <= len(config[key].get('Gsols_channel',gsols[1])) else gsols[1]]
            bsols_ = config[key].get('Bsols', bsols)

            step = 'calibrate_{0:d}_{1:d}'.format(num, i)
            recipe.add('cab/calibrator', step,
               {
                 "skymodel"             : calmodel,  #in case I don't want to use a sky model
                 "add-vis-model"        : vismodel,
                 "model-column"         : modelcolumn,
                 "msname"               : msname,
                 "threads"              : ncpu,
                 "column"               : "DATA",
                 "output-data"          : config[key].get('output_data', 'CORR_DATA')[num-1 if len(config[key].get('output_data')) >= num else -1],
                 "output-column"        : "CORRECTED_DATA",
                 "prefix"               : '{0:s}-{1:d}_meqtrees'.format(pipeline.dataid[i], num),
                 "label"                : 'cal{0:d}'.format(num),
                 "read-flags-from-ms"   : True,
                 "read-flagsets"        : "-stefcal",
                 "write-flagset"        : "stefcal",
                 "write-flagset-policy" : "replace",
                 "Gjones"               : True,
                 "Gjones-solution-intervals" : sdm.dismissable(gsols_ or None),
                 "Gjones-matrix-type"   : config[key].get('gain_matrix_type', 'GainDiag')[num-1 if len(config[key].get('gain_matrix_type')) >= num else -1], 
                 "Gjones-ampl-clipping"      : True,
                 "Gjones-ampl-clipping-low"  : config.get('cal_gain_amplitude_clip_low', 0.5),
                 "Gjones-ampl-clipping-high" : config.get('cal_gain_amplitude_clip_high', 1.5),
                 "Bjones"                    : config[key].get('Bjones', False),
                 "Bjones-ampl-clipping"      : True,
                 "Bjones-solution-intervals" : sdm.dismissable(bsols_ or None),
                 "Bjones-ampl-clipping"      : config[key].get('Bjones', bjones),
                 "Bjones-ampl-clipping-low"  : config.get('cal_gain_amplitude_clip_low', 0.5),
                 "Bjones-ampl-clipping-high" : config.get('cal_gain_amplitude_clip_high', 1.5),
                 "make-plots"           : True,
                 "tile-size"            : time_chunk,
               },
               input=pipeline.input,
               output=pipeline.output,
               label="{0:s}:: Calibrate step {1:d} ms={2:s}".format(step, num, msname))

    def calibrate_cubical(num):
        key = 'calibrate'
        if num == cal_niter:
            vismodel = config[key].get('add_vis_model', False)  
        else:
            vismodel = False
        #force to calibrate with model data column if specified by user

        if config[key].get('model_mode', None) == 'pybdsm_vis':
            vismodel = True
            calmodel = '{0:s}_{1:d}-nullmodel.txt'.format(prefix, num)
            model = config[key].get('model', num)[num-1]
            with open(os.path.join(pipeline.input, calmodel), 'w') as stdw:
                stdw.write('#format: ra_d dec_d i\n')
                stdw.write('0.0 -30.0 1e-99')
            for i, msname in enumerate(mslist):
                predict_from_fits(num, model, i)

            modelcolumn = None
        
        elif config[key].get('model_mode', None) == 'pybdsm_only':
            model = config[key].get('model', num)[num-1]
            if isinstance(model, str) and len(model.split('+')) > 1:
                mm = model.split('+')
                calmodel, fits_model = combine_models(mm, num,
                                           enable=False if pipeline.enable_task(
                                           config, 'aimfast') else True)
            else:
                model = int(model)
                calmodel = '{0:s}_{1:d}-pybdsm.lsm.html:output'.format(prefix, model)
                fits_model = '{0:s}/{1:s}_{2:d}-pybdsm.fits'.format(pipeline.output, prefix, model)
        if config[key].get('Gsols', gsols) == [] or \
                       config[key].get('Bsols', gsols) == []:
            config[key]['Bjones'] = True

        if config[key].get('Bjones', bjones):
            jones_chain = 'G,B'
        else:
            jones_chain = 'G' 

        for i,msname in enumerate(mslist):
            if not config[key].get('Gsols_time') or \
               not config[key].get('Gsols_channel'):
                gsols_ = gsols
            else:
                gsols_ = [config[key].get('Gsols_time', gsols[0])[num-1] if num <= len(config[key].get('Gsols_time',gsols[0])) else gsols[0],
                          config[key].get('Gsols_channel', gsols[1])[num-1] if num <= len(config[key].get('Gsols_channel',gsols[1])) else gsols[1]]
            bsols_ = config[key].get('Bsols', bsols)

            step = 'calibrate_cubical_{0:d}_{1:d}'.format(num, i)
            recipe.add('cab/cubical', step, 
                {   
                    "data-ms"          : msname, 
                    "data-column"      : 'DATA',
                    "sol-term-iters"   : '200',
                   # "model-column"     : 'MODEL_DATA' if config[key].get('add_vis_model', False) else ' "" ',
                   # "j2-term-iters"    : 200,
                    "model-list"       : [calmodel, 'MODEL_DATA'] if (config[key].get('add_vis_model', False) and num == cal_niter) else [calmodel],
                    "data-time-chunk"  : time_chunk,
                    "sel-ddid"         : sdm.dismissable(config[key].get('spwid', None)),
                    "dist-ncpu"        : ncpu,
                    "sol-jones"        : jones_chain,
                    "out-name"         : '{0:s}-{1:d}_cubical'.format(pipeline.dataid[i], num),
                    "out-mode"         : CUBICAL_OUT[config[key].get('output_data', 'CORR_DATA')[num-1 if len(config[key].get('output_data')) >= num else -1]],
                    "out-plots"        : True,
                    "weight-column"    : config[key].get('weight_column', 'WEIGHT'),
                    "montblanc-dtype"  : 'float',
                    "g-solvable"      : True,
                    "g-type"          : CUBICAL_MT[config[key].get('gain_matrix_type','Gain2x2')[num-1 if len(config[key].get('gain_matrix_type')) >= num else -1]],
                    "g-time-int"      : gsols_[0],
                    "g-freq-int"      : gsols_[1],
                    "g-clip-low"      : config.get('cal_gain_amplitude_clip_low', 0.5),
                    "g-clip-high"     : config.get('cal_gain_amplitude_clip_high', 1.5),
                    "b-solvable"      : config[key].get('Bjones', bjones),
                    "b-type"          : CUBICAL_MT[config[key].get('gain_matrix_type', 'Gain2x2')[num-1 if len(config[key].get('gain_matrix_type')) >= num else -1]],
                    "b-time-int"      : bsols_[0],
                    "b-freq-int"      : bsols_[1],
                    "b-clip-low"      : config.get('cal_gain_amplitude_clip_low', 0.5),
                    "b-clip-high"     : config.get('cal_gain_amplitude_clip_high', 1.5),
                    "madmax-enable"   : config[key].get('madmax_flagging',True),
                    "madmax-plot"     : True if (config[key].get('madmax_flagging')) else False,
                    "madmax-threshold" : config[key].get('madmax_flag_thresh', [0,10]),
                    "madmax-estimate" : 'corr',
                },  
                input=pipeline.input,
                output=pipeline.output,
                shared_memory='100Gb',
                label="{0:s}:: Calibrate step {1:d} ms={2:s}".format(step, num, msname))

    def apply_gains_to_fullres(num_iter, enable=True):
        key = 'calibrate'
        if num_iter == cal_niter:
            vismodel = config[key].get('add_vis_model', False)  
        else:
            vismodel = False
        #force to calibrate with model data column if specified by user

        if config[key].get('model_mode', None) == 'pybdsm_vis':
            vismodel = True
            calmodel = '{0:s}_{1:d}-nullmodel.txt'.format(prefix, num)
            model = config[key].get('model', num)[num-1]
            with open(os.path.join(pipeline.input, calmodel), 'w') as stdw:
                stdw.write('#format: ra_d dec_d i\n')
                stdw.write('0.0 -30.0 1e-99')
            for i, msname in enumerate(mslist):
                predict_from_fits(num, model, i)

            modelcolumn = None
        
        elif config[key].get('model_mode', None) == 'pybdsm_only':
            model = config[key].get('model', num_iter)[num_iter-1]
            if isinstance(model, str) and len(model.split('+')) > 1:
                mm_f = model.split('+')
                calmodel, fits_model = combine_models(mm_f, num_iter,
                                           enable=False if pipeline.enable_task(
                                           config, 'aimfast') else True)
            else:
                model = int(model)
                calmodel = '{0:s}_{1:d}-pybdsm.lsm.html:output'.format(prefix, model)
                fits_model = '{0:s}/{1:s}_{2:d}-pybdsm.fits'.format(pipeline.output, prefix, model)

        calwith = config.get('calibrate_with', 'meqtrees').lower()
        if(calwith=='meqtrees'):
           enable = False
           meerkathi.log.info('Gains cannot be interpolated with MeqTrees, please switch to CubiCal')
        hires_switch = config['calibrate'].get('hires_interpol', 'True')
        if (hires_switch==False):
            enable = False
        if(enable==True):
                model = config[key].get('model', num_iter)[-1]
                if isinstance(model, str) and len(model.split('+'))>1:
                    mod = model.split('+')
                    calmodel, fits_model = combine_models(mod, num_iter)
                else:
                    model = int(model)
                    calmodel = '{0:s}_{1:d}-pybdsm.lsm.html:output'.format(prefix, model)
                    fits_model = '{0:s}/{1:s}_{2:d}-pybdsm.fits'.format(pipeline.output, prefix, model)


                if config[key].get('Bjones', bjones):
                    jones_chain = 'G,B'
                else:
                    jones_chain = 'G'

                for i,himsname in enumerate(hires_mslist):

                    step = 'apply_cubical_gains_{0:d}_{1:d}'.format(num_iter, i)
                    recipe.add('cab/cubical', step,
                        {
                            "data-ms"          : himsname,
                            "data-column"      : 'DATA',
                            "data-time-chunk"  : time_chunk,
                            "sel-ddid"         : sdm.dismissable(config[key].get('spwid', None)),
                            "dist-ncpu"        : ncpu,
                           # "model-list"       : [calmodel, 'MODEL_DATA'] if config[key].get('add_vis_model', False) else [calmodel],
                            "out-name"         : '{0:s}-{1:d}_cubical'.format(pipeline.dataid[i], num_iter),
                            "out-mode"         : 'ac',
                            "weight-column"    : config[key].get('weight_column', 'WEIGHT'),
                            "montblanc-dtype"  : 'float',
                            "g-xfer-from"       : 'cubical_gaintab_gjones_{0:s}_{1:d}.parmdb:output'.format((himsname.replace(label_hires, label)).split('.ms')[0],num_iter),
                            "b-xfer-from"       : 'cubical_gaintab_bjones_{0:s}_{1:d}.parmdb:output'.format((himsname.replace(label_hires, label)).split('.ms')[0],num_iter),
                            "g-solvable"      : False,
                            "b-solvable"      : False,
                        },
                        input=pipeline.input,
                        output=pipeline.output,
                        shared_memory='100Gb',
                        label="{0:s}:: Apply cubical gains ms={1:s}".format(step, himsname))
                step = 'make_hires_image'
              
                if(':' not in pipeline.hires_spw):
                   msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, pipeline.prefix[0])     ##Assumes the number of channels are the same in all MSs.
                   with open(msinfo, 'r') as stdr:
                     nchans_full = yaml.load(stdr)['SPW']['NUM_CHAN']
                else:
                   chanst, chanend = ((pipeline.hires_spw).split(":")[1]).split('~')
                   nchans_full = (int(chanend)-int(chanst))+1
                recipe.add('cab/wsclean', step,
                  {
                    "msname"    : hires_mslist,
                    "column"    : 'CORRECTED_DATA',
                    "weight"    : 'briggs {}'.format(config[key].get('robust', robust)),
                    "npix"      : config[key].get('npix', npix),
                    "trim"      : config[key].get('trim', trim),
                    "scale"     : config[key].get('cell', cell),
                    "prefix"    : '{0:s}_{1:s}'.format(prefix, label_hires),
                    "niter"     : config[key].get('niter', niter),
                    "mgain"     : config[key].get('mgain', mgain),
                    "pol"       : config[key].get('pol', pol),
                    "taper-gaussian" : sdm.dismissable(config[key].get('uvtaper', taper)),
                    "channelsout"     : nchans_full,
                    "joinchannels"    : config[key].get('joinchannels', joinchannels),
                    "fit-spectral-pol": config[key].get('fit_spectral_pol', fit_spectral_pol),
                    "auto-threshold": config[key].get('auto_threshold',[auto_thresh])[-1],
                    "multiscale" : config[key].get('multi_scale', False),
                    "multiscale-scales" : sdm.dismissable(config[key].get('multi_scale_scales', None)),
                  },
                  input=pipeline.input,
                  output=pipeline.output,
                  shared_memory='100Gb',
                  label="{0:s}:: Make a high res image".format(step, himsname))



    def get_aimfast_data(filename='{0:s}/fidelity_results.json'.format(pipeline.output)):
        "Extracts data from the json data file"
        with open(filename) as f:
            data = json.load(f)
        return data

    def quality_check(n, enable=True):
        "Examine the aimfast results to see if they meet specified conditions"
        # If total number of iterations is reached stop
        if n == cal_niter+1:
           meerkathi.log.info('Number of iterations reached: {:d}'.format(cal_niter))
           return False
        if enable:
            # The recipe has to be executed at this point to get the image fidelity results
            recipe.run()
            # Empty job que after execution
            recipe.jobs = []
            key = 'aimfast'
            dr_tolerance = config[key].get('dr_tolerance', 0.10)
            normality_tolerance = config[key].get('normality_tolerance', 0.10)
            fidelity_data = get_aimfast_data()
            # Ensure atleast one iteration is ran to compare previous and subsequent images
            if n >= 2:
                dr0 = fidelity_data['meerkathi_{0}-residual'.format(
                        n-1)][
                        'meerkathi_{0}-model'.format(n - 1)]['DR']
                dr1 = fidelity_data['meerkathi_{0}-residual'.format(n)][
                        'meerkathi_{0}-model'.format(n)]['DR']
                dr_delta = (dr1 - dr0)/float(dr0)
                # Confirm that previous image DR is smaller than subsequent image
                # Also make sure the fractional difference is greater than the tolerance
                if dr_delta < dr_tolerance:
                    meerkathi.log.info('Stopping criterion: Dynamic range')
                    meerkathi.log.info('{:f} < {:f}'.format(dr_delta, dr_tolerance))
                    return False
            if n >= 2:
                residual0 = fidelity_data['meerkathi_{0}-residual'.format(n - 1)]
                residual1 = fidelity_data['meerkathi_{0}-residual'.format(n)]
                normality_delta = residual0['NORM'][0] - residual1['NORM'][0]
                # Confirm that previous image normality statistic is smaller than subsequent image
                # Also make sure the difference is greater than the tolerance
                if normality_delta < normality_tolerance*residual0['NORM'][0]:
                    meerkathi.log.info('Stopping criterion: Normality test')
                    meerkathi.log.info('{:f} < {:f}'.format(
                        normality_delta, normality_tolerance*residual0['NORM'][0]))
                    return False
        # If no condition is met return true to continue
        return True

    def image_quality_assessment(num):
        # Check if more than two calibration iterations to combine successive models
        # Combine models <num-1> (or combined) to <num> creat <num+1>-pybdsm-combine
        if len(config['extract_sources'].get('thresh_pix', thresh_pix)) >= num:
            model = config['calibrate'].get('model', num)[num-1]
            if isinstance(model, str) and len(model.split('+'))==2:
                mm = model.split('+')
                combine_models(mm, num)
        #else:
            # If the iterations go beyond the length of the thresh_pix array the sources are no longer extracted.
            #model = config['calibrate'].get('model', num)[len(config['extract_sources'].get('thresh_pix', thresh_pix))-1]
        step = 'aimfast'
        recipe.add('cab/aimfast', step,
                {
                    "tigger-model"         : '{0:s}_{1:d}-pybdsm{2:s}.lsm.html:output'.format(
                                                 prefix, num if num <= len(config['calibrate'].get('model', num))
                                                 else len(config['calibrate'].get('model', num)),
                                                 '-combined' if len(model.split('+')) >= 2 else ''),
                    "residual-image"       : '{0:s}_{1:d}{2:s}-residual.fits:output'.format(
                                                 prefix, num, mfsprefix),
                    "normality-model"      : config[step].get(
                                                 'normality_model', 'normaltest'),
                    "area-factor"          : config[step].get('area_factor', 10),
                    "label"                : "meerkathi_{}".format(num),
                },
                input=pipeline.output,
                output=pipeline.output,
                label="{0:s}_{1:d}:: Image fidelity assessment for {2:d}".format(
                          step, num, num))

    # decide which tool to use for calibration
    calwith = config.get('calibrate_with', 'meqtrees').lower()
    if calwith == 'meqtrees':
        calibrate = calibrate_meqtrees
    elif calwith == 'cubical':
        calibrate = calibrate_cubical

    # selfcal loop
    iter_counter = config.get('start_at_iter', 1)
    if pipeline.enable_task(config, 'image'):
        if config['calibrate'].get('hires_interpol')==True:
            meerkathi.log.info("Interpolating gains")

        image(iter_counter)
    if pipeline.enable_task(config, 'extract_sources'):
        extract_sources(iter_counter)
    if pipeline.enable_task(config, 'aimfast'):
        image_quality_assessment(iter_counter)
    while quality_check(iter_counter,
                        enable=True if pipeline.enable_task(
                            config, 'aimfast') else False):
        if pipeline.enable_task(config, 'calibrate'):
            calibrate(iter_counter)
        iter_counter += 1
        if pipeline.enable_task(config, 'image'):
            image(iter_counter)
        if pipeline.enable_task(config, 'extract_sources'):
            extract_sources(iter_counter)
        if pipeline.enable_task(config, 'aimfast'):
            image_quality_assessment(iter_counter)

    if config['calibrate'].get('hires_interpol')==True:
        print "Interpolating gains"
        substep = int(config.get('apply_step', cal_niter))
        apply_gains_to_fullres(substep,enable=True if (config['calibrate'].get('hires_interpol')==True) else False)

    if pipeline.enable_task(config, 'restore_model'):
        if config['restore_model']['model']:
            num = config['restore_model']['model']
            if isinstance(num, str) and len(num.split('+')) == 2:
                mm = num.split('+')
                if int(mm[-1]) > iter_counter:
                    num = str(iter_counter)
        else:
            extract_sources = len(config['extract_sources'].get(
                                  'thresh_isl', [iter_counter]))
            if extract_sources > 1:
                num = '{:d}+{:d}'.format(iter_counter-1, iter_counter)
            else:
                num = iter_counter

        if isinstance(num, str) and len(num.split('+')) == 2:
            mm = num.split('+')
            models = ['{0:s}_{1:s}-pybdsm.lsm.html:output'.format(
                      prefix, m) for m in mm]
            final = '{0:s}_final-pybdsm.lsm.html:output'.format(prefix)

            step = 'create_final_lsm_{0:s}_{1:s}'.format(*mm)
            recipe.add('cab/tigger_convert', step,
                {
                    "input-skymodel"    : models[0],
                    "append"            : models[1],
                    "output-skymodel"   : final,
                    "rename"            : True,
                    "force"             : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Combined models'.format(step))

        elif isinstance(num, str) and num.isdigit():
            inputlsm = '{0:s}_{1:s}-pybdsm.lsm.html:output'.format(prefix, num)
            final = '{0:s}_final-pybdsm.lsm.html:output'.format(prefix)
            step = 'create_final_lsm_{0:s}'.format(num)
            recipe.add('cab/tigger_convert', step,
                {
                    "input-skymodel"    : inputlsm,
                    "output-skymodel"   : final,
                    "rename"  : True,
                    "force"   : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Combined models'.format(step))
        else:
            raise ValueError("restore_model_model should be integer-valued string or indicate which models to be appended, eg. 2+3")

        if config['restore_model'].get('clean_model', None):
            num = int(config['restore_model'].get('clean_model', None))
            if num > iter_counter:
                num = iter_counter

            conv_model = prefix + '-convolved_model.fits:output'
            recipe.add('cab/fitstool', step,
                {
                    "image"    : [prefix+'_{0:d}{2:s}-{1:s}.fits:output'.format(num, im, mfsprefix) for im in ('image','residual')],
                    "output"   : conv_model,
                    "diff"     : True,
                    "force"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Make convolved model'.format(step))

            with_cc = prefix + '-with_cc.fits:output'
            recipe.add('cab/fitstool', step,
                {
                    "image"    : [prefix+'_{0:d}{1:s}-image.fits:output'.format(num, mfsprefix), conv_model],
                    "output"   : with_cc,
                    "sum"      : True,
                    "force"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Add clean components'.format(step))

            recipe.add('cab/tigger_restore', step,
                {
                    "input-image"    : with_cc,
                    "input-skymodel" : final,
                    "output-image"   : prefix+'.fullrest.fits',
                    "force"          : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Add extracted skymodel'.format(step))

        for i,msname in enumerate(mslist):
            if pipeline.enable_task(config, 'flagging_summary'):
                step = 'flagging_summary_image_selfcal_{0:d}'.format(i)
                recipe.add('cab/casa_flagdata', step,
                    {
                      "vis"         : msname,
                      "mode"        : 'summary',
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))
